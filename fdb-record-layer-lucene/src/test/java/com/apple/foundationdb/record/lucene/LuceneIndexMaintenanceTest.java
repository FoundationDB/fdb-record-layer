/*
 * LuceneIndexValidationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsGroupedParentChildProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Tests of the consistency of the Lucene Index.
 */
@Tag(Tags.RequiresFDB)
public class LuceneIndexMaintenanceTest extends FDBRecordStoreTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexMaintenanceTest.class);

    static Stream<Arguments> configurationArguments() {
        // This has found situations that should have explicit tests:
        //      1. Multiple groups
        //      2. When the size of first partition is exactly highWatermark+repartitionCount
        return Stream.concat(
                Stream.of(
                        // there's not much special about which flags are enabled and the numbers are used, it's just
                        // to make sure we have some variety, and make sure we have a test with each boolean true, and
                        // false.
                        // For partitionHighWatermark vs repartitionCount it is important to have both an even factor,
                        // and not.
                        Arguments.of(true, false, false, 13, 3, 20, 9237590782644L),
                        Arguments.of(true, true, true, 10, 2, 23, -644766138635622644L),
                        Arguments.of(false, true, true, 11, 4, 20, -1089113174774589435L),
                        Arguments.of(false, false, false, 5, 1, 18, 6223372946177329440L)),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextInt(20) + 2,
                                random.nextInt(10) + 1,
                                0,
                                random.nextLong())));
    }

    @ParameterizedTest(name = "randomizedRepartitionTest({argumentsWithNames})")
    @MethodSource("configurationArguments")
    void randomizedRepartitionTest(boolean isGrouped,
                                   boolean isSynthetic,
                                   boolean primaryKeySegmentIndexEnabled,
                                   int partitionHighWatermark,
                                   int repartitionCount,
                                   int minDocumentCount,
                                   long seed) throws IOException {
        Random random = new Random(seed);
        final boolean optimizedStoredFields = random.nextBoolean();
        final Map<String, String> options = Map.of(
                LuceneIndexOptions.INDEX_PARTITION_BY_TIMESTAMP, isSynthetic ? "parent.timestamp" : "timestamp",
                LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(partitionHighWatermark),
                LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, String.valueOf(optimizedStoredFields),
                LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, String.valueOf(primaryKeySegmentIndexEnabled));
        LOGGER.info(KeyValueLogMessage.of("Running randomizedRepartitionTest",
                "isGrouped", isGrouped,
                "isSynthetic", isSynthetic,
                "repartitionCount", repartitionCount,
                "options", options,
                "seed", seed));

        final RecordMetaDataBuilder metaDataBuilder = createBaseMetaDataBuilder();
        final KeyExpression rootExpression = createRootExpression(isGrouped, isSynthetic);
        Index index = addIndex(isSynthetic, rootExpression, options, metaDataBuilder);
        final RecordMetaData metadata = metaDataBuilder.build();
        Consumer<FDBRecordContext> schemaSetup = context -> createOrOpenRecordStore(context, metadata);

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .build();

        // Generate random documents
        Map<Tuple, Map<Tuple, Long>> ids = new HashMap<>();
        final int transactionCount = random.nextInt(15) + 1;
        final long start = Instant.now().toEpochMilli();
        Map<Integer, Set<Long>> allExistingTimestamps = new HashMap<>();
        int i = 0;
        while (i < transactionCount ||
                // keep inserting data until at least two groups have at least minDocumentCount
                ids.entrySet().stream()
                        .map(entry -> entry.getValue().size())
                        .sorted(Comparator.reverseOrder())
                        .limit(2).skip(isGrouped ? 1 : 0).findFirst()
                        .orElse(0) < minDocumentCount) {
            final int docCount = random.nextInt(10) + 1;
            try (FDBRecordContext context = openContext(contextProps)) {
                schemaSetup.accept(context);
                for (int j = 0; j < docCount; j++) {
                    final int group = isGrouped ? random.nextInt(random.nextInt(10) + 1) : 0; // irrelevant if !isGrouped
                    final Tuple groupTuple = isGrouped ? Tuple.from(group) : Tuple.from();
                    final int countInGroup = ids.computeIfAbsent(groupTuple, key -> new HashMap<>()).size();
                    // we currently don't support multiple records with the same timestamp, specifically at the boundaries
                    long timestamp = start + countInGroup + random.nextInt(20) - 5;
                    final Set<Long> existingTimestamps = allExistingTimestamps.computeIfAbsent(group, key -> new HashSet<>());
                    while (!existingTimestamps.add(timestamp)) {
                        timestamp++;
                    }
                    final Tuple primaryKey = saveRecords(isSynthetic, group, countInGroup, timestamp, random);
                    ids.computeIfAbsent(groupTuple, key -> new HashMap<>()).put(primaryKey, timestamp);
                }
                commit(context);
            }
            i++;
        }

        explicitMergeIndex(index, contextProps, schemaSetup);

        new LuceneIndexTestValidator(() -> openContext(contextProps), context -> {
            schemaSetup.accept(context);
            return recordStore;
        }).validate(index, ids, repartitionCount, isSynthetic ? "child_str_value:forth" : "text_value:about");

        if (isGrouped) {
            validateDeleteWhere(isSynthetic, repartitionCount, ids, contextProps, schemaSetup, index);
        }
    }

    @Nonnull
    private static RecordMetaDataBuilder createBaseMetaDataBuilder() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsGroupedParentChildProto.getDescriptor());
        metaDataBuilder.getRecordType("MyParentRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields("group", "rec_no"));
        metaDataBuilder.getRecordType("MyChildRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields("group", "rec_no"));
        return metaDataBuilder;
    }

    private void validateDeleteWhere(final boolean isSynthetic, final int repartitionCount, final Map<Tuple, Map<Tuple, Long>> ids, final RecordLayerPropertyStorage contextProps, final Consumer<FDBRecordContext> schemaSetup, final Index index) throws IOException {
        final List<Tuple> groups = List.copyOf(ids.keySet());
        for (final Tuple group : groups) {
            try (FDBRecordContext context = openContext(contextProps)) {
                schemaSetup.accept(context);
                recordStore.deleteRecordsWhere(Query.field("group").equalsValue(group.getLong(0)));
                context.commit();
            }
            ids.remove(group);
            new LuceneIndexTestValidator(() -> openContext(contextProps), context -> {
                schemaSetup.accept(context);
                return recordStore;
            }).validate(index, ids, repartitionCount, isSynthetic ? "child_str_value:forth" : "text_value:about");
        }
    }

    @Nonnull
    private Tuple saveRecords(final boolean isSynthetic, final int group, final int countInGroup, final long timestamp, final Random random) {
        var parent = TestRecordsGroupedParentChildProto.MyParentRecord.newBuilder()
                .setGroup(group)
                .setRecNo(1000L + countInGroup)
                .setTimestamp(timestamp)
                .setTextValue("A word about what I want to say")
                .setIntValue(random.nextInt())
                .build();
        Tuple primaryKey;
        if (isSynthetic) {
            var child = TestRecordsGroupedParentChildProto.MyChildRecord.newBuilder()
                    .setGroup(group)
                    .setRecNo(1000L - countInGroup)
                    .setStrValue("Four score and seven years ago our fathers brought forth")
                    .setOtherValue(random.nextInt())
                    .build();
            final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                    .getSyntheticRecordType("JoinChildren")
                    .getRecordTypeKeyTuple();
            primaryKey = Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                    recordStore.saveRecord(parent).getPrimaryKey().getItems(),
                    recordStore.saveRecord(child).getPrimaryKey().getItems());
        } else {
            primaryKey = recordStore.saveRecord(parent).getPrimaryKey();
        }
        return primaryKey;
    }

    @Nonnull
    private static Index addIndex(final boolean isSynthetic, final KeyExpression rootExpression, final Map<String, String> options, final RecordMetaDataBuilder metaDataBuilder) {
        Index index;
        index = new Index("joinNestedConcat", rootExpression, LuceneIndexTypes.LUCENE, options);

        if (isSynthetic) {
            final JoinedRecordTypeBuilder joinBuilder = metaDataBuilder.addJoinedRecordType("JoinChildren");
            joinBuilder.addConstituent("parent", "MyParentRecord");
            joinBuilder.addConstituent("child", "MyChildRecord");
            joinBuilder.addJoin("parent", Key.Expressions.field("group"),
                    "child", Key.Expressions.field("group"));
            joinBuilder.addJoin("parent", Key.Expressions.field("child_rec_no"),
                    "child", Key.Expressions.field("rec_no"));
            metaDataBuilder.addIndex("JoinChildren", index);
        } else {
            metaDataBuilder.addIndex("MyParentRecord", index);
        }
        return index;
    }

    @Nonnull
    private static KeyExpression createRootExpression(final boolean isGrouped, final boolean isSynthetic) {
        ThenKeyExpression baseExpression;
        KeyExpression groupingExpression;
        if (isSynthetic) {
            baseExpression = Key.Expressions.concat(
                    Key.Expressions.field("parent")
                            .nest(Key.Expressions.function(LuceneFunctionNames.LUCENE_STORED,
                                    Key.Expressions.field("int_value"))),
                    Key.Expressions.field("child")
                            .nest(Key.Expressions.function(LuceneFunctionNames.LUCENE_TEXT,
                                    Key.Expressions.field("str_value"))),
                    Key.Expressions.field("parent")
                            .nest(Key.Expressions.function(LuceneFunctionNames.LUCENE_SORTED,
                                    Key.Expressions.field("timestamp")))
            );
            groupingExpression = Key.Expressions.field("parent").nest("group");
        } else {
            baseExpression = Key.Expressions.concat(
                    Key.Expressions.function(LuceneFunctionNames.LUCENE_STORED,
                            Key.Expressions.field("int_value")),
                    Key.Expressions.function(LuceneFunctionNames.LUCENE_TEXT,
                            Key.Expressions.field("text_value")),
                    Key.Expressions.function(LuceneFunctionNames.LUCENE_SORTED,
                            Key.Expressions.field("timestamp"))
            );
            groupingExpression = Key.Expressions.field("group");
        }
        KeyExpression rootExpression;
        if (isGrouped) {
            rootExpression = baseExpression.groupBy(groupingExpression);
        } else {
            rootExpression = baseExpression;
        }
        return rootExpression;
    }


    private void explicitMergeIndex(Index index, RecordLayerPropertyStorage contextProps, Consumer<FDBRecordContext> schemaSetup) {
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setTimer(timer)
                    .build()) {
                indexBuilder.mergeIndex();
            }
        }
    }
}
