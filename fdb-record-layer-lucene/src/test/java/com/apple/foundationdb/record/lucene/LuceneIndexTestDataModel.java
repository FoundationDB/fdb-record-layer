/*
 * LuceneIndexModel.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

/**
 * Model for creating a lucene appropriate dataset with various configurations.
 */
public class LuceneIndexTestDataModel {

    final boolean isGrouped;
    final boolean isSynthetic;
    final boolean primaryKeySegmentIndexEnabled;
    final int partitionHighWatermark;
    final Random random;
    final RandomTextGenerator textGenerator;
    final Index index;
    final Function<FDBRecordContext, FDBRecordStore> schemaSetup;
    /**
     * A mapping from groupingKey to primary key to the partitioning key.
     * <p>
     *     The reason that the value of the inner map is the partitioning key is so that we can order them and then
     *     slice to get the expected partitioning.
     * </p>
     */
    final Map<Tuple, Map<Tuple, Tuple>> groupingKeyToPrimaryKeyToPartitionKey;

    private LuceneIndexTestDataModel(final Builder builder) {
        random = builder.random;
        textGenerator = builder.textGenerator;
        isGrouped = builder.isGrouped;
        isSynthetic = builder.isSynthetic;
        primaryKeySegmentIndexEnabled = builder.primaryKeySegmentIndexEnabled;
        partitionHighWatermark = builder.partitionHighWatermark;

        final Map<String, String> options = new HashMap<>();
        options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, String.valueOf(primaryKeySegmentIndexEnabled));
        if (partitionHighWatermark > 0) {
            options.put(LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "parent.timestamp" : "timestamp");
            options.put(LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(partitionHighWatermark));
        }

        final RecordMetaDataBuilder metaDataBuilder = LuceneIndexTestDataModel.createBaseMetaDataBuilder();
        final KeyExpression rootExpression = LuceneIndexTestDataModel.createRootExpression(isGrouped, isSynthetic);
        index = LuceneIndexTestDataModel.addIndex(isSynthetic, rootExpression, options, metaDataBuilder);
        final RecordMetaData metadata = metaDataBuilder.build();
        final StoreBuilderSupplier storeBuilderSupplier = builder.storeBuilderSupplier;
        final KeySpacePath path = builder.path;
        schemaSetup = context -> {
            final FDBRecordStore store = storeBuilderSupplier.get(context, metadata, path).createOrOpen();
            store.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
            return store;
        };
        groupingKeyToPrimaryKeyToPartitionKey = new HashMap<>();
    }

    @Override
    public String toString() {
        return "LuceneIndexDataModel{" +
                "isGrouped=" + isGrouped +
                ", isSynthetic=" + isSynthetic +
                ", primaryKeySegmentIndexEnabled=" + primaryKeySegmentIndexEnabled +
                ", partitionHighWatermark=" + partitionHighWatermark +
                '}';
    }

    public Set<Tuple> groupingKeys() {
        return groupingKeyToPrimaryKeyToPartitionKey.keySet();
    }

    public Set<Tuple> primaryKeys(Tuple groupingKey) {
        return groupingKeyToPrimaryKeyToPartitionKey.get(groupingKey).keySet();
    }

    public void deleteRecord(final FDBRecordContext context, final Tuple primaryKey) {
        FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
        recordStore.deleteRecord(primaryKey);
    }

    void saveRecords(int count, long start, FDBRecordContext context, final int group) {
        FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
        for (int j = 0; j < count; j++) {
            LuceneIndexTestDataModel.saveRecord(isGrouped, isSynthetic, random, groupingKeyToPrimaryKeyToPartitionKey,
                    textGenerator, start, recordStore, group);
        }
    }

    static void saveRecord(final boolean isGrouped, final boolean isSynthetic, final Random random,
                           final Map<Tuple, Map<Tuple, Tuple>> groupingKeyToPrimaryKeyToPartitionKey,
                           final RandomTextGenerator textGenerator, final long start, final FDBRecordStore recordStore,
                           final int group) {
        final Tuple groupTuple = isGrouped ? Tuple.from(group) : Tuple.from();
        final int countInGroup = groupingKeyToPrimaryKeyToPartitionKey.computeIfAbsent(groupTuple, key -> new HashMap<>()).size();
        long timestamp = start + countInGroup + random.nextInt(20) - 5;
        final Tuple primaryKey = saveRecord(recordStore, isSynthetic, group, countInGroup, timestamp, textGenerator, random);
        groupingKeyToPrimaryKeyToPartitionKey.computeIfAbsent(groupTuple, key -> new HashMap<>())
                .put(primaryKey, Tuple.from(timestamp).addAll(primaryKey));
    }

    @Nonnull
    static Tuple saveRecord(final FDBRecordStore recordStore,
                            final boolean isSynthetic,
                            final int group,
                            final int countInGroup,
                            final long timestamp,
                            final RandomTextGenerator textGenerator,
                            final Random random) {
        var parent = TestRecordsGroupedParentChildProto.MyParentRecord.newBuilder()
                .setGroup(group)
                .setRecNo(1001L + countInGroup)
                .setTimestamp(timestamp)
                .setTextValue(isSynthetic ? "This is not the text that goes in lucene"
                              : textGenerator.generateRandomText("about"))
                .setIntValue(random.nextInt())
                .setChildRecNo(1000L - countInGroup)
                .build();
        Tuple primaryKey;
        if (isSynthetic) {
            var child = TestRecordsGroupedParentChildProto.MyChildRecord.newBuilder()
                    .setGroup(group)
                    .setRecNo(1000L - countInGroup)
                    .setStrValue(textGenerator.generateRandomText("forth"))
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
    static Index addIndex(final boolean isSynthetic, final KeyExpression rootExpression,
                          final Map<String, String> options, final RecordMetaDataBuilder metaDataBuilder) {
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
    static KeyExpression createRootExpression(final boolean isGrouped, final boolean isSynthetic) {
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

    @Nonnull
    static RecordMetaDataBuilder createBaseMetaDataBuilder() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsGroupedParentChildProto.getDescriptor());
        metaDataBuilder.getRecordType("MyParentRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields("group", "rec_no"));
        metaDataBuilder.getRecordType("MyChildRecord")
                .setPrimaryKey(Key.Expressions.concatenateFields("group", "rec_no"));
        return metaDataBuilder;
    }

    public Integer nextInt(final int bound) {
        return random.nextInt(bound);
    }

    static class Builder {
        private final Random random;
        private final StoreBuilderSupplier storeBuilderSupplier;
        private final RandomTextGenerator textGenerator;
        private final KeySpacePath path;
        boolean isGrouped;
        boolean isSynthetic;
        boolean primaryKeySegmentIndexEnabled = true;
        int partitionHighWatermark;
        int repartitionCount;

        public Builder(final long seed, StoreBuilderSupplier storeBuilderSupplier,
                       TestKeySpacePathManagerExtension pathManager) {
            this.random = new Random(seed);
            this.storeBuilderSupplier = storeBuilderSupplier;
            textGenerator = new RandomTextGenerator(random);
            this.path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        }

        public Builder setIsGrouped(final boolean isGrouped) {
            this.isGrouped = isGrouped;
            return this;
        }

        public Builder setIsSynthetic(final boolean isSynthetic) {
            this.isSynthetic = isSynthetic;
            return this;
        }

        public Builder setPrimaryKeySegmentIndexEnabled(final boolean primaryKeySegmentIndexEnabled) {
            this.primaryKeySegmentIndexEnabled = primaryKeySegmentIndexEnabled;
            return this;
        }

        public Builder setPartitionHighWatermark(final int partitionHighWatermark) {
            this.partitionHighWatermark = partitionHighWatermark;
            return this;
        }

        public LuceneIndexTestDataModel build() {
            return new LuceneIndexTestDataModel(this);
        }
    }

    /**
     * Factory for a {@link FDBRecordStore.Builder}.
     */
    @FunctionalInterface
    public interface StoreBuilderSupplier {
        FDBRecordStore.Builder get(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData,
                                   @Nonnull final KeySpacePath path);
    }
}
