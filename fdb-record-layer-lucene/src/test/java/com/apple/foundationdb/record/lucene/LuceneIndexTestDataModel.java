/*
 * LuceneIndexTestDataModel.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Model for creating a lucene appropriate dataset with various configurations.
 */
public class LuceneIndexTestDataModel {
    public static final String PARENT_SEARCH_TERM = "text_value:about";
    public static final String CHILD_SEARCH_TERM = "child_str_value:forth";

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
    final ConcurrentMap<Tuple, ConcurrentMap<Tuple, Tuple>> groupingKeyToPrimaryKeyToPartitionKey;
    private final ConcurrentMap<Tuple, RecordUnderTest> recordsUnderTest;
    final ConcurrentMap<Tuple, AtomicInteger> nextRecNoInGroup;
    private LuceneIndexTestValidator validator;

    private LuceneIndexTestDataModel(@Nonnull final Builder builder,
                                     @Nonnull final Function<FDBRecordContext, FDBRecordStore> schemaSetup) {
        random = builder.random;
        textGenerator = builder.textGenerator;
        isGrouped = builder.isGrouped;
        isSynthetic = builder.isSynthetic;
        primaryKeySegmentIndexEnabled = builder.primaryKeySegmentIndexEnabled;
        partitionHighWatermark = builder.partitionHighWatermark;
        index = builder.index;
        this.schemaSetup = schemaSetup;
        groupingKeyToPrimaryKeyToPartitionKey = new ConcurrentHashMap<>();
        recordsUnderTest = new ConcurrentHashMap<>();
        nextRecNoInGroup = new ConcurrentHashMap<>();
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

    public List<RecordUnderTest> recordsUnderTest() {
        assertFalse(isSynthetic, "RecordsUnderTest is not implemented yet for synthetic records");
        return List.copyOf(recordsUnderTest.values());
    }

    @Nonnull
    public FDBRecordStore createOrOpenRecordStore(final FDBRecordContext context) {
        return Objects.requireNonNull(schemaSetup.apply(context));
    }

    public void deleteRecord(final FDBRecordContext context, final Tuple primaryKey) {
        FDBRecordStore recordStore = createOrOpenRecordStore(context);
        recordStore.deleteRecord(primaryKey);
    }

    void saveManyRecords(final int minDocumentCount,
                         @Nonnull final Supplier<FDBRecordContext> openContext,
                         final int transactionCount) {
        final long start = Instant.now().toEpochMilli();
        int i = 0;
        while (i < transactionCount ||
                // keep inserting data until at least two groups have at least minDocumentCount
                groupingKeyToPrimaryKeyToPartitionKey.values().stream()
                        .map(Map::size)
                        .sorted(Comparator.reverseOrder())
                        .limit(2).skip(isGrouped ? 1 : 0).findFirst()
                        .orElse(0) < minDocumentCount) {
            final int docCount = random.nextInt(10) + 1;
            try (FDBRecordContext context = openContext.get()) {
                saveRecords(docCount, start, context);
                context.commit();
            }
            i++;
        }
    }

    void saveRecords(int count, long start, FDBRecordContext context) {
        FDBRecordStore recordStore = createOrOpenRecordStore(context);
        for (int j = 0; j < count; j++) {
            final int group = isGrouped ? random.nextInt(random.nextInt(10) + 1) : 0;
            saveRecord(start, recordStore, group);
        }
    }

    void saveRecords(int count, long start, FDBRecordContext context, final int group) {
        FDBRecordStore recordStore = createOrOpenRecordStore(context);
        for (int j = 0; j < count; j++) {
            saveRecordToSync(true, start, recordStore, group);
        }
    }

    public Tuple saveEmptyRecord(final long start, final FDBRecordStore recordStore, final int group) {
        return saveRecordToSync(false, start, recordStore, group);
    }

    public Tuple saveRecord(final long start, final FDBRecordStore recordStore, final int group) {
        return saveRecordToSync(true, start, recordStore, group);
    }

    private Tuple saveRecordToSync(final boolean withContent, final long start, final FDBRecordStore recordStore,
                                   final int group) {
        return recordStore.getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD,
                saveRecordAsync(withContent, start, recordStore, group));
    }

    public CompletableFuture<Tuple> saveRecordAsync(final boolean withContent, final long start, final FDBRecordStore recordStore, final int group) {
        final Tuple groupTuple = calculateGroupTuple(isGrouped, group);
        final int uniqueCounter = nextRecNoInGroup.computeIfAbsent(groupTuple, key -> new AtomicInteger(0))
                .incrementAndGet();
        long timestamp = start + uniqueCounter + random.nextInt(20) - 5;
        return saveParentRecord(withContent, recordStore, group, uniqueCounter, timestamp)
                .thenCompose(parentPrimaryKey -> {
                    recordsUnderTest.put(parentPrimaryKey, new ParentRecord(groupTuple, parentPrimaryKey));
                    if (isSynthetic) {
                        return saveChildRecord(withContent, recordStore, group, uniqueCounter)
                                .thenApply(childPrimaryKey -> createSyntheticPrimaryKey(recordStore, parentPrimaryKey, childPrimaryKey));
                    } else {
                        return CompletableFuture.completedFuture(parentPrimaryKey);
                    }
                })
                .thenApply(primaryKey -> {
                    groupingKeyToPrimaryKeyToPartitionKey.computeIfAbsent(groupTuple, key -> new ConcurrentHashMap<>())
                            .put(primaryKey, Tuple.from(timestamp).addAll(primaryKey));
                    return primaryKey;
                });
    }

    @Nonnull
    private static Tuple createSyntheticPrimaryKey(final FDBRecordStore recordStore, final Tuple parentPrimaryKey, final Tuple childPrimaryKey) {
        final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                .getSyntheticRecordType("JoinChildren")
                .getRecordTypeKeyTuple();
        return Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                parentPrimaryKey.getItems(),
                childPrimaryKey.getItems());
    }

    @Nonnull
    private CompletableFuture<Tuple> saveParentRecord(final boolean withContent, final FDBRecordStore recordStore,
                                                      final int group, final int uniqueCounter, final long timestamp) {
        var parentBuilder = TestRecordsGroupedParentChildProto.MyParentRecord.newBuilder()
                .setGroup(group)
                .setRecNo(1001L + uniqueCounter)
                .setTimestamp(timestamp)
                .setChildRecNo(1000L - uniqueCounter);
        if (withContent) {
            parentBuilder
                    .setTextValue(isSynthetic ? "This is not the text that goes in lucene"
                                                   : textGenerator.generateRandomText("about"))
                    .setIntValue(random.nextInt());
        }
        var parent = parentBuilder.build();
        return recordStore.saveRecordAsync(parent).thenApply(FDBStoredRecord::getPrimaryKey);
    }


    @Nonnull
    private CompletableFuture<Tuple> saveChildRecord(final boolean withContent, final FDBRecordStore recordStore, final int group, final int countInGroup) {
        var childBuilder = TestRecordsGroupedParentChildProto.MyChildRecord.newBuilder()
                .setGroup(group)
                .setRecNo(1000L - countInGroup);
        if (withContent) {
            childBuilder
                    .setStrValue(textGenerator.generateRandomText("forth"))
                    .setOtherValue(random.nextInt());
        }
        var child = childBuilder.build();
        return recordStore.saveRecordAsync(child).thenApply(FDBStoredRecord::getPrimaryKey);
    }

    public void validate(final Supplier<FDBRecordContext> openContext) throws IOException {
        if (validator == null) {
            validator = new LuceneIndexTestValidator(openContext,
                    this::createOrOpenRecordStore);
        }
        validator.validate(index, groupingKeyToPrimaryKeyToPartitionKey, isSynthetic ? CHILD_SEARCH_TERM : PARENT_SEARCH_TERM);
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
    static Tuple calculateGroupTuple(final boolean isGrouped, final int group) {
        return isGrouped ? Tuple.from(group) : Tuple.from();
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
        private final TestKeySpacePathManagerExtension pathManager;
        private RandomTextGenerator textGenerator;
        boolean isGrouped;
        boolean isSynthetic;
        boolean primaryKeySegmentIndexEnabled = true;
        int partitionHighWatermark;
        @Nullable
        private Index index;
        @Nullable
        private RecordMetaData metadata;

        public Builder(final long seed, StoreBuilderSupplier storeBuilderSupplier,
                       TestKeySpacePathManagerExtension pathManager) {
            this.random = new Random(seed);
            this.storeBuilderSupplier = storeBuilderSupplier;
            this.pathManager = pathManager;
        }

        public Builder setIsGrouped(final boolean isGrouped) {
            this.isGrouped = isGrouped;
            metadata = null;
            return this;
        }

        public Builder setIsSynthetic(final boolean isSynthetic) {
            this.isSynthetic = isSynthetic;
            metadata = null;
            return this;
        }

        public Builder setPrimaryKeySegmentIndexEnabled(final boolean primaryKeySegmentIndexEnabled) {
            this.primaryKeySegmentIndexEnabled = primaryKeySegmentIndexEnabled;
            metadata = null;
            return this;
        }

        public Builder setPartitionHighWatermark(final int partitionHighWatermark) {
            this.partitionHighWatermark = partitionHighWatermark;
            metadata = null;
            return this;
        }

        public Builder setTextGeneratorWithNewRandom(final RandomTextGenerator textGenerator) {
            this.textGenerator = textGenerator.withNewRandom(random);
            return this;
        }

        /**
         * Create a new {@link LuceneIndexTestDataModel} as per the settings here.
         * <p>
         *     If this is called multiple times, without calling any setters in-between, it will use the same
         *     {@link RecordMetaData}, but use a new path.
         *     The new data model will have it's own records that it is tracking.
         * </p>
         * @return a new {@link LuceneIndexTestDataModel}
         */
        public LuceneIndexTestDataModel build() {
            if (textGenerator == null) {
                textGenerator = new RandomTextGenerator(random);
            }
            final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
            if (this.metadata == null) {
                final Map<String, String> options = getOptions();
                final RecordMetaDataBuilder metaDataBuilder = LuceneIndexTestDataModel.createBaseMetaDataBuilder();
                final KeyExpression rootExpression = LuceneIndexTestDataModel.createRootExpression(isGrouped, isSynthetic);
                this.index = LuceneIndexTestDataModel.addIndex(isSynthetic, rootExpression, options, metaDataBuilder);
                this.metadata = metaDataBuilder.build();
            }
            final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context -> {
                final FDBRecordStore store = storeBuilderSupplier.get(context, metadata, path).createOrOpen();
                store.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
                return store;
            };
            return new LuceneIndexTestDataModel(this, schemaSetup);
        }

        @Nonnull
        private Map<String, String> getOptions() {
            final Map<String, String> options = new HashMap<>();
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, String.valueOf(primaryKeySegmentIndexEnabled));
            if (partitionHighWatermark > 0) {
                options.put(LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "parent.timestamp" : "timestamp");
                options.put(LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(partitionHighWatermark));
            }
            return options;
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


    private CompletableFuture<Void> updateRecord(final FDBRecordStore recordStore,
                                                 Tuple primaryKey,
                                                 Function<Message, Message> updateMessage) {
        return recordStore.loadRecordAsync(primaryKey).thenAccept(existingRecord -> {
            recordStore.saveRecord(updateMessage.apply(existingRecord.getRecord()));
        });
    }

    /**
     * Model representing the record under test, which may be synthetic, or a proper record.
     */
    public interface RecordUnderTest {
        CompletableFuture<Void> updateOtherValue(FDBRecordStore recordStore);

        CompletableFuture<Void> deleteRecord(FDBRecordStore recordStore);
    }

    private class ParentRecord implements RecordUnderTest {
        @Nonnull
        final Tuple groupingKey;
        @Nonnull
        final Tuple primaryKey;

        private ParentRecord(@Nonnull final Tuple groupingKey, @Nonnull final Tuple primaryKey) {
            this.groupingKey = groupingKey;
            this.primaryKey = primaryKey;
        }

        @Override
        public CompletableFuture<Void> updateOtherValue(FDBRecordStore recordStore) {
            return updateRecord(recordStore, primaryKey, existingRecord -> {
                final var builder = TestRecordsGroupedParentChildProto.MyParentRecord.newBuilder();
                builder.mergeFrom(existingRecord);
                builder.setIntValue(random.nextInt());
                return builder.build();
            });
        }

        @Override
        public CompletableFuture<Void> deleteRecord(FDBRecordStore recordStore) {
            groupingKeyToPrimaryKeyToPartitionKey.get(groupingKey).remove(primaryKey);
            recordsUnderTest.remove(primaryKey);
            return recordStore.deleteRecordAsync(primaryKey)
                    .thenAccept(wasDeleted -> assertTrue(wasDeleted, () -> primaryKey + " should have been deletable"));
        }
    }
}
