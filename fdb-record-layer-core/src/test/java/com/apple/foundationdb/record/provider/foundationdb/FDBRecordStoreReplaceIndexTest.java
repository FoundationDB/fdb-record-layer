/*
 * FDBRecordStoreReplaceIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that check that an index is removed if its replacement index(es) are all built.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreReplaceIndexTest extends FDBRecordStoreTestBase {

    private Index setReplacementIndexes(@Nonnull Index index, Index... replacementIndexes) {
        Map<String, String> newOptions = new HashMap<>(index.getOptions());
        // Remove any existing replacement indexes
        final List<String> optionsToRemove = index.getOptions().keySet().stream()
                .filter(option -> option.startsWith(IndexOptions.REPLACED_BY_OPTION_PREFIX))
                .collect(Collectors.toList());
        optionsToRemove.forEach(newOptions::remove);

        // Replace with the new indexes
        final String[] replacementIndexNames = new String[replacementIndexes.length];
        for (int i = 0; i < replacementIndexes.length; i++) {
            final String replacementIndexName = replacementIndexes[i].getName();
            newOptions.put(IndexOptions.REPLACED_BY_OPTION_PREFIX + "_" + i, replacementIndexName);
            replacementIndexNames[i] = replacementIndexName;
        }
        Index newIndex = new IndexWithOptions(index, newOptions, false);
        assertThat(newIndex.getReplacedByIndexNames(), containsInAnyOrder(replacementIndexNames));
        assertEquals(index.getLastModifiedVersion(), newIndex.getLastModifiedVersion());
        assertEquals(index.getSubspaceKey(), newIndex.getSubspaceKey());

        return newIndex;
    }

    private RecordMetaDataHook addIndexHook(@Nonnull String recordTypeName, @Nonnull Index index) {
        return metaDataBuilder -> metaDataBuilder.addIndex(recordTypeName, index);
    }

    private RecordMetaDataHook addIndexAndReplacements(@Nonnull String recordTypeName, @Nonnull Index origIndex, @Nonnull Index... newIndexes) {
        return metaDataBuilder -> {
            final Index origIndexWithReplacement = setReplacementIndexes(origIndex, newIndexes);
            metaDataBuilder.addIndex(recordTypeName, origIndexWithReplacement);
            for (Index newIndex : newIndexes) {
                metaDataBuilder.addIndex(recordTypeName, newIndex);
            }
        };
    }

    private RecordMetaDataHook bumpMetaDataVersionHook() {
        return metaDataBuilder -> metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 1);
    }

    protected RecordMetaDataHook composeHooks(RecordMetaDataHook... hooks) {
        return metaDataBuilder -> {
            for (RecordMetaDataHook hook : hooks) {
                hook.apply(metaDataBuilder);
            }
        };
    }

    private List<IndexEntry> scanIndex(Index index) {
        return recordStore.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS, recordStore.scanIndex(
                recordStore.getRecordMetaData().getIndex(index.getName()),
                IndexScanType.BY_VALUE,
                TupleRange.ALL,
                null,
                ScanProperties.FORWARD_SCAN
        ).asList());
    }

    private boolean disableIndex(Index index) {
        return recordStore.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_DROP_INDEX,
                recordStore.markIndexDisabled(index));
    }

    private boolean uncheckedMarkIndexReadable(Index index) {
        return recordStore.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ADD_INDEX,
                recordStore.uncheckedMarkIndexReadable(index.getName()));
    }

    private void buildIndex(Index index) {
        recordStore.getRecordContext().asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX,
                recordStore.rebuildIndex(recordStore.getRecordMetaData().getIndex(index.getName())));
    }

    @Test
    public void markExistingAsReplacement() {
        final String recordTypeName = "MyOtherRecord";
        final Index origIndex = new Index("MyOtherRecord$(num_value_2, num_value_3_indexed)", "num_value_2", "num_value_3_indexed");
        final Index newIndex = new Index("MyOtherRecord$(num_value_3_indexed, num_value_2)", "num_value_3_indexed", "num_value_2");
        final RecordMetaDataHook addBothIndexes = composeHooks(addIndexHook(recordTypeName, origIndex), addIndexHook(recordTypeName, newIndex));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, addBothIndexes);
            assertTrue(recordStore.isIndexReadable(origIndex), "Old index should be readable at start");
            assertTrue(recordStore.isIndexReadable(newIndex), "New index should be readable at start");

            recordStore.saveRecord(TestRecords1Proto.MyOtherRecord.newBuilder()
                    .setRecNo(1415L)
                    .setNumValue2(42)
                    .setNumValue3Indexed(3)
                    .build());

            final List<IndexEntry> oldIndexEntries = scanIndex(origIndex);
            assertThat(oldIndexEntries, hasSize(1));
            assertEquals(Tuple.from(1415L), oldIndexEntries.get(0).getPrimaryKey());
            assertEquals(Tuple.from(42L, 3L, 1415L), oldIndexEntries.get(0).getKey());

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, composeHooks(addIndexAndReplacements(recordTypeName, origIndex, newIndex), bumpMetaDataVersionHook()));
            assertTrue(recordStore.isIndexDisabled(origIndex.getName()));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, composeHooks(addBothIndexes, bumpMetaDataVersionHook(), bumpMetaDataVersionHook()));
            assertTrue(uncheckedMarkIndexReadable(origIndex));
            assertThat(scanIndex(origIndex), empty());
        }
    }

    @Test
    public void buildReplacement() {
        final String recordTypeName = "MySimpleRecord";
        final Index origIndex = new Index("MySimpleRecord$(num_value_2, str_value_indexed)", "num_value_2", "str_value_indexed");
        final Index newIndex = new Index("MySimpleRecord$(str_value_indexed, num_value_2)", "str_value_indexed", "num_value_2");
        final RecordMetaDataHook addBothIndexes = composeHooks(addIndexHook(recordTypeName, origIndex), addIndexHook(recordTypeName, newIndex));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, addBothIndexes);
            assertTrue(disableIndex(newIndex));
            assertTrue(recordStore.isIndexReadable(origIndex), "Old index should be readable at start");

            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(32)
                    .setStrValueIndexed("hello")
                    .build());

            commit(context);
        }

        final RecordMetaDataHook withReplacementsHook = composeHooks(addIndexAndReplacements(recordTypeName, origIndex, newIndex), bumpMetaDataVersionHook());
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, withReplacementsHook);
            assertTrue(recordStore.isIndexReadable(origIndex.getName()), "Old index should be readable until replacement index is built");
            final List<IndexEntry> oldIndexEntries = scanIndex(origIndex);
            assertThat(oldIndexEntries, hasSize(1));
            assertEquals(Tuple.from(1066L), oldIndexEntries.get(0).getPrimaryKey());
            assertEquals(Tuple.from(32, "hello", 1066L), oldIndexEntries.get(0).getKey());

            buildIndex(newIndex);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, withReplacementsHook);
            assertTrue(recordStore.isIndexDisabled(origIndex.getName()), "Old index should be disabled once replacement index is built");
            commit(context);
        }

        // Verify the data in the old index was removed by removing the replacement information and marking the old index as readable, then
        // scan the index
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, composeHooks(addBothIndexes, bumpMetaDataVersionHook(), bumpMetaDataVersionHook()));
            assertTrue(uncheckedMarkIndexReadable(origIndex));
            final List<IndexEntry> oldIndexEntries = scanIndex(origIndex);
            assertThat("Index data should have been deleted", oldIndexEntries, empty());
        }
    }

    @Test
    public void buildReplacementsInMultipleStores() {
        final KeySpacePath multiStoreRoot = pathManager.createPath(TestKeySpace.MULTI_RECORD_STORE);
        try {
            final String recordTypeName = "MySimpleRecord";
            final Index origIndex = new Index("MySimpleRecord$repeater", Key.Expressions.field("repeater", KeyExpression.FanType.FanOut));
            final Index newIndex = new Index("MySimpleRecord$(num_value_2, repeater)",
                    Key.Expressions.concat(Key.Expressions.field("num_value_2"), Key.Expressions.field("repeater", KeyExpression.FanType.FanOut)));
            final RecordMetaDataHook allIndexesHook = composeHooks(addIndexHook(recordTypeName, origIndex), addIndexHook(recordTypeName, newIndex));
            final List<String> stores = IntStream.range(0, 10).mapToObj(i -> "store_" + i).collect(Collectors.toList());

            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, allIndexesHook);

                // Create a bunch of stores and disable the new index in all of them
                forEachStore(multiStoreRoot, stores, (storePathName, subStore) -> {
                    assertTrue(context.asyncToSync(FDBStoreTimer.Waits.WAIT_DROP_INDEX, subStore.markIndexDisabled(newIndex)));

                    subStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                            .setRecNo(1066L)
                            .addRepeater(42)
                            .addRepeater(800)
                            .setStrValueIndexed(storePathName)
                            .build());

                });

                commit(context);
            }

            final RecordMetaDataHook withReplacementHook = composeHooks(addIndexAndReplacements(recordTypeName, origIndex, newIndex), bumpMetaDataVersionHook());
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, withReplacementHook);

                forEachStore(multiStoreRoot, stores, (storePathName, subStore) -> {
                    final List<FDBIndexedRecord<Message>> records = context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_RECORDS, subStore.scanIndexRecords(origIndex.getName()).asList());
                    assertThat(records, hasSize(2));
                    records.stream()
                            .map(FDBIndexedRecord::getRecord)
                            .map(msg -> msg.getField(msg.getDescriptorForType().findFieldByName("str_value_indexed")))
                            .map(fieldValue -> {
                                assertThat(fieldValue, instanceOf(String.class));
                                return (String)fieldValue;
                            })
                            .forEach(strValue -> assertEquals(storePathName, strValue));

                    context.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, subStore.rebuildIndex(subStore.getRecordMetaData().getIndex(newIndex.getName())));
                });

                commit(context);
            }

            // Validate that each store has had the original index removed (because the replacement index was built)
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, withReplacementHook);
                forEachStore(multiStoreRoot, stores, (storePathName, subStore) -> assertTrue(subStore.isIndexDisabled(origIndex.getName())));
                commit(context);
            }

            // Validate each store has had the old index data cleaned out
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, composeHooks(allIndexesHook, bumpMetaDataVersionHook(), bumpMetaDataVersionHook()));
                forEachStore(multiStoreRoot, stores, (storePathName, subStore) -> {
                    assertTrue(context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADD_INDEX, subStore.uncheckedMarkIndexReadable(origIndex.getName())));
                    assertEquals(Collections.emptyList(), context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS, subStore.scanIndexRecords(origIndex.getName()).asList()));
                });
            }

        } finally {
            try (FDBRecordContext context = openContext()) {
                multiStoreRoot.deleteAllData(context);
                commit(context);
            }
        }
    }

    private void forEachStore(@Nonnull KeySpacePath root, @Nonnull List<String> storePaths, @Nonnull BiConsumer<String, FDBRecordStore> subStoreConsumer) {
        for (String storePathName : storePaths) {
            final KeySpacePath storePath = root.add(TestKeySpace.STORE_PATH, storePathName);
            final FDBRecordStore subStore = recordStore.asBuilder()
                    .setKeySpacePath(storePath)
                    .createOrOpen();
            subStoreConsumer.accept(storePathName, subStore);
        }
    }

    @Test
    public void buildTwoReplacements() {
        final String recordTypeName = "MySimpleRecord";
        final Index origIndex = new Index("MySimpleRecord$(str_value_indexed, num_value_2)", "str_value_indexed", "num_value_2");
        final Index newIndex1 = new Index("MySimpleRecord$(str_value_indexed, num_value_2, num_value_3_indexed)", "str_value_indexed", "num_value_2", "num_value_3_indexed");
        final Index newIndex2 = new Index("MySimpleRecord$(str_value_indexed, num_value_2, num_value_unique)", "str_value_indexed", "num_value_2", "num_value_unique");
        final RecordMetaDataHook addAllIndexesHook = composeHooks(addIndexHook(recordTypeName, origIndex), addIndexHook(recordTypeName, newIndex1), addIndexHook(recordTypeName, newIndex2));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, addAllIndexesHook);
            assertTrue(disableIndex(newIndex1));
            assertTrue(disableIndex(newIndex2));

            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(800L)
                    .setStrValueIndexed("a_value")
                    .setNumValue2(962)
                    .setNumValue3Indexed(1806)
                    .build());

            commit(context);
        }

        // Mark the index as replaced by the two new indexes and expect the index to be gone once both new indexes are built
        RecordMetaDataHook withReplacementsHook = composeHooks(addIndexAndReplacements(recordTypeName, origIndex, newIndex1, newIndex2), bumpMetaDataVersionHook());
        final List<IndexEntry> origEntries;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, withReplacementsHook);

            assertTrue(recordStore.isIndexReadable(origIndex));
            origEntries = scanIndex(origIndex);
            assertThat(origEntries, hasSize(1));
            assertEquals(Tuple.from(800L), origEntries.get(0).getPrimaryKey());
            assertEquals(Tuple.from("a_value", 962L, 800L), origEntries.get(0).getKey());

            buildIndex(newIndex1);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, withReplacementsHook);

            assertTrue(recordStore.isIndexReadable(origIndex));
            assertEquals(origEntries, scanIndex(origIndex));

            disableIndex(newIndex1);
            buildIndex(newIndex2);

            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, withReplacementsHook);

            assertTrue(recordStore.isIndexReadable(origIndex));
            assertEquals(origEntries, scanIndex(origIndex));

            buildIndex(newIndex1);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, withReplacementsHook);
            assertTrue(recordStore.isIndexDisabled(origIndex));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, composeHooks(addAllIndexesHook, bumpMetaDataVersionHook(), bumpMetaDataVersionHook()));

            assertTrue(uncheckedMarkIndexReadable(origIndex));
            assertThat(scanIndex(origIndex), empty());
        }
    }

    @Test
    public void rebuildAllIndexesDoesNotRebuildIfReplaced() {
        final String recordTypeName = "MySimpleRecord";
        final Index origIndex = new Index("origIndex", Key.Expressions.field("num_value_2"));
        final Index newIndex = new Index("newIndex", Key.Expressions.field("num_value_2"));
        final RecordMetaDataHook metaDataHook = addIndexAndReplacements(recordTypeName, origIndex, newIndex);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataHook);
            assertTrue(recordStore.isIndexDisabled(origIndex), "index with replacements should begin disabled");
            recordStore.rebuildAllIndexes().join();
            assertTrue(recordStore.isIndexDisabled(origIndex), "index with replacements should not be built with all indexes");
            commit(context);
        }
    }

    @Test
    public void excludeIndexWithReplacementsFromSetToBuild() {
        final String recordTypeName = "MySimpleRecord";
        final Index origIndex = new Index("origIndex", Key.Expressions.field("num_value_2"));
        final Index newIndex = new Index("newIndex", Key.Expressions.field("num_value_2"));
        final RecordMetaDataHook metaDataHook = addIndexAndReplacements(recordTypeName, origIndex, newIndex);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataHook);
            assertTrue(recordStore.isIndexDisabled(origIndex), "index with replacements should begin disabled");
            assertTrue(recordStore.isIndexReadable(newIndex), "newIndex should begin built");
            assertTrue(recordStore.getIndexesToBuild().keySet().stream().noneMatch(index -> index.getName().equals(origIndex.getName())),
                    "index with replacements should not be listed as index to build even if unbuilt");
            commit(context);
        }
    }

    @Test
    public void replacementIndexMissingInMetaDataFails() {
        try (FDBRecordContext context = openContext()) {
            MetaDataException err = assertThrows(MetaDataException.class, () -> openSimpleRecordStore(context, metaDataBuilder -> {
                final Index index = new Index("indexWithFakeReplacement", Key.Expressions.field("num_value_2"),
                        IndexTypes.VALUE,
                        Collections.singletonMap(IndexOptions.REPLACED_BY_OPTION_PREFIX, "fakeIndex"));
                metaDataBuilder.addIndex("MySimpleRecord", index);
            }));
            assertThat(err.getMessage(), containsString("Index indexWithFakeReplacement has replacement index fakeIndex that is not in the meta-data"));
        }
    }

    @Test
    public void replacementIndexPartiallyMissingInMetaDataFails() {
        try (FDBRecordContext context = openContext()) {
            MetaDataException err = assertThrows(MetaDataException.class, () -> openSimpleRecordStore(context, metaDataBuilder -> {
                final Index index = new Index("indexWithOneFakeReplacement", Key.Expressions.field("num_value_2"),
                        IndexTypes.VALUE,
                        ImmutableMap.of(
                                IndexOptions.REPLACED_BY_OPTION_PREFIX + "_00", "fakeIndex",
                                IndexOptions.REPLACED_BY_OPTION_PREFIX + "_01", "MySimpleRecord$str_value_indexed"));
                metaDataBuilder.addIndex("MySimpleRecord", index);
            }));
            assertThat(err.getMessage(), containsString("Index indexWithOneFakeReplacement has replacement index fakeIndex that is not in the meta-data"));
        }
    }

    @Test
    public void replacementIndexCycleFails() {
        try (FDBRecordContext context = openContext()) {
            MetaDataException err = assertThrows(MetaDataException.class, () -> openSimpleRecordStore(context, metaDataBuilder -> {
                final Index index1 = new Index("firstIndex", Key.Expressions.field("num_value_2"), IndexTypes.VALUE,
                        Collections.singletonMap(IndexOptions.REPLACED_BY_OPTION_PREFIX, "secondIndex"));
                final Index index2 = new Index("secondIndex", Key.Expressions.field("num_value_2"), IndexTypes.VALUE,
                        Collections.singletonMap(IndexOptions.REPLACED_BY_OPTION_PREFIX, "firstIndex"));
                metaDataBuilder.addIndex("MySimpleRecord", index1);
                metaDataBuilder.addIndex("MySimpleRecord", index2);
            }));
            assertThat(err.getMessage(), containsString("has replacement indexes"));
        }
    }

    @Test
    public void replacementIndexWithSelfCycleFails() {
        try (FDBRecordContext context = openContext()) {
            MetaDataException err = assertThrows(MetaDataException.class, () -> openSimpleRecordStore(context, metaDataBuilder -> {
                final Index index = new Index("indexWithSelfReplacement", Key.Expressions.field("num_value_2"),
                        IndexTypes.VALUE,
                        Collections.singletonMap(IndexOptions.REPLACED_BY_OPTION_PREFIX, "indexWithSelfReplacement"));
                metaDataBuilder.addIndex("MySimpleRecord", index);
            }));
            assertThat(err.getMessage(), containsString("Index indexWithSelfReplacement has replacement index indexWithSelfReplacement that itself has replacement indexes"));
        }
    }

    @Test
    public void replacementLineFails() {
        try (FDBRecordContext context = openContext()) {
            MetaDataException err = assertThrows(MetaDataException.class, () -> openSimpleRecordStore(context, metaDataBuilder -> {
                final KeyExpression expr = Key.Expressions.field("num_value_2");
                final Index indexA = new Index("indexA", expr, IndexTypes.VALUE,
                        Collections.singletonMap(IndexOptions.REPLACED_BY_OPTION_PREFIX, "indexB"));
                final Index indexB = new Index("indexB", expr, IndexTypes.VALUE,
                        Collections.singletonMap(IndexOptions.REPLACED_BY_OPTION_PREFIX, "indexC"));
                final Index indexC = new Index("indexC", expr);
                metaDataBuilder.addIndex("MySimpleRecord", indexA);
                metaDataBuilder.addIndex("MySimpleRecord", indexB);
                metaDataBuilder.addIndex("MySimpleRecord", indexC);
            }));
            assertThat(err.getMessage(), containsString("has replacement indexes"));
        }
    }

    @Test
    public void replacementMoreComplicatedGraphFails() {
        try (FDBRecordContext context = openContext()) {
            MetaDataException err = assertThrows(MetaDataException.class, () -> openSimpleRecordStore(context, metaDataBuilder -> {
                final KeyExpression expr = Key.Expressions.field("num_value_2");
                final Index indexA = new Index("indexA", expr, IndexTypes.VALUE,
                        ImmutableMap.of(IndexOptions.REPLACED_BY_OPTION_PREFIX + "_0", "indexB",
                                IndexOptions.REPLACED_BY_OPTION_PREFIX + "_1", "indexC"));
                final Index indexB = new Index("indexB", expr, IndexTypes.VALUE,
                        Collections.singletonMap(IndexOptions.REPLACED_BY_OPTION_PREFIX, "indexD"));
                final Index indexC = new Index("indexC", expr, IndexTypes.VALUE,
                        Collections.singletonMap(IndexOptions.REPLACED_BY_OPTION_PREFIX, "indexD"));
                final Index indexD = new Index("indexD", expr);
                metaDataBuilder.addIndex("MySimpleRecord", indexA);
                metaDataBuilder.addIndex("MySimpleRecord", indexB);
                metaDataBuilder.addIndex("MySimpleRecord", indexC);
                metaDataBuilder.addIndex("MySimpleRecord", indexD);
            }));
            assertThat(err.getMessage(), containsString("has replacement indexes"));
        }
    }
}
