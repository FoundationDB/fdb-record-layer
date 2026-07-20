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
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
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
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
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

    private static final String RECORD_TYPE = "MySimpleRecord";

    // The single record shared by the replacement tests, and its field values, so that expected index-entry keys can
    // be written directly in terms of these constants.
    private static final long REC_NO = 1066L;
    private static final int NUM_VALUE_2 = 32;
    private static final int NUM_VALUE_3 = 1806;
    private static final int NUM_VALUE_UNIQUE = 7;
    private static final String STR_VALUE = "hello";

    // Expected index-entry keys for the shared record, in each index's field order followed by the primary key.
    private static final Tuple PRIMARY_KEY = Tuple.from(REC_NO);
    private static final Tuple NUM_THEN_STR_KEY = Tuple.from((long)NUM_VALUE_2, STR_VALUE, REC_NO);
    private static final Tuple STR_THEN_NUM_KEY = Tuple.from(STR_VALUE, (long)NUM_VALUE_2, REC_NO);
    private static final Tuple STR_NUM_THEN_NUM3_KEY = Tuple.from(STR_VALUE, (long)NUM_VALUE_2, (long)NUM_VALUE_3, REC_NO);
    private static final Tuple STR_NUM_THEN_UNIQUE_KEY = Tuple.from(STR_VALUE, (long)NUM_VALUE_2, (long)NUM_VALUE_UNIQUE, REC_NO);

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

    private static TestRecords1Proto.MySimpleRecord sampleRecord() {
        return TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(REC_NO)
                .setNumValue2(NUM_VALUE_2)
                .setNumValue3Indexed(NUM_VALUE_3)
                .setNumValueUnique(NUM_VALUE_UNIQUE)
                .setStrValueIndexed(STR_VALUE)
                .build();
    }

    // Fresh index instances (indexes are mutated when added to a meta-data builder, so each test needs its own).
    private static Index numThenStrIndex() {
        return new Index("MySimpleRecord$(num_value_2, str_value_indexed)", "num_value_2", "str_value_indexed");
    }

    private static Index strThenNumIndex() {
        return new Index("MySimpleRecord$(str_value_indexed, num_value_2)", "str_value_indexed", "num_value_2");
    }

    private static Index strNumThenNum3Index() {
        return new Index("MySimpleRecord$(str_value_indexed, num_value_2, num_value_3_indexed)", "str_value_indexed", "num_value_2", "num_value_3_indexed");
    }

    private static Index strNumThenUniqueIndex() {
        return new Index("MySimpleRecord$(str_value_indexed, num_value_2, num_value_unique)", "str_value_indexed", "num_value_2", "num_value_unique");
    }

    /** Open a fresh store with no user-defined indexes and save the shared sample record. */
    private void saveRecordBeforeIndexesExist() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, null);
            recordStore.saveRecord(sampleRecord());
            commit(context);
        }
    }

    /**
     * Open the store with {@code origIndex} configured as replaced by {@code newIndexes} (all added at once), a bumped
     * meta-data version to trigger {@code checkVersion}, and the checker for {@code rebuildOption}.
     */
    private void openWithReplacements(@Nonnull FDBRecordContext context, @Nonnull RebuildOption rebuildOption,
                                      @Nonnull Index origIndex, @Nonnull Index... newIndexes) {
        final RecordMetaDataHook hook = composeHooks(addIndexAndReplacements(RECORD_TYPE, origIndex, newIndexes), bumpMetaDataVersionHook());
        final String[] newIndexNames = Arrays.stream(newIndexes).map(Index::getName).toArray(String[]::new);
        openWithChecker(context, hook, rebuildOption.userVersionChecker(origIndex.getName(), newIndexNames));
    }

    /** Open the store applying {@code hook} to the meta-data and using the given (possibly null) checker. */
    private void openWithChecker(@Nonnull FDBRecordContext context, @Nullable RecordMetaDataHook hook,
                                 @Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker) {
        recordStore = getStoreBuilder(context, simpleMetaData(hook))
                .setUserVersionChecker(userVersionChecker)
                .createOrOpen();
    }

    /**
     * Remove the {@code replacedBy} configuration, force {@code origIndex} readable, and assert it holds no data. This
     * is how a caller confirms that a replaced original index had its data cleared.
     */
    private void assertOriginalIndexDataCleared(@Nonnull Index origIndex, @Nonnull Index... newIndexes) {
        final RecordMetaDataHook addAllPlainIndexes = composeHooks(
                addIndexHook(RECORD_TYPE, origIndex),
                composeHooks(Arrays.stream(newIndexes).map(index -> addIndexHook(RECORD_TYPE, index)).toArray(RecordMetaDataHook[]::new)));
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, composeHooks(addAllPlainIndexes, bumpMetaDataVersionHook(), bumpMetaDataVersionHook()));
            assertTrue(uncheckedMarkIndexReadable(origIndex));
            assertThat("Original index data should have been cleared", scanIndex(origIndex), empty());
        }
    }

    /**
     * Assert that {@code index} is in {@code expectedState}, and, when that state is {@link IndexState#READABLE}, that
     * it contains exactly the saved record with key {@code expectedKey}. This encodes two correctness properties: the
     * {@code UserVersionChecker}'s requested state must be honored regardless of {@code replacedBy}, and a readable
     * index must reflect its records.
     */
    private void assertIndexStateAndContents(@Nonnull Index index, @Nonnull IndexState expectedState, @Nonnull Tuple expectedKey) {
        assertEquals(expectedState, recordStore.getIndexState(index.getName()),
                "Index " + index.getName() + " should be " + expectedState);
        if (expectedState == IndexState.READABLE) {
            final List<IndexEntry> entries = scanIndex(index);
            assertThat("Readable index " + index.getName() + " should contain the saved record", entries, hasSize(1));
            assertEquals(PRIMARY_KEY, entries.get(0).getPrimaryKey());
            assertEquals(expectedKey, entries.get(0).getKey());
        }
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

    @ParameterizedTest
    @EnumSource(RebuildOption.class)
    void buildReplacementInlineWhenOriginalReadable(RebuildOption rebuildOption) {
        final Index origIndex = numThenStrIndex();
        final Index newIndex = strThenNumIndex();

        // Start with only the original index present and readable, then save a record
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, addIndexHook(RECORD_TYPE, origIndex));
            assertTrue(recordStore.isIndexReadable(origIndex), "Old index should be readable at start");
            recordStore.saveRecord(sampleRecord());
            commit(context);
        }

        // Introduce the replacement index and the replacedBy relationship at the same time. The checker decides whether
        // the new index is built inline; that in turn decides whether the readable original gets replaced.
        try (FDBRecordContext context = openContext()) {
            openWithReplacements(context, rebuildOption, origIndex, newIndex);
            if (rebuildOption.buildsNewIndex()) {
                assertIndexStateAndContents(newIndex, IndexState.READABLE, STR_THEN_NUM_KEY);
                assertIndexStateAndContents(origIndex, IndexState.DISABLED, NUM_THEN_STR_KEY);
            } else {
                assertIndexStateAndContents(newIndex, IndexState.DISABLED, STR_THEN_NUM_KEY);
                // The original index is untouched, so it remains readable and still holds the record
                assertIndexStateAndContents(origIndex, IndexState.READABLE, NUM_THEN_STR_KEY);
            }
            commit(context);
        }

        if (rebuildOption.buildsNewIndex()) {
            assertOriginalIndexDataCleared(origIndex, newIndex);
        }
    }

    @ParameterizedTest
    @EnumSource(RebuildOption.class)
    void buildReplacementInlineWhenOriginalDisabled(RebuildOption rebuildOption) {
        final Index origIndex = numThenStrIndex();
        final Index newIndex = strThenNumIndex();

        // Start with only the original index present, mark it disabled, then save a record
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, addIndexHook(RECORD_TYPE, origIndex));
            assertTrue(disableIndex(origIndex));
            assertTrue(recordStore.isIndexDisabled(origIndex), "Old index should be disabled at start");
            recordStore.saveRecord(sampleRecord());
            commit(context);
        }

        // Introduce the replacement index and the replacedBy relationship at the same time. Whatever happens to the
        // new index, the original was disabled from the start and stays disabled.
        try (FDBRecordContext context = openContext()) {
            openWithReplacements(context, rebuildOption, origIndex, newIndex);
            assertIndexStateAndContents(origIndex, IndexState.DISABLED, NUM_THEN_STR_KEY);
            assertIndexStateAndContents(newIndex, rebuildOption.expectedReplacementState(), STR_THEN_NUM_KEY);
            commit(context);
        }

        // The original was disabled throughout (and cleared if replaced), so it holds no data
        assertOriginalIndexDataCleared(origIndex, newIndex);
    }

    /**
     * Save a record, then add both the original index (carrying its {@code replacedBy} configuration) and its
     * replacement index at the same time, and open the store. The checker's requested state must be honored for both
     * indexes, and any index that ends up readable must contain the previously-saved record.
     *
     * <p>This exposes a bug: {@code RecordMetaData.getIndexesToBuildSince} excludes indexes that have replacements,
     * so a brand-new original index carrying {@code replacedBy} is never handed to the {@code UserVersionChecker} and
     * is never built. When its replacement is also not built (so the original is not disabled by
     * {@code removeReplacedIndexes}), the original index is left marked readable but empty even when the checker asked
     * for it to be disabled, which is incorrect.</p>
     */
    @ParameterizedTest
    @EnumSource(RebuildOption.class)
    void readableIndexHasCorrectContentsWhenAddedWithReplacementTogether(RebuildOption rebuildOption) {
        final Index origIndex = numThenStrIndex();
        final Index newIndex = strThenNumIndex();

        saveRecordBeforeIndexesExist();

        // Add both indexes (with the replacedBy relationship) at the same time, then open the store
        try (FDBRecordContext context = openContext()) {
            openWithReplacements(context, rebuildOption, origIndex, newIndex);
            // The checker's requested state must be honored, and any readable index must reflect the saved record
            // TODO here we are opening the store, and checkVersion says the replaced should be built, but the replacement
            //      should not. Do we want to have the UserVersionChecker control at all
            //      i.e. two potential, correct-ish behaviors:
            //      1. UserVersionChecker sees the original, and then we see that it's replacements are flagged to be
            //      readable, and change its desired state to disabled, overriding UserVersionChecker's response
            //      2. UserVersionChecker does not see the original or opine, and we set it to disabled after
            //      (2) means that we could end up in a state where neither is readable. This could be problematic in a
            //          a situation where the UserVersionChecker is trying to control the rollout a bit so it doesn't
            //          build the new version too aggressively, resulting in a single store not having either
            //      (1) could mean wasted resources trying to calculate count or size to determine whether to build
            //          new indexes, but unless your in a state where the replacement is older than the replaced, you'll
            //          have to do those calculations anyways.
            assertIndexStateAndContents(origIndex, rebuildOption.expectedOriginalState(), NUM_THEN_STR_KEY);
            assertIndexStateAndContents(newIndex, rebuildOption.expectedReplacementState(), STR_THEN_NUM_KEY);
            commit(context);
        }
    }

    /**
     * Same as {@link #readableIndexHasCorrectContentsWhenAddedWithReplacementTogether}, but the original index is
     * replaced by two indexes that are all added together after a record is saved. The checker's requested state must
     * be honored for every index, and any readable index must contain the saved record.
     */
    @ParameterizedTest
    @EnumSource(RebuildOption.class)
    void readableIndexHasCorrectContentsWhenAddedWithTwoReplacementsTogether(RebuildOption rebuildOption) {
        final Index origIndex = strThenNumIndex();
        final Index newIndex1 = strNumThenNum3Index();
        final Index newIndex2 = strNumThenUniqueIndex();

        saveRecordBeforeIndexesExist();

        // Add the original index (replaced by both new indexes) and the two replacements together, then open
        try (FDBRecordContext context = openContext()) {
            openWithReplacements(context, rebuildOption, origIndex, newIndex1, newIndex2);
            assertIndexStateAndContents(origIndex, rebuildOption.expectedOriginalState(), STR_THEN_NUM_KEY);
            assertIndexStateAndContents(newIndex1, rebuildOption.expectedReplacementState(), STR_NUM_THEN_NUM3_KEY);
            assertIndexStateAndContents(newIndex2, rebuildOption.expectedReplacementState(), STR_NUM_THEN_UNIQUE_KEY);
            commit(context);
        }
    }

    /**
     * A replacement index that finishes building as a unique index over data with duplicate values lands in
     * {@link IndexState#READABLE_UNIQUE_PENDING}, which is not fully readable. The original index it replaces should NOT
     * be dropped in that case, otherwise the store would be left with no readable index over that data.
     */
    @Test
    void doesNotDropOriginalWhenReplacementIsUniquePending() {
        final Index origIndex = new Index("origIndexBeingReplaced", Key.Expressions.field("num_value_2"));
        final Index replacementUnique = new Index("replacementUniqueIndex", Key.Expressions.field("num_value_2"), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);

        // Start with the original index readable and populated with two records that share a num_value_2 value, so a
        // unique index over that field cannot become fully readable. (num_value_unique must differ to satisfy the
        // built-in unique index on that field.)
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, addIndexHook(RECORD_TYPE, origIndex));
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1L).setNumValue2(10).setNumValueUnique(901).build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(2L).setNumValue2(10).setNumValueUnique(902).build());
            assertTrue(recordStore.isIndexReadable(origIndex), "original index should be readable at start");
            commit(context);
        }

        // Introduce the unique replacement, but keep it write-only: a checker prevents the inline unique rebuild,
        // which would otherwise fail on the duplicate values.
        final RecordMetaDataHook withReplacementHook = composeHooks(addIndexAndReplacements(RECORD_TYPE, origIndex, replacementUnique), bumpMetaDataVersionHook());
        try (FDBRecordContext context = openContext()) {
            openWithChecker(context, withReplacementHook, new SelectiveUserVersionChecker(ImmutableMap.of(replacementUnique.getName(), IndexState.WRITE_ONLY)));
            assertTrue(recordStore.isIndexReadable(origIndex), "original index should remain readable");
            assertEquals(IndexState.WRITE_ONLY, recordStore.getIndexState(replacementUnique.getName()));
            commit(context);

            // Build the unique replacement over the duplicate data. With allowUniquePendingState it lands in
            // READABLE_UNIQUE_PENDING rather than READABLE (or throwing on the duplicate).
            try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setTargetIndexes(List.of(replacementUnique))
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState())
                    .build()) {
                indexer.buildIndex();
            }
        }

        // Reopen with a bumped version so checkVersion runs removeReplacedIndexes. Because the replacement is only
        // unique-pending (not readable), the original must survive with its data intact.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, composeHooks(withReplacementHook, bumpMetaDataVersionHook()));
            assertEquals(IndexState.READABLE_UNIQUE_PENDING, recordStore.getIndexState(replacementUnique.getName()),
                    "replacement should be readable-unique-pending because of the duplicate values");
            assertTrue(recordStore.isIndexReadable(origIndex),
                    "original index must not be dropped while its replacement is only unique-pending");
            assertThat(scanIndex(origIndex), hasSize(2));
            commit(context);
        }
    }

    /**
     * When the last replacement index is built and marked readable, the original index is dropped as part of that same
     * commit (via a commit check), not only on a later {@code checkVersion}. The final reopen uses the SAME meta-data
     * version (no bump), so {@code checkVersion} does not run {@code removeReplacedIndexes}: the original being disabled
     * can only be the result of the commit-check path triggered by marking the replacement readable. This is verified
     * both for the in-transaction {@code rebuildIndex} build and for the {@link OnlineIndexer} build, which mark the
     * index readable through the same code path.
     */
    @ParameterizedTest
    @BooleanSource("useOnlineIndexer")
    void originalRemovedAtCommitWhenReplacementMarkedReadable(boolean useOnlineIndexer) {
        final Index origIndex = numThenStrIndex();
        final Index newIndex = strThenNumIndex();

        // Original readable and populated; replacement present but disabled.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, composeHooks(addIndexHook(RECORD_TYPE, origIndex), addIndexHook(RECORD_TYPE, newIndex)));
            assertTrue(disableIndex(newIndex));
            recordStore.saveRecord(sampleRecord());
            commit(context);
        }

        final RecordMetaDataHook withReplacementHook = composeHooks(addIndexAndReplacements(RECORD_TYPE, origIndex, newIndex), bumpMetaDataVersionHook());
        // Build the replacement and mark it readable. Marking it readable schedules removeReplacedIndexes as a commit
        // check, which drops the replaced original when that build's transaction commits.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, withReplacementHook);
            assertTrue(recordStore.isIndexReadable(origIndex), "original readable before the replacement is built");
            if (useOnlineIndexer) {
                // The OnlineIndexer runs its own transactions, so persist the replacedBy meta-data first; it then
                // drops the original in the transaction where it marks the replacement readable.
                commit(context);
                try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                        .setRecordStore(recordStore)
                        .setTargetIndexes(List.of(newIndex))
                        .build()) {
                    indexer.buildIndex();
                }
            } else {
                buildIndex(newIndex); // builds + marks readable in this transaction
                assertTrue(recordStore.isIndexReadable(origIndex), "original is not dropped until the transaction commits");
                commit(context);
            }
        }

        // Reopen with the SAME version (no additional bump), so checkVersion does not trigger removeReplacedIndexes.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, withReplacementHook);
            assertTrue(recordStore.isIndexDisabled(origIndex), "original should have been dropped at commit time");
            assertTrue(recordStore.isIndexReadable(newIndex), "replacement should be readable");
            commit(context);
        }
    }

    /**
     * Aborting a migration: after configuring {@code replacedBy} with the replacement left unbuilt (so the original
     * keeps serving), removing the {@code replacedBy} option again must leave the original readable and populated. The
     * original must never have been dropped.
     */
    @Test
    void removingReplacedByBeforeReplacementBuiltKeepsOriginal() {
        final Index origIndex = numThenStrIndex();
        final Index newIndex = strThenNumIndex();

        // Original readable and populated.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, addIndexHook(RECORD_TYPE, origIndex));
            recordStore.saveRecord(sampleRecord());
            commit(context);
        }

        // Configure replacedBy but keep the replacement disabled (unbuilt). The original keeps serving.
        final RecordMetaDataHook withReplacementHook = composeHooks(addIndexAndReplacements(RECORD_TYPE, origIndex, newIndex), bumpMetaDataVersionHook());
        try (FDBRecordContext context = openContext()) {
            openWithChecker(context, withReplacementHook, new SelectiveUserVersionChecker(ImmutableMap.of(newIndex.getName(), IndexState.DISABLED)));
            assertIndexStateAndContents(origIndex, IndexState.READABLE, NUM_THEN_STR_KEY);
            assertEquals(IndexState.DISABLED, recordStore.getIndexState(newIndex.getName()));
            commit(context);
        }

        // Abort: drop the replacedBy relationship (both indexes plain) and bump the version. Removing the option is a
        // rebuild-free change, and the original must remain readable and populated.
        final RecordMetaDataHook abortHook = composeHooks(addIndexHook(RECORD_TYPE, origIndex), addIndexHook(RECORD_TYPE, newIndex),
                bumpMetaDataVersionHook(), bumpMetaDataVersionHook());
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, abortHook);
            assertIndexStateAndContents(origIndex, IndexState.READABLE, NUM_THEN_STR_KEY);
            commit(context);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void buildReplacementsInMultipleStores(boolean buildInAllStores) {
        final KeySpacePath multiStoreRoot = pathManager.createPath(TestKeySpace.MULTI_RECORD_STORE);
        try {
            final String recordTypeName = "MySimpleRecord";
            final Index origIndex = new Index("MySimpleRecord$repeater", Key.Expressions.field("repeater", KeyExpression.FanType.FanOut));
            final Index newIndex = new Index("MySimpleRecord$(num_value_2, repeater)",
                    Key.Expressions.concat(Key.Expressions.field("num_value_2"), Key.Expressions.field("repeater", KeyExpression.FanType.FanOut)));
            final RecordMetaDataHook allIndexesHook = composeHooks(addIndexHook(recordTypeName, origIndex), addIndexHook(recordTypeName, newIndex));
            final List<String> stores = IntStream.range(0, 10).mapToObj(i -> "store_" + i).collect(Collectors.toList());
            // When not building in all stores, build the replacement in only the even-numbered stores. Removal is
            // per-store, so only those stores should drop the original.
            final Set<String> storesToBuild = stores.stream()
                    .filter(name -> buildInAllStores || Integer.parseInt(name.substring("store_".length())) % 2 == 0)
                    .collect(Collectors.toSet());
            if (buildInAllStores) {
                assertEquals(stores.size(), storesToBuild.size(), "All stores should be scheduled to build");
            } else {
                assertEquals(stores.size() / 2, storesToBuild.size(), "Half of the stores should be scheduled to build");
            }


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

                    if (storesToBuild.contains(storePathName)) {
                        context.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX,
                                subStore.rebuildIndex(subStore.getRecordMetaData().getIndex(newIndex.getName())));
                    }
                });

                commit(context);
            }

            // Validate that only stores where the replacement was built have had the original index removed
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, withReplacementHook);
                forEachStore(multiStoreRoot, stores, (storePathName, subStore) -> {
                    if (storesToBuild.contains(storePathName)) {
                        assertTrue(subStore.isIndexDisabled(origIndex.getName()), storePathName + ": original should be dropped once its replacement is built");
                    } else {
                        assertTrue(subStore.isIndexReadable(origIndex.getName()), storePathName + ": original should still be readable where the replacement was not built");
                        assertThat(context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_RECORDS, subStore.scanIndexRecords(origIndex.getName()).asList()), hasSize(2));
                    }
                });
                commit(context);
            }

            // Validate the built stores had the old index data cleaned out, and the others still hold their data
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, composeHooks(allIndexesHook, bumpMetaDataVersionHook(), bumpMetaDataVersionHook()));
                forEachStore(multiStoreRoot, stores, (storePathName, subStore) -> {
                    if (storesToBuild.contains(storePathName)) {
                        assertTrue(context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADD_INDEX, subStore.uncheckedMarkIndexReadable(origIndex.getName())));
                        assertEquals(Collections.emptyList(), context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS, subStore.scanIndexRecords(origIndex.getName()).asList()));
                    } else {
                        assertThat(context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_RECORDS, subStore.scanIndexRecords(origIndex.getName()).asList()), hasSize(2));
                    }
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

    /**
     * The behaviors of a {@link FDBRecordStoreBase.UserVersionChecker} to exercise when a replacement index is
     * introduced at the same time as its {@code replacedBy} configuration. Each option describes what the checker
     * asks for, and {@link #expectedOriginalState()} / {@link #expectedReplacementState()} record the correct
     * resulting states: a {@code DISABLED} decision from the checker must be honored regardless of {@code replacedBy}.
     */
    private enum RebuildOption {
        /** No checker: the default record-count heuristic builds the new index inline (few records), so the original is replaced and disabled. */
        NULL_CHECKER(IndexState.DISABLED, IndexState.READABLE),
        /** Checker always tries to build both inline, ignoring record count. */
        ALWAYS_BUILD_BOTH(IndexState.DISABLED, IndexState.READABLE),
        /** Checker refuses to build any index: both the original and the replacement must be disabled. */
        NEVER_BUILD_ANY(IndexState.DISABLED, IndexState.DISABLED),
        /** Checker refuses to build the original index: the original must be disabled, and the replacement is built. */
        NEVER_BUILD_ORIGINAL(IndexState.DISABLED, IndexState.READABLE),
        /** Checker refuses to build the replacement index: the replacement is disabled, so the original is not replaced and must stay a readable, populated index. */
        NEVER_BUILD_NEW(IndexState.READABLE, IndexState.DISABLED);

        private final IndexState expectedOriginalState;
        private final IndexState expectedReplacementState;

        RebuildOption(IndexState expectedOriginalState, IndexState expectedReplacementState) {
            this.expectedOriginalState = expectedOriginalState;
            this.expectedReplacementState = expectedReplacementState;
        }

        boolean buildsNewIndex() {
            return expectedReplacementState == IndexState.READABLE;
        }

        IndexState expectedOriginalState() {
            return expectedOriginalState;
        }

        IndexState expectedReplacementState() {
            return expectedReplacementState;
        }

        @Nullable
        FDBRecordStoreBase.UserVersionChecker userVersionChecker(@Nonnull String origIndexName, @Nonnull String... newIndexNames) {
            final Map<String, IndexState> immediateStates = new HashMap<>();
            return switch (this) {
                case NULL_CHECKER -> null;
                case NEVER_BUILD_ANY -> {
                    immediateStates.put(origIndexName, IndexState.DISABLED);
                    for (String newIndexName : newIndexNames) {
                        immediateStates.put(newIndexName, IndexState.DISABLED);
                    }
                    yield new SelectiveUserVersionChecker(immediateStates);
                }
                case NEVER_BUILD_ORIGINAL -> {
                    immediateStates.put(origIndexName, IndexState.DISABLED);
                    for (String newIndexName : newIndexNames) {
                        immediateStates.put(newIndexName, IndexState.READABLE);
                    }
                    yield new SelectiveUserVersionChecker(immediateStates);
                }
                case NEVER_BUILD_NEW -> {
                    immediateStates.put(origIndexName, IndexState.READABLE);
                    for (String newIndexName : newIndexNames) {
                        immediateStates.put(newIndexName, IndexState.DISABLED);
                    }
                    yield new SelectiveUserVersionChecker(immediateStates);
                }
                case ALWAYS_BUILD_BOTH -> {
                    immediateStates.put(origIndexName, IndexState.READABLE);
                    for (String newIndexName : newIndexNames) {
                        immediateStates.put(newIndexName, IndexState.READABLE);
                    }
                    yield new SelectiveUserVersionChecker(immediateStates);
                }
            };
        }
    }

    /**
     * A {@link FDBRecordStoreBase.UserVersionChecker} that returns a fixed {@link IndexState} for named indexes and
     * consults the record count for the rest.
     */
    private static class SelectiveUserVersionChecker implements FDBRecordStoreBase.UserVersionChecker {
        private final Map<String, IndexState> immediateStates;

        SelectiveUserVersionChecker(Map<String, IndexState> immediateStates) {
            this.immediateStates = immediateStates;
        }

        @Override
        public CompletableFuture<Integer> checkUserVersion(@Nonnull RecordMetaDataProto.DataStoreInfo storeHeader,
                                                           RecordMetaDataProvider metaData) {
            return CompletableFuture.completedFuture(storeHeader.getUserVersion());
        }

        @Deprecated
        @Override
        public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion,
                                                           RecordMetaDataProvider metaData) {
            return Assertions.fail(); // if we've hit this, then we've gone down an unexpected path
        }

        @Nonnull
        @Override
        public CompletableFuture<IndexState> needRebuildIndex(Index index,
                                                              Supplier<CompletableFuture<Long>> lazyRecordCount,
                                                              Supplier<CompletableFuture<Long>> lazyEstimatedSize,
                                                              boolean indexOnNewRecordTypes) {
            IndexState immediateState = immediateStates.get(index.getName());
            if (immediateState != null) {
                return CompletableFuture.completedFuture(immediateState);
            }
            // TODO should this error
            // Default: consult the record count
            return lazyRecordCount.get().thenApply(count ->
                    FDBRecordStore.disabledIfTooManyRecordsForRebuild(count, indexOnNewRecordTypes));
        }
    }

}
