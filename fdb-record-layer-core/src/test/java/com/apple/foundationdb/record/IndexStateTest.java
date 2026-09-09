/*
 * IndexStateTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IndexState} and for the index-state accessors that delegate to its predicates.
 */
@Tag(Tags.RequiresFDB)
class IndexStateTest extends FDBRecordStoreTestBase {

    private static final String UNIQUE_INDEX = "MySimpleRecord$num_value_unique";
    private static final String STR_INDEX = "MySimpleRecord$str_value_indexed";
    private static final String NUM_3_INDEX = "MySimpleRecord$num_value_3_indexed";
    private static final String MULTI_TYPE_INDEX = "simple&other$num_value_2";
    private static final String UNIVERSAL_INDEX = COUNT_INDEX_NAME;
    private static final String NEW_UNIQUE_INDEX = "simple$num_value_2_unique";

    /**
     * The expected value of every predicate, per state. Deliberately spelled out rather than derived, so that a
     * change in {@link IndexState} has to be restated here.
     */
    static Stream<Arguments> predicateMatrix() {
        return Stream.of(
                //            state                                 readable uniquePending scannable writeOnlyNoQueue writeOnlyWithQueue writeOnly disabled
                Arguments.of(IndexState.READABLE,                   true,    false,        true,     false,           false,             false,    false),
                Arguments.of(IndexState.READABLE_UNIQUE_PENDING,    false,   true,         true,     false,           false,             false,    false),
                Arguments.of(IndexState.WRITE_ONLY,                 false,   false,        false,    true,            false,             true,     false),
                Arguments.of(IndexState.WRITE_ONLY_WITH_QUEUE,      false,   false,        false,    false,           true,              true,     false),
                Arguments.of(IndexState.DISABLED,                   false,   false,        false,    false,           false,             false,    true));
    }

    @ParameterizedTest
    @MethodSource("predicateMatrix")
    void predicatesMatchTheState(IndexState state, boolean readable, boolean uniquePending, boolean scannable,
                                 boolean writeOnlyNoQueue, boolean writeOnlyWithQueue, boolean writeOnly,
                                 boolean disabled) {
        assertThat(state.isReadable()).as("isReadable").isEqualTo(readable);
        assertThat(state.isReadableUniquePending()).as("isReadableUniquePending").isEqualTo(uniquePending);
        assertThat(state.isScannable()).as("isScannable").isEqualTo(scannable);
        assertThat(state.isWriteOnlyNoQueue()).as("isWriteOnlyNoQueue").isEqualTo(writeOnlyNoQueue);
        assertThat(state.isWriteOnlyWithQueue()).as("isWriteOnlyWithQueue").isEqualTo(writeOnlyWithQueue);
        assertThat(state.isWriteOnly()).as("isWriteOnly").isEqualTo(writeOnly);
        assertThat(state.isDisabled()).as("isDisabled").isEqualTo(disabled);
    }

    @Test
    void predicateMatrixCoversEveryState() {
        assertThat(predicateMatrix().map(args -> args.get()[0]))
                .containsExactlyInAnyOrder((Object[])IndexState.values());
    }

    /**
     * A new state must not slip in without its own predicate.
     */
    @ParameterizedTest
    @EnumSource(IndexState.class)
    void exactlyOnePrimaryPredicateHolds(IndexState state) {
        final List<Boolean> primaries = List.of(state.isReadable(), state.isReadableUniquePending(),
                state.isWriteOnlyNoQueue(), state.isWriteOnlyWithQueue(), state.isDisabled());
        assertThat(primaries.stream().filter(Boolean::booleanValue)).hasSize(1);
    }

    @ParameterizedTest
    @EnumSource(IndexState.class)
    void fromCodeRoundTrips(IndexState state) {
        assertThat(IndexState.fromCode(state.code())).isSameAs(state);
    }

    @Test
    void recordStoreStatePredicatesFollowTheState() {
        final RecordStoreState storeState = new RecordStoreState(null, ImmutableMap.of(
                STR_INDEX, IndexState.DISABLED,
                NUM_3_INDEX, IndexState.WRITE_ONLY_WITH_QUEUE,
                NEW_UNIQUE_INDEX, IndexState.READABLE_UNIQUE_PENDING));

        assertThat(storeState.isReadableUniquePending(NEW_UNIQUE_INDEX)).isTrue();
        assertThat(storeState.isScannable(NEW_UNIQUE_INDEX)).isTrue();
        assertThat(storeState.isReadable(NEW_UNIQUE_INDEX)).isFalse();
        assertThat(storeState.isWriteOnly(NUM_3_INDEX)).isTrue();
        assertThat(storeState.isDisabled(STR_INDEX)).isTrue();
        assertThat(storeState.isReadableUniquePending(STR_INDEX)).isFalse();
        // An index absent from the map defaults to READABLE.
        assertThat(storeState.isReadable(UNIQUE_INDEX)).isTrue();
        assertThat(storeState.isReadableUniquePending(UNIQUE_INDEX)).isFalse();
    }

    @Test
    void withIndexesInStateSetsNonReadableAndClearsReadable() {
        final RecordStoreState empty = new RecordStoreState(null, null);

        final RecordStoreState disabled = empty.withIndexesInState(List.of(STR_INDEX, NUM_3_INDEX),
                IndexState.DISABLED);
        assertThat(disabled.getIndexStates())
                .containsOnlyKeys(STR_INDEX, NUM_3_INDEX)
                .containsValue(IndexState.DISABLED);

        // READABLE is the default, so those entries are removed rather than stored.
        final RecordStoreState restored = disabled.withIndexesInState(List.of(STR_INDEX), IndexState.READABLE);
        assertThat(restored.getIndexStates()).containsOnlyKeys(NUM_3_INDEX);
        assertThat(restored.isReadable(STR_INDEX)).isTrue();
        assertThat(restored.isDisabled(NUM_3_INDEX)).isTrue();
    }

    @Test
    void storeIndexStateFollowsTheMarkedState() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.markIndexDisabled(STR_INDEX).join();
            recordStore.markIndexWriteOnlyWithQueue(NUM_3_INDEX).join();

            assertThat(recordStore.getIndexState(UNIQUE_INDEX).isReadable()).isTrue();
            assertThat(recordStore.getIndexState(STR_INDEX).isDisabled()).isTrue();
            assertThat(recordStore.getIndexState(NUM_3_INDEX).isWriteOnlyWithQueue()).isTrue();
            assertThat(recordStore.getIndexState(NUM_3_INDEX).isWriteOnly()).isTrue();
            assertThat(recordStore.getIndexState(NUM_3_INDEX).isWriteOnlyNoQueue()).isFalse();

            // Testing a deprecated function.
            assertThat(recordStore.isIndexReadable(UNIQUE_INDEX)).isTrue();
            assertThat(recordStore.isIndexReadable(index(UNIQUE_INDEX))).isTrue();
            assertThat(recordStore.isIndexReadable(STR_INDEX)).isFalse();
            // Testing a deprecated function.
            assertThat(recordStore.isIndexDisabled(STR_INDEX)).isTrue();
            assertThat(recordStore.isIndexDisabled(index(STR_INDEX))).isTrue();
            assertThat(recordStore.isIndexDisabled(UNIQUE_INDEX)).isFalse();
            // Testing a deprecated function.
            assertThat(recordStore.isIndexWriteOnlyWithQueue(NUM_3_INDEX)).isTrue();
            assertThat(recordStore.isIndexWriteOnlyWithQueue(index(NUM_3_INDEX))).isTrue();
            assertThat(recordStore.isIndexWriteOnlyWithQueue(UNIQUE_INDEX)).isFalse();
            // Testing a deprecated function.
            assertThat(recordStore.isIndexWriteOnly(NUM_3_INDEX)).isTrue();
            assertThat(recordStore.isIndexWriteOnlyNoQueue(NUM_3_INDEX)).isFalse();
            assertThat(recordStore.isIndexScannable(UNIQUE_INDEX)).isTrue();
            assertThat(recordStore.isIndexReadableUniquePending(UNIQUE_INDEX)).isFalse();
            commit(context);
        }
    }

    @Test
    void storeIndexStateReadableUniquePending() throws Exception {
        final Index uniqueIndex = new Index(NEW_UNIQUE_INDEX, field("num_value_2"), EmptyKeyExpression.EMPTY,
                IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        final RecordMetaDataHook hook = metaData -> metaData.addIndex("MySimpleRecord", uniqueIndex);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            // markIndexWriteOnly keeps the built range, so the index stays markable as readable later.
            recordStore.markIndexWriteOnly(uniqueIndex).join();
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L).setNumValue2(42).build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(2L).setNumValue2(42).build());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            assertThat(recordStore.markIndexReadableOrUniquePending(uniqueIndex).join()).isTrue();

            final IndexState state = recordStore.getIndexState(NEW_UNIQUE_INDEX);
            assertThat(state).isEqualTo(IndexState.READABLE_UNIQUE_PENDING);
            assertThat(state.isReadableUniquePending()).isTrue();
            assertThat(state.isScannable()).isTrue();
            assertThat(state.isReadable()).isFalse();
            assertThat(recordStore.getRecordStoreState().isReadableUniquePending(NEW_UNIQUE_INDEX)).isTrue();

            // Testing a deprecated function.
            assertThat(recordStore.isIndexReadableUniquePending(NEW_UNIQUE_INDEX)).isTrue();
            assertThat(recordStore.isIndexReadableUniquePending(uniqueIndex)).isTrue();
            assertThat(recordStore.isIndexScannable(NEW_UNIQUE_INDEX)).isTrue();
            assertThat(recordStore.isIndexReadable(NEW_UNIQUE_INDEX)).isFalse();

            // Not readable, so it is not among the readable indexes.
            assertThat(recordStore.getReadableIndexes(simpleRecordType())).doesNotContain(uniqueIndex);
            commit(context);
        }
    }

    @Test
    void readableIndexAccessorsExcludeNonReadableIndexes() throws Exception {
        final Index multiTypeIndex = new Index(MULTI_TYPE_INDEX, "num_value_2");
        // The simple meta-data already carries the universal count indexes.
        final RecordMetaDataHook hook = metaData -> metaData.addMultiTypeIndex(
                List.of(metaData.getRecordType("MySimpleRecord"), metaData.getRecordType("MyOtherRecord")),
                multiTypeIndex);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            // Without a non-readable index the accessors short-circuit and return everything.
            recordStore.markIndexDisabled(STR_INDEX).join();

            assertThat(recordStore.getReadableIndexes(simpleRecordType()))
                    .contains(index(UNIQUE_INDEX), index(NUM_3_INDEX))
                    .doesNotContain(index(STR_INDEX));
            assertThat(recordStore.getReadableMultiTypeIndexes(simpleRecordType())).contains(multiTypeIndex);
            assertThat(recordStore.getReadableUniversalIndexes()).contains(index(UNIVERSAL_INDEX));

            recordStore.markIndexDisabled(MULTI_TYPE_INDEX).join();
            recordStore.markIndexDisabled(UNIVERSAL_INDEX).join();
            assertThat(recordStore.getReadableMultiTypeIndexes(simpleRecordType())).doesNotContain(multiTypeIndex);
            assertThat(recordStore.getReadableUniversalIndexes()).doesNotContain(index(UNIVERSAL_INDEX));
            commit(context);
        }
    }

    @Nonnull
    private Index index(@Nonnull final String indexName) {
        return recordStore.getRecordMetaData().getIndex(indexName);
    }

    @Nonnull
    private RecordType simpleRecordType() {
        return recordStore.getRecordMetaData().getRecordType("MySimpleRecord");
    }
}
