/*
 * IndexEntryStorageLayoutTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the raw stored key/value layout of one index of each interesting type.
 * <p>
 * The point is to establish, by observation rather than by reading maintainer code, which parts of a
 * {@link com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression} end up in an entry's key, which end
 * up in its value, and which are not stored as decodable columns at all. Every index below is defined over the same
 * record so the entries can be compared directly.
 * </p>
 */
@Tag(Tags.RequiresFDB)
class IndexEntryStorageLayoutTest extends FDBRecordStoreTestBase {

    private static final String VALUE_INDEX = "layout_value";
    private static final String AGGREGATE_INDEX = "layout_max_ever";
    private static final String PERMUTED_INDEX = "layout_permuted_max";
    private static final String RANK_INDEX = "layout_rank";

    /** The single record every index below indexes: numValue2=10, numValue3Indexed=20, numValueUnique=100, recNo=1. */
    private static final int GROUP = 10;
    private static final int SECOND_GROUP = 20;
    private static final int AGGREGATED = 100;
    private static final long PRIMARY_KEY = 1L;

    @Nonnull
    private static RecordMetaDataHook hook() {
        return metaData -> {
            metaData.addIndex("MySimpleRecord", new Index(VALUE_INDEX,
                    Key.Expressions.keyWithValue(
                            concatenateFields("num_value_2", "num_value_3_indexed", "str_value_indexed"), 2),
                    IndexTypes.VALUE));
            metaData.addIndex("MySimpleRecord", new Index(AGGREGATE_INDEX,
                    field("num_value_unique").groupBy(concatenateFields("num_value_2", "num_value_3_indexed")),
                    IndexTypes.MAX_EVER_LONG));
            metaData.addIndex("MySimpleRecord", new Index(PERMUTED_INDEX,
                    field("num_value_unique").groupBy(concatenateFields("num_value_2", "num_value_3_indexed")),
                    IndexTypes.PERMUTED_MAX, Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "1")));
            metaData.addIndex("MySimpleRecord", new Index(RANK_INDEX,
                    field("num_value_unique").groupBy(field("num_value_2")),
                    IndexTypes.RANK));
        };
    }

    /**
     * A value index stores its key columns followed by the primary key, and its value columns in the entry's value.
     */
    @Test
    void valueIndexSplitsKeyColumnsFromValueColumns() {
        withOneRecord(context -> {
            assertThat(primaryEntries(context, VALUE_INDEX))
                    .containsExactly(Tuple.from(GROUP, SECOND_GROUP, PRIMARY_KEY));
            assertThat(primaryValues(context, VALUE_INDEX))
                    .containsExactly(Tuple.from("s"));
            assertThat(secondaryEntries(context, VALUE_INDEX)).isEmpty();
        });
    }

    /**
     * An atomic-mutation aggregate index stores only the grouping columns in the key -- no primary key, since the entry
     * stands for a group rather than a record -- and the aggregate in the value, packed as a little-endian long rather
     * than as a tuple.
     */
    @Test
    void aggregateIndexStoresGroupingInKeyAndAggregateInValue() {
        withOneRecord(context -> {
            assertThat(primaryEntries(context, AGGREGATE_INDEX))
                    .containsExactly(Tuple.from(GROUP, SECOND_GROUP));

            final var value = rawPrimaryValues(context, AGGREGATE_INDEX);
            assertThat(value).hasSize(1);
            assertThat(value.get(0)).hasSize(Long.BYTES);
            assertThat(ByteBuffer.wrap(value.get(0)).order(ByteOrder.LITTLE_ENDIAN).getLong())
                    .isEqualTo(AGGREGATED);

            assertThat(secondaryEntries(context, AGGREGATE_INDEX)).isEmpty();
        });
    }

    /**
     * A permuted extremum index keeps a per-record entry in its primary subspace, and puts the permuted ordering --
     * grouping prefix, then the extremum, then the {@code permutedSize} grouping columns that follow it -- in its
     * secondary subspace. Both carry an empty value.
     */
    @Test
    void permutedAggregateIndexKeepsThePermutedOrderInItsSecondarySubspace() {
        withOneRecord(context -> {
            assertThat(primaryEntries(context, PERMUTED_INDEX))
                    .containsExactly(Tuple.from(GROUP, SECOND_GROUP, AGGREGATED, PRIMARY_KEY));
            assertThat(secondaryEntries(context, PERMUTED_INDEX))
                    .containsExactly(Tuple.from(GROUP, AGGREGATED, SECOND_GROUP));
            assertThat(rawPrimaryValues(context, PERMUTED_INDEX)).allMatch(value -> value.length == 0);
        });
    }

    /**
     * A rank index stores the whole key -- grouping columns and the ranked column alike -- plus the primary key, all in
     * the entry's key, with an empty value. The rank itself is in neither: it lives in the ranked set held in the
     * index's secondary subspace, so it has to be computed rather than decoded from an entry.
     */
    @Test
    void rankIndexStoresTheWholeKeyAndKeepsTheRankOutOfTheEntry() {
        withOneRecord(context -> {
            assertThat(primaryEntries(context, RANK_INDEX))
                    .containsExactly(Tuple.from(GROUP, AGGREGATED, PRIMARY_KEY));
            assertThat(rawPrimaryValues(context, RANK_INDEX)).allMatch(value -> value.length == 0);

            //
            // The secondary subspace holds the ranked set, a skip list keyed by (group, level, score). Its exact
            // contents depend on the level count and hash function, so only its shape is pinned here: it is non-empty,
            // and every key is scoped to the group.
            //
            final var rankedSet = secondaryEntries(context, RANK_INDEX);
            assertThat(rankedSet).isNotEmpty();
            assertThat(rankedSet).allMatch(key -> key.getLong(0) == GROUP);
        });
    }



    // ---------------------------------------------------------------------------------------------------------------
    // What a scan of each index yields, that is what an IndexEntry looks like to a plan such as
    // RecordQueryIndexPlan. Note this is not the same as the stored layout above: the maintainer may decode the
    // stored value, and for some index types the scan type selects which subspace is read.
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * A value index scan yields the key columns, the primary key, and the value columns in the entry's value.
     */
    @Test
    void valueIndexScanYieldsKeyColumnsAndValueColumns() {
        withOneRecord(context -> {
            final var entry = onlyEntry(VALUE_INDEX, IndexScanType.BY_VALUE, TupleRange.ALL);
            assertEquals(Tuple.from(GROUP, SECOND_GROUP, PRIMARY_KEY), entry.getKey());
            assertEquals(Tuple.from("s"), entry.getValue());
            assertEquals(Tuple.from(PRIMARY_KEY), entry.getPrimaryKey());
        });
    }

    /**
     * An aggregate index can only be scanned by group, and the maintainer decodes the packed aggregate into the entry's
     * value, so the entry's value is a tuple even though the stored bytes are not. The entry has no primary key, since
     * it stands for a group.
     */
    @Test
    void aggregateIndexScanYieldsGroupAndDecodedAggregate() {
        withOneRecord(context -> {
            final var entry = onlyEntry(AGGREGATE_INDEX, IndexScanType.BY_GROUP, TupleRange.ALL);
            assertEquals(Tuple.from(GROUP, SECOND_GROUP), entry.getKey());
            assertEquals(Tuple.from(AGGREGATED), entry.getValue());
            assertThatThrownBy(entry::getPrimaryKey).isInstanceOf(IllegalArgumentException.class);

            assertThatThrownBy(() -> onlyEntry(AGGREGATE_INDEX, IndexScanType.BY_VALUE, TupleRange.ALL))
                    .hasMessageContaining("Can only scan aggregate index by group");
        });
    }

    /**
     * For a permuted extremum index the scan type selects which subspace is read: {@code BY_GROUP} reads the permuted
     * secondary entries, {@code BY_VALUE} reads the per-record primary entries.
     */
    @Test
    void permutedIndexScanTypeSelectsWhichSubspaceIsRead() {
        withOneRecord(context -> {
            assertEquals(Tuple.from(GROUP, AGGREGATED, SECOND_GROUP), onlyEntry(PERMUTED_INDEX, IndexScanType.BY_GROUP, TupleRange.ALL).getKey());
            assertEquals(Tuple.from(GROUP, SECOND_GROUP, AGGREGATED, PRIMARY_KEY), onlyEntry(PERMUTED_INDEX, IndexScanType.BY_VALUE, TupleRange.ALL).getKey());
        });
    }

    /**
     * The point of the whole exercise: a {@code BY_RANK} scan yields exactly the same entry as a {@code BY_VALUE} scan.
     * The rank appears nowhere in it -- the rank range is only used to derive a score range -- so a plan reading these
     * entries cannot decode a rank out of them. A {@code BY_RANK} range must also include the group.
     */
    @Test
    void rankIndexScanByRankYieldsNoRankInTheEntry() {
        withOneRecord(context -> {
            final var byRank = onlyEntry(RANK_INDEX, IndexScanType.BY_RANK, TupleRange.allOf(Tuple.from(GROUP)));
            assertEquals(Tuple.from(GROUP, AGGREGATED, PRIMARY_KEY), byRank.getKey());
            assertEquals(Tuple.from(), byRank.getValue());
            assertEquals(Tuple.from(PRIMARY_KEY), byRank.getPrimaryKey());

            // identical to a by-value scan: the rank is not part of the entry
            final var byValue = onlyEntry(RANK_INDEX, IndexScanType.BY_VALUE, TupleRange.ALL);
            assertEquals(byValue.getKey(), byRank.getKey());
            assertEquals(byValue.getValue(), byRank.getValue());

            assertThatThrownBy(() -> onlyEntry(RANK_INDEX, IndexScanType.BY_RANK, TupleRange.ALL))
                    .hasMessageContaining("Ranked scan range does not include group");
        });
    }

    @Nonnull
    private IndexEntry onlyEntry(@Nonnull final String indexName,
                                 @Nonnull final IndexScanType scanType,
                                 @Nonnull final TupleRange range) {
        final var index = recordStore.getRecordMetaData().getIndex(indexName);
        final var entries = recordStore.scanIndex(index, scanType, range, null, ScanProperties.FORWARD_SCAN)
                .asList().join();
        assertThat(entries).hasSize(1);
        return entries.get(0);
    }

    private void withOneRecord(@Nonnull final java.util.function.Consumer<FDBRecordContext> body) {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(PRIMARY_KEY)
                    .setStrValueIndexed("s")
                    .setNumValue2(GROUP)
                    .setNumValue3Indexed(SECOND_GROUP)
                    .setNumValueUnique(AGGREGATED)
                    .build());
            body.accept(context);
            commit(context);
        }
    }

    @Nonnull
    private List<Tuple> primaryEntries(@Nonnull final FDBRecordContext context, @Nonnull final String indexName) {
        return keys(context, recordStore.indexSubspace(recordStore.getRecordMetaData().getIndex(indexName)));
    }

    @Nonnull
    private List<Tuple> secondaryEntries(@Nonnull final FDBRecordContext context, @Nonnull final String indexName) {
        return keys(context, recordStore.indexSecondarySubspace(recordStore.getRecordMetaData().getIndex(indexName)));
    }

    @Nonnull
    private List<Tuple> primaryValues(@Nonnull final FDBRecordContext context, @Nonnull final String indexName) {
        return rawPrimaryValues(context, indexName).stream()
                .map(Tuple::fromBytes)
                .collect(Collectors.toList());
    }

    @Nonnull
    private List<byte[]> rawPrimaryValues(@Nonnull final FDBRecordContext context, @Nonnull final String indexName) {
        final var subspace = recordStore.indexSubspace(recordStore.getRecordMetaData().getIndex(indexName));
        return context.ensureActive().getRange(subspace.range()).asList().join().stream()
                .map(KeyValue::getValue)
                .collect(Collectors.toList());
    }

    @Nonnull
    private static List<Tuple> keys(@Nonnull final FDBRecordContext context, @Nonnull final Subspace subspace) {
        return context.ensureActive().getRange(subspace.range()).asList().join().stream()
                .map(keyValue -> subspace.unpack(keyValue.getKey()))
                .collect(Collectors.toList());
    }
}
