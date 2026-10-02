/*
 * RankIndexEntryRankTest.java
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.RankScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Scanning a rank index through {@link RankScanBounds} reports each entry's rank in the entry's value, which a rank
 * index otherwise leaves empty.
 * <p>
 * Every test runs over the same seven records, {@code num_value_3_indexed} ranked within groups of {@code num_value_2}:
 * </p>
 * <pre>
 *   recNo:  1    2    3    4    5  |  6   7
 *   group: 10   10   10   10   10  | 20  20
 *   score: 100  200  200  300  400 | 50  60
 *   rank:    0    1    1    2    3 |  0   1
 * </pre>
 * <p>
 * Group 10 ties on score 200, so its ranks are dense over distinct scores rather than over records: both tied records
 * are rank 1 and the next distinct score is rank 2, not 3.
 * </p>
 */
@Tag(Tags.RequiresFDB)
class RankIndexEntryRankTest {

    private static final String RANK_INDEX = "rank_by_group";
    private static final int GROUP = 10;
    private static final int OTHER_GROUP = 20;

    /**
     * Every entry of the tied group, in score order, with the rank each one should report.
     */
    private static final List<Tuple> GROUP_KEYS = List.of(
            Tuple.from(GROUP, 100, 1L),
            Tuple.from(GROUP, 200, 2L),
            Tuple.from(GROUP, 200, 3L),
            Tuple.from(GROUP, 300, 4L),
            Tuple.from(GROUP, 400, 5L));
    private static final List<Tuple> GROUP_RANKS = List.of(
            Tuple.from(0L), Tuple.from(1L), Tuple.from(1L), Tuple.from(2L), Tuple.from(3L));

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase fdb;
    private KeySpacePath path;
    private FDBRecordStore recordStore;

    @BeforeEach
    void setUp() {
        fdb = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
    }

    /**
     * A rank index supports a scan by score and a scan by rank, and nothing else.
     */
    @Test
    void onlyByValueAndByRankAreAcceptedAsScanTypes() {
        assertEquals(IndexScanType.BY_RANK, byRank(TupleRange.ALL).getScanType());
        assertEquals(IndexScanType.BY_VALUE, byValue(TupleRange.ALL).getScanType());
        for (final IndexScanType scanType : List.of(IndexScanType.BY_GROUP, IndexScanType.BY_DISTANCE)) {
            assertThrows(RecordCoreArgumentException.class,
                    () -> new RankScanBounds(scanType, TupleRange.ALL, true),
                    scanType + " should not be accepted by a rank index");
        }
    }

    /**
     * Scanning a whole group by rank labels every entry with its rank, leaving the keys as they were.
     */
    @Test
    void byRankReportsTheRankInTheEntryValue() {
        withRecords(() -> {
            final List<IndexEntry> entries = scan(byRank(TupleRange.allOf(Tuple.from(GROUP))));
            assertEquals(GROUP_KEYS, keysOf(entries));
            assertEquals(GROUP_RANKS, valuesOf(entries));
        });
    }

    /**
     * The rank is derived from the entry rather than from the bounds, so a by-score traversal reports it just as a
     * by-rank traversal does.
     */
    @Test
    void byValueReportsTheRankInTheEntryValue() {
        withRecords(() -> {
            final List<IndexEntry> entries = scan(byValue(TupleRange.allOf(Tuple.from(GROUP))));
            assertEquals(GROUP_KEYS, keysOf(entries));
            assertEquals(GROUP_RANKS, valuesOf(entries));
        });
    }

    /**
     * Traversing a group by rank yields exactly what traversing it by score yields, ranks included. The two ranges are
     * expressed differently but neither the entries nor their ranks distinguish them.
     */
    @Test
    void byRankAndByValueYieldTheSameEntries() {
        withRecords(() -> {
            final List<IndexEntry> byRank = scan(byRank(TupleRange.allOf(Tuple.from(GROUP))));
            final List<IndexEntry> byValue = scan(byValue(TupleRange.allOf(Tuple.from(GROUP))));
            assertEquals(keysOf(byValue), keysOf(byRank));
            assertEquals(valuesOf(byValue), valuesOf(byRank));
        });
    }

    /**
     * Records tied on a score share a rank, so asking for a single rank can return more than one entry.
     */
    @Test
    void tiedScoresShareARank() {
        withRecords(() -> {
            final List<IndexEntry> entries = scan(byRank(rank(1)));
            assertEquals(List.of(Tuple.from(GROUP, 200, 2L), Tuple.from(GROUP, 200, 3L)), keysOf(entries));
            assertEquals(List.of(Tuple.from(1L), Tuple.from(1L)), valuesOf(entries));
        });
    }

    /**
     * Ranks count distinct scores rather than records, so the score following a tie takes the very next rank and no rank
     * is skipped over.
     */
    @Test
    void theScoreAfterATieTakesTheNextRank() {
        withRecords(() -> {
            final List<IndexEntry> entries = scan(byRank(rank(2)));
            assertEquals(List.of(Tuple.from(GROUP, 300, 4L)), keysOf(entries));
            assertEquals(List.of(Tuple.from(2L)), valuesOf(entries));
        });
    }

    /**
     * A rank range still filters as it did, and the ranks reported are absolute ranks within the group rather than
     * positions within the returned window.
     */
    @Test
    void boundedRangeReportsAbsoluteRanksNotWindowPositions() {
        withRecords(() -> {
            final List<IndexEntry> entries = scan(byRank(
                    TupleRange.betweenInclusive(Tuple.from(GROUP, 2), Tuple.from(GROUP, 3))));
            assertEquals(List.of(Tuple.from(GROUP, 300, 4L), Tuple.from(GROUP, 400, 5L)), keysOf(entries));
            assertEquals(List.of(Tuple.from(2L), Tuple.from(3L)), valuesOf(entries));
        });
    }

    /**
     * A range that spans a tie returns every entry tied within it, so it can hold more entries than ranks.
     */
    @Test
    void rangeSpanningATieReturnsEveryTiedEntry() {
        withRecords(() -> {
            final List<IndexEntry> entries = scan(byRank(
                    TupleRange.betweenInclusive(Tuple.from(GROUP, 1), Tuple.from(GROUP, 2))));
            assertEquals(List.of(Tuple.from(GROUP, 200, 2L), Tuple.from(GROUP, 200, 3L),
                    Tuple.from(GROUP, 300, 4L)), keysOf(entries));
            assertEquals(List.of(Tuple.from(1L), Tuple.from(1L), Tuple.from(2L)), valuesOf(entries));
        });
    }

    /**
     * There are four distinct scores in the tied group, so rank 4 is past its end.
     */
    @Test
    void rankPastTheEndOfTheGroupYieldsNoEntries() {
        withRecords(() -> assertEquals(List.of(), scan(byRank(rank(4)))));
    }

    /**
     * Each group is ranked separately, so a scan across the whole index restarts the ranks at each group rather than
     * numbering entries as it goes.
     */
    @Test
    void ranksAreCountedWithinEachGroupSeparately() {
        withRecords(() -> {
            final List<IndexEntry> entries = scan(byValue(TupleRange.ALL));
            assertEquals(List.of(Tuple.from(GROUP, 100, 1L), Tuple.from(GROUP, 200, 2L),
                    Tuple.from(GROUP, 200, 3L), Tuple.from(GROUP, 300, 4L), Tuple.from(GROUP, 400, 5L),
                    Tuple.from(OTHER_GROUP, 50, 6L), Tuple.from(OTHER_GROUP, 60, 7L)), keysOf(entries));
            assertEquals(List.of(Tuple.from(0L), Tuple.from(1L), Tuple.from(1L), Tuple.from(2L), Tuple.from(3L),
                    Tuple.from(0L), Tuple.from(1L)), valuesOf(entries));
        });
    }

    /**
     * The rank reported for an entry is the rank the record function computes for the same record, for every record in
     * both groups. That is the property that makes the reported value trustworthy.
     */
    @Test
    void reportedRanksAgreeWithTheRecordFunction() {
        withRecords(() -> {
            for (final int group : new int[] {GROUP, OTHER_GROUP}) {
                for (final IndexEntry entry : scan(byRank(TupleRange.allOf(Tuple.from(group))))) {
                    final long recNo = entry.getKey().getLong(entry.getKey().size() - 1);
                    assertEquals(rankOf(recNo), entry.getValue().getLong(0),
                            "entry " + entry.getKey() + " reported the wrong rank");
                }
            }
        });
    }

    /**
     * Reporting a rank costs a ranked-set lookup, so it is only computed when it was asked for.
     */
    @Test
    void withoutTheOptionTheValueStaysEmpty() {
        withRecords(() -> {
            final List<IndexEntry> entries = scan(new RankScanBounds(IndexScanType.BY_RANK,
                    TupleRange.allOf(Tuple.from(GROUP)), false));
            assertEquals(GROUP_KEYS, keysOf(entries));
            assertEquals(Collections.nCopies(GROUP_KEYS.size(), Tuple.from()), valuesOf(entries));
        });
    }

    @Nonnull
    private static RecordMetaData metaData() {
        final RecordMetaDataBuilder metaDataBuilder =
                RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", new Index(RANK_INDEX,
                field("num_value_3_indexed").groupBy(field("num_value_2")),
                IndexTypes.RANK));
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RankScanBounds byRank(@Nonnull final TupleRange rankRange) {
        return new RankScanBounds(IndexScanType.BY_RANK, rankRange, true);
    }

    @Nonnull
    private static RankScanBounds byValue(@Nonnull final TupleRange scoreRange) {
        return new RankScanBounds(IndexScanType.BY_VALUE, scoreRange, true);
    }

    /**
     * The range selecting exactly one rank of the tied group.
     */
    @Nonnull
    private static TupleRange rank(final int rank) {
        return TupleRange.betweenInclusive(Tuple.from(GROUP, rank), Tuple.from(GROUP, rank));
    }

    private void withRecords(@Nonnull final Runnable body) {
        try (FDBRecordContext context = fdb.openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setMetaDataProvider(metaData())
                    .setContext(context)
                    .setKeySpacePath(path)
                    .createOrOpen();
            saveRecord(1, GROUP, 100);
            saveRecord(2, GROUP, 200);
            saveRecord(3, GROUP, 200);
            saveRecord(4, GROUP, 300);
            saveRecord(5, GROUP, 400);
            saveRecord(6, OTHER_GROUP, 50);
            saveRecord(7, OTHER_GROUP, 60);
            body.run();
            context.commit();
        }
    }

    @Nonnull
    private List<IndexEntry> scan(@Nonnull final IndexScanBounds scanBounds) {
        return recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(RANK_INDEX), scanBounds, null,
                ScanProperties.FORWARD_SCAN).asList().join();
    }

    @Nonnull
    private static List<Tuple> keysOf(@Nonnull final List<IndexEntry> entries) {
        return entries.stream().map(IndexEntry::getKey).collect(Collectors.toList());
    }

    @Nonnull
    private static List<Tuple> valuesOf(@Nonnull final List<IndexEntry> entries) {
        return entries.stream().map(IndexEntry::getValue).collect(Collectors.toList());
    }

    private long rankOf(final long recNo) {
        final var rankFunction = Query.rank(field("num_value_3_indexed").groupBy(field("num_value_2"))).getFunction();
        return recordStore.evaluateRecordFunction(rankFunction, recordStore.loadRecord(Tuple.from(recNo))).join();
    }

    private void saveRecord(final long recNo, final int group, final int score) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValueUnique((int)recNo)
                .setNumValue2(group)
                .setNumValue3Indexed(score)
                .build());
    }
}
