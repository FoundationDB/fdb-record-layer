/*
 * RangeSetTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.MultipleTransactions;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.tuple.ByteArrayUtil.compareUnsigned;
import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RangeSet}.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
public class RangeSetTest {
    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension rsSubspaceExtension = new TestSubspaceExtension(dbExtension);
    private Database db;
    private Subspace rsSubspace;
    private RangeSet rs;

    private static final byte[] DEADC0DE = new byte[]{(byte)0xde, (byte)0xad, (byte)0xc0, (byte)0xde};

    private List<byte[]> createKeys() {
        List<byte[]> keys = new ArrayList<>();
        for (int i = 0x00; i < 0xff; i++) {
            keys.add(new byte[]{(byte) i});
            keys.add(new byte[]{(byte) i, 0x00});
            keys.add(new byte[]{(byte) i, 0x10});
            keys.add(new byte[]{(byte) i, (byte)0xff});
        }
        return keys;
    }

    private int checkConsistent(@Nonnull List<Range> ranges, @Nonnull List<byte[]> keys) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        int present = 0;
        for (byte[] key : keys) {
            boolean expected = ranges.stream().anyMatch(range ->
                    ByteArrayUtil.compareUnsigned(range.begin, key) <= 0 && ByteArrayUtil.compareUnsigned(key, range.end) < 0
            );
            futures.add(rs.contains(db, key).thenAccept(contained ->
                    assertEquals(expected, contained,
                            "Byte array " + ByteArrayUtil.printable(key) + " did not match with ranges " + ranges)
            ));

            present += (expected ? 1 : 0);
        }
        AsyncUtil.getAll(futures).join();
        return present;
    }

    private boolean disjoint(@Nonnull Range r1, @Nonnull Range r2) {
        return ByteArrayUtil.compareUnsigned(r1.end, r2.begin) <= 0 || ByteArrayUtil.compareUnsigned(r2.end, r1.begin) <= 0;
    }

    private void checkIncreasing() {
        List<KeyValue> kvs = db.readAsync(tr -> tr.getRange(rsSubspace.range()).asList()).join();
        byte[] last = null;
        for (KeyValue kv : kvs) {
            byte[] key = rsSubspace.unpack(kv.getKey()).getBytes(0);
            assertTrue(compareUnsigned(key, kv.getValue()) < 0, "Key " + printable(key) + " is not less than value " + printable(kv.getValue()));
            if (last != null) {
                assertTrue(compareUnsigned(last, key) <= 0, "Last value " + printable(last) + " is after key " + printable(key));
            }
            last = kv.getValue();
        }
    }

    @BeforeEach
    void setUp() {
        db = dbExtension.getDatabase();
        rsSubspace = rsSubspaceExtension.getSubspace();
        rs = new RangeSet(rsSubspace);
        rs.clear(db).join();
    }

    @Test
    void clear() {
        db.run(tr -> {
            tr.set(rsSubspace.pack(new byte[]{(byte)0xde, (byte)0xad}), new byte[]{(byte)0xc0, (byte)0xde});
            return null;
        });
        assertEquals(1, db.readAsync(tr -> tr.getRange(rsSubspace.range()).asList()).join().size(), "Key does not appear to be added");
        assertFalse(rs.isEmpty(db).join());
        rs.clear(db).join();
        assertTrue(db.readAsync(tr -> tr.getRange(rsSubspace.range()).asList()).join().isEmpty(), "Clear did not remove key");
        assertTrue(rs.isEmpty(db).join());
    }

    @Test
    void contains() {
        // Insert a few ranges manually.
        db.run(tr -> {
            tr.set(rsSubspace.pack(new byte[]{(byte)0x10}), new byte[]{(byte)0x66});
            tr.set(rsSubspace.pack(new byte[]{(byte)0x77}), new byte[]{(byte)0x88});
            tr.set(rsSubspace.pack(new byte[]{(byte)0x88}), new byte[]{(byte)0x99});
            tr.set(rsSubspace.pack(new byte[]{(byte)0x99}), new byte[]{(byte)0x99, (byte)0x00});
            return null;
        });

        // Before range.
        assertFalse(rs.contains(db, new byte[]{(byte) 0x05}).join(), "Key \\x05 inside when shouldn't be");

        // Within range.
        assertTrue(rs.contains(db, new byte[]{(byte) 0x10}).join(), "Key \\x10 outside when shouldn't be");
        assertTrue(rs.contains(db, new byte[]{(byte) 0x10, (byte) 0x66}).join(), "Key \\x10\\x66 outside when shouldn't be");
        assertTrue(rs.contains(db, new byte[]{(byte) 0x55}).join(), "Key \\x55 outside when shouldn't be");

        // After range.
        assertFalse(rs.contains(db, new byte[]{(byte) 0x66}).join(), "Key \\x66 inside when shouldn't be");
        assertFalse(rs.contains(db, new byte[]{(byte) 0x66, (byte) 0x00}).join(), "Key \\x66\\x00 inside when shouldn't be");
        assertFalse(rs.contains(db, new byte[]{(byte) 0x70, (byte) 0x00}).join(), "Key \\x70\\x00 inside when shouldn't be");

        // Within second and third ranges.
        assertTrue(rs.contains(db, new byte[]{(byte) 0x77}).join(), "Key \\x77 outside when shouldn't be");
        assertTrue(rs.contains(db, new byte[]{(byte) 0x77, (byte) 0x00}).join(), "Key \\x77\\x00 outside when shouldn't be");
        assertTrue(rs.contains(db, new byte[]{(byte) 0x79, (byte) 0x00}).join(), "Key \\x79\\x00 outside when shouldn't be");
        assertTrue(rs.contains(db, new byte[]{(byte) 0x88}).join(), "Key \\x88 outside when shouldn't be");
        assertTrue(rs.contains(db, new byte[]{(byte) 0x99}).join(), "Key \\x99 outside when shouldn't be");

        // Outside last set of ranges.
        assertFalse(rs.contains(db, new byte[]{(byte) 0x99, (byte) 0x00}).join(), "Key \\x99\\x00 inside when shouldn't be");
        assertFalse(rs.contains(db, new byte[]{(byte) 0xaa}).join(), "Key \\xaa inside when shouldn't be");
    }

    @Test
    void containsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> rs.contains(db, new byte[0]).join());
    }

    @Test
    void containsPastFF1() {
        assertThrows(IllegalArgumentException.class, () -> rs.contains(db, new byte[]{(byte)0xff}).join());
    }

    @Test
    void containsPastFF2() {
        assertThrows(IllegalArgumentException.class, () -> rs.contains(db, new byte[]{(byte)0xff, (byte)0x00}).join());
    }

    @Test
    @Tag(Tags.Slow)
    void insert() {
        List<byte[]> keys = createKeys();
        List<Range> rangeSource = Arrays.asList(
                Range.startsWith(new byte[]{0x10}), // Step 1: An initial range
                Range.startsWith(new byte[]{0x30}), // Step 2: An second disjoint range.
                Range.startsWith(new byte[]{0x20}), // Step 3: A third disjoint range between them.
                new Range(new byte[]{0x05}, new byte[]{0x10, 0x10}), // Step 4: A range overlapping the first range.
                new Range(new byte[]{0x2f}, new byte[]{0x30, 0x10}), // Step 5: A range overlapping the third range.
                new Range(new byte[]{0x30, 0x10}, new byte[]{0x40}), // Step 6: A range overlapping the third range.
                new Range(new byte[]{0x05, 0x10}, new byte[]{0x12}), // Step 7: A range with just a little at the end.
                new Range(new byte[]{0x20, 0x14}, new byte[]{0x30, 0x00}), // Step 8: A range that goes between 0x20 and 0x30 ranges.
                new Range(new byte[]{0x03}, new byte[]{0x42}), // Step 9: A range that goes over whole range.
                new Range(new byte[]{0x10, 0x11}, new byte[]{0x41}), // Step 10: A range entirely within the given ranges.
                new Range(new byte[]{0x50}, new byte[]{0x50, 0x00}) // Step 11: A range that contains only 1 key.
        );
        List<Range> ranges = new ArrayList<>();
        int last = 0;

        // First, we'll go through and add all of the ranges. Then clear, and then do it again where we also
        // check for emptiness.
        for (int i = 0; i < 2; i++) {
            for (Range range : rangeSource) {
                boolean empty = ranges.stream().allMatch(r -> disjoint(range, r));
                boolean changes = rs.insertRange(db, range, i == 1).join();
                if (i == 1) {
                    assertEquals(empty, changes, "Changes even though this was non-empty");
                    if (!empty) {
                        changes = rs.insertRange(db, range).join();
                    }
                }
                ranges.add(range);
                checkIncreasing();
                int present = checkConsistent(ranges, keys);
                // This check works only because each range added adds at least one key from keys.
                assertEquals( present != last, changes,
                        "Keys changed even though ranges were supposedly static: present = " +  present
                                + ", last = " + last + ", changes = " + changes);
                last = present;
            }

            last = 0;
            rs.clear(db).join();
            ranges.clear();
        }
    }

    @Test
    void insertEmpty() {
        assertThrows(IllegalArgumentException.class, () -> rs.insertRange(db, new byte[0], new byte[]{0x00}));
    }

    @Test
    void insertInverted() {
        assertThrows(IllegalArgumentException.class, () -> rs.insertRange(db, new byte[]{0x15}, new byte[]{0x05}));
    }

    @Test
    void insertPastEnd() {
        assertThrows(IllegalArgumentException.class, () -> rs.insertRange(db, new byte[]{(byte)0xff, (byte)0x10}, new byte[]{(byte)0xff, (byte)0x15}));
    }

    @Test
    void readAndEditNonOverlappingRanges() {
        try (MultipleTransactions multi = MultipleTransactions.create(db, 3)) {
            Range r1 = new Range(new byte[]{0x00}, new byte[]{0x01});
            Range r2 = new Range(new byte[]{0x02}, new byte[]{0x03});
            Range r3 = new Range(new byte[]{0x04}, new byte[]{0x05});

            // Insert r2
            Transaction tr1 = multi.get(0);
            assertTrue(rs.insertRange(tr1, r2, true).join());

            // In separate transactions, read the ranges r1 and r3. One of them
            // is before r2 and the other other after
            Transaction tr2 = multi.get(1);
            assertEquals(List.of(r1), rs.missingRanges(tr2, r1).asList().join());
            Transaction tr3 = multi.get(2);
            assertEquals(List.of(r3), rs.missingRanges(tr3, r3).asList().join());

            // As all three transactions are on non-overlapping ranges, all three should
            // commit
            multi.commit();
        }
    }

    @Test
    void concurrentlyExpandReadRange() {
        Range r1 = new Range(new byte[]{0x02}, new byte[]{0x03});
        rs.insertRange(db, r1).join();

        try (MultipleTransactions multi = MultipleTransactions.create(db, 18)) {
            int currTr = 0;
            Transaction tr1 = multi.get(currTr++);
            assertTrue(rs.insertRange(tr1, new Range(new byte[]{0x03}, new byte[]{0x04}), true).join());

            // Should succeed
            assertTrue(rs.contains(multi.get(currTr++), new byte[]{0x02, 0x01}).join());
            assertFalse(rs.contains(multi.get(currTr++), new byte[]{0x01, 0x01}).join());
            assertFalse(rs.contains(multi.get(currTr++), new byte[]{0x04}).join());
            assertEquals(0, rs.missingRanges(multi.get(currTr++), new Range(new byte[]{0x02, 0x01}, new byte[]{0x02, 0x02}))
                    .asList().join().size());
            assertEquals(1, rs.missingRanges(multi.get(currTr++), new Range(new byte[]{0x01, 0x01}, new byte[]{0x01, 0x02}))
                    .asList().join().size());
            assertEquals(1, rs.missingRanges(multi.get(currTr++), new Range(new byte[]{0x01, 0x01}, new byte[]{0x02}))
                    .asList().join().size());
            assertEquals(1, rs.missingRanges(multi.get(currTr++), new Range(new byte[]{0x04}, new byte[]{0x05}))
                    .asList().join().size());
            assertEquals(2, rs.missingRanges(multi.get(currTr++).snapshot()) // Should succeed if read at snapshot
                    .asList().join().size());
            assertFalse(rs.isEmpty(multi.get(currTr++).snapshot()).join());

            // Should fail
            final int failAfter = currTr;
            assertFalse(rs.contains(multi.get(currTr++), new byte[]{0x03}).join());
            assertFalse(rs.contains(multi.get(currTr++), new byte[]{0x03, 0x01}).join());
            assertTrue(rs.contains(multi.get(currTr++), new byte[]{0x02}).join()); // fails because the key 0x02 itself is updated
            assertEquals(2, rs.missingRanges(multi.get(currTr++), new Range(new byte[]{0x01}, new byte[]{0x05}))
                    .asList().join().size());
            assertEquals(1, rs.missingRanges(multi.get(currTr++), new Range(new byte[]{0x01}, new byte[]{0x02, 0x01}))
                    .asList().join().size());
            assertEquals(0, rs.missingRanges(multi.get(currTr++), r1)
                    .asList().join().size());
            assertEquals(1, rs.missingRanges(multi.get(currTr++), new Range(new byte[]{0x03, 0x01}, new byte[]{0x04, 0x01}))
                    .asList().join().size());
            assertFalse(rs.isEmpty(multi.get(currTr++)).join());

            assertEquals(multi.size(), currTr);
            multi.commit(IntStream.range(0, multi.size()).filter(i -> i >= failAfter).boxed().collect(Collectors.toSet()));
        }
    }

    @Test
    void failIfKeyInRangeIsInsertedConcurrently() {
        try (MultipleTransactions multi = MultipleTransactions.create(db, 2)) {
            Range r = new Range(new byte[]{0x01}, new byte[]{0x02});
            Range overlappingRange = new Range(new byte[]{0x00, 0x01}, new byte[]{0x01, 0x01});

            // Insert a range into the set
            Transaction tr1 = multi.get(0);
            assertTrue(rs.insertRange(tr1, overlappingRange, true).join());

            // Read from a range that overlaps this range
            Transaction tr2 = multi.get(1);
            assertEquals(List.of(r), rs.missingRanges(tr2, r).asList().join());

            // The second transaction should fail to commit
            multi.commit(Set.of(1));
        }
    }

    @Test
    void insertNonOverlappingRanges() {
        final int transactionCount = 10;
        try (MultipleTransactions multi = MultipleTransactions.create(db, transactionCount)) {
            for (int i = 0; i < multi.size(); i++) {
                Transaction tr = multi.get(i);
                assertTrue(rs.insertRange(tr, new Range(new byte[]{(byte)(i + 1)}, new byte[]{(byte)(i + 1), 0x00}), i % 2 == 0).join());
            }

            // All transactions should be able to commit
            multi.commit();
        }
        Range fullRange = new Range(new byte[]{1}, new byte[]{(byte) (transactionCount + 1)});
        List<Range> gaps = IntStream.range(0, transactionCount)
                .mapToObj(i -> new Range(new byte[]{(byte)(i + 1), 0x00}, new byte[]{(byte)(i + 2)}))
                .toList();
        assertGaps(fullRange, gaps, new byte[]{(byte)(transactionCount), 0x00});
    }

    @Test
    void insertOverlappingRanges() {
        final int transactionCount = 10;
        try (MultipleTransactions multi = MultipleTransactions.create(db, transactionCount)) {
            for (int i = 0; i < multi.size(); i++) {
                Transaction tr = multi.get(i);
                assertTrue(rs.insertRange(tr, new Range(new byte[]{(byte)(i + 1)}, new byte[]{(byte)(i + 2), 0x00}), false).join());
            }

            // Even transactions should be able to commit. This is because the transactions are committed in order,
            // so what will happen is:
            //  1. Transaction 0 commits
            //  2. Transaction 1 overlaps with transaction 0's range, so doesn't commit
            //  3. Transaction 2 does not overlap with transaction 0, so commits. (It does overlap with transaction 1,
            //     but that doesn't matter because it didn't commit)
            //  4. Transaction 3 overlaps with transaction 2, so doesn't commit
            //  5. Transaction 4 doesn't overlap with transactions 0 or 2 so commits
            //  ...
            // And so on
            multi.commit(IntStream.range(0, multi.size()).filter(i -> i % 2 != 0).boxed().collect(Collectors.toSet()));
        }
        final Range fullRange = new Range(new byte[]{1}, new byte[]{(byte) (transactionCount + 1)});
        List<Range> gaps = IntStream.range(0, transactionCount)
                .filter(i -> i % 2 != 0)
                .mapToObj(i -> new Range(new byte[]{(byte)(i + 1), 0x00}, new byte[]{(byte)(i + 2)}))
                .toList();
        assertGaps(fullRange, gaps, new byte[]{(byte)(transactionCount), 0x00});
    }

    @Test
    void insertAbuttingRanges() {
        final int transactionCount = 10;
        try (MultipleTransactions multi = MultipleTransactions.create(db, transactionCount)) {
            for (int i = 0; i < multi.size(); i++) {
                Transaction tr = multi.get(i);
                assertTrue(rs.insertRange(tr, new Range(new byte[]{(byte)(i + 1)}, new byte[]{(byte)(i + 2)}), i % 2 == 0).join());
            }

            // All transactions should be able to commit as the ranges touch but do not overlap
            multi.commit();
        }

        final Range fullRange = new Range(new byte[]{1}, new byte[]{(byte) (transactionCount + 1)});
        assertGaps(fullRange, Collections.emptyList(), new byte[]{(byte) (transactionCount + 1)});
    }

    private void assertGaps(Range fullRange, List<Range> gaps, byte[] lastInRange) {
        try (Transaction tr = db.createTransaction()) {
            List<Range> missing = rs.missingRanges(tr, fullRange).asList().join();
            assertEquals(gaps, missing);

            List<Range> expectedAll = new ArrayList<>();
            expectedAll.add(new Range(new byte[]{0}, fullRange.begin));
            if (!gaps.isEmpty()) {
                expectedAll.addAll(gaps.subList(0, gaps.size() - 1));
            }
            expectedAll.add(new Range(lastInRange, new byte[]{(byte)0xff}));
            List<Range> allMissing = rs.missingRanges(tr).asList().join();
            assertEquals(expectedAll, allMissing);

            rs.insertRange(tr, fullRange, false).join();
            assertTrue(rs.missingRanges(tr, fullRange).asList().join().isEmpty());
            List<Range> allMissingAfterInsert = rs.missingRanges(tr).asList().join();
            assertEquals(List.of(new Range(new byte[]{0}, fullRange.begin), new Range(fullRange.end, new byte[]{(byte)0xff})), allMissingAfterInsert);
        }
    }

    @Test
    void concurrentlyReadAndFillInGaps() {
        List<Range> ranges = IntStream.range(0, 10)
                .mapToObj(i -> new Range(new byte[]{(byte) (2 * i + 1)}, new byte[]{(byte) (2 * i + 2)}))
                .toList();
        try (Transaction tr = db.createTransaction()) {
            for (Range r : ranges) {
                rs.insertRange(tr, r, true).join();
            }
            tr.commit().join();
        }

        int maxRangeByte = 2 * ranges.size() + 1;
        try (MultipleTransactions multi = MultipleTransactions.create(db, maxRangeByte + 1)) {
            // Insert the full range with the first transaction
            assertTrue(rs.insertRange(multi.get(0), null, null, false).join());

            // Check on a key in each previously inserted range
            Set<Integer> conflicts = new HashSet<>();
            for (int i = 0; i < maxRangeByte; i++) {
                byte[] key = new byte[]{(byte) i, 0x00};
                Transaction tr = multi.get(i + 1);
                boolean inARange = i % 2 != 0;
                assertEquals(inARange, rs.contains(tr, key).join(), () -> "key " + ByteArrayUtil.printable(key) + " was in a range");
                if (!inARange) {
                    // The check should conflict with the insert if (and only if) the key wasn't in a range
                    conflicts.add(i + 1);
                }
            }

            multi.commit(conflicts);
        }
    }

    // Test out a few of the wild and wacky things that can happen when there are insertions going concurrently.
    @Test
    void concurrentWithInsert() {
        final List<byte[]> keys = createKeys();
        final List<Range> ranges = new ArrayList<>();

        // Two disjoint ranges -- both should succeed.
        try (MultipleTransactions multi = MultipleTransactions.create(db, 2)) {
            Transaction tr1 = multi.get(0);
            Transaction tr2 = multi.get(1);

            Range r1 = new Range(new byte[]{0x01}, new byte[]{0x02});
            Range r2 = new Range(new byte[]{0x02}, new byte[]{0x03});
            CompletableFuture<Boolean> future1 = rs.insertRange(tr1, r1);
            CompletableFuture<Boolean> future2 = rs.insertRange(tr2, r2);
            assertTrue(future1.join(), "Range 1 did not do insertion.");
            assertTrue(future2.join(), "Range 2 did not do insertion.");

            multi.commit();
            ranges.add(r1);
            ranges.add(r2);
        }

        checkConsistent(ranges, keys);
        checkIncreasing();

        // Two non-disjoint ranges. The second should fail.
        try (MultipleTransactions multi = MultipleTransactions.create(db, 2)) {
            Transaction tr1 = multi.get(0);
            Transaction tr2 = multi.get(1);

            Range r1 = new Range(new byte[]{0x03}, new byte[]{0x05});
            Range r2 = new Range(new byte[]{0x04}, new byte[]{0x06});
            CompletableFuture<Boolean> future1 = rs.insertRange(tr1, r1);
            CompletableFuture<Boolean> future2 = rs.insertRange(tr2, r2);

            assertTrue(future1.join(), "Range 1 did not do insertion");
            assertTrue(future2.join(), "Range 2 did not do insertion");
            multi.commit(Set.of(1));

            ranges.add(r1);
        }

        checkConsistent(ranges, keys);
        checkIncreasing();

        // Read during write - the reads in the range should fail.
        List<byte[]> specificKeys = Arrays.asList(
                new byte[]{0x06, (byte)0xff},
                new byte[]{0x07},
                new byte[]{0x07, 0x00},
                new byte[]{0x07, 0x10},
                new byte[]{0x08},
                new byte[]{0x08, 0x00},
                new byte[]{0x08, 0x10},
                new byte[]{0x09}
        );
        try (MultipleTransactions multi = MultipleTransactions.create(db, specificKeys.size() + 1)) {
            Transaction tr1 = multi.get(0);
            Range r1 = new Range(new byte[]{0x07}, new byte[]{0x08});
            CompletableFuture<Boolean> future1 = rs.insertRange(tr1, r1);
            List<CompletableFuture<Boolean>> futures = new ArrayList<>(specificKeys.size());
            Set<Integer> conflicting = new HashSet<>();
            for (int i = 0; i < specificKeys.size(); i++) {
                Transaction tr = multi.get(i + 1);
                byte[] key = specificKeys.get(i);
                futures.add(rs.contains(tr, specificKeys.get(i)));
                if (ByteArrayUtil.compareUnsigned(r1.begin, key) <= 0
                        && ByteArrayUtil.compareUnsigned(key, r1.end) < 0) {
                    conflicting.add(i + 1);
                }
            }

            assertTrue(future1.join(), "Range 1 did not do insertion");
            futures.forEach(future -> assertFalse(future.join(), "Key should not be present"));

            multi.commit(conflicting);
            ranges.add(r1);
        }

        checkConsistent(ranges, keys);
        checkIncreasing();
    }

    @Test
    void isFirstOrFinalKey() {
        final byte[] firstKey = new byte[]{(byte)0x00};
        final byte[] finalKey = new byte[]{(byte)0xff};
        final byte[] justAKey = new byte[]{(byte)0x77};

        assertTrue(RangeSet.isFirstKey(firstKey));
        assertFalse(RangeSet.isFirstKey(finalKey));
        assertFalse(RangeSet.isFirstKey(justAKey));

        assertFalse(RangeSet.isFinalKey(firstKey));
        assertTrue(RangeSet.isFinalKey(finalKey));
        assertFalse(RangeSet.isFinalKey(justAKey));
    }

    @Test
    void missingRanges() {
        // First, from empty.
        List<Range> ranges = db.readAsync(tr -> rs.missingRanges(tr).asList()).join();
        assertEquals(1, ranges.size());
        Range r1 = ranges.get(0);
        assertArrayEquals(new byte[]{0x00}, r1.begin);
        assertArrayEquals(new byte[]{(byte)0xff}, r1.end);

        // Now, add some ranges to make the gaps interesting.
        final List<Range> toAdd = Arrays.asList(
                new Range(new byte[]{0x10}, new byte[]{0x20}),
                new Range(new byte[]{0x20}, new byte[]{0x20, 0x00}),
                new Range(new byte[]{0x30}, new byte[]{0x40}),
                new Range(new byte[]{0x40, 0x00}, new byte[]{0x50}),
                new Range(new byte[]{0x60}, new byte[]{0x6a}),
                new Range(new byte[]{0x62}, new byte[]{0x70}),
                new Range(new byte[]{0x71}, new byte[]{0x72}),
                new Range(new byte[]{0x72}, new byte[]{0x7f})
        );
        final List<Range> expected = Arrays.asList(
                new Range(new byte[]{0x00}, new byte[]{0x10}),
                new Range(new byte[]{0x20, 0x00}, new byte[]{0x30}),
                new Range(new byte[]{0x40}, new byte[]{0x40, 0x00}),
                new Range(new byte[]{0x50}, new byte[]{0x60}),
                new Range(new byte[]{0x70}, new byte[]{0x71}),
                new Range(new byte[]{0x7f}, new byte[]{(byte)0xff})
        );
        final List<Boolean> added = db.runAsync(tr -> {
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();
            for (Range range : toAdd) {
                futures.add(rs.insertRange(tr, range));
            }
            return AsyncUtil.getAll(futures);
        }).join();
        assertTrue(added.stream().allMatch(b -> b), "Some of the insertions didn't change things");

        ranges = rs.missingRanges(db).join();
        assertEquals(6, ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            assertArrayEquals(expected.get(i).begin, ranges.get(i).begin);
            assertArrayEquals(expected.get(i).end, ranges.get(i).end);
        }

        ranges = rs.missingRanges(db, new byte[]{0x00}, new byte[]{0x60}).join();
        assertEquals(4, ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            assertArrayEquals(expected.get(i).begin, ranges.get(i).begin);
            assertArrayEquals(expected.get(i).end, ranges.get(i).end);
        }

        ranges = rs.missingRanges(db, new Range(new byte[]{0x00}, new byte[]{0x60})).join();
        assertEquals(4, ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            assertArrayEquals(expected.get(i).begin, ranges.get(i).begin);
            assertArrayEquals(expected.get(i).end, ranges.get(i).end);
        }

        ranges = rs.missingRanges(db, new byte[]{0x00}, new byte[]{0x60}, 2).join();
        assertEquals(2, ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            assertArrayEquals(expected.get(i).begin, ranges.get(i).begin);
            assertArrayEquals(expected.get(i).end, ranges.get(i).end);
        }

        ranges = rs.missingRanges(db, new byte[]{0x00}, new byte[]{0x60}, 1).join();
        assertEquals(1, ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            assertArrayEquals(expected.get(i).begin, ranges.get(i).begin);
            assertArrayEquals(expected.get(i).end, ranges.get(i).end);
        }
    }
}
