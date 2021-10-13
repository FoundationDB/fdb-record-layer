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
import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.FDBTestBase;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.apple.foundationdb.tuple.ByteArrayUtil.compareUnsigned;
import static com.apple.foundationdb.tuple.ByteArrayUtil.printable;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link RangeSet}.
 */
@Tag(Tags.RequiresFDB)
public class RangeSetTest extends FDBTestBase {
    private Database db;
    private Subspace rsSubspace;
    private RangeSet rs;

    private static final byte[] DEADC0DE = new byte[]{(byte)0xde,(byte)0xad,(byte)0xc0,(byte)0xde};

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
    public void setUp() throws Exception {
        db = FDB.instance().open();
        rsSubspace = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(getClass().getSimpleName())).get();
        rs = new RangeSet(rsSubspace);
        rs.clear(db).join();
    }

    @Test
    public void clear() {
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
    public void contains() {
        // Insert a few ranges manually.
        db.run(tr -> {
            tr.set(rsSubspace.pack(new byte[]{(byte)0x10}), new byte[]{(byte)0x66});
            tr.set(rsSubspace.pack(new byte[]{(byte)0x77}), new byte[]{(byte)0x88});
            tr.set(rsSubspace.pack(new byte[]{(byte)0x88}), new byte[]{(byte)0x99});
            tr.set(rsSubspace.pack(new byte[]{(byte)0x99}), new byte[]{(byte)0x99,(byte)0x00});
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
    public void containsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> rs.contains(db, new byte[0]).join());
    }

    @Test
    public void containsPastFF1() {
        assertThrows(IllegalArgumentException.class, () -> rs.contains(db, new byte[]{(byte)0xff}).join());
    }

    @Test
    public void containsPastFF2() {
        assertThrows(IllegalArgumentException.class, () -> rs.contains(db, new byte[]{(byte)0xff,(byte)0x00}).join());
    }

    @Test
    @Tag(Tags.Slow)
    public void insert() {
        List<byte[]> keys = createKeys();
        List<Range> rangeSource = Arrays.asList(
                Range.startsWith(new byte[]{0x10}), // Step 1: An initial range
                Range.startsWith(new byte[]{0x30}), // Step 2: An second disjoint range.
                Range.startsWith(new byte[]{0x20}), // Step 3: A third disjoint range between them.
                new Range(new byte[]{0x05}, new byte[]{0x10, 0x10}), // Step 4: A range overlapping the first range.
                new Range(new byte[]{0x2f}, new byte[]{0x30, 0x10}), // Step 5: A range overlapping the third range.
                new Range(new byte[]{0x30,0x10}, new byte[]{0x40}), // Step 6: A range overlapping the third range.
                new Range(new byte[]{0x05,0x10}, new byte[]{0x12}), // Step 7: A range with just a little at the end.
                new Range(new byte[]{0x20,0x14}, new byte[]{0x30,0x00}), // Step 8: A range that goes between 0x20 and 0x30 ranges.
                new Range(new byte[]{0x03}, new byte[]{0x42}), // Step 9: A range that goes over whole range.
                new Range(new byte[]{0x10,0x11}, new byte[]{0x41}), // Step 10: A range entirely within the given ranges.
                new Range(new byte[]{0x50}, new byte[]{0x50,0x00}) // Step 11: A range that contains only 1 key.
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
    public void insertEmpty() {
        assertThrows(IllegalArgumentException.class, () -> rs.insertRange(db, new byte[0], new byte[]{0x00}));
    }

    @Test
    public void insertInverted() {
        assertThrows(IllegalArgumentException.class, () -> rs.insertRange(db, new byte[]{0x15}, new byte[]{0x05}));
    }

    @Test
    public void insertPastEnd() {
        assertThrows(IllegalArgumentException.class, () -> rs.insertRange(db, new byte[]{(byte)0xff, (byte)0x10}, new byte[]{(byte)0xff, (byte)0x15}));
    }

    // Test out a few of the wild and wacky things that can happen when there are insertions going concurrently.
    @Test
    @Tag(Tags.Slow)
    public void concurrentWithInsert() {
        final List<byte[]> keys = createKeys();
        final List<Range> ranges = new ArrayList<>();

        // Two disjoint ranges -- both should succeed.
        Transaction tr1 = db.createTransaction();
        Transaction tr2 = db.createTransaction();
        Range r1 = new Range(new byte[]{0x01}, new byte[]{0x02});
        Range r2 = new Range(new byte[]{0x02}, new byte[]{0x03});
        CompletableFuture<Boolean> future1 = rs.insertRange(tr1, r1);
        CompletableFuture<Boolean> future2 = rs.insertRange(tr2, r2);
        assertTrue(future1.join(), "Range 1 did not do insertion.");
        assertTrue(future2.join(), "Range 2 did not do insertion.");
        tr1.commit().join();
        tr2.commit().join();
        ranges.add(r1);
        ranges.add(r2);
        checkConsistent(ranges, keys);
        checkIncreasing();

        // Two non-disjoint ranges. The second should fail.
        tr1 = db.createTransaction();
        tr2 = db.createTransaction();
        r1 = new Range(new byte[]{0x03}, new byte[]{0x05});
        r2 = new Range(new byte[]{0x04}, new byte[]{0x06});
        future1 = rs.insertRange(tr1, r1);
        future2 = rs.insertRange(tr2, r2);
        assertTrue(future1.join(), "Range 1 did not do insertion");
        assertTrue(future2.join(), "Range 2 did not do insertion");
        tr1.commit().join();
        tr2.commit().handle((vignore, e) -> {
            assertNotNull(e, "No error thrown from commit");
            assertTrue(e instanceof FDBException, "Non-FDBException " + e.toString());
            FDBException fdbe = (FDBException)e;
            assertEquals(FDBError.NOT_COMMITTED.code(), fdbe.getCode(), "Did not get not-committed error.");
            return vignore;
        }).join();
        ranges.add(r1);
        checkConsistent(ranges, keys);
        checkIncreasing();

        // Read during write - the reads in the range should fail.
        r1 = new Range(new byte[]{0x07}, new byte[]{0x08});
        List<byte[]> specificKeys = Arrays.asList(
                new byte[]{0x06,(byte)0xff},
                new byte[]{0x07},
                new byte[]{0x07,0x00},
                new byte[]{0x07,0x10},
                new byte[]{0x08},
                new byte[]{0x08,0x00},
                new byte[]{0x08,0x10},
                new byte[]{0x09}
        );

        tr1 = db.createTransaction();
        List<Transaction> transactions = specificKeys.stream().map(ignore -> db.createTransaction()).collect(Collectors.toList());
        // Add write conflict ranges to each key so that they are not read only.
        transactions.forEach(tr -> tr.addWriteConflictKey(DEADC0DE));

        future1 = rs.insertRange(tr1, r1);
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < specificKeys.size(); i++) {
            futures.add(rs.contains(transactions.get(i), specificKeys.get(i)));
        }

        assertTrue(future1.join(), "Range 1 did not do insertion");
        futures.forEach(future -> assertFalse(future.join(), "Key should not be present"));

        tr1.commit().join();
        Range range = r1;
        for (int i = 0; i < transactions.size(); i++) {
            final int index = i;
            transactions.get(i).commit().handle((vignore, e) -> {
                byte[] key = specificKeys.get(index);
                String repr = ByteArrayUtil.printable(key);
                if (ByteArrayUtil.compareUnsigned(range.begin, key) <= 0
                        && ByteArrayUtil.compareUnsigned(key, range.end) < 0 ) {
                    assertNotNull(e, "No error from commit when key is " + repr);
                    assertTrue(e instanceof FDBException, "Non-FDBException " + e.toString() + " with key " + repr);
                    FDBException fdbe = (FDBException)e;
                    assertEquals(FDBError.NOT_COMMITTED.code(), fdbe.getCode(), "Did not get non-committed error when key is " + repr);
                } else {
                    assertNull(e, "Error when key is " + repr);
                }
                return vignore;
            }).join();
        }
        ranges.add(r1);
        checkConsistent(ranges, keys);
        checkIncreasing();
    }

    @Test
    public void isFirstOrFinalKey() {
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
    public void missingRanges() {
        // First, from empty.
        List<Range> ranges = db.readAsync(tr -> rs.missingRanges(tr).asList()).join();
        assertEquals(1, ranges.size());
        Range r1 = ranges.get(0);
        assertArrayEquals(new byte[]{0x00}, r1.begin);
        assertArrayEquals(new byte[]{(byte)0xff}, r1.end);

        // Now, add some ranges to make the gaps interesting.
        List<Range> toAdd = Arrays.asList(
                new Range(new byte[]{0x10}, new byte[]{0x20}),
                new Range(new byte[]{0x20}, new byte[]{0x20,0x00}),
                new Range(new byte[]{0x30}, new byte[]{0x40}),
                new Range(new byte[]{0x40,0x00}, new byte[]{0x50}),
                new Range(new byte[]{0x60}, new byte[]{0x6a}),
                new Range(new byte[]{0x62}, new byte[]{0x70}),
                new Range(new byte[]{0x71}, new byte[]{0x72}),
                new Range(new byte[]{0x72}, new byte[]{0x7f})
        );
        List<Range> expected = Arrays.asList(
                new Range(new byte[]{0x00}, new byte[]{0x10}),
                new Range(new byte[]{0x20,0x00}, new byte[]{0x30}),
                new Range(new byte[]{0x40}, new byte[]{0x40,0x00}),
                new Range(new byte[]{0x50}, new byte[]{0x60}),
                new Range(new byte[]{0x70}, new byte[]{0x71}),
                new Range(new byte[]{0x7f}, new byte[]{(byte)0xff})
        );
        List<Boolean> added = db.runAsync(tr -> {
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
