/*
 * BunchedMapTest.java
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

package com.apple.foundationdb.map;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.foundationdb.util.LoggableException;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link BunchedMap}.
 */
@Tag(Tags.RequiresFDB)
public class BunchedMapTest {
    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension bmSubspaceExtension = new TestSubspaceExtension(dbExtension);
    private static final BunchedTupleSerializer serializer = BunchedTupleSerializer.instance();
    private static final BunchedMap<Tuple, Tuple> map;
    private Database db;
    private Subspace bmSubspace;

    static {
        map = new BunchedMap<>(BunchedTupleSerializer.instance(), Comparator.naturalOrder(), 10);
    }

    @BeforeEach
    public void setUp() {
        db = dbExtension.getDatabase();
        bmSubspace = bmSubspaceExtension.getSubspace();
    }

    @Nullable
    private static FDBException unwrapException(@Nullable Throwable err) {
        Throwable curr = err;
        while (curr != null && !(curr instanceof FDBException))  {
            curr = curr.getCause();
        }
        if (curr != null) {
            return (FDBException) curr;
        } else {
            return null;
        }
    }

    private static List<KeyValue> inconsistentScan(@Nonnull Database db, @Nonnull Subspace subspace) {
        Transaction tr = db.createTransaction();  // Note that tr is mutated in the block, hence not using try-with-resources
        try {
            KeySelector begin = KeySelector.firstGreaterOrEqual(subspace.range().begin);
            KeySelector end = KeySelector.firstGreaterOrEqual(subspace.range().end);
            KeyValue lastSeen = null;
            AsyncIterator<KeyValue> rangeIterator = tr.getRange(begin, end).iterator();
            List<KeyValue> rangeKVs = new ArrayList<>();
            boolean done = false;
            while (!done) { // Might loop if there are timeouts encountered within loop.
                try {
                    while (rangeIterator.hasNext()) {
                        KeyValue next = rangeIterator.next();
                        rangeKVs.add(next);
                        lastSeen = next;
                    }
                    done = true;
                } catch (RuntimeException e) {
                    FDBException fdbE = unwrapException(e);
                    if (fdbE == null || fdbE.getCode() != FDBError.TRANSACTION_TOO_OLD.code()) {
                        throw e;
                    } else {
                        // Timed out. Restart transaction and keep going.
                        tr.close();
                        tr = db.createTransaction();
                        if (lastSeen != null) {
                            // Update begin if we have any results.
                            begin = KeySelector.firstGreaterThan(lastSeen.getKey());
                            lastSeen = null;
                        }
                        rangeIterator = tr.getRange(begin, end).iterator();
                    }
                }
            }
            return rangeKVs;
        } finally {
            tr.close();
        }
    }

    @Test
    public void insertSingleKey() {
        List<Tuple> testTuples = Stream.of(1066L, 1776L, 1415L, 800L).map(Tuple::from).collect(Collectors.toList());
        Tuple value = Tuple.from(1415L);
        db.run(tr -> {
            Tuple minSoFar = null;
            for (int i = 0; i < testTuples.size(); i++) {
                Tuple key = testTuples.get(i);
                minSoFar = (minSoFar == null || key.compareTo(minSoFar) < 0) ? key : minSoFar;

                map.put(tr, bmSubspace, key, value).join();
                for (int j = 0; j < testTuples.size(); j++) {
                    assertEquals(j <= i, map.containsKey(tr, bmSubspace, testTuples.get(j)).join());
                }

                List<KeyValue> rangeKVs = tr.getRange(bmSubspace.range()).asList().join();
                assertEquals(1, rangeKVs.size());
                assertArrayEquals(bmSubspace.pack(minSoFar), rangeKVs.get(0).getKey());
                List<Map.Entry<Tuple, Tuple>> entryList = testTuples.subList(0, i + 1).stream()
                        .sorted()
                        .map(t -> new AbstractMap.SimpleImmutableEntry<>(t, value))
                        .collect(Collectors.toList());
                assertArrayEquals(serializer.serializeEntries(entryList), rangeKVs.get(0).getValue());
            }

            return null;
        });
    }

    @Test
    public void insertTwoKeys() throws ExecutionException, InterruptedException {
        final Tuple value = Tuple.from("hello", "there");
        final List<Tuple> firstTuples = LongStream.range(100L, 110L).boxed().map(Tuple::from).collect(Collectors.toList());
        final List<Tuple> secondTuples = LongStream.range(120L, 130L).boxed().map(Tuple::from).collect(Collectors.toList());

        db.run(tr -> {
            firstTuples.forEach(t -> map.put(tr, bmSubspace, t, value).join());
            secondTuples.forEach(t -> map.put(tr, bmSubspace, t, value).join());

            List<KeyValue> rangeKVs = tr.getRange(bmSubspace.range()).asList().join();
            assertEquals(2, rangeKVs.size());
            firstTuples.forEach(t -> assertTrue(map.containsKey(tr, bmSubspace, t).join(), t.toString() + " not in map"));
            secondTuples.forEach(t -> assertTrue(map.containsKey(tr, bmSubspace, t).join(), t.toString() + " not in map"));
            return null;
        });

        // Attempt a few different insertion types with this setup (without committing)

        try (Transaction tr = db.createTransaction()) {
            // Insert in the middle.
            List<Tuple> middleTuples = Stream.of(115L, 118L, 119L, 114L).map(Tuple::from).collect(Collectors.toList());
            Tuple minSoFar = null;

            for (int i = 0; i < middleTuples.size(); i++) {
                Tuple t = middleTuples.get(i);
                map.put(tr, bmSubspace, t, value).join();
                minSoFar = (minSoFar == null || t.compareTo(minSoFar) < 0) ? t : minSoFar;

                for (int j = 0; j < middleTuples.size(); j++) {
                    assertEquals(j <= i, map.containsKey(tr, bmSubspace, middleTuples.get(j)).get());
                }

                List<KeyValue> rangeKVs = tr.getRange(bmSubspace.range()).asList().join();
                assertEquals(3, rangeKVs.size());
                List<Tuple> keys = rangeKVs.stream().map(KeyValue::getKey).map(bmSubspace::unpack).collect(Collectors.toList());
                assertEquals(Arrays.asList(Tuple.from(100L), minSoFar, Tuple.from(120L)), keys);
            }

            tr.cancel();
        }

        try (Transaction tr = db.createTransaction()) {
            // Remove a key from the end of first tuple collection.
            assertTrue(map.remove(tr, bmSubspace, Tuple.from(109L)).get().isPresent());
            assertFalse(map.remove(tr, bmSubspace, Tuple.from(109L)).get().isPresent());
            map.put(tr, bmSubspace, Tuple.from(110L), value).get();
            List<KeyValue> rangeKVs = tr.getRange(bmSubspace.range()).asList().get();
            assertEquals(2, rangeKVs.size());

            // Now insert in the middle to force it to split.
            map.put(tr, bmSubspace, Tuple.from(109L), value).get();
            rangeKVs = tr.getRange(bmSubspace.range()).asList().get();
            assertEquals(3, rangeKVs.size());
            List<Tuple> keys = rangeKVs.stream().map(KeyValue::getKey).map(bmSubspace::unpack).collect(Collectors.toList());
            assertEquals(Arrays.asList(Tuple.from(100L), Tuple.from(105L), Tuple.from(120L)), keys);

            tr.cancel();
        }
    }

    private void verifyBoundaryKeys(@Nonnull List<Tuple> boundaryKeys) throws ExecutionException, InterruptedException {
        try (Transaction tr = db.createTransaction()) {
            map.verifyIntegrity(tr, bmSubspace).get();
            List<KeyValue> rangeKVs = tr.getRange(bmSubspace.range()).asList().get();
            List<Tuple> actualBoundaryKeys = rangeKVs.stream()
                    .map(KeyValue::getKey)
                    .map(bmSubspace::unpack)
                    .collect(Collectors.toList());
            List<Map.Entry<Tuple, Tuple>> entryList = rangeKVs.stream()
                    .flatMap(kv -> serializer.deserializeEntries(bmSubspace.unpack(kv.getKey()), kv.getValue()).stream())
                    .collect(Collectors.toList());
            System.out.println(entryList);
            assertEquals(boundaryKeys, actualBoundaryKeys);
            tr.cancel();
        }
    }

    private void runWithTwoTrs(@Nonnull BiConsumer<? super Transaction, ? super Transaction> operation,
                               boolean legal,
                               @Nonnull List<Tuple> boundaryKeys) throws ExecutionException, InterruptedException {
        final String id = "two-trs-" + UUID.randomUUID().toString();
        try (Transaction tr1 = db.createTransaction(); Transaction tr2 = db.createTransaction()) {
            tr1.options().setDebugTransactionIdentifier(id + "-1");
            tr1.options().setLogTransaction();
            tr2.options().setDebugTransactionIdentifier(id + "-2");
            tr2.options().setLogTransaction();
            CompletableFuture.allOf(tr1.getReadVersion(), tr2.getReadVersion()).get();
            tr1.addWriteConflictKey(new byte[]{0x01});
            tr2.addWriteConflictKey(new byte[]{0x02});
            operation.accept(tr1, tr2);
            tr1.commit().get();
            if (legal) {
                tr2.commit().get();
            } else {
                ExecutionException e = assertThrows(ExecutionException.class, () -> tr2.commit().get());
                assertNotNull(e.getCause());
                assertTrue(e.getCause() instanceof FDBException);
                FDBException fdbE = (FDBException)e.getCause();
                assertEquals(FDBError.NOT_COMMITTED.code(), fdbE.getCode());
            }
        }
        verifyBoundaryKeys(boundaryKeys);
    }

    @Test
    public void concurrentLegalUpdates() throws ExecutionException, InterruptedException {
        final Tuple value = Tuple.from((Object)null);

        // From initial database, essentially any two updates will cause each one
        // to get its own key.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(1066L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(1415L), value).join();
        }, true, Arrays.asList(Tuple.from(1066L), Tuple.from(1415L)));
        try (Transaction tr = db.createTransaction()) {
            tr.clear(bmSubspace.range());
            tr.commit().get();
        }

        final List<Tuple> tuples = LongStream.range(100L, 115L).boxed().map(Tuple::from).collect(Collectors.toList());
        db.run(tr -> {
            tuples.forEach(t -> map.put(tr, bmSubspace, t, value).join());
            return null;
        });

        // Case 1: Transaction reads the same key as another, but it
        // doesn't actually need the part that is different.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(116L), value).join();
            assertEquals(value, map.get(tr2, bmSubspace, Tuple.from(112L)).join().get());
        }, true, Arrays.asList(Tuple.from(100L), Tuple.from(110L)));

        // Case 2: Transaction reads the same key in a way while
        // another transaction writes the same value into the key.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(105L), value).join();
            assertEquals(value, map.get(tr2, bmSubspace, Tuple.from(105L)).join().get());
        }, true, Arrays.asList(Tuple.from(100L), Tuple.from(110L)));

        // Case 3: Transaction ranges read will overlap
        runWithTwoTrs((tr1, tr2) -> {
            // As the first one is full, the logic chooses to put (109L, null)
            // as the first key of the second set of things.
            map.put(tr1, bmSubspace, Tuple.from(109L, null), value).join();
            // As the split is in the middle, it will choose to put
            // (107L, null) in the first group of transactions.
            map.put(tr2, bmSubspace, Tuple.from(107L, null), value).join();
        }, true, Arrays.asList(Tuple.from(100L), Tuple.from(105L), Tuple.from(109L, null)));
        try (Transaction tr = db.createTransaction()) {
            map.verifyIntegrity(tr, bmSubspace).get();
            // Fill up the (100L,) to (105L,) range.
            LongStream.range(0L, 5L)
                    .boxed()
                    .map(l -> Tuple.from(104L, l))
                    .forEach(t -> map.put(tr, bmSubspace, t, value).join());
            tr.commit().get();
        }

        // Case 4: Read a value that is rewritten to the same value when appending
        // to the beginning.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(104L, 100L), value).join();
            assertEquals(value, map.get(tr2, bmSubspace, Tuple.from(107L)).join().get());
        }, true, Arrays.asList(Tuple.from(100L), Tuple.from(104L, 100L), Tuple.from(109L, null)));
        try (Transaction tr = db.createTransaction()) {
            // Fill up (104L, 100L) to (109, null).
            LongStream.range(101L, 104L)
                    .boxed()
                    .map(l -> Tuple.from(104L, l))
                    .forEach(t -> map.put(tr, bmSubspace, t, value).join());
            tr.commit().get();
        }

        // Case 5: Two things going in the middle of two filled ranges.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(104L, 42L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(104L, 43L), value).join();
        }, true, Arrays.asList(Tuple.from(100L), Tuple.from(104L, 42L), Tuple.from(104L, 43L), Tuple.from(104L, 100L), Tuple.from(109L, null)));

        // Case 6: Two keys before all filled ranges.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(42L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(43L), value).join();
        }, true, Arrays.asList(Tuple.from(42L), Tuple.from(43L), Tuple.from(100L), Tuple.from(104L, 42L), Tuple.from(104L, 43L), Tuple.from(104L, 100L), Tuple.from(109L, null)));
        try (Transaction tr = db.createTransaction()) {
            // Fill up the last range.
            LongStream.range(117L, 120L)
                    .boxed()
                    .map(Tuple::from)
                    .forEach(t -> map.put(tr, bmSubspace, t, value).join());
            tr.commit().get();
        }

        // Case 7: Two keys after filled ranges.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(120L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(121L), value).join();
        }, true, Arrays.asList(Tuple.from(42L), Tuple.from(43L), Tuple.from(100L), Tuple.from(104L, 42L), Tuple.from(104L, 43L), Tuple.from(104L, 100L), Tuple.from(109L, null), Tuple.from(120L), Tuple.from(121L)));

        // Case 8: Adding to a full range while simultaneously adding something after the range.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(102L, 0L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(104L, 41L), value).join();
        }, true, Arrays.asList(Tuple.from(42L), Tuple.from(43L), Tuple.from(100L), Tuple.from(104L), Tuple.from(104L, 41L), Tuple.from(104L, 43L), Tuple.from(104L, 100L), Tuple.from(109L, null), Tuple.from(120L), Tuple.from(121L)));

        // Compact the data to a minimal number of keys.
        try (Transaction tr = db.createTransaction()) {
            assertNull(map.compact(tr, bmSubspace, 0, null).get());
            map.verifyIntegrity(tr, bmSubspace).get();
            tr.commit().get();
        }
        verifyBoundaryKeys(Arrays.asList(Tuple.from(42L), Tuple.from(104L, 2L), Tuple.from(105L), Tuple.from(113L)));
    }

    @Test
    public void concurrentIllegalUpdates() throws ExecutionException, InterruptedException {
        final Tuple value = Tuple.from(Tuple.from((Object)null));

        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(0L), value).join();
            map.put(tr1, bmSubspace, Tuple.from(5L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(3L), value).join();
        }, false, Collections.singletonList(Tuple.from(0L)));
        try (Transaction tr = db.createTransaction()) {
            tr.clear(bmSubspace.range());
            tr.commit().get();
        }

        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(5L), value).join();
            map.put(tr1, bmSubspace, Tuple.from(0L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(3L), value).join();
        }, false, Collections.singletonList(Tuple.from(0L)));
        try (Transaction tr = db.createTransaction()) {
            tr.clear(bmSubspace.range());
            tr.commit().get();
        }

        final List<Tuple> tuples = LongStream.range(100L, 115L).boxed().map(Tuple::from).collect(Collectors.toList());
        db.run(tr -> {
            tr.clear(bmSubspace.range());
            tuples.forEach(t -> map.put(tr, bmSubspace, t, value).join());
            return null;
        });

        // Case 1: Transaction reads the value of a boundary key while
        // that boundary is updated.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(116L), value).join();
            assertEquals(value, map.get(tr2, bmSubspace, Tuple.from(110L)).join().get());
        }, false, Arrays.asList(Tuple.from(100L), Tuple.from(110L)));

        // Case 2: Transaction reads the same key while
        // a put goes onto the same key and changes the value.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(105L), value.add(3.14d)).join();
            assertEquals(value, map.get(tr2, bmSubspace, Tuple.from(105L)).join().get());
        }, false, Arrays.asList(Tuple.from(100L), Tuple.from(110L)));
        assertEquals(value.add(3.14d), map.get(db, bmSubspace, Tuple.from(105L)).get().get());

        // Case 3: One put changes a value while another put tries to set it to
        // the same value.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(105L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(105L), value.add(3.14d)).join();
        }, false, Arrays.asList(Tuple.from(100L), Tuple.from(110L)));
        assertEquals(value, map.get(db, bmSubspace, Tuple.from(105L)).get().get());

        // Case 4: Two puts happen at the same value at the same time.
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(105L), value.add(3.14d)).join();
            map.put(tr2, bmSubspace, Tuple.from(105L), value.add(2.72d)).join();
        }, false, Arrays.asList(Tuple.from(100L), Tuple.from(110L)));
        assertEquals(value.add(3.14d), map.get(db, bmSubspace, Tuple.from(105L)).get().get());

        // Case 5: Something attempts to re-write something in the
        // interior of the boundary while writing
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(116L), value.add(3.14d)).join();
            map.put(tr2, bmSubspace, Tuple.from(117L), value).join();
        }, false, Arrays.asList(Tuple.from(100L), Tuple.from(110L)));
        assertFalse(map.containsKey(db, bmSubspace, Tuple.from(117L)).get());

        // Case 6: Write a value that would end up being overwritten in a split
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(102L, null), value).join();
            map.put(tr2, bmSubspace, Tuple.from(107L), value.add(3.14d)).join();
        }, false, Arrays.asList(Tuple.from(100L), Tuple.from(104L), Tuple.from(110L)));
        assertEquals(value, map.get(db, bmSubspace, Tuple.from(107L)).get().get());

        // Case 7: Write a key before the current value that eats the value
        // from a boundary key while another transaction does the same thing
        // (Note that this test fails if the read conflict ranges are added
        // after the writes, so this tests to make sure that that is done
        // properly.)
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(98L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(99L), value).join();
        }, false, Arrays.asList(Tuple.from(98L), Tuple.from(104L), Tuple.from(110L)));

        // Case 8: The same as case 7, but the greater key wins instead of
        // the smaller key
        runWithTwoTrs((tr1, tr2) -> {
            map.put(tr1, bmSubspace, Tuple.from(97L), value).join();
            map.put(tr2, bmSubspace, Tuple.from(96L), value).join();
        }, false, Arrays.asList(Tuple.from(97L), Tuple.from(104L), Tuple.from(110L)));

        try (Transaction tr = db.createTransaction()) {
            List<Map.Entry<Tuple, Tuple>> entryList = AsyncUtil.collectRemaining(map.scan(tr, bmSubspace)).get();
            System.out.println(entryList);
        }
    }

    private byte[] getLogKey(@Nonnull Subspace logSubspace, int mapIndex, @Nonnull AtomicInteger localOrder) {
        return logSubspace.subspace(Tuple.from(mapIndex)).packWithVersionstamp(Tuple.from(Versionstamp.incomplete(localOrder.getAndIncrement())));
    }

    private void stressTest(final Random r, final int trTotal, final int opTotal, final int keyCount, final int workerCount, boolean addBytesToValue, AtomicLong globalTrCount, int mapCount) throws InterruptedException, ExecutionException {
        final long initialTrCount = globalTrCount.get();
        final Subspace logSubspace = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(getClass().getName(), "log")).get();
        db.run(tr -> {
            tr.clear(bmSubspace.range());
            tr.clear(logSubspace.range());
            // If the database is empty, putting these here stop scans from hitting the log subspace within a transaction
            tr.set(logSubspace.getKey(), new byte[0]);
            tr.set(ByteArrayUtil.join(logSubspace.getKey(), new byte[]{(byte)0xff}), new byte[0]);
            return null;
        });

        final List<CompletableFuture<Void>> workers = Stream.generate(() -> {
            int bunchSize = r.nextInt(15) + 1;
            BunchedMap<Tuple, Tuple> workerMap = new BunchedMap<>(serializer, Comparator.naturalOrder(), bunchSize);
            AtomicInteger trCount = new AtomicInteger(0);
            return AsyncUtil.whileTrue(() -> {
                final Transaction tr = db.createTransaction();
                tr.options().setDebugTransactionIdentifier("stress-tr-" + globalTrCount.getAndIncrement());
                tr.options().setLogTransaction();
                final AtomicInteger opCount = new AtomicInteger(0);
                final AtomicInteger localOrder = new AtomicInteger(0);
                return AsyncUtil.whileTrue(() -> {
                    int opCode = r.nextInt(4);
                    CompletableFuture<?> op;
                    if (opCode == 0) {
                        // Random put
                        CompletableFuture<?>[] futures = new CompletableFuture<?>[mapCount];
                        for (int i = 0; i < mapCount; i++) {
                            if (r.nextBoolean()) {
                                Tuple key = Tuple.from(r.nextInt(keyCount));
                                Tuple value;
                                if (addBytesToValue) {
                                    int byteLength = r.nextInt(5000);
                                    byte[] bytes = new byte[byteLength];
                                    r.nextBytes(bytes);
                                    value = Tuple.from(r.nextLong(), bytes);
                                } else {
                                    value = Tuple.from(r.nextLong());
                                }
                                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY,
                                        getLogKey(logSubspace, i, localOrder),
                                        Tuple.from("PUT", key, value).pack()
                                );
                                futures[i] = workerMap.put(tr, bmSubspace.subspace(Tuple.from(i)), key, value);
                            } else {
                                futures[i] = AsyncUtil.DONE;
                            }
                        }
                        op = CompletableFuture.allOf(futures);
                    } else if (opCode == 1) {
                        // Read a random key.
                        int mapIndex = r.nextInt(mapCount);
                        Tuple key = Tuple.from(r.nextInt(keyCount));
                        op = workerMap.get(tr, bmSubspace.get(mapIndex), key).thenAccept(optionalValue ->
                                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, getLogKey(logSubspace, mapIndex, localOrder), Tuple.from("GET", key, optionalValue.orElse(null)).pack())
                        );
                    } else if (opCode == 2) {
                        // Check contains key
                        int mapIndex = r.nextInt(mapCount);
                        Tuple key = Tuple.from(r.nextInt(keyCount));
                        op = workerMap.containsKey(tr, bmSubspace.subspace(Tuple.from(mapIndex)), key).thenAccept(wasPresent ->
                                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, getLogKey(logSubspace, mapIndex, localOrder), Tuple.from("CONTAINS_KEY", key, wasPresent).pack())
                        );
                    } else {
                        // Remove a random key
                        int mapIndex = r.nextInt(mapCount);
                        Tuple key = Tuple.from(r.nextInt(keyCount));
                        op = workerMap.remove(tr, bmSubspace.subspace(Tuple.from(mapIndex)), key).thenAccept(oldValue ->
                                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, getLogKey(logSubspace, mapIndex, localOrder), Tuple.from("REMOVE", key, oldValue.orElse(null)).pack())
                        );
                    }
                    return op.thenApply(ignore -> opCount.incrementAndGet() < opTotal);
                }).thenCompose(vignore -> tr.commit()).handle((vignore, err) -> {
                    tr.close();
                    if (err != null) {
                        FDBException fdbE = unwrapException(err);
                        if (fdbE != null) {
                            if (fdbE.getCode() != FDBError.NOT_COMMITTED.code() && fdbE.getCode() != FDBError.TRANSACTION_TOO_OLD.code()) {
                                throw fdbE;
                            }
                        } else {
                            if (err instanceof RuntimeException) {
                                throw (RuntimeException)err;
                            } else {
                                throw new RuntimeException("verification error", err);
                            }
                        }
                    }
                    return trCount.incrementAndGet() < trTotal;
                });
            });
        }).limit(workerCount).collect(Collectors.toList());

        final AtomicBoolean stillWorking = new AtomicBoolean(true);
        final CompletableFuture<Void> verifierWorker = AsyncUtil.whileTrue(() -> {
            Transaction tr = db.createTransaction();
            AtomicLong versionRef = new AtomicLong(-1L);
            return tr.getReadVersion().thenCompose(version -> {
                versionRef.set(version);
                // Grab the mutation list.
                AtomicInteger mapIndex = new AtomicInteger(0);
                return AsyncUtil.whileTrue(() -> {
                    Subspace mapSubspace = bmSubspace.subspace(Tuple.from(mapIndex.get()));
                    Subspace mapLogSubspace = logSubspace.subspace(Tuple.from(mapIndex.get()));
                    CompletableFuture<List<Tuple>> logFuture = AsyncUtil.mapIterable(tr.getRange(mapLogSubspace.range()), kv -> Tuple.fromBytes(kv.getValue())).asList();
                    // Verify integrity and then grab all of the keys and values.
                    CompletableFuture<List<Map.Entry<Tuple, Tuple>>> contentFuture = AsyncUtil.collectRemaining(map.scan(tr, mapSubspace));
                    CompletableFuture<Void> integrityFuture = map.verifyIntegrity(tr, mapSubspace);
                    return integrityFuture.thenCompose(vignore -> contentFuture.thenCombine(logFuture, (mapContents, logEntries) -> {
                        Map<Tuple, Tuple> mapCopy = new TreeMap<>();
                        for (Tuple logEntry : logEntries) {
                            String op = logEntry.getString(0);
                            if (op.equals("PUT")) {
                                mapCopy.put(logEntry.getNestedTuple(1), logEntry.getNestedTuple(2));
                            } else if (op.equals("GET")) {
                                assertEquals(logEntry.getNestedTuple(2), mapCopy.get(logEntry.getNestedTuple(1)));
                            } else if (op.equals("CONTAINS_KEY")) {
                                assertEquals(logEntry.getBoolean(2), mapCopy.containsKey(logEntry.getNestedTuple(1)));
                            } else if (op.equals("REMOVE")) {
                                Tuple oldValue = mapCopy.remove(logEntry.getNestedTuple(1));
                                assertEquals(logEntry.getNestedTuple(2), oldValue);
                            } else {
                                fail("Unexpected operation " + op);
                            }
                        }
                        assertEquals(new ArrayList<>(mapCopy.entrySet()), mapContents);
                        return mapIndex.incrementAndGet() < mapCount;
                    })).handle((res, err) -> {
                        // Report error information unless it was just a transaction timeout (in which case we'll retry).
                        FDBException fdbE = unwrapException(err);
                        if (err != null && (fdbE == null || fdbE.getCode() != FDBError.TRANSACTION_TOO_OLD.code())) {
                            System.err.println("Error verifying consistency: " + err);
                            err.printStackTrace();

                            List<Map.Entry<Tuple, Tuple>> contents = contentFuture.join();
                            System.err.println("Map contents:");
                            contents.forEach(entry -> System.err.println("  " + entry.getKey() + " -> " + entry.getValue()));

                            System.err.println("DB contents:");
                            List<KeyValue> rangeKVs = tr.getRange(bmSubspace.range()).asList().join();
                            rangeKVs.forEach(kv -> {
                                Tuple boundaryKey = bmSubspace.unpack(kv.getKey());
                                System.err.println("  " + boundaryKey + " -> " + serializer.deserializeEntries(boundaryKey, kv.getValue()));
                            });

                            List<Tuple> logEntries = logFuture.join();
                            System.err.println("Log contents:");
                            logEntries.forEach(logEntry -> System.err.println(" " + logEntry));

                            if (err instanceof RuntimeException) {
                                throw (RuntimeException) err;
                            } else {
                                throw new LoggableException("unable to complete consistency check", err);
                            }
                        }
                        return res;
                    });
                });
            }).whenComplete((v, t) -> tr.close()).thenApply(vignore -> stillWorking.get());
        });

        AtomicInteger mapIndex = new AtomicInteger(0);
        CompletableFuture<Void> compactingWorker = AsyncUtil.whileTrue(() -> {
            AtomicReference<byte[]> continuation = new AtomicReference<>(null);
            return AsyncUtil.whileTrue(() -> map.compact(db, bmSubspace.subspace(Tuple.from(mapIndex.get())), 5, continuation.get()).thenApply(nextContinuation -> {
                continuation.set(nextContinuation);
                return nextContinuation != null;
            })).thenApply(vignore -> {
                mapIndex.getAndUpdate(oldIndex -> (oldIndex + 1) % mapCount);
                return stillWorking.get();
            });
        });

        // Wait for all workers to stop working.
        AsyncUtil.whenAll(workers).whenComplete((vignore, err) -> stillWorking.set(false))
                .thenAcceptBoth(verifierWorker, (vignore1, vignore2) -> { })
                .thenAcceptBoth(compactingWorker, (vignore1, vignore2) -> { })
                .whenComplete((vignore, err) -> {
                    System.out.printf("Completed stress test with %d workers, %d keys, and %d transactions %s (large values=%s).%n",
                            workerCount, keyCount, globalTrCount.get() - initialTrCount, (err == null ? "successfully" : "with an error"), addBytesToValue);
                    if (err != null) {
                        err.printStackTrace();
                    }
                    for (int i = 0; i < mapCount; i++) {
                        System.out.println(" Map " + i + ":");
                        Subspace mapSubspace = bmSubspace.subspace(Tuple.from(i));
                        List<KeyValue> rangeKVs = inconsistentScan(db, mapSubspace);
                        System.out.println("  Boundary keys: " + rangeKVs.stream().map(kv -> mapSubspace.unpack(kv.getKey())).collect(Collectors.toList()));
                        System.out.println("  Boundary info:");
                        rangeKVs.forEach(kv -> {
                            Tuple boundaryKey = mapSubspace.unpack(kv.getKey());
                            System.out.printf("    %s: %d - %s%n",
                                    boundaryKey,
                                    serializer.deserializeEntries(boundaryKey, kv.getValue()).size(),
                                    serializer.deserializeKeys(boundaryKey, kv.getValue()));
                        });
                    }
                    int opsCount = inconsistentScan(db, logSubspace).size();
                    System.out.println("  Committed ops: " + opsCount);
                }).get();
    }

    // Compare the value of doing these operations from just doing them
    // using a straightforward map. The commented out sections make this test
    // take about 13 minutes. It "only" takes 33 seconds with the values left in.
    @Test
    @Disabled("can cause CI server to freeze because it does too much work")
    public void stressTest() throws InterruptedException, ExecutionException {
        // final int trTotal = 1000;
        final int trTotal = 50;
        final int opTotal = 10;
        final int workerCount = 10;
        final Random r = new Random();
        AtomicLong globalTrCount = new AtomicLong(0);

        // Try with successive numbers of keys.
        // final List<Integer> keyCounts = Arrays.asList(10, 20, 50, 100, 1000);
        final List<Integer> keyCounts = Arrays.asList(10, 1000);
        for (int keyCount : keyCounts) {
            stressTest(r, trTotal, opTotal, keyCount, workerCount, false, globalTrCount, 10);
            stressTest(r, trTotal, opTotal, keyCount, workerCount, true, globalTrCount, 10);
        }
    }
}
