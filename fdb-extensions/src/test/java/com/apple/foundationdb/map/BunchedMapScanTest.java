/*
 * BunchedMapScanTest.java
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
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncPeekIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for scanning in {@link BunchedMap}.
 */
@Tag(Tags.RequiresFDB)
public class BunchedMapScanTest {
    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension bmSubspaceExtension = new TestSubspaceExtension(dbExtension);
    private static Database db;
    private Subspace bmSubspace;
    private List<Subspace> subSubspaces;
    private static BunchedMap<Tuple, Tuple> map;
    private static List<Tuple> keys;
    private static Tuple value;

    private SubspaceSplitter<Long> splitter = new SubspaceSplitter<>() {
        @Nonnull
        @Override
        public Subspace subspaceOf(@Nonnull byte[] keyBytes) {
            try {
                Tuple t = bmSubspace.unpack(keyBytes);
                return bmSubspace.subspace(TupleHelpers.subTuple(t, 0, 1));
            } catch (IllegalArgumentException e) {
                System.out.println("key: " + ByteArrayUtil2.loggable(keyBytes));
                System.out.println("subspace: " + ByteArrayUtil2.loggable(bmSubspace.getKey()));
                throw e;
            }
        }

        @Nullable
        @Override
        public Long subspaceTag(@Nonnull Subspace subspace) {
            return bmSubspace.unpack(subspace.getKey()).getLong(0);
        }
    };

    @BeforeAll
    public static void setup() throws InterruptedException, ExecutionException {
        db = dbExtension.getDatabase();
        map = new BunchedMap<>(BunchedTupleSerializer.instance(), Comparator.naturalOrder(), 10);
        keys = LongStream.range(100L, 500L).boxed().map(Tuple::from).collect(Collectors.toList());
        value = Tuple.from(1066L);
    }

    @BeforeEach
    void setUp() {
        bmSubspace = bmSubspaceExtension.getSubspace();
        subSubspaces = LongStream.range(0L, 50L).boxed().map(l -> bmSubspace.subspace(Tuple.from(l))).collect(Collectors.toList());
    }

    private void clearAndPopulate() {
        // Populate data
        db.run(tr -> {
            tr.clear(bmSubspace.range());
            keys.forEach(k -> map.put(tr, bmSubspace, k, value).join());
            return null;
        });
    }

    private void clearAndPopulateMulti() {
        db.run(tr -> {
            tr.clear(bmSubspace.range());
            for (int i = 0; i < keys.size(); i++) {
                map.put(tr, subSubspaces.get(i % subSubspaces.size()), keys.get(i), value).join();
            }
            return null;
        });
    }

    private void testScan(int limit, boolean reverse, @Nonnull BiFunction<Transaction, byte[], BunchedMapIterator<Tuple, Tuple>> iteratorFunction) {
        try (Transaction tr = db.createTransaction()) {
            byte[] continuation = null;
            List<Tuple> readKeys = new ArrayList<>();
            Tuple lastKey = null;
            do {
                int returned = 0;
                BunchedMapIterator<Tuple, Tuple> bunchedMapIterator = iteratorFunction.apply(tr, continuation);
                while (bunchedMapIterator.hasNext()) {
                    Tuple toAdd = bunchedMapIterator.peek().getKey();
                    readKeys.add(toAdd);
                    assertEquals(toAdd, bunchedMapIterator.next().getKey());
                    if (lastKey != null) {
                        assertEquals(reverse ? 1 : -1, lastKey.compareTo(toAdd));
                    }
                    lastKey = toAdd;
                    returned += 1;
                }
                assertFalse(bunchedMapIterator.hasNext());
                assertThrows(NoSuchElementException.class, bunchedMapIterator::peek);
                assertThrows(NoSuchElementException.class, bunchedMapIterator::next);
                continuation = bunchedMapIterator.getContinuation();
                if (limit == ReadTransaction.ROW_LIMIT_UNLIMITED || returned < limit) {
                    assertNull(continuation);
                } else {
                    assertNotNull(continuation);
                }
            } while (continuation != null);
            if (reverse) {
                readKeys = Lists.reverse(readKeys);
            }
            assertEquals(keys, readKeys);
            tr.cancel();
        }
    }

    private void getKeys(boolean reverse) throws InterruptedException, ExecutionException {
        testScan(ReadTransaction.ROW_LIMIT_UNLIMITED, reverse, (tr, bignore) -> map.scan(tr, bmSubspace, null, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse));
    }

    @Test
    public void getKeys() throws InterruptedException, ExecutionException {
        clearAndPopulate();
        getKeys(false);
        getKeys(true);
    }

    private void getKeysContinuationRescan(int limit, boolean reverse) {
        testScan(limit, reverse, (tr, continuation) -> {
            Tuple continuationKey = continuation == null ? null : Tuple.fromBytes(continuation);
            return new BunchedMapIterator<>(
                    AsyncPeekIterator.wrap(tr.getRange(bmSubspace.range(), ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator()),
                    tr,
                    bmSubspace,
                    bmSubspace.getKey(),
                    map,
                    continuationKey,
                    limit,
                    reverse
            );
        });
    }

    private void getKeysContinuation(int limit, boolean reverse) {
        testScan(limit, reverse, (tr, continuation) -> map.scan(tr, bmSubspace, continuation, limit, reverse));
    }

    private void getKeysContinuation(@Nonnull Transaction tr, boolean reverse) throws InterruptedException, ExecutionException {
        getKeysContinuation(10, reverse); // Bunch size limit
        getKeysContinuation(5, reverse); // Limit is half of bunch size
        getKeysContinuation(7, reverse); // Limit that doesn't hit the boundary well.
        getKeysContinuation(1, reverse); // Limit of 1 to test every key

        getKeysContinuationRescan(10, reverse); // Bunch size limit
        getKeysContinuationRescan(5, reverse); // Limit is half of bunch size
        getKeysContinuationRescan(7, reverse); // Limit that doesn't hit the boundary well.
        getKeysContinuationRescan(1, reverse); // Limit of 1 to test every key

        // Unlimited should return null continuation.
        BunchedMapIterator<Tuple, Tuple> iterator = map.scan(tr, bmSubspace, null, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
        List<Tuple> readKeys = AsyncUtil.collectRemaining(AsyncUtil.mapIterator(iterator, Map.Entry::getKey)).get();
        if (reverse) {
            readKeys = Lists.reverse(readKeys);
        }
        assertEquals(keys, readKeys);
        assertNull(iterator.getContinuation());

        // Limited that returns everything because it is is limit aligned should return a non-null continuation.
        iterator = map.scan(tr, bmSubspace, null, keys.size(), reverse);
        readKeys = AsyncUtil.collectRemaining(AsyncUtil.mapIterator(iterator, Map.Entry::getKey)).get();
        if (reverse) {
            readKeys = Lists.reverse(readKeys);
        }
        assertEquals(keys, readKeys);
        assertNotNull(iterator.getContinuation());
        iterator = map.scan(tr, bmSubspace, iterator.getContinuation(), ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
        assertFalse(iterator.hasNext());

        // Limited with a limit greater than the actual number of keys should return a null continuation.
        iterator = map.scan(tr, bmSubspace, null, keys.size() + 1, reverse);
        readKeys = AsyncUtil.collectRemaining(AsyncUtil.mapIterator(iterator, Map.Entry::getKey)).get();
        if (reverse) {
            readKeys = Lists.reverse(readKeys);
        }
        assertEquals(keys, readKeys);
        assertNull(iterator.getContinuation());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void getKeysContinuation() throws InterruptedException, ExecutionException {
        clearAndPopulate();
        try (Transaction tr = db.createTransaction()) {
            getKeysContinuation(tr, false);
            getKeysContinuation(tr, true);
        }
    }

    @Test
    public void scanWithConflict() throws InterruptedException, ExecutionException {
        clearAndPopulate();
        try (Transaction tr1 = db.createTransaction(); Transaction tr2 = db.createTransaction()) {
            CompletableFuture.allOf(tr1.getReadVersion(), tr2.getReadVersion()).get();

            BunchedMapIterator<Tuple, Tuple> iterator = map.scan(tr1, bmSubspace);
            int count = MoreAsyncUtil.reduce(iterator, 0, (oldCount, item) -> oldCount + 1).get();
            assertEquals(keys.size(), count);
            tr1.addWriteConflictKey(Tuple.from(count).pack());

            assertFalse(map.put(tr2, bmSubspace, Tuple.from(keys.get(keys.size() - 1).getLong(0) + 1), value).get().isPresent());

            tr2.commit().get();
            CompletionException e = assertThrows(CompletionException.class, () -> tr1.commit().join());
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof FDBException);
            FDBException fdbE = (FDBException)e.getCause();
            assertEquals(FDBError.NOT_COMMITTED.code(), fdbE.getCode());
        }
        byte[] continuation = null;
        for (int i = 0; i < keys.size(); i++) {
            
        }
        try (Transaction tr1 = db.createTransaction(); Transaction tr2 = db.createTransaction()) {
            CompletableFuture.allOf(tr1.getReadVersion(), tr2.getReadVersion()).get();

            BunchedMapIterator<Tuple, Tuple> iterator = map.scan(tr1, bmSubspace);

        }
    }

    private void continuationWithDeletes(int limit, boolean reverse) {
        try (Transaction tr = db.createTransaction()) {
            byte[] continuation = null;
            List<Tuple> readKeys = new ArrayList<>();
            do {
                List<Tuple> mostRecentReadKeys = new ArrayList<>();
                int returned = 0;
                BunchedMapIterator<Tuple, Tuple> bunchedMapIterator = map.scan(tr, subSubspaces.get(1), continuation, limit, reverse);
                while (bunchedMapIterator.hasNext()) {
                    Tuple toAdd = bunchedMapIterator.peek().getKey();
                    assertEquals(toAdd, bunchedMapIterator.next().getKey());
                    mostRecentReadKeys.add(toAdd);
                    returned += 1;
                }
                assertFalse(bunchedMapIterator.hasNext());
                assertThrows(NoSuchElementException.class, bunchedMapIterator::peek);
                assertThrows(NoSuchElementException.class, bunchedMapIterator::next);
                continuation = bunchedMapIterator.getContinuation();
                if (returned != limit) {
                    assertNull(continuation);
                } else {
                    assertNotNull(continuation);
                }
                // Remove all of the keys that were most recently read.
                mostRecentReadKeys.forEach(k -> map.remove(tr, subSubspaces.get(1), k).join());
                readKeys.addAll(mostRecentReadKeys);
            } while (continuation != null);
            if (reverse) {
                readKeys = Lists.reverse(readKeys);
            }
            List<Tuple> expectedKeys = IntStream.range(0, keys.size())
                    .filter(i -> i % subSubspaces.size() == 1)
                    .mapToObj(keys::get)
                    .collect(Collectors.toList());
            assertEquals(expectedKeys, readKeys);
            tr.cancel();
        }
    }

    private void continuationWithDeletes(boolean reverse) {
        continuationWithDeletes(50, reverse);
        continuationWithDeletes(10, reverse);
        continuationWithDeletes(5, reverse);
        continuationWithDeletes(7, reverse);
        continuationWithDeletes(1, reverse);
    }

    @Test
    public void continuationWithDeletes() {
        clearAndPopulateMulti();
        continuationWithDeletes(false);
        continuationWithDeletes(true);
    }

    private void testScanMulti(int limit, boolean reverse, List<List<Tuple>> keyLists,
                               @Nonnull BiFunction<Transaction, byte[], BunchedMapMultiIterator<Tuple, Tuple, Long>> iteratorFunction) {
        try (Transaction tr = db.createTransaction()) {
            byte[] continuation = null;
            List<BunchedMapScanEntry<Tuple, Tuple, Long>> entryList = new ArrayList<>();
            BunchedMapScanEntry<Tuple, Tuple, Long> lastEntry = null;
            do {
                BunchedMapMultiIterator<Tuple, Tuple, Long> iterator = iteratorFunction.apply(tr, continuation);
                int returned = 0;
                while (iterator.hasNext()) {
                    BunchedMapScanEntry<Tuple, Tuple, Long> toAdd = iterator.peek();
                    assertEquals(toAdd, iterator.next());
                    if (lastEntry != null) {
                        if (toAdd.getSubspaceTag().equals(lastEntry.getSubspaceTag())) {
                            assertEquals(reverse ? 1 : -1, Integer.signum(lastEntry.getKey().compareTo(toAdd.getKey())));
                        } else {
                            assertEquals(reverse ? 1 : -1, Integer.signum(lastEntry.getSubspaceTag().compareTo(toAdd.getSubspaceTag())));
                        }
                    }
                    entryList.add(toAdd);
                    lastEntry = toAdd;
                    returned++;
                }
                continuation = iterator.getContinuation();
                if (limit == ReadTransaction.ROW_LIMIT_UNLIMITED || returned < limit) {
                    assertNull(continuation);
                } else {
                    assertNotNull(continuation);
                }
            } while (continuation != null);
            if (reverse) {
                entryList = Lists.reverse(entryList);
            }
            Long tag = null;
            int pos = 0;
            int totalRead = 0;
            for (BunchedMapScanEntry<Tuple, Tuple, Long> entry : entryList) {
                if (tag == null || !tag.equals(entry.getSubspaceTag())) {
                    if (tag != null) {
                        assertEquals(tag + 1, entry.getSubspaceTag().longValue());
                    }
                    tag = entry.getSubspaceTag();
                    pos = 0;
                }
                assertEquals(keyLists.get(tag.intValue()).get(pos), entry.getKey());
                assertEquals(value, entry.getValue());
                assertEquals(subSubspaces.get(tag.intValue()), entry.getSubspace());
                pos++;
                totalRead++;
            }
            int totalToSee = keyLists.stream().mapToInt(List::size).sum();
            assertEquals(totalToSee, totalRead);
        }
    }

    private void scanFullMulti(int limit, boolean reverse, List<List<Tuple>> keyLists) throws InterruptedException, ExecutionException {
        testScanMulti(limit, reverse, keyLists, (tr, continuation) -> map.scanMulti(tr, bmSubspace, splitter, continuation, limit, reverse));
        testScanMulti(limit, reverse, keyLists, (tr, continuation) ->
                new BunchedMapMultiIterator<>(
                        AsyncPeekIterator.wrap(tr.getRange(bmSubspace.range(), ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator()),
                        tr,
                        bmSubspace,
                        bmSubspace.getKey(),
                        splitter,
                        map,
                        continuation,
                        limit,
                        reverse
                )
        );
    }

    private void scanOneMulti(int limit, boolean reverse, List<List<Tuple>> keyLists) throws InterruptedException, ExecutionException {
        byte[] start = Tuple.from(1L).pack();
        byte[] end = Tuple.from(2L).pack();
        testScanMulti(limit, reverse, keyLists, (tr, continuation) -> map.scanMulti(tr, bmSubspace, splitter, start, end, continuation, limit, reverse));
        testScanMulti(limit, reverse, keyLists, (tr, continuation) ->
                new BunchedMapMultiIterator<>(
                        AsyncPeekIterator.wrap(tr.getRange(bmSubspace.pack(1L), bmSubspace.pack(2L), ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator()),
                        tr,
                        bmSubspace,
                        bmSubspace.getKey(),
                        splitter,
                        map,
                        continuation,
                        limit,
                        reverse
                )
        );
    }

    private void scanOneByteMulti(int limit, boolean reverse, List<List<Tuple>> keyLists) throws InterruptedException, ExecutionException {
        byte[] one = Tuple.from(1L).pack();
        byte[] start = new byte[]{one[0]};
        byte[] end = new byte[]{(byte)(one[0] + 1)};
        testScanMulti(limit, reverse, keyLists, (tr, continuation) -> map.scanMulti(tr, bmSubspace, splitter, start, end, continuation, limit, reverse));

        byte[] fullStart = ByteArrayUtil.join(bmSubspace.getKey(), start);
        byte[] fullEnd = ByteArrayUtil.join(bmSubspace.getKey(), end);
        testScanMulti(limit, reverse, keyLists, (tr, continuation) ->
                new BunchedMapMultiIterator<>(
                        AsyncPeekIterator.wrap(tr.getRange(fullStart, fullEnd, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator()),
                        tr,
                        bmSubspace,
                        bmSubspace.getKey(),
                        splitter,
                        map,
                        continuation,
                        limit,
                        reverse
                )
        );
    }

    private void scanEmptyRangeMulti(int limit, boolean reverse) throws InterruptedException, ExecutionException {
        byte[] start = Tuple.from(subSubspaces.size() + 1).pack();
        testScanMulti(limit, reverse, Collections.emptyList(), (tr, continuation) -> map.scanMulti(tr, bmSubspace, splitter, start, null, continuation, limit, reverse));
        byte[] fullStart = ByteArrayUtil.join(bmSubspace.getKey(), start);
        byte[] fullEnd = bmSubspace.range().end;
        testScanMulti(limit, reverse, Collections.emptyList(), (tr, continuation) ->
                new BunchedMapMultiIterator<>(
                        AsyncPeekIterator.wrap(tr.getRange(fullStart, fullEnd, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator()),
                        tr,
                        bmSubspace,
                        bmSubspace.getKey(),
                        splitter,
                        map,
                        continuation,
                        limit,
                        reverse
                )
        );

        byte[] end = Tuple.from(-1).pack();
        testScanMulti(limit, reverse, Collections.emptyList(), (tr, continuation) -> map.scanMulti(tr, bmSubspace, splitter, null, end, continuation, limit, reverse));
        byte[] fullStart2 = bmSubspace.range().begin;
        byte[] fullEnd2 = ByteArrayUtil.join(bmSubspace.getKey(), end);
        testScanMulti(limit, reverse, Collections.emptyList(), (tr, continuation) ->
                new BunchedMapMultiIterator<>(
                        AsyncPeekIterator.wrap(tr.getRange(fullStart2, fullEnd2, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse).iterator()),
                        tr,
                        bmSubspace,
                        bmSubspace.getKey(),
                        splitter,
                        map,
                        continuation,
                        limit,
                        reverse
                )
        );
    }

    private void scanTagAligned(boolean reverse, List<List<Tuple>> keyLists) throws InterruptedException, ExecutionException {
        for (int i = 0; i < keyLists.size(); i++) {
            final int itr = i;
            List<List<Tuple>> projectedLists = new ArrayList<>(keyLists.size());
            for (int j = 0; j < keyLists.size(); j++) {
                if (i == j) {
                    projectedLists.add(keyLists.get(i));
                } else {
                    projectedLists.add(Collections.emptyList());
                }
            }

            AtomicInteger loops = new AtomicInteger(0);
            testScanMulti(keyLists.get(i).size(), reverse, projectedLists, (tr, continuation) -> {
                loops.incrementAndGet();
                return map.scanMulti(tr, bmSubspace, splitter, Tuple.from(itr).pack(), Tuple.from(itr + 1).pack(), continuation, keyLists.get(itr).size(), reverse);
            });
            assertEquals(2, loops.get());
            loops.set(0);
        }

        if (keyLists.stream().map(List::size).distinct().count() == 1L) {
            int limit = keyLists.get(0).size();
            AtomicInteger loops = new AtomicInteger(0);
            testScanMulti(limit, reverse, keyLists, (tr, continuation) -> {
                loops.incrementAndGet();
                return map.scanMulti(tr, bmSubspace, splitter, continuation, limit, reverse);
            });
            assertEquals(keyLists.size() + 1, loops.get());
            loops.set(0);
            testScanMulti(limit, reverse, keyLists, (tr, continuation) -> {
                int iteration = loops.getAndIncrement();
                if (iteration != 0) {
                    assertNotNull(continuation);
                } else {
                    assertNull(continuation);
                }
                // Here, the limit goes out in front of the continuation, so we note that the continuation
                // is already satisfied and don't do more work.
                if (reverse) {
                    return map.scanMulti(tr, bmSubspace, splitter, null, Tuple.from(keyLists.size() - iteration).pack(), continuation, limit, true);
                } else {
                    return map.scanMulti(tr, bmSubspace, splitter, Tuple.from(iteration).pack(), null, continuation, limit, false);
                }
            });
            assertEquals(keyLists.size() + 1, loops.get());
        }
    }

    @Test
    public void scanMulti() throws InterruptedException, ExecutionException {
        scanMultiTest(false);
    }

    @Test
    public void scanMultiReversed() throws InterruptedException, ExecutionException {
        scanMultiTest(true);
    }

    private void scanMultiTest(boolean reversed) throws InterruptedException, ExecutionException {
        clearAndPopulateMulti();
        final List<Integer> limits = Arrays.asList(ReadTransaction.ROW_LIMIT_UNLIMITED, 100, 50, 10, 7, 1);

        final List<List<Tuple>> keyLists = Stream.generate((Supplier<ArrayList<Tuple>>) ArrayList::new).limit(subSubspaces.size()).collect(Collectors.toList());
        for (int i = 0; i < keys.size(); i++) {
            keyLists.get(i % subSubspaces.size()).add(keys.get(i));
        }

        for (int limit : limits) {
            scanFullMulti(limit, reversed, keyLists);
        }

        // Scan over only those beginning with 1L.
        final List<List<Tuple>> keyLists1 = Arrays.asList(Collections.emptyList(), keyLists.get(1));
        for (int limit : limits) {
            scanOneMulti(limit, reversed, keyLists1);
        }

        // Scan over those beginning with the type code indicating a 1 byte long
        final List<List<Tuple>> keyLists1Byte = IntStream.range(0, subSubspaces.size())
                .mapToObj(i -> (Tuple.from(i).pack().length == 2 ? keyLists.get(i) : Collections.<Tuple>emptyList()))
                .collect(Collectors.toList());
        for (int limit : limits) {
            scanOneByteMulti(limit, reversed, keyLists1Byte);
        }

        for (int limit : limits) {
            scanEmptyRangeMulti(limit, reversed);
        }

        scanTagAligned(reversed, keyLists);

    }
}
