/*
 * RankedSet.java
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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.async.AsyncUtil.DONE;
import static com.apple.foundationdb.async.AsyncUtil.READY_FALSE;

/**
 * RankedSet supports the efficient retrieval of elements by their rank as
 * defined by lexiographic order.
 *
 * <p>
 * Elements are added or removed from the set by key. The rank of any element
 * can then be quickly determined, and an element can be quickly retrieved by
 * its rank.
 * </p>
 */
public class RankedSet
{
    // TODO: Constructor arg? Almost certainly too small for large sets.
    private static final int MAX_LEVELS = 6;
    private static final int LEVEL_FAN_POW = 4;
    private static final int[] LEVEL_FAN_VALUES; // 2^(l * FAN) - 1 per level

    static {
        LEVEL_FAN_VALUES = new int[MAX_LEVELS];
        for (int i = 0; i < MAX_LEVELS; ++i) {
            LEVEL_FAN_VALUES[i] = (int)Math.pow(2, i * LEVEL_FAN_POW) - 1;
        }
    }

    private static final byte[] ZERO_ARRAY = new byte[] { 0 };

    private static byte[] encodeLong(long count) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(count).array();
    }

    private static long decodeLong(byte[] v) {
        return ByteBuffer.wrap(v).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    protected final Subspace subspace;
    protected final Executor executor;

    public RankedSet(Subspace subspace, Executor executor) {
        this.subspace = subspace;
        this.executor = executor;
    }

    public CompletableFuture<Void> init(TransactionContext tc) {
        return initLevels(tc);
    }

    /**
     * Add a key to the set.
     * @param tc the transaction to use to access the database
     * @param key the key to add
     * @return a future that completes to {@code true} if the key was not already present
     */
    public CompletableFuture<Boolean> add(TransactionContext tc, byte[] key) {
        checkKey(key);
        // TODO: Does using the hash of the key, instead a p value and randomLevel, bias this in any undesirable way?
        long keyHash = hashKey(key);
        return tc.runAsync(tr ->
            contains(tr, key)
                .thenCompose(exists -> {
                    if (exists) {
                        return READY_FALSE;
                    }
                    List<CompletableFuture<Void>> futures = new ArrayList<>(MAX_LEVELS);
                    for (int li = 0; li < MAX_LEVELS; ++li) {
                        int level = li;
                        CompletableFuture<Void> future;
                        if (level == 0) {
                            tr.set(subspace.pack(Tuple.from(level, key)), encodeLong(1));
                            future = DONE;
                        } else if ((keyHash & LEVEL_FAN_VALUES[level]) != 0) {
                            future = getPreviousKey(tr, level, key).thenApply(prevKey -> {
                                tr.mutate(MutationType.ADD, subspace.pack(Tuple.from(level, prevKey)), encodeLong(1));
                                return null;
                            });
                        } else {
                            // Insert into this level by looking at the count of the previous key in the level
                            // and recounting the next lower level to correct the counts.
                            // Must complete lower levels first for count to be accurate.
                            future = AsyncUtil.whenAll(futures);
                            futures = new ArrayList<>(MAX_LEVELS - li);
                            future = future.thenCompose(vignore ->
                                getPreviousKey(tr, level, key).thenCompose(prevKey -> {
                                    CompletableFuture<Long> prevCount = tr.get(subspace.pack(Tuple.from(level, prevKey))).thenApply(RankedSet::decodeLong);
                                    CompletableFuture<Long> newPrevCount = countRange(tr, level - 1, prevKey, key);
                                    return CompletableFuture.allOf(prevCount, newPrevCount)
                                        .thenApply(vignore2 -> {
                                            long count = prevCount.join() - newPrevCount.join() + 1;
                                            tr.set(subspace.pack(Tuple.from(level, prevKey)), encodeLong(newPrevCount.join()));
                                            tr.set(subspace.pack(Tuple.from(level, key)), encodeLong(count));
                                            return null;
                                        });
                                }));
                        }
                        futures.add(future);
                    }
                    return AsyncUtil.whenAll(futures).thenApply(vignore -> true);
                }));
    }

    /**
     * Clears the entire set.
     * @param tc the transaction to use to access the database
     * @return a future that completes when the ranked set has been cleared
     */
    public CompletableFuture<Void> clear(TransactionContext tc) {
        Range range = subspace.range();
        return tc.runAsync(tr -> {
            tr.clear(range);
            return initLevels(tr);
        });
    }

    /**
     * Checks for the presence of a key in the set.
     * @param tc the transaction to use to access the database
     * @param key the key to check for
     * @return a future that completes to {@code true} if the key is present in the ranked set
     */
    public CompletableFuture<Boolean> contains(ReadTransactionContext tc, byte[] key) {
        checkKey(key);
        return tc.readAsync(tr -> tr.get(subspace.pack(Tuple.from(0, key))).thenApply(b -> b != null));
    }

    class NthLookup implements Lookup {
        private long rank;
        private byte[] key = new byte[0];
        private int level = MAX_LEVELS;
        private Subspace levelSubspace;
        private AsyncIterator<KeyValue> asyncIterator = null;

        public NthLookup(long rank) {
            this.rank = rank;
        }

        public byte[] getKey() {
            return key;
        }

        @Override
        public CompletableFuture<Boolean> next(ReadTransaction tr) {
            final boolean newIterator = asyncIterator == null;
            if (newIterator) {
                level--;
                if (level < 0) {
                    // Down to finest level without finding enough.
                    key = null;
                    return READY_FALSE;
                }
                levelSubspace = subspace.get(level);
                asyncIterator = lookupIterator(tr.getRange(levelSubspace.pack(key), levelSubspace.range().end,
                        ReadTransaction.ROW_LIMIT_UNLIMITED,
                        false,
                        StreamingMode.WANT_ALL));
            }
            final long startTime = System.nanoTime();
            final CompletableFuture<Boolean> onHasNext = asyncIterator.onHasNext();
            final boolean wasDone = onHasNext.isDone();
            return onHasNext.thenApply(hasNext -> {
                if (!wasDone) {
                    nextLookupKey(System.nanoTime() - startTime, newIterator, hasNext, level, false);
                }
                if (!hasNext) {
                    // Not enough on this level.
                    key = null;
                    return false;
                }
                KeyValue kv = asyncIterator.next();
                key = levelSubspace.unpack(kv.getKey()).getBytes(0);
                if (rank == 0 && key.length > 0) {
                    // Moved along correct rank, this is the key.
                    return false;
                }
                long count = decodeLong(kv.getValue());
                if (count > rank) {
                    // Narrow search in next finer level.
                    asyncIterator = null;
                    return true;
                }
                rank -= count;
                return true;
            });
        }
    }

    /**
     * Return the Nth item in the set.
     * This operation is also referred to as <i>select</i>.
     * @param tc the transaction to use to access the database
     * @param rank the rank index to find
     * @return a future that completes to the key for the {@code rank}th item or {@code null} if that index is out of bounds
     * @see #rank
     */
    public CompletableFuture<byte[]> getNth(ReadTransactionContext tc, long rank) {
        if (rank < 0) {
            return CompletableFuture.completedFuture((byte[])null);
        }
        return tc.readAsync(tr -> {
            NthLookup nth = new NthLookup(rank);
            return AsyncUtil.whileTrue(() -> nextLookup(nth, tr), executor).thenApply(vignore -> nth.getKey());
        });
    }

    /**
     * Returns the ordered set of keys in a given range.
     * @param tc the transaction to use to access the database
     * @param beginKey the (inclusive) lower bound for the range
     * @param endKey the (exclusive) upper bound for the range
     * @return a list of keys in the ranked set within the given range
     */
    public List<byte[]> getRangeList(ReadTransactionContext tc, byte[] beginKey, byte[] endKey) {
        return tc.read(tr -> getRange(tr, beginKey, endKey).asList().join());
    }

    public AsyncIterable<byte[]> getRange(ReadTransaction tr, byte[] beginKey, byte[] endKey) {
        checkKey(beginKey);
        return AsyncUtil.mapIterable(tr.getRange(subspace.pack(Tuple.from(0, beginKey)),
                subspace.pack(Tuple.from(0, endKey))),
                keyValue -> {
                    Tuple t = subspace.unpack(keyValue.getKey());
                    return t.getBytes(1);
                });
    }

    /**
     * Read the deeper, likely empty, levels to get them into the RYW cache, since individual lookups may only
     * add pieces, requiring additional requests as keys increase.
     * @param tr the transaction to use to access the database
     * @return a future that is complete when the deeper levels have been loaded
     */
    public CompletableFuture<Void> preloadForLookup(ReadTransaction tr) {
        return tr.getRange(subspace.range(), MAX_LEVELS, true).asList().thenApply(l -> null);
    }

    protected CompletableFuture<Boolean> nextLookup(Lookup lookup, ReadTransaction tr) {
        return lookup.next(tr);
    }

    protected <T> AsyncIterator<T> lookupIterator(AsyncIterable<T> iterable) {
        return iterable.iterator();
    }

    protected void nextLookupKey(long duration, boolean newIter, boolean hasNext, int level, boolean rankLookup) {
    }

    protected interface Lookup {
        public CompletableFuture<Boolean> next(ReadTransaction tr);
    }

    class RankLookup implements Lookup {
        private byte[] key;
        private byte[] rankKey = new byte[0];
        private long rank = 0;
        private Subspace levelSubspace;
        private int level = MAX_LEVELS;
        private AsyncIterator<KeyValue> asyncIterator = null;
        private long lastCount;

        public RankLookup(byte[] key) {
            this.key = key;
        }

        public long getRank() {
            return rank;
        }

        @Override
        public CompletableFuture<Boolean> next(ReadTransaction tr) {
            final boolean newIterator = asyncIterator == null;
            if (newIterator) {
                level--;
                if (level < 0) {
                    // Finest level: rank is accurate.
                    return READY_FALSE;
                }
                levelSubspace = subspace.get(level);
                asyncIterator = lookupIterator(tr.getRange(
                        KeySelector.firstGreaterOrEqual(levelSubspace.pack(rankKey)),
                        KeySelector.firstGreaterThan(levelSubspace.pack(key)),
                        ReadTransaction.ROW_LIMIT_UNLIMITED,
                        false,
                        StreamingMode.WANT_ALL));
                lastCount = 0;
            }
            final long startTime = System.nanoTime();
            final CompletableFuture<Boolean> onHasNext = asyncIterator.onHasNext();
            final boolean wasDone = onHasNext.isDone();
            return onHasNext.thenApply(hasNext -> {
                if (!wasDone) {
                    nextLookupKey(System.nanoTime() - startTime, newIterator, hasNext, level, true);
                }
                if (!hasNext) {
                    // Totalled this level: move to next.
                    asyncIterator = null;
                    rank -= lastCount;
                    if (Arrays.equals(rankKey, key)) {
                        // Exact match on this level: no need for finer.
                        return false;
                    }
                    return true;
                }
                KeyValue kv = asyncIterator.next();
                rankKey = levelSubspace.unpack(kv.getKey()).getBytes(0);
                lastCount = decodeLong(kv.getValue());
                rank += lastCount;
                return true;
            });

        }
    }

    /**
     * Return the index of a key within the set.
     * @param tc the transaction to use to access the database
     * @param key the key to find
     * @return a future that completes to the index of {@code key} in the ranked set or {@code null} if it is not present
     * @see #getNth
     */
    public CompletableFuture<Long> rank(ReadTransactionContext tc, byte[] key) {
        checkKey(key);
        return tc.readAsync(tr ->
            contains(tr, key).thenCompose(exists -> {
                if (!exists) {
                    return CompletableFuture.completedFuture((Long)null);
                }
                RankLookup rank = new RankLookup(key);
                return AsyncUtil.whileTrue(() -> nextLookup(rank, tr), executor).thenApply(vignore -> rank.getRank());
            }));
    }

    /**
     * Removes a key from the set.
     * @param tc the transaction to use to access the database
     * @param key the key to remove
     * @return a future that completes to {@code true} if the key was present before this operation
     */
    public CompletableFuture<Boolean> remove(TransactionContext tc, byte[] key) {
        checkKey(key);
        return tc.runAsync(tr ->
                contains(tr, key)
                        .thenCompose(exists -> {
                            if (!exists) {
                                return READY_FALSE;
                            }
                            List<CompletableFuture<Void>> futures = new ArrayList<>(MAX_LEVELS);
                            for (int li = 0; li < MAX_LEVELS; ++li) {
                                int level = li;
                                // This could be optimized with hash (?? Comment from Ruby version)
                                byte[] k = subspace.pack(Tuple.from(level, key));

                                CompletableFuture<Void> future;
                                CompletableFuture<byte[]> cf = tr.get(k);

                                if (level == 0) {
                                    future = cf.thenApply(c -> {
                                        if (c != null) {
                                            tr.clear(k);
                                        }
                                        return null;
                                    });
                                } else {
                                    CompletableFuture<byte[]> prevKeyF = getPreviousKey(tr, level, key);
                                    future = CompletableFuture.allOf(cf, prevKeyF)
                                            .thenApply(vignore -> {
                                                byte[] c = cf.join();
                                                long countChange = -1;
                                                if (c != null) {
                                                    countChange += decodeLong(c);
                                                    tr.clear(k);
                                                }
                                                tr.mutate(MutationType.ADD, subspace.pack(Tuple.from(level, prevKeyF.join())), encodeLong(countChange));
                                                return null;
                                            });
                                }
                                futures.add(future);
                            }
                            return AsyncUtil.whenAll(futures).thenApply(vignore -> true);
                        }));
    }

    /**
     * Count the items in the set.
     * @param tc the transaction to use to access the database
     * @return a future that completes to the number of items in the set
     */
    public CompletableFuture<Long> size(ReadTransactionContext tc) {
        Range r = subspace.get(MAX_LEVELS - 1).range();
        return tc.readAsync(tr -> AsyncUtil.mapIterable(tr.getRange(r), keyValue -> decodeLong(keyValue.getValue()))
                .asList()
                .thenApply(longs -> longs.stream().reduce(0L, Long::sum)));
    }

    protected Consistency checkConsistency(ReadTransactionContext tc) {
        return tc.read(tr -> {
            for (int level = 1; level < MAX_LEVELS; ++level) {
                byte[] prevKey = null;
                long prevCount = 0;
                AsyncIterator<KeyValue> it = tr.getRange(subspace.range(Tuple.from(level))).iterator();
                while (true) {
                    boolean more = it.hasNext();

                    KeyValue kv = more ? it.next() : null;
                    byte[] nextKey = kv == null ? null : subspace.unpack(kv.getKey()).getBytes(1);
                    if (prevKey != null) {
                        long count = countRange(tr, level - 1, prevKey, nextKey).join();
                        if (prevCount != count) {
                            return new Consistency(level, prevCount, count, toDebugString(tc));
                        }
                    }
                    if (!more) {
                        break;
                    }
                    prevKey = nextKey;
                    prevCount = decodeLong(kv.getValue());
                }
            }
            return new Consistency();
        });
    }

    protected String toDebugString(ReadTransactionContext tc) {
        return tc.read(tr -> {
            StringBuilder str = new StringBuilder();
            for (int level = 0; level < MAX_LEVELS; ++level) {
                if (level > 0) {
                    str.setLength(str.length() - 2);
                    str.append("\n");
                }
                str.append("L").append(level).append(": ");
                for (KeyValue kv : tr.getRange(subspace.range(Tuple.from(level)))) {
                    byte[] key = subspace.unpack(kv.getKey()).getBytes(1);
                    long count = decodeLong(kv.getValue());
                    str.append("'").append(ByteArrayUtil2.loggable(key)).append("': ").append(count).append(", ");
                }
            }
            return str.toString();
        });
    }

    //
    // Internal
    //

    private static void checkKey(byte[] key) {
        if (key.length == 0) {
            throw new IllegalArgumentException("Empty key not allowed");
        }
    }

    private CompletableFuture<Long> countRange(ReadTransactionContext tc, int level, byte[] beginKey, byte[] endKey) {
        return tc.readAsync(tr ->
                AsyncUtil.mapIterable(tr.getRange(beginKey == null ?
                                subspace.range(Tuple.from(level)).begin :
                                subspace.pack(Tuple.from(level, beginKey)),
                        endKey == null ?
                                subspace.range(Tuple.from(level)).end :
                                subspace.pack(Tuple.from(level, endKey))),
                        keyValue -> decodeLong(keyValue.getValue()))
                        .asList()
                        .thenApply(longs -> longs.stream().reduce(0L, Long::sum)));
    }

    private CompletableFuture<byte[]> getPreviousKey(TransactionContext tc, int level, byte[] key) {
        byte[] k = subspace.pack(Tuple.from(level, key));
        CompletableFuture<byte[]> kf = tc.run(tr ->
                tr.snapshot()
                        .getRange(KeySelector.lastLessThan(k), KeySelector.firstGreaterOrEqual(k), 1)
                        .asList()
                        .thenApply(kvs -> {
                            byte[] prevk = kvs.get(0).getKey();
                            // If another key were inserted after between this and the target key,
                            // it wouldn't be the one we should increment any more.
                            // But do not conflict when key itself is incremented.
                            byte[] exclusiveBegin = ByteArrayUtil.join(prevk, ZERO_ARRAY);
                            tr.addReadConflictRange(exclusiveBegin, k);
                            // Do conflict if key is removed entirely.
                            tr.addReadConflictKey(subspace.pack(Tuple.from(0, subspace.unpack(prevk).getBytes(1))));
                            return prevk;
                        }));
        return kf.thenApply(prevk -> subspace.unpack(prevk).getBytes(1));
    }

    private long hashKey(byte[] k) {
        return Arrays.hashCode(k);
    }

    private CompletableFuture<Void> initLevels(TransactionContext tc) {
        return tc.runAsync(tr -> {
            List<CompletableFuture<Void>> futures = new ArrayList<>(MAX_LEVELS);
            for (int level = 0; level < MAX_LEVELS; ++level) {
                byte[] k = subspace.pack(Tuple.from(level, new byte[0]));
                byte[] v = encodeLong(0);
                futures.add(tr.get(k).thenApply(value -> {
                    if (value == null) {
                        tr.set(k, v);
                    }
                    return null;
                }));
            }
            return AsyncUtil.whenAll(futures);
        });
    }

    protected static class Consistency {


        private final boolean consistent;
        private final int level;
        private final long prevCount;
        private final long count;
        private String structure;

        public Consistency(int level, long prevCount, long count, String structure) {
            this.level = level;
            this.prevCount = prevCount;
            this.count = count;
            this.structure = structure;
            consistent = false;
        }

        public Consistency() {
            consistent = true;
            level = 0;
            prevCount = 0;
            count = 0;
            structure = null;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(67);
            sb.append("Consistency{")
                    .append("consistent:").append(isConsistent())
                    .append(", level:").append(level)
                    .append(", prevCount:").append(prevCount)
                    .append(", count:").append(count)
                    .append(", structure:'").append(structure).append('\'')
                    .append('}');
            return sb.toString();
        }

        public boolean isConsistent() {
            return consistent;
        }
    }
}
