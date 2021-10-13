/*
 * RangeSet.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.ReadTransactionContext;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * RangeSet supports efficient adding of ranges of keys into the database to support marking
 * work done elsewhere as completed as well as checking if specific keys are already completed.
 *
 * <p>
 * This is useful if one is going to be doing work that will carve out pieces from another
 * subspace and work on those separately. The methods in here will allow for a (more-or-less)
 * append only set that can be used to keep track of the progress that that job is making.
 * </p>
 */
@API(API.Status.MAINTAINED)
public class RangeSet {
    @Nonnull private Subspace subspace;
    @Nonnull private static final byte[] FIRST_KEY = new byte[]{(byte)0x00};
    @Nonnull private static final byte[] FINAL_KEY = new byte[]{(byte)0xff};

    private static final Range COMPLETE_RANGE = new Range(FIRST_KEY, FINAL_KEY);

    /**
     * Value indicating that there should be no limit. This should
     * be passed to {@link RangeSet#missingRanges(ReadTransaction, byte[], byte[], int) missingRanges}
     * to indicate that the read should not limit the number of results it returns.
     */
    public static final int UNLIMITED = Integer.MAX_VALUE;

    /**
     * Creates a new RangeSet that will write its data to the given subspace provided.
     * The contents of this subspace should either be empty or contain the data
     * used by another RangeSet object.
     * @param subspace the subspace in which to write data
     */
    public RangeSet(@Nonnull Subspace subspace) {
        this.subspace = subspace;
    }

    private static void checkKey(@Nonnull byte[] key) {
        if (key.length == 0 || ByteArrayUtil.compareUnsigned(key, FINAL_KEY) >= 0) {
            // NOTE: Perhaps this should instead return a completable future completed in exceptional state...
            throw new IllegalArgumentException("Key " + ByteArrayUtil.printable(key) + " outside of accepted key range of [\\x00,\\xff)");
        }
    }

    private static void checkRange(@Nonnull byte[] begin, @Nonnull byte[] end) {
        if (ByteArrayUtil.compareUnsigned(begin, end) > 0) {
            throw new IllegalArgumentException("Inverted range; " + ByteArrayUtil.printable(begin) + " is greater than " + ByteArrayUtil.printable(end));
        }
    }

    public static boolean isFirstKey(@Nonnull byte[] key) {
        return Arrays.equals(key, FIRST_KEY);
    }

    public static boolean isFinalKey(@Nonnull byte[] key) {
        return Arrays.equals(key, FINAL_KEY);
    }

    // This returns the next possible key after another key (i.e., a key that is greater than current key but
    // every key greater than this key will be greater than or equal to the returned key).
    @Nonnull
    private byte[] keyAfter(@Nonnull byte[] key) {
        byte[] ret = new byte[key.length + 1];
        System.arraycopy(key, 0, ret, 0, key.length);
        ret[key.length] = (byte)0;
        return ret;
    }

    /**
     * Determines if a single key is contained within the range set. If it is, this will return
     * <code>true</code>, and if it is not, it will return <code>false</code>. In terms of isolation, this adds a read-
     * conflict to the key corresponding to the key being checked but to nothing else even
     * though it has to do a range read that might be larger. This means that updates to keys
     * before this won't conflict unless they actually change whether this key is contained within
     * the range set.
     * @param tc transaction or database in which to run operation
     * @param key the key to check presence in set
     * @return a future that contains whether some range in the set contains the key
     */
    @Nonnull
    public CompletableFuture<Boolean> contains(@Nonnull TransactionContext tc, @Nonnull byte[] key) {
        checkKey(key);
        return tc.runAsync(tr -> {
            // Add a read conflict to only the key being checked so that if this gets
            // overwritten somewhere else, this causes a conflict.
            byte[] frobnicated = subspace.pack(key);
            tr.addReadConflictKey(frobnicated);
            AsyncIterator<KeyValue> iterator = tr.snapshot().getRange(subspace.range().begin, keyAfter(frobnicated), 1, true).iterator();
            return iterator.onHasNext().thenApply(hasNext -> {
                if (!hasNext) {
                    return false;
                } else {
                    byte[] endRange = iterator.next().getValue();
                    return ByteArrayUtil.compareUnsigned(key, endRange) < 0;
                }
            });
        });
    }

    /**
     * Inserts a range into the set. This behaves the same way as the four-parameter version of
     * {@link RangeSet#insertRange(TransactionContext, byte[], byte[], boolean) RangeSet.insertRange} (including conflict
     * settings), but it gets its begin and end from the given {@link Range} object and assumes that
     * <code>requiresEmpty</code> is <code>false</code>, i.e., it is okay for there already to be data within the
     * given range.
     *
     * @param tc the transaction or database in which to operate
     * @param r the range to add to the set
     * @return a future that is <code>true</code> if there were any modifications to the database and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> insertRange(@Nonnull TransactionContext tc, @Nonnull Range r) {
        return insertRange(tc, r.begin, r.end);
    }

    /**
     * Inserts a range into the set. This behaves the same way as the four-parmater version of
     * {@link RangeSet#insertRange(TransactionContext, byte[], byte[], boolean) RangeSet.insertRange} (including conflict
     * settings), but it gets its begin and end from the given {@link Range} object.
     *
     * @param tc the transaction or database in which to operate
     * @param r the range to add to the set
     * @param requireEmpty whether this should only be added if this range is initally empty
     * @return a future that is <code>true</code> if there were any modifications to the database and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> insertRange(@Nonnull TransactionContext tc, @Nonnull Range r, boolean requireEmpty) {
        return insertRange(tc, r.begin, r.end, requireEmpty);
    }

    /**
     * Inserts a range into the set. This behaves the same way as the four-parameter version of
     * {@link RangeSet#insertRange(TransactionContext, byte[], byte[], boolean) RangeSet.insertRange} (including conflict
     * settings), but it assumes that <code>requiresEmpty</code> is <code>false</code>, i.e., it is okay for
     * there already to be data within the given range.
     *
     * @param tc the transaction or database in which to operate
     * @param begin the (inclusive) beginning of the range to add
     * @param end the (exclusive) end of the range to add
     * @return a future that is <code>true</code> if there were any modifications to the database and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> insertRange(@Nonnull TransactionContext tc, @Nullable byte[] begin, @Nullable byte[] end) {
        return insertRange(tc, begin, end, false);
    }

    /**
     * Inserts a range into the set. The range inserted will begin at <code>begin</code> (inclusive) and end at
     * <code>end</code> (exclusive). If the <code>requireEmpty</code> is set, then this will only actually change the
     * database in the case that the range being added is not yet included in the set. If this flag is set to
     * <code>false</code>, then this will "fill in the gaps" between ranges present so that the whole range is
     * present following this transactions operation. The return value will (when ready) be equal to <code>true</code>
     * if and only if there are changes (i.e., writes) to the database that need to be made, i.e., the range was not
     * already included in the set. If the initial end point is less than the begin point, then this will
     * throw an {@link IllegalArgumentException} indicating that one has passed an inverted range. If <code>begin</code>
     * and <code>end</code> are equal, then this will immediately return a future that is set to <code>false</code>
     * (corresponding to adding an empty range). If <code>null</code> is set for either endpoint, this will insert
     * a range all the way to the end of the total range.
     *
     * <p>
     * In terms of isolation, this method will add both read- and write-conflict ranges. It adds a read-conflict range
     * corresponding to the range being added, i.e., for the keys within the range from <code>begin</code> to <code>end</code>.
     * This is so that if this range is modified concurrently by another writer, this transaction will fail (as the exact
     * writes done depend on these keys not being modified.) It will also a write-conflict ranges corresponding
     * to all of the individual ranges added to the database. That means that if the range is initially empty,
     * a write-conflict range corresponding to the keys from <code>begin</code> to <code>end</code>. This is done
     * so that if another transaction checks to see if a key in the range we are writing is within the range set
     * and finds that it is not, this write will then cause that transaction to fail if it is committed after this
     * one. If the range is not empty initially, write conflict ranges are added for all of the "gaps" that have
     * to be added. (So, if the range is already full, then no write conflict ranges are added at all.)
     * </p>
     *
     * @param tc the transaction or database in which to operate
     * @param begin the (inclusive) beginning of the range to add
     * @param end the (exclusive) end of the range to add
     * @param requireEmpty whether this should only be added if this range is initially empty
     * @return a future that is <code>true</code> if there were any modifications to the database and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> insertRange(@Nonnull TransactionContext tc, @Nullable byte[] begin, @Nullable byte[] end, boolean requireEmpty) {
        byte[] beginNonNull = (begin == null) ? FIRST_KEY : begin;
        byte[] endNonNull = (end == null) ? FINAL_KEY : end;
        checkKey(beginNonNull);
        checkRange(beginNonNull, endNonNull);

        if (ByteArrayUtil.compareUnsigned(beginNonNull, endNonNull) == 0) {
            return AsyncUtil.READY_FALSE;
        }

        return tc.runAsync(tr -> {
            // Add a read range for the keys corresponding to the bounds of this range.
            byte[] frobnicatedBegin = subspace.pack(beginNonNull);
            byte[] frobnicatedEnd = subspace.pack(endNonNull);
            tr.addReadConflictRange(frobnicatedBegin, frobnicatedEnd);

            // Look to see what is already in this database to see what of this range is already present.
            // Note: the two range reads are done in parallel, which essentially means we get the before read
            // "for free".
            byte[] keyAfterBegin = keyAfter(frobnicatedBegin);
            ReadTransaction snapshot = tr.snapshot();
            AsyncIterator<KeyValue> beforeIterator = snapshot.getRange(subspace.range().begin, keyAfterBegin, 1, true).iterator();
            AsyncIterator<KeyValue> afterIterator = snapshot.getRange(keyAfterBegin, frobnicatedEnd,
                    (requireEmpty ? 1 : ReadTransaction.ROW_LIMIT_UNLIMITED), false).iterator();

            return beforeIterator.onHasNext().thenCompose(hasBefore -> {
                AtomicReference<byte[]> lastSeen = new AtomicReference<>(frobnicatedBegin);
                KeyValue before = hasBefore ? beforeIterator.next() : null;

                // If the before key is in some range, we don't have to update from before to the
                // end of that range.
                if (hasBefore) {
                    byte[] beforeEnd = before.getValue();
                    if (ByteArrayUtil.compareUnsigned(beginNonNull, beforeEnd) < 0) {
                        if (requireEmpty) {
                            return AsyncUtil.READY_FALSE;
                        } else {
                            lastSeen.set(subspace.pack(beforeEnd));
                        }
                    }
                }

                if (requireEmpty) {
                    // If we will only add on the empty case, then the after iterator has to be empty.
                    return afterIterator.onHasNext().thenApply(hasNext -> {
                        if (hasNext) {
                            return false;
                        } else {
                            if (before != null && ByteArrayUtil.compareUnsigned(beginNonNull, before.getValue()) == 0) {
                                // This consolidation is done to make the simple case of a single writer
                                // going forward more space compact.
                                tr.addReadConflictKey(before.getKey());
                                tr.set(before.getKey(), endNonNull);
                            } else {
                                tr.set(frobnicatedBegin, endNonNull);
                            }
                            tr.addWriteConflictRange(frobnicatedBegin, frobnicatedEnd);
                            return true;
                        }
                    });
                } else {
                    AtomicBoolean changed = new AtomicBoolean(false);
                    // If we are allowing non-empty ranges, then we just need to fill in the gaps.
                    return AsyncUtil.whileTrue(() -> {
                        byte[] lastSeenBytes = lastSeen.get();
                        if (MoreAsyncUtil.isCompletedNormally(afterIterator.onHasNext()) && afterIterator.hasNext()) {
                            KeyValue kv = afterIterator.next();
                            if (ByteArrayUtil.compareUnsigned(lastSeenBytes, kv.getKey()) < 0) {
                                tr.set(lastSeenBytes, subspace.unpack(kv.getKey()).getBytes(0));
                                tr.addWriteConflictRange(lastSeenBytes, kv.getKey());
                                changed.set(true);
                            }
                            lastSeen.set(subspace.pack(kv.getValue()));
                        }
                        return afterIterator.onHasNext();
                    }, tc.getExecutor()).thenApply(vignore -> {
                        byte[] lastSeenBytes = lastSeen.get();
                        // Get from lastSeen to the end (the last gap).
                        if (ByteArrayUtil.compareUnsigned(lastSeenBytes, frobnicatedEnd) < 0) {
                            tr.set(lastSeenBytes, endNonNull);
                            tr.addWriteConflictRange(lastSeenBytes, frobnicatedEnd);
                            changed.set(true);
                        }
                        return changed.get();
                    });
                }
            });
        });
    }

    /**
     * Returns all of the ranges that are missing within this set as list. See the three-parameter
     * version of {@link RangeSet#missingRanges(ReadTransaction, byte[], byte[]) RangeSet.missingRanges}
     * for more details, but this will look from the beginning of the valid keys within this set to
     * the end and find any gaps between ranges that need to be filled.
     *
     * @param tc transaction that will be used to access the database
     * @return an iterable that will produce all of the missing ranges
     */
    @Nonnull
    public CompletableFuture<List<Range>> missingRanges(@Nonnull ReadTransactionContext tc) {
        return tc.readAsync(tr -> {
            AsyncIterable<Range> ranges = missingRanges(tr);
            return ranges.asList();
        });
    }

    /**
     * Returns all of the ranges that are missing within this set. See the three-parameter
     * version of {@link RangeSet#missingRanges(ReadTransaction, byte[], byte[]) RangeSet.missingRanges}
     * for more details, but this will look from the beginning of the valid keys within this set to
     * the end and find any gaps between ranges that need to be filled.
     *
     * @param tr transaction that will be used to access the database
     * @return an iterable that will produce all of the missing ranges
     */
    @Nonnull
    public AsyncIterable<Range> missingRanges(@Nonnull ReadTransaction tr) {
        return missingRanges(tr, null, null);
    }

    /**
     * Returns all of the ranges that are missing within a given range as a list. See the four-parameter
     * version of {@link RangeSet#missingRanges(ReadTransaction, byte[], byte[], int) RangeSet.missingRanges}
     * for more details, but this will look for ranges that aren't already within the set.
     *
     * @param tc transaction that will be used to access the database
     * @param superRange the range within to search for additional ranges
     * @return an iterable that will produce all of the missing ranges
     */
    @Nonnull
    public CompletableFuture<List<Range>> missingRanges(@Nonnull ReadTransactionContext tc, @Nonnull Range superRange) {
        return tc.readAsync(tr -> {
            AsyncIterable<Range> ranges = missingRanges(tr, superRange);
            return ranges.asList();
        });
    }

    /**
     * Returns all of the ranges that are missing within a given range. See the four-parameter
     * version of {@link RangeSet#missingRanges(ReadTransaction, byte[], byte[], int) RangeSet.missingRanges}
     * for more details, but this will look for ranges that aren't already within the set.
     *
     * @param tr transaction that will be used to access the database
     * @param superRange the range within to search for additional ranges
     * @return an iterable that will produce all of the missing ranges
     */
    @Nonnull
    public AsyncIterable<Range> missingRanges(@Nonnull ReadTransaction tr, @Nonnull Range superRange) {
        return missingRanges(tr, superRange.begin, superRange.end);
    }

    /**
     * Returns all of the ranges that are missing within a given set of bounds as a list. See the four-parameter
     * version of {@link RangeSet#missingRanges(ReadTransaction, byte[], byte[], int) RangeSet.missingRanges}
     * for more details, but this will look for ranges that aren't already within the set.
     *
     * @param tc transaction that will be used to access the database
     * @param begin the beginning (inclusive) of the range to look for gaps
     * @param end the end (inclusive) of the range to look for gaps
     * @return an iterable that will produce all of the missing ranges
     */
    @Nonnull
    public CompletableFuture<List<Range>> missingRanges(@Nonnull ReadTransactionContext tc, @Nullable byte[] begin, @Nullable byte[] end) {
        return tc.readAsync(tr -> {
            AsyncIterable<Range> ranges = missingRanges(tr, begin, end);
            return ranges.asList();
        });
    }

    /**
     * Returns all of the ranges that are missing within a given set of bounds as a list. See the four-parameter
     * version of {@link RangeSet#missingRanges(ReadTransaction, byte[], byte[], int) RangeSet.missingRanges}
     * for more details, but this will look for ranges that aren't already within the set. It will not
     * limit the number of results that it will return.
     *
     * @param tr transaction that will be used to access the database
     * @param begin the beginning (inclusive) of the range to look for gaps
     * @param end the end (inclusive) of the range to look for gaps
     * @return an iterable that will produce all of the missing ranges
     */
    @Nonnull
    public AsyncIterable<Range> missingRanges(@Nonnull ReadTransaction tr, @Nullable byte[] begin, @Nullable byte[] end) {
        return missingRanges(tr, begin, end, Integer.MAX_VALUE);
    }

    /**
     * Returns all of the ranges that are missing within a given set of bounds as a list. See the four-parameter
     * version of {@link RangeSet#missingRanges(ReadTransaction, byte[], byte[], int) RangeSet.missingRanges}
     * for more details, but this will look for ranges that aren't already within the set. It will not
     * limit the number of results that it will return.
     *
     * @param tc transaction that will be used to access the database
     * @param begin the beginning (inclusive) of the range to look for gaps
     * @param end the end (inclusive) of the range to look for gaps
     * @param limit the maximum number of results to return
     * @return an iterable that will produce all of the missing ranges
     */
    @Nonnull
    public CompletableFuture<List<Range>> missingRanges(@Nonnull ReadTransactionContext tc, @Nullable byte[] begin, @Nullable byte[] end, int limit) {
        return tc.readAsync(tr -> {
            AsyncIterable<Range> ranges = missingRanges(tr, begin, end, limit);
            return ranges.asList();
        });
    }

    /**
     * Returns all of the ranges that are missing within a given set of bounds. In particular, this will look
     * for "gaps" in the key-value pairs between begin (inclusive) and end (exclusive) so that at the end, we
     * know what is missing. This takes in a read transaction (which could, theoretically, be a snapshot read
     * if we so desired). If this transaction is committed before the iterator is cancelled or completes,
     * this can cause problems.
     *
     * @param tr transaction that will be used to access the database
     * @param begin the beginning (inclusive) of the range to look for gaps
     * @param end the end (inclusive) of the range to look for gaps
     * @param limit the maximum number of results to return
     * @return an iterable that will produce all of the missing ranges
     */
    @Nonnull
    public AsyncIterable<Range> missingRanges(@Nonnull ReadTransaction tr, @Nullable byte[] begin, @Nullable byte[] end, int limit) {
        byte[] beginNonNull = (begin == null) ? FIRST_KEY : begin;
        byte[] endNonNull = (end == null) ? FINAL_KEY : end;
        checkKey(beginNonNull);
        checkRange(beginNonNull, endNonNull);

        // Return an AsyncIterable with the pertinent information.
        return new AsyncIterable<Range>() {
            @Override
            public AsyncIterator<Range> iterator() {
                return new MissingRangeIterator(tr, beginNonNull, endNonNull, limit);
            }

            @Override
            public CompletableFuture<List<Range>> asList() {
                return AsyncUtil.collect(this);
            }
        };
    }

    /**
     * Determine whether this range set is empty. If given a {@link com.apple.foundationdb.Database}, this will create
     * a transaction and use it to read from the database. If given a {@link ReadTransaction}, it will use the provided
     * transaction. See {@link #isEmpty(ReadTransaction)} for more details on the semantics of this function.
     *
     * @param rtc transaction or database object that will be used to read from the database
     * @return a future that will contain {@code true} if there are no ranges in this set or {@code false} otherwise
     * @see #isEmpty(ReadTransaction)
     */
    public CompletableFuture<Boolean> isEmpty(@Nonnull ReadTransactionContext rtc) {
        return rtc.readAsync(this::isEmpty);
    }

    /**
     * Determine whether this range set is empty. This will scan the given range set and return {@code true} if no
     * ranges have been inserted into the data structure. Otherwise, it will return {@code false}.
     *
     * @param rtr transaction that will be used to read from the database
     * @return a future that will contain {@code true} if there are no ranges in this set or {@code false} otherwise
     */
    public CompletableFuture<Boolean> isEmpty(@Nonnull ReadTransaction rtr) {
        final AsyncIterator<Range> missing = missingRanges(rtr, null, null, 1).iterator();
        return missing.onHasNext().thenApply(doesHaveNext -> {
            if (doesHaveNext) {
                Range firstMissing = missing.next();
                return firstMissing.equals(COMPLETE_RANGE);
            } else {
                // No missing ranges, so the set isn't empty
                return false;
            }
        });
    }

    // Iterator that computes the missing ranges. It will go through and find gaps within the
    // range. It will stop after the limit has been acheived unless the limit is set
    // to UNLIMITED.
    private class MissingRangeIterator implements CloseableAsyncIterator<Range> {
        @Nonnull private final byte[] endNonNull;
        @Nonnull private AsyncIterator<KeyValue> before;
        @Nonnull private AsyncIterator<KeyValue> after;

        @Nonnull private byte[] currBegin;
        @Nullable private Range next;
        private boolean found;
        private int limit;
        private int numFound;
        private final Executor executor;
        @Nonnull private CompletableFuture<Boolean> nextFuture;

        public MissingRangeIterator(@Nonnull ReadTransaction tr, @Nonnull byte[] beginNonNull, @Nonnull byte[] endNonNull, int limit) {
            this.endNonNull = endNonNull;
            this.numFound = 0;
            this.limit = limit;

            byte[] frobnicatedBegin = subspace.pack(beginNonNull);
            byte[] frobnicatedEnd = subspace.pack(endNonNull);
            before = tr.getRange(subspace.range().begin, keyAfter(frobnicatedBegin), 1, true).iterator();
            after = tr.getRange(keyAfter(frobnicatedBegin), frobnicatedEnd).iterator();
            next = null;
            currBegin = beginNonNull;
            found = false;

            executor = tr.getExecutor();
            nextFuture = before.onHasNext().thenAccept(hasBefore -> {
                if (hasBefore) {
                    byte[] lastEnd = before.next().getValue(); //subspace.unpack(before.next().getValue()).getBytes(0);
                    if (ByteArrayUtil.compareUnsigned(beginNonNull, lastEnd) < 0) {
                        currBegin = lastEnd;
                    }
                }
            }).thenCompose(vignore -> getNext());
        }

        private CompletableFuture<Boolean> getNext() {
            return AsyncUtil.whileTrue(() -> {
                if (MoreAsyncUtil.isCompletedNormally(after.onHasNext())) {
                    if (after.hasNext()) {
                        KeyValue kv = after.next();
                        byte[] currBeg = currBegin;
                        byte[] nextBeg = subspace.unpack(kv.getKey()).getBytes(0);
                        if (ByteArrayUtil.compareUnsigned(currBeg, nextBeg) < 0) {
                            next = new Range(currBeg, nextBeg);
                            found = true;
                        }
                        currBegin = kv.getValue();
                        if (found) {
                            return AsyncUtil.READY_FALSE; // stop looping.
                        } else {
                            return after.onHasNext();
                        }
                    } else {
                        return AsyncUtil.READY_FALSE;
                    }
                } else {
                    return after.onHasNext();
                }
            }, executor).thenApply(vignore -> {
                if (found) {
                    numFound += 1;
                    return true;
                } else {
                    if (ByteArrayUtil.compareUnsigned(currBegin, endNonNull) < 0) {
                        next = new Range(currBegin, endNonNull);
                        currBegin = endNonNull;
                        return true;
                    } else {
                        close();
                        return false;
                    }
                }
            });
        }

        @Override
        public CompletableFuture<Boolean> onHasNext() {
            return nextFuture;
        }

        @Override
        public boolean hasNext() {
            return nextFuture.join();
        }

        @Override
        public Range next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Attempted to get next missing range when none were present");
            }
            Range ret = next;
            found = false;
            if (limit == UNLIMITED || numFound < limit) {
                nextFuture = getNext();
            } else {
                close();
                nextFuture = AsyncUtil.READY_FALSE;
            }
            return ret;
        }

        @Override
        public void close() {
            MoreAsyncUtil.closeIterator(before);
            MoreAsyncUtil.closeIterator(after);
        }
    }

    /**
     * Clears the subspace used by this RangeSet instance. This will delete the records of any
     * data used by this set.
     * @param tc transaction or database in which to run operation
     * @return a future that is completed when the range has been cleared
     */
    @Nonnull
    public CompletableFuture<Void> clear(@Nonnull TransactionContext tc) {
        return tc.runAsync(tr -> {
            tr.clear(subspace.range());
            return AsyncUtil.DONE;
        });
    }

    @Nonnull
    public CompletableFuture<String> rep(@Nonnull ReadTransactionContext tc) {
        return tc.readAsync(tr -> {
            StringBuilder sb = new StringBuilder();
            AsyncIterable<KeyValue> iterable = tr.getRange(subspace.range());
            return iterable.asList().thenApply((List<KeyValue> list) -> {
                for (KeyValue kv : list) {
                    byte[] key = subspace.unpack(kv.getKey()).getBytes(0);
                    byte[] value = kv.getValue();
                    sb.append(ByteArrayUtil.printable(key));
                    sb.append(" -> ");
                    sb.append(ByteArrayUtil.printable(value));
                    sb.append('\n');
                }
                return sb.toString();
            });
        });
    }

}
