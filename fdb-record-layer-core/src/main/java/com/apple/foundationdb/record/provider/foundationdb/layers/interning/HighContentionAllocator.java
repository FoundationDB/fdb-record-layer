/*
 * HighContentionAllocator.java
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

package com.apple.foundationdb.record.provider.foundationdb.layers.interning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A supplier of unique integers that tries to balance size of the integer and conflicts on the assignment.
 *
 * Values are chosen randomly from a window that moves forward as the available lower-numbered space fills up.
 */
@API(API.Status.INTERNAL)
public class HighContentionAllocator {
    private static final byte[] LITTLE_ENDIAN_LONG_ONE = {1, 0, 0, 0, 0, 0, 0, 0};
    private static final byte[] KEY_UPDATING_BYTE = new byte[0];
    private static final byte[] INVALID_ALLOCATION_VALUE = new byte[]{(byte) 0xFD};
    private static final Function<Long, CompletableFuture<Boolean>> NOOP_CHECK = ignored -> CompletableFuture.completedFuture(true);
    private final Subspace counterSubspace;
    private final Subspace allocationSubspace;
    private final Transaction transaction;
    private final Function<Long, CompletableFuture<Boolean>> candidateCheck;

    public HighContentionAllocator(@Nonnull FDBRecordContext context,
                                   @Nonnull KeySpacePath basePath) {
        this(context, basePath, NOOP_CHECK);
    }

    public HighContentionAllocator(@Nonnull FDBRecordContext context,
                                   @Nonnull KeySpacePath basePath,
                                   @Nonnull Function<Long, CompletableFuture<Boolean>> candidateCheck) {
        this(context, basePath.toSubspace(context), candidateCheck);
    }

    public HighContentionAllocator(@Nonnull FDBRecordContext context,
                                   @Nonnull Subspace basePathSubspace,
                                   @Nonnull Function<Long, CompletableFuture<Boolean>> candidateCheck) {
        this(context, basePathSubspace.get(0), basePathSubspace.get(1), candidateCheck);
    }

    public HighContentionAllocator(@Nonnull FDBRecordContext context,
                                   @Nonnull Subspace counterSubspace,
                                   @Nonnull Subspace allocationSubspace) {
        this(context, counterSubspace, allocationSubspace, NOOP_CHECK);
    }

    protected HighContentionAllocator(@Nonnull FDBRecordContext context,
                                      @Nonnull Subspace counterSubspace,
                                      @Nonnull Subspace allocationSubspace,
                                      @Nonnull Function<Long, CompletableFuture<Boolean>> candidateCheck) {
        this.transaction = context.ensureActive();
        this.counterSubspace = counterSubspace;
        this.allocationSubspace = allocationSubspace;
        this.candidateCheck = candidateCheck;
    }

    public static HighContentionAllocator forRoot(@Nonnull FDBRecordContext context,
                                                  @Nonnull Subspace counterSubspace,
                                                  @Nonnull Subspace allocationSubspace) {
        return new HighContentionAllocator(context, counterSubspace, allocationSubspace,
                value -> hasConflictAtRoot(context.ensureActive(), value));
    }

    public static HighContentionAllocator forRoot(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath basePath) {
        return new HighContentionAllocator(context, basePath, value -> hasConflictAtRoot(context.ensureActive(), value));
    }

    @VisibleForTesting
    public Subspace getAllocationSubspace() {
        return allocationSubspace;
    }

    public CompletableFuture<Long> allocate(final String valueToStore) {
        final byte[] valueBytes = Tuple.from(valueToStore).pack();
        return initialWindow()
                .thenCompose(initialWindow -> chooseWindow(initialWindow, false))
                .thenCompose(window -> chooseCandidate(window, valueBytes));
    }

    private CompletableFuture<Optional<KeyValue>> currentCounter() {
        return transaction.snapshot().getRange(counterSubspace.range(), 1, true)
                .asList()
                .thenApply(list ->
                        list.isEmpty() ? Optional.empty() : Optional.of(list.get(0)));
    }

    private CompletableFuture<AllocationWindow> initialWindow() {
        return currentCounter().thenApply(counter ->
                counter.map(kv -> AllocationWindow.startingFrom(counterSubspace.unpack(kv.getKey()).getLong(0)))
                        .orElse(AllocationWindow.startingFrom(0))
        );
    }

    private CompletableFuture<AllocationWindow> chooseWindow(final AllocationWindow currentWindow, boolean wipeOld) {
        byte[] counterKey = counterSubspace.pack(currentWindow.getStart());
        Range oldCounters = new Range(counterSubspace.getKey(), counterKey);

        CompletableFuture<byte[]> newCount;

        synchronized (transaction) {
            if (wipeOld) {
                transaction.clear(oldCounters);
            }
            transaction.mutate(MutationType.ADD, counterKey, LITTLE_ENDIAN_LONG_ONE);
            newCount = transaction.snapshot().get(counterKey);
        }
        return newCount
                .thenApply(ByteArrayUtil::decodeInt)
                .thenCompose(count -> {
                    if (count * 2 > currentWindow.size()) {
                        // advance the window and retry
                        final AllocationWindow newWindow = AllocationWindow.startingFrom(currentWindow.getEnd());
                        return chooseWindow(newWindow, true);
                    }
                    return CompletableFuture.completedFuture(currentWindow);
                });
    }

    private CompletableFuture<Long> chooseCandidate(final AllocationWindow window, final byte[] valueToStore) {
        final long candidate = window.random();
        final byte[] allocationKey = allocationSubspace.pack(candidate);

        CompletableFuture<byte[]> previousAllocationValue;
        CompletableFuture<Boolean> check = candidateCheck.apply(candidate);
        synchronized (transaction) {
            previousAllocationValue = transaction.get(allocationKey);
            transaction.options().setNextWriteNoWriteConflictRange();
            transaction.set(allocationKey, KEY_UPDATING_BYTE);
        }

        return previousAllocationValue
                .thenCombine(check, (valueBytes, isGood) -> {
                    if (valueBytes == null) {
                        synchronized (transaction) {
                            transaction.set(allocationKey, isGood ? valueToStore : INVALID_ALLOCATION_VALUE);
                        }
                        if (!isGood) {
                            throw new IllegalStateException("database already has keys in allocation range");
                        }
                        return CompletableFuture.completedFuture(candidate);
                    }

                    if (!Arrays.equals(valueBytes, KEY_UPDATING_BYTE)) {
                        // we overwrote a real allocation value, need to roll it back
                        synchronized (transaction) {
                            transaction.options().setNextWriteNoWriteConflictRange();
                            transaction.set(allocationKey, valueBytes);
                        }
                    }
                    return chooseCandidate(window, valueToStore);
                }).thenCompose(Function.identity());
    }

    public void setWindow(long count) {
        transaction.mutate(MutationType.ADD, counterSubspace.pack(count), LITTLE_ENDIAN_LONG_ONE);
    }

    public void forceAllocate(@Nonnull String key, @Nonnull Long value) {
        transaction.set(allocationSubspace.pack(value), Tuple.from(key).pack());
    }

    private static CompletableFuture<Boolean> hasConflictAtRoot(Transaction context, Long value) {
        return hasConflictInSubspace(context, new Subspace(), value);
    }

    private static CompletableFuture<Boolean> hasConflictInSubspace(@Nonnull Transaction transaction,
                                                                    @Nonnull Subspace subspace,
                                                                    @Nonnull Long value) {
        Range checkRange = Range.startsWith(subspace.pack(value));
        return transaction.snapshot().getRange(checkRange, 1).iterator().onHasNext().thenApply(hasKeys -> !hasKeys);
    }

    /**
     * A range of possible values to try.
     */
    public static class AllocationWindow {
        private static Random random = new Random();
        private final long start;
        private final long end;

        private AllocationWindow(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public static AllocationWindow startingFrom(long start) {
            return new AllocationWindow(start, start + getWindowSize(start));
        }

        // values taken from FDB implementation
        private static long getWindowSize(long windowBegin) {
            if (windowBegin < 255) {
                return 1 << 6;
            }
            if (windowBegin < 65_535) {
                return 1 << 10;
            }
            return 1 << 12;
        }

        public long random() {
            return start + random.nextInt(size());
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

        public int size() {
            return Math.toIntExact(end - start);
        }

        @Override
        public String toString() {
            return "AllocationWindow[start=" + start + ", end=" + end + "]";
        }
    }

}
