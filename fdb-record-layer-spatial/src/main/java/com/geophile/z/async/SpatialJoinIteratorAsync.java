/*
 * SpatialJoinIteratorAsync.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2019 Apple Inc. and the FoundationDB project authors
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

package com.geophile.z.async;

import com.geophile.z.Pair;
import com.geophile.z.Record;
import com.geophile.z.SpatialIndex;
import com.geophile.z.SpatialObject;
import com.geophile.z.index.RecordWithSpatialObject;
import com.geophile.z.index.sortedarray.SortedArray;
import com.geophile.z.space.SpaceImpl;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

// T is either Pair or SpatialObject

/**
 * Asynchronous version of {@code com.geophile.z.spatialjoin.SpatialJoinIterator}.
 * @param <T> element type
 */
@SuppressWarnings({"checkstyle:MethodTypeParameterName", "PMD.GenericsNaming", "checkstyle:ClassTypeParameterName"})
class SpatialJoinIteratorAsync<LEFT_RECORD extends Record, RIGHT_RECORD extends Record, T> implements IteratorAsync<T>
{
    // Class state

    private static final Logger LOG = Logger.getLogger(SpatialJoinIteratorAsync.class.getName());
    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    // Object state

    private final String name = String.format("sj(%s)", idGenerator.getAndIncrement());
    private final SpatialJoinInputAsync<LEFT_RECORD, RIGHT_RECORD> left;
    private final SpatialJoinInputAsync<RIGHT_RECORD, LEFT_RECORD> right;
    private final Queue<T> pending = new ArrayDeque<>();
    private boolean leftStarted;
    private boolean rightStarted;

    // Object interface

    @Override
    public String toString()
    {
        return name;
    }

    // IteratorAsync interface

    @Override
    public CompletableFuture<T> nextAsync() {
        return ensurePending().thenApply(vignore -> {
            if (pending.isEmpty()) {
                return null;
            }
            T next = pending.poll();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "{0} -> {1}", new Object[]{this, next});
            }
            return next;
        });
    }

    // SpatialJoinIterator interface

    public static <LEFT_RECORD extends Record, RIGHT_RECORD extends Record> SpatialJoinIteratorAsync<LEFT_RECORD, RIGHT_RECORD, Pair<LEFT_RECORD, RIGHT_RECORD>>
            pairIteratorAsync(SpatialIndexImplAsync<LEFT_RECORD> leftSpatialIndex,
                              SpatialIndexImplAsync<RIGHT_RECORD> rightSpatialIndex,
                              SpatialJoinAsync.Filter<LEFT_RECORD, RIGHT_RECORD> filter,
                              SpatialJoinAsync.InputObserver leftInputObserver,
                              SpatialJoinAsync.InputObserver rightInputObserver)
    {
        return new SpatialJoinIteratorAsync<>(leftSpatialIndex,
                                         rightSpatialIndex,
                                         new PairOutputGenerator<>(),
                                         filter,
                                         leftInputObserver,
                                         rightInputObserver);
    }

    public static <RECORD extends Record> SpatialJoinIteratorAsync<RecordWithSpatialObject, RECORD, RECORD>
            spatialObjectIteratorAsync(SpatialObject leftSpatialObject,
                                       SpatialIndexImplAsync<RECORD> rightSpatialIndex,
                                       SpatialJoinAsync.Filter<SpatialObject, RECORD> filter,
                                       SpatialJoinAsync.InputObserver leftInputObserver,
                                       SpatialJoinAsync.InputObserver rightInputObserver)
    {

        return new SpatialJoinIteratorAsync<>(leftSpatialObject,
                                              rightSpatialIndex,
                                              new RecordOutputGenerator<>(),
                                              filter,
                                              leftInputObserver,
                                              rightInputObserver);
    }

    // For use by this class

    private SpatialJoinIteratorAsync(SpatialIndexImplAsync<LEFT_RECORD> leftSpatialIndex,
                                     SpatialIndexImplAsync<RIGHT_RECORD> rightSpatialIndex,
                                     final OutputGenerator<LEFT_RECORD, RIGHT_RECORD, T> outputGenerator,
                                     final SpatialJoinAsync.Filter<LEFT_RECORD, RIGHT_RECORD> filter,
                                     SpatialJoinAsync.InputObserver leftInputObserver,
                                     SpatialJoinAsync.InputObserver rightInputObserver)
    {
        SpatialJoinOutputAsync<LEFT_RECORD, RIGHT_RECORD> pendingLeftRight =
                (left, right) -> {
                    if (filter.overlap(left, right)) {
                        pending.add(outputGenerator.generateOutput(left, right));
                    }
                };
        left = SpatialJoinInputAsync.newSpatialJoinInputAsync(leftSpatialIndex, pendingLeftRight, leftInputObserver);
        SpatialJoinOutputAsync<RIGHT_RECORD, LEFT_RECORD> pendingRightLeft =
                (right, left) -> {
                    if (filter.overlap(left, right)) {
                        pending.add(outputGenerator.generateOutput(left, right));
                    }
                };
        right = SpatialJoinInputAsync.newSpatialJoinInputAsync(rightSpatialIndex, pendingRightLeft, rightInputObserver);
        left.otherInput(right);
        right.otherInput(left);
        if (LOG.isLoggable(Level.INFO)) {
            LOG.log(Level.INFO,
                    "SpatialJoinIterator {0}: {1} x {2}",
                    new Object[]{this, left, right});
        }
    }

    @SuppressWarnings("unchecked")
    private SpatialJoinIteratorAsync(final SpatialObject querySpatialObject,
                                     SpatialIndexImplAsync<RIGHT_RECORD> dataSpatialIndex,
                                     final OutputGenerator<LEFT_RECORD, RIGHT_RECORD, RIGHT_RECORD> outputGenerator,
                                     final SpatialJoinAsync.Filter<SpatialObject, RIGHT_RECORD> filter,
                                     SpatialJoinAsync.InputObserver leftInputObserver,
                                     SpatialJoinAsync.InputObserver rightInputObserver)
    {
        final SortedArray<RecordWithSpatialObject> queryIndex = new SortedArray.OfBaseRecord();
        try {
            final SpatialIndex<RecordWithSpatialObject> querySpatialIndex = SpatialIndex.newSpatialIndex(dataSpatialIndex.space(),
                    queryIndex,
                    querySpatialObject.maxZ() == 1
                    ? SpatialIndex.Options.SINGLE_CELL
                    : SpatialIndex.Options.DEFAULT);
            querySpatialIndex.add(querySpatialObject,
                    () -> {
                        RecordWithSpatialObject queryRecord = queryIndex.newRecord();
                        queryRecord.spatialObject(querySpatialObject);
                        return queryRecord;
                    });
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        final IndexAsync<LEFT_RECORD> queryIndexAsync = (IndexAsync<LEFT_RECORD>)IndexAsync.fromSync(queryIndex);
        final SpatialIndexImplAsync<LEFT_RECORD> querySpatialIndexAsync = new SpatialIndexImplAsync<>((SpaceImpl) dataSpatialIndex.space(),
                queryIndexAsync,
                querySpatialObject.maxZ() == 1 ? SpatialIndexAsync.Options.SINGLE_CELL : SpatialIndexAsync.Options.DEFAULT);
        SpatialJoinOutputAsync<LEFT_RECORD, RIGHT_RECORD> pendingLeftRight =
                (left, right) -> {
                    if (filter.overlap(querySpatialObject, right)) {
                        pending.add((T)outputGenerator.generateOutput(left, right));
                    }
                };
        left = SpatialJoinInputAsync.newSpatialJoinInputAsync(querySpatialIndexAsync,
                                                    pendingLeftRight,
                                                    leftInputObserver);
        SpatialJoinOutputAsync<RIGHT_RECORD, LEFT_RECORD> pendingRightLeft =
                (right, left) -> {
                    if (filter.overlap(querySpatialObject, right)) {
                        pending.add((T)outputGenerator.generateOutput(left, right));
                    }
                };
        right = SpatialJoinInputAsync.newSpatialJoinInputAsync(dataSpatialIndex,
                                                     pendingRightLeft,
                                                     rightInputObserver);
        left.otherInput(right);
        right.otherInput(left);
        if (LOG.isLoggable(Level.INFO)) {
            LOG.log(Level.INFO,
                    "SpatialJoinIterator {0}: {1} x {2}",
                    new Object[]{this, left, right});
        }
    }

    private CompletableFuture<Void> ensurePending()
    {
        if (pending.isEmpty()) {
            return findPairs();
        } else {
            return CompletableFutures.NULL;
        }
    }

    // The synchronous version also calls this from the constructors. That's hard when it's async, so just don't bother.
    private CompletableFuture<Void> findPairs()
    {
        assert pending.isEmpty();
        return CompletableFutures.whileTrue(() -> {
            if (!leftStarted) {
                return left.start().thenApply(vignore -> {
                    leftStarted = true;
                    return true;
                });
            }
            if (!rightStarted) {
                return right.start().thenApply(vignore -> {
                    rightStarted = true;
                    return true;
                });
            }
            long zLeftEnter = left.nextEntry();
            long zLeftExit = left.nextExit();
            long zRightEnter = right.nextEntry();
            long zRightExit = right.nextExit();
            long zMin = min(zLeftEnter, zLeftExit, zRightEnter, zRightExit);
            if (zMin < SpatialJoinInputAsync.EOF) {
                // Prefer entry to exit to avoid missing join output
                if (zMin == zLeftEnter) {
                    return left.enterZ().thenApply(vigore -> pending.isEmpty());
                } else if (zMin == zRightEnter) {
                    return right.enterZ().thenApply(vigore -> pending.isEmpty());
                } else if (zMin == zLeftExit) {
                    left.exitZ();
                } else {
                    right.exitZ();
                }
                return CompletableFuture.completedFuture(pending.isEmpty());
            } else {
                return CompletableFutures.FALSE;
            }
        });
    }

    private long min(long a, long b, long c, long d)
    {
        return Math.min(Math.min(a, b), Math.min(c, d));
    }

    // Inner classes

    private interface OutputGenerator<LEFT_RECORD extends Record, RIGHT_RECORD extends Record, T>
    {
        T generateOutput(LEFT_RECORD left, RIGHT_RECORD right);
    }

    private static class PairOutputGenerator<LEFT_RECORD extends Record, RIGHT_RECORD extends Record> implements OutputGenerator<LEFT_RECORD, RIGHT_RECORD, Pair<LEFT_RECORD, RIGHT_RECORD>>
    {
        @Override
        public Pair<LEFT_RECORD, RIGHT_RECORD> generateOutput(LEFT_RECORD left, RIGHT_RECORD right)
        {
            assert left != null;
            assert right != null;
            return new Pair<>(left, right);
        }
    }

    private static class RecordOutputGenerator<RECORD extends Record> implements OutputGenerator<RecordWithSpatialObject, RECORD, RECORD>
    {
        @Override
        public RECORD generateOutput(RecordWithSpatialObject left, RECORD right)
        {
            assert right != null;
            return right;
        }
    }
}
