/*
 * SpatialJoinInputAsync.java
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

import com.geophile.z.Record;
import com.geophile.z.space.SpaceImpl;
import com.geophile.z.spatialjoin.SpatialJoinImpl;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.geophile.z.space.SpaceImpl.formatZ;

/**
 * Asynchronous version of {@code SpatialJoinInput}.
 * @param <RECORD> type for spatial records
 */
@SuppressWarnings({"checkstyle:ClassTypeParameterName", "PMD.GenericsNaming", "checkstyle:MethodTypeParameterName"})
class SpatialJoinInputAsync<RECORD extends Record, OTHER_RECORD extends Record>
{
    // Class state

    // A 64-bit z-value is definitely less than Long.MAX_VALUE. The maxium z-value length is 57, which is recorded
    // in a 6-bit length field. So the length field will always have some zeros in the last 6 bits,
    // while Long.MAX_VALUE doesn't.
    public static final long EOF = Long.MAX_VALUE;
    private static final Logger LOG = Logger.getLogger(SpatialJoinInputAsync.class.getName());
    private static final AtomicInteger idGenerator = new AtomicInteger(0);
    private static final SpatialJoinAsync.InputObserver DEFAULT_OBSERVER = new SpatialJoinAsync.InputObserver();

    // Object state

    private final int id = idGenerator.getAndIncrement();
    private final SpatialIndexImplAsync<RECORD> spatialIndex;
    private final boolean stableRecords;
    private final boolean singleCell;
    private SpatialJoinInputAsync<OTHER_RECORD, RECORD> that;
    private final SpatialJoinOutputAsync<RECORD, OTHER_RECORD> spatialJoinOutput;
    // nest contains z-values that have been entered but not exited. current is the next z-value to enter,
    // and cursor contains later z-values.
    private final Deque<RECORD> nest = new ArrayDeque<>();
    private final CursorAsync<RECORD> cursor;
    private RECORD current;
    private final RECORD randomAccessKey;
    // For use in finding ancestors
    @SuppressWarnings("checkstyle:MemberName")
    private final long[] zCandidates = new long[SpaceImpl.MAX_Z_BITS];
    private long lastZRandomAccess; // For observing access pattern
    private boolean eof = false;
    private final boolean singleCellOptimization;
    private final SpatialJoinAsync.InputObserver observer;

    // Object interface

    @Override
    public final String toString() {
        return name();
    }

    // SpatialJoinInputAsync interface

    public long nextEntry() {
        return eof ? EOF : SpaceImpl.zLo(current.z());
    }

    public long nextExit() {
        return nest.isEmpty() ? EOF : SpaceImpl.zHi(nest.peek().z());
    }

    // This happens inside the constructor of the synchronous version, but needs to be async operations here.
    public CompletableFuture<Void> start() {
        IndexAsync<RECORD> index = spatialIndex.index();
        RECORD zMinKey = index.newKeyRecord();
        zMinKey.z(SpaceImpl.Z_MIN);
        cursorGoTo(cursor, zMinKey);
        return cursorNext(cursor).thenAccept(record -> {
            copyToCurrent(record);
            log("initialize");
        });
    }
        
    public CompletableFuture<Void> enterZ() {
        assert !eof;
        observer.enter(current.z());
        CompletableFuture<Void> enter;
        if (currentOverlapsOtherNest() ||
                !that.eof && overlap(current.z(), that.current.z())) {
            // Enter current
            if (!nest.isEmpty()) {
                long topZ = nest.peek().z();
                assert SpaceImpl.contains(topZ, current.z());
            }
            push(current);
            enter = cursorNext(cursor).thenAccept(this::copyToCurrent);
        } else {
            enter = advanceCursor();
        }
        return enter.thenAccept(vignore -> log("enter"));
    }

    public void exitZ()
    {
        assert !nest.isEmpty();
        RECORD top = nest.pop();
        observer.exit(top.z());
        that.generateSpatialJoinOutput(top);
        log("exit");
    }

    public final void otherInput(SpatialJoinInputAsync<OTHER_RECORD, RECORD> that)
    {
        this.that = that;
    }

    public static <RECORD extends Record, OTHER_RECORD extends Record> SpatialJoinInputAsync<RECORD, OTHER_RECORD> newSpatialJoinInputAsync(SpatialIndexImplAsync<RECORD> spatialIndex,
                                                                                                                                            SpatialJoinOutputAsync<RECORD, OTHER_RECORD> spatialJoinOutput,
                                                                                                                                            SpatialJoinAsync.InputObserver observer)
    {
        return new SpatialJoinInputAsync<>(spatialIndex, spatialJoinOutput, observer);
    }

    // For use by this class

    private CompletableFuture<Void> advanceCursor()
    {
        // Use that.current to skip ahead
        if (that.eof) {
            // If that.current is EOF, then we can skip to the end on this side too.
            this.eof = true;
        } else {
            assert !eof; // Should have been checked in caller, but just to be sure.
            long thisCurrentZ = this.current.z();
            long thatCurrentZ = that.current.z();
            assert thatCurrentZ >= thisCurrentZ; // otherwise, we would have entered that.current
            if (thatCurrentZ > thisCurrentZ) {
                if (singleCellOptimization && singleCell) {
                    randomAccessKey.z(thatCurrentZ);
                    cursorGoTo(cursor, randomAccessKey);
                    return cursorNext(cursor).thenAccept(this::copyToCurrent);
                } else {
                    // Why this works: There are two cases to consider.
                    // 1) thatCurrentZ contains thisCurrentZ: thisCurrentZ might be the correct place to
                    //    resume. But it is also possible that there is a larger, containing ancestor z-value,
                    //    a, such that a.lo() < thatCurrentZ. This call finds that ancestor.
                    // 2) thatCurrentZ does not contain thisCurrentZ, which means that thatCurrentZ.hi() <=
                    //    thisCurrentZ.lo(). We need to find an ancestor of thisCurrentZ, a. If a.lo() >
                    //    thatCurrentZ.hi(), then the random access would have discovered it, so this case can't happen.
                    //    We must therefore look for a container of thisCurrentZ that ALSO contains thatCurrentZ.
                    //    So again, we can start the search for an ancestor at thatCurrentZ.
                    return advanceToNextOrAncestor(thatCurrentZ, thisCurrentZ);
                }
            }
        }
        return CompletableFutures.NULL;
    }

    // Advance to an ancestor of zStart, or if there is none, to the z-value after zStart.
    private CompletableFuture<Void> advanceToNextOrAncestor(long zStart, long zLowerBound)
    {
        // Generate all the ancestors that need to be considered.
        int nCandidates = 0;
        {
            long zCandidate = zStart;
            while (zCandidate > zLowerBound) {
                zCandidates[nCandidates++] = zCandidate;
                zCandidate = SpaceImpl.parent(zCandidate);
            }
        }
        // In the caller, thatCurrentZ > thisCurrentZ, so zStart > zLowerBound, and zCandidate is initialized to
        // zStart. So zCandidate > zLowerBound has to be true at least once, and nCandidates > 0.
        assert nCandidates > 0;
        // Find the largest ancestor among the candidates that exists.
        AtomicInteger c = new AtomicInteger(nCandidates);
        AtomicBoolean foundAncestor = new AtomicBoolean(false);
        return CompletableFutures.whileTrue(() -> {
            int ci = c.decrementAndGet();
            if (ci < 0) {
                return CompletableFutures.FALSE;
            }
            long zCandidate = zCandidates[ci];
            randomAccessKey.z(zCandidate);
            cursorGoTo(cursor, randomAccessKey);
            return cursorNext(cursor).thenApply(record -> {
                if (ci == 0) {
                    // No ancestors were found. Go to the record following zStart.
                    assert zCandidate == zStart;
                    copyToCurrent(record);
                    return false;
                } else if (record != null && record.z() == zCandidate) {
                    copyToCurrent(record);
                    foundAncestor.set(true);
                    return false;
                } else {
                    return true;
                }
            });
        }).thenAccept(vignore -> {
            if (eof) {
                cursor.close();
            } else {
                assert current.z() >= zLowerBound;
            }
            observer.ancestorSearch(cursor,
                    zStart,
                    foundAncestor.get() ? current.z() : SpaceImpl.Z_NULL);
        });
    }

    private boolean currentOverlapsOtherNest()
    {
        boolean overlap = false;
        Record thatNestTop = that.nestTop();
        if (thatNestTop != null) {
            long thisCurrentZ = current.z();
            overlap =
                SpaceImpl.contains(thisCurrentZ, thatNestTop.z()) ||
                SpaceImpl.contains(that.nestBottom().z(), thisCurrentZ);
        }
        return overlap;
    }

    private Record nestTop()
    {
        return nest.peek();
    }

    private Record nestBottom()
    {
        return nest.peekLast();
    }

    private void generateSpatialJoinOutput(OTHER_RECORD thatRecord)
    {
        for (RECORD thisRecord : nest) {
            spatialJoinOutput.add(thisRecord, thatRecord);
        }
    }

    private boolean overlap(long z1, long z2)
    {
        return SpaceImpl.contains(z1, z2) || SpaceImpl.contains(z2, z1);
    }

    private String name()
    {
        return String.format("sjinput(%s)", id);
    }

    private SpatialJoinInputAsync(SpatialIndexImplAsync<RECORD> spatialIndex,
                                  SpatialJoinOutputAsync<RECORD, OTHER_RECORD> spatialJoinOutput,
                                  SpatialJoinAsync.InputObserver observer)
    {
        IndexAsync<RECORD> index = spatialIndex.index();
        this.stableRecords = index.stableRecords();
        this.spatialIndex = spatialIndex;
        this.observer = observer == null ? DEFAULT_OBSERVER : observer;
        // Initialize cursor
        this.cursor = index.cursorAsync();
        this.current = stableRecords ? null : index.newRecord();
        this.randomAccessKey = index.newKeyRecord();
        this.spatialJoinOutput = spatialJoinOutput;
        this.singleCell = spatialIndex.singleCell();
        this.singleCellOptimization = SpatialJoinImpl.singleCellOptimization();
    }

    private void copyToCurrent(RECORD record)
    {
        if (record == null) {
            eof = true;
        } else {
            if (stableRecords) {
                current = record;
            } else {
                record.copyTo(current);
            }
            eof = false;
        }
    }

    private void push(RECORD record)
    {
        if (stableRecords) {
            nest.push(record);
        } else {
            RECORD copy = spatialIndex.index().newRecord();
            record.copyTo(copy);
            nest.push(copy);
        }
    }

    private void cursorGoTo(CursorAsync<RECORD> cursor, RECORD key)
    {
        cursor.goTo(key);
        lastZRandomAccess = key.z();
        observer.randomAccess(cursor, lastZRandomAccess);
    }

    private CompletableFuture<RECORD> cursorNext(CursorAsync<RECORD> cursor)
    {
        return cursor.next().thenApply(record -> {
            observer.sequentialAccess(cursor, lastZRandomAccess, record);
            return record;
        });
    }

    private void log(String label)
    {
        if (LOG.isLoggable(Level.FINE)) {
            StringBuilder buffer = new StringBuilder();
            Iterator<RECORD> nestScan = nest.descendingIterator();
            long[] zs = new long[64];
            int[] counts = new int[64];
            int n = 0;
            while (nestScan.hasNext()) {
                Record record = nestScan.next();
                long z = record.z();
                if (n > 0 && zs[n - 1] == z) {
                    counts[n - 1]++;
                } else {
                    zs[n] = z;
                    counts[n] = 1;
                    n++;
                }
            }
            for (int i = 0; i < n; i++) {
                long z = zs[i];
                buffer.append(' ');
                buffer.append(formatZ(z));
                if (counts[i] > 1) {
                    buffer.append('[');
                    buffer.append(counts[i]);
                    buffer.append(']');
                }
            }
            String nextZ = eof ? "eof" : formatZ(current.z());
            LOG.log(Level.FINE,
                    "{0} {1}: nest:{2}, current: {3}",
                    new Object[]{this, label, buffer.toString(), nextZ});
        }
    }
}
