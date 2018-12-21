/*
 * KeyValueCursor.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.API;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.KeyRange;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * The basic cursor for scanning ranges of the FDB database.
 */
@API(API.Status.MAINTAINED)
public class KeyValueCursor implements BaseCursor<KeyValue> {
    @Nullable
    private final FDBRecordContext context;
    private final int prefixLength;
    @Nonnull
    private final AsyncIterator<KeyValue> iter;
    @Nonnull
    private final CursorLimitManager limitManager;
    private int limitRemaining;
    // the pointer may be mutated, but the actual array must never be mutated or continuations will break
    @Nullable
    private byte[] lastKey;
    @Nullable
    private CompletableFuture<Boolean> hasNextFuture = null;
    private RecordCursorResult<KeyValue> nextResult;

    private KeyValueCursor(@Nonnull final FDBRecordContext recordContext,
                           @Nonnull Subspace subspace,
                           @Nonnull byte[] lowBytes,
                           @Nonnull byte[] highBytes,
                           @Nonnull EndpointType lowEndpoint,
                           @Nonnull EndpointType highEndpoint,
                           @Nullable byte[] continuation,
                           @Nonnull ScanProperties scanProperties) {
        this.context = recordContext;

        // Handle the continuation and then turn the endpoints into one byte array on the
        // left (inclusive) and another on the right (exclusive).
        int length = subspace.pack().length;
        while ((length < lowBytes.length) &&
               (length < highBytes.length) &&
               (lowBytes[length] == highBytes[length])) {
            length++;
        }
        this.prefixLength = length;

        final boolean reverse = scanProperties.isReverse();
        if (continuation != null) {
            final byte[] continuationBytes = new byte[length + continuation.length];
            System.arraycopy(lowBytes, 0, continuationBytes, 0, length);
            System.arraycopy(continuation, 0, continuationBytes, length, continuation.length);
            if (reverse) {
                highBytes = continuationBytes;
                highEndpoint = EndpointType.CONTINUATION;
            } else {
                lowBytes = continuationBytes;
                lowEndpoint = EndpointType.CONTINUATION;
            }
        }
        Range byteRange = TupleRange.toRange(lowBytes, highBytes, lowEndpoint, highEndpoint);
        lowBytes = byteRange.begin;
        highBytes = byteRange.end;

        // Begin the scan with the new arrays
        KeySelector begin = KeySelector.firstGreaterOrEqual(lowBytes);
        KeySelector end = KeySelector.firstGreaterOrEqual(highBytes);
        if (scanProperties.getExecuteProperties().getSkip() > 0) {
            if (reverse) {
                end = end.add(- scanProperties.getExecuteProperties().getSkip());
            } else {
                begin = begin.add(scanProperties.getExecuteProperties().getSkip());
            }
        }

        this.limitManager = new CursorLimitManager(recordContext, scanProperties);
        final int limit = scanProperties.getExecuteProperties().getReturnedRowLimit();
        this.limitRemaining = scanProperties.getExecuteProperties().getReturnedRowLimitOrMax();

        final StreamingMode streamingMode;
        if (scanProperties.getCursorStreamingMode() == CursorStreamingMode.ITERATOR) {
            streamingMode = StreamingMode.ITERATOR;
        } else if (limit == ReadTransaction.ROW_LIMIT_UNLIMITED) {
            streamingMode = StreamingMode.WANT_ALL;
        } else {
            streamingMode = StreamingMode.EXACT;
        }

        final long startTime = System.nanoTime();
        this.iter = context.readTransaction(scanProperties.getExecuteProperties().getIsolationLevel().isSnapshot())
                .getRange(begin, end, limit, reverse, streamingMode)
                .iterator();
        if (context.getTimer() != null) {
            context.getTimer().instrument(FDBStoreTimer.DetailEvents.GET_SCAN_RANGE_RAW_FIRST_CHUNK, iter.onHasNext(),
                    context.getExecutor(), startTime);
        }
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<KeyValue>> onNext() {
        if (limitManager.tryRecordScan()) {
            return iter.onHasNext().thenApply(hasNext -> {
                if (hasNext) {
                    KeyValue kv = iter.next();
                    if (context != null) {
                        context.increment(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY);
                        context.increment(FDBStoreTimer.Counts.LOAD_KEY_VALUE);
                    }
                    // Note that this mutates the pointer and NOT the array.
                    // If the value of lastKey is mutated, the Continuation class will break.
                    lastKey = kv.getKey();
                    limitRemaining--;
                    nextResult = RecordCursorResult.withNextValue(kv, continuationHelper());
                } else if (limitRemaining <= 0) {
                    // Source iterator hit limit that we passed down.
                    nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), NoNextReason.RETURN_LIMIT_REACHED);
                } else {
                    // Source iterator is exhausted.
                    nextResult = RecordCursorResult.exhausted();
                }
                return nextResult;
            });
        } else { // a limit must have been exceeded
            final Optional<NoNextReason> stoppedReason = limitManager.getStoppedReason();
            if (!stoppedReason.isPresent()) {
                throw new RecordCoreException("limit manager stopped KeyValueCursor but did not report a reason");
            }
            nextResult = RecordCursorResult.withoutNextValue(continuationHelper(), stoppedReason.get());
            return CompletableFuture.completedFuture(nextResult);
        }
    }

    @Nonnull
    private RecordCursorContinuation continuationHelper() {
        return new Continuation(lastKey, prefixLength);
    }

    @Override
    public boolean hasNext() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, onHasNext());
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> onHasNext() {
        if (hasNextFuture == null) {
            hasNextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return hasNextFuture;
    }

    @Nonnull
    @Override
    public KeyValue next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        hasNextFuture = null;
        return nextResult.get();
    }

    @Nullable
    @Override
    public byte[] getContinuation() {
        return nextResult.getContinuation().toBytes();
    }

    @Override
    public NoNextReason getNoNextReason() {
        return nextResult.getNoNextReason();
    }

    @Override
    public void close() {
        MoreAsyncUtil.closeIterator(iter);
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return context.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    private static class Continuation implements RecordCursorContinuation {
        @Nullable
        private final byte[] lastKey;
        private final int prefixLength;

        public Continuation(@Nullable final byte[] lastKey, final int prefixLength) {
            // Note that doing this without a full copy is dangerous if the array is ever mutated.
            // Currently, this never happens and the only thing that changes is which array lastKey points to.
            // However, if logic in KeyValueCursor or KeyValue changes, this could break continuations.
            // To resolve it, we could resort to doing a full copy here, although that's somewhat expensive.
            this.lastKey = lastKey;
            this.prefixLength = prefixLength;
        }

        @Override
        public boolean isEnd() {
            return lastKey == null;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (lastKey == null) {
                return null;
            }
            return Arrays.copyOfRange(lastKey, prefixLength, lastKey.length);
        }
    }

    /**
     * A builder for {@link KeyValueCursor}.
     *
     * <pre><code>
     * KeyValueCursor.Builder.withSubspace(subspace)
     *                     .setContext(context)
     *                     .setRange(TupleRange.ALL)
     *                     .setContinuation(null)
     *                     .setScanProperties(ScanProperties.FORWARD_SCAN)
     *                     .build()
     * </code></pre>
     */
    @API(API.Status.MAINTAINED)
    public static class Builder {
        private FDBRecordContext context = null;
        private final Subspace subspace;
        private byte[] continuation = null;
        private ScanProperties scanProperties = null;
        private byte[] lowBytes = null;
        private byte[] highBytes = null;
        private EndpointType lowEndpoint = null;
        private EndpointType highEndpoint = null;

        private Builder(@Nonnull Subspace subspace) {
            this.subspace = subspace;
        }

        public static Builder withSubspace(@Nonnull Subspace subspace) {
            return new Builder(subspace);
        }

        public KeyValueCursor build() {
            if (lowBytes == null) {
                lowBytes = subspace.pack();
            }
            if (highBytes == null) {
                highBytes = subspace.pack();
            }
            if (lowEndpoint == null) {
                lowEndpoint = EndpointType.TREE_START;
            }
            if (highEndpoint == null) {
                highEndpoint = EndpointType.TREE_END;
            }

            return new KeyValueCursor(context, subspace, lowBytes, highBytes, lowEndpoint, highEndpoint,
                    continuation, scanProperties);
        }

        public Builder setContext(FDBRecordContext context) {
            this.context = context;
            return this;
        }

        @SpotBugsSuppressWarnings(value = "EI2", justification = "copies are expensive")
        public Builder setContinuation(@Nullable byte[] continuation) {
            this.continuation = continuation;
            return this;
        }

        public Builder setScanProperties(@Nonnull ScanProperties scanProperties) {
            this.scanProperties = scanProperties;
            return this;
        }

        public Builder setRange(@Nonnull KeyRange range) {
            setLow(range.getLowKey(), range.getLowEndpoint());
            setHigh(range.getHighKey(), range.getHighEndpoint());
            return this;
        }

        public Builder setRange(@Nonnull TupleRange range) {
            setLow(range.getLow(), range.getLowEndpoint());
            setHigh(range.getHigh(), range.getHighEndpoint());
            return this;
        }

        public Builder setLow(@Nullable Tuple low, @Nonnull EndpointType lowEndpoint) {
            return setLow(low != null ? subspace.pack(low) : subspace.pack(), lowEndpoint);
        }

        @SpotBugsSuppressWarnings(value = "EI2", justification = "copies are expensive")
        public Builder setLow(@Nonnull byte[] lowBytes, @Nonnull EndpointType lowEndpoint) {
            this.lowBytes = lowBytes;
            this.lowEndpoint = lowEndpoint;
            return this;
        }

        public Builder setHigh(@Nullable Tuple high, @Nonnull EndpointType highEndpoint) {
            return setHigh(high != null ? subspace.pack(high) : subspace.pack(), highEndpoint);
        }

        @SpotBugsSuppressWarnings(value = "EI2", justification = "copies are expensive")
        public Builder setHigh(@Nonnull byte[] highBytes, @Nonnull EndpointType highEndpoint) {
            this.highBytes = highBytes;
            this.highEndpoint = highEndpoint;
            return this;
        }
    }
}
