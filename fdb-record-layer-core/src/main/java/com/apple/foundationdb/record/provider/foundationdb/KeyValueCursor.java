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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.KeyRange;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.AsyncIteratorCursor;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * The basic cursor for scanning ranges of the FDB database.
 */
@API(API.Status.MAINTAINED)
public class KeyValueCursor extends AsyncIteratorCursor<KeyValue> implements BaseCursor<KeyValue> {
    @Nullable
    private final FDBRecordContext context;
    private final int prefixLength;
    @Nonnull
    private final CursorLimitManager limitManager;
    private int valuesLimit;
    // the pointer may be mutated, but the actual array must never be mutated or continuations will break
    @Nullable
    private byte[] lastKey;

    private KeyValueCursor(@Nonnull final FDBRecordContext context,
                           @Nonnull final AsyncIterator<KeyValue> iterator,
                           int prefixLength,
                           @Nonnull final CursorLimitManager limitManager,
                           int valuesLimit) {
        super(context.getExecutor(), iterator);

        this.context = context;
        this.prefixLength = prefixLength;
        this.limitManager = limitManager;
        this.valuesLimit = valuesLimit;

        context.instrument(FDBStoreTimer.DetailEvents.GET_SCAN_RANGE_RAW_FIRST_CHUNK, iterator.onHasNext());
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<KeyValue>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            // This guard is needed to guarantee that if onNext is called multiple times after the cursor has
            // returned a result without a value, then the same NoNextReason is returned each time. Without this guard,
            // one might return SCAN_LIMIT_REACHED (for example) after returning a result with SOURCE_EXHAUSTED because
            // of the tryRecordScan check.
            return CompletableFuture.completedFuture(nextResult);
        } else if (limitManager.tryRecordScan()) {
            return iterator.onHasNext().thenApply(hasNext -> {
                mayGetContinuation = !hasNext;
                if (hasNext) {
                    KeyValue kv = iterator.next();
                    if (context != null) {
                        context.increment(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY);
                        context.increment(FDBStoreTimer.Counts.LOAD_KEY_VALUE);
                    }
                    limitManager.reportScannedBytes(kv.getKey().length + kv.getValue().length);
                    // Note that this mutates the pointer and NOT the array.
                    // If the value of lastKey is mutated, the Continuation class will break.
                    lastKey = kv.getKey();
                    valuesSeen++;
                    nextResult = RecordCursorResult.withNextValue(kv, continuationHelper());
                } else if (valuesSeen >= valuesLimit) {
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

    @Override
    @Nonnull
    public RecordCursorResult<KeyValue> getNext() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, onNext());
    }

    @Nonnull
    private RecordCursorContinuation continuationHelper() {
        return new Continuation(lastKey, prefixLength);
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

        public KeyValueCursor build() throws RecordCoreException {
            if (subspace == null) {
                throw new RecordCoreException("record subspace must be supplied");
            }

            if (context == null) {
                throw new RecordCoreException("record context must be supplied");
            }

            if (scanProperties == null) {
                throw new RecordCoreException("record scanProperties must be supplied");
            }

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

            // Handle the continuation and then turn the endpoints into one byte array on the
            // left (inclusive) and another on the right (exclusive).
            int prefixLength = subspace.pack().length;
            while ((prefixLength < lowBytes.length) &&
                   (prefixLength < highBytes.length) &&
                   (lowBytes[prefixLength] == highBytes[prefixLength])) {
                prefixLength++;
            }

            final boolean reverse = scanProperties.isReverse();
            if (continuation != null) {
                final byte[] continuationBytes = new byte[prefixLength + continuation.length];
                System.arraycopy(lowBytes, 0, continuationBytes, 0, prefixLength);
                System.arraycopy(continuation, 0, continuationBytes, prefixLength, continuation.length);
                if (reverse) {
                    highBytes = continuationBytes;
                    highEndpoint = EndpointType.CONTINUATION;
                } else {
                    lowBytes = continuationBytes;
                    lowEndpoint = EndpointType.CONTINUATION;
                }
            }
            final Range byteRange = TupleRange.toRange(lowBytes, highBytes, lowEndpoint, highEndpoint);
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

            final int limit = scanProperties.getExecuteProperties().getReturnedRowLimit();
            final StreamingMode streamingMode;
            if (scanProperties.getCursorStreamingMode() == CursorStreamingMode.ITERATOR) {
                streamingMode = StreamingMode.ITERATOR;
            } else if (limit == ReadTransaction.ROW_LIMIT_UNLIMITED) {
                streamingMode = StreamingMode.WANT_ALL;
            } else {
                streamingMode = StreamingMode.EXACT;
            }

            final AsyncIterator<KeyValue> iterator = context.readTransaction(scanProperties.getExecuteProperties().getIsolationLevel().isSnapshot())
                    .getRange(begin, end, limit, reverse, streamingMode)
                    .iterator();

            final CursorLimitManager limitManager = new CursorLimitManager(context, scanProperties);
            final int valuesLimit = scanProperties.getExecuteProperties().getReturnedRowLimitOrMax();

            return new KeyValueCursor(context, iterator, prefixLength, limitManager, valuesLimit);
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
