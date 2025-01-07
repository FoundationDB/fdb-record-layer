/*
 * ChainedCursor.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that iterates over a set of data that is dynamically generated a single value at a time. The cursor is
 * driven by a value generation function.  The function initially takes an <code>Optional.empty()</code> as
 * input, and from that produces an initial value.  Upon subsequent iterations the function is given the
 * value it previously returned and the next value is computed.  For example, the following generator would
 * produce the values from 1 to 10:
 * <pre>
 *     maybePreviousValue -&gt; {
 *         if (maybePreviousValue.isPresent()) {
 *             if (maybePreviousValue.get() &lt; 10) {
 *                 return Optional.of(maybePreviousValue.get() + 1);
 *             } else {
 *                 return Optional.empty();
 *             }
 *         }
 *         return Optional.of(1);
 *     }
 * </pre>
 * Given this function, the <code>ChainedCursor</code> would iteratively generate the value from 1 to 10.
 *
 * <p>In order to support continuations, the cursor must additionally be provided a functions that can
 * encode and decode the values it produces into binary continuation form.  So the fully defined cursor
 * to perform our iteration from 1 to 10, would look like:
 * <pre>
 *     new ChainedCursor(
 *         // Value generator
 *         maybePreviousValue -&gt; {
 *             if (maybePreviousValue.isPresent()) {
 *                 if (maybePreviousValue.get() &lt; 10) {
 *                     return Optional.of(maybePreviousValue.get() + 1);
 *                 } else {
 *                     return Optional.empty();
 *                 }
 *             }
 *             return Optional.of(1);
 *         },
 *         // Continuation encoder
 *         previousValue -&gt; Tuple.from(previousValue).pack(),
 *         // Continuation decoder
 *         encodedContinuation -&gt; Tuple.fromBytes(encodedContinuation).getLong(0)
 *         executor
 *     )
 * </pre>
 *
 * <h2>Resource Governing</h2>
 *
 * When the <code>ChainedCursor</code> is created with a set of {@link ScanProperties} it will attempt to count each
 * iteration of the cursor against the limits that are indicated by the properties.  This can lead to an interesting
 * behavior if the same set of <code>ScanProperties</code> are also used by cursors that are present in the <code>nextGenerator</code>
 * that is provided and can lead to double-counting.  For example, if the <code>nextGenerator</code> is driven
 * by the read of a row key via the <code>KeyValueCursor</code>, the cursor could end up counting that read, and the
 * <code>ChainedCursor</code> would then, again, count that read. As such, the caller should take care when
 * specifying the <code>ScanProperties</code> to the cursor and within the <code>nextGenerator</code>.
 *
 * <p>In addition, while the <code>ChainedCursor</code> can track and limit on scanned records and scan time,
 * it has no visibility into any bytes that may have been read by cursor managed by the <code>nextGenerator</code>
 * and, thus, cannot enforce any byte scan limits.
 *
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.UNSTABLE)
public class ChainedCursor<T> implements BaseCursor<T> {
    @Nonnull
    private final Function<Optional<T>, CompletableFuture<Optional<T>>> nextGenerator;
    @Nonnull
    private final Function<T, byte[]> continuationEncoder;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private Optional<T> lastValue;
    @Nullable
    private RecordCursorResult<T> lastResult;

    @Nonnull
    private final CursorLimitManager limitManager;
    private final long maxReturnedRows;
    private long returnedRowCount;
    private boolean closed = false;

    /**
     * Creates a new <code>ChainedCursor</code>.  When created in this fashion any resource limits that may be
     * specified in cursors that are used within the <code>nextGenerator</code> will be honored.
     *
     * @param nextGenerator a function that that is called to perform iteration
     * @param continuationEncoder a function that takes a value that was returned by <code>nextGenerator</code> and
     *     encodes it as a continuation value
     * @param continuationDecoder a function that takes a continuation that was produced by <code>continuationEncoder</code>
     *     and decodes it back to its original value as returned by <code>nextGenerator</code>
     * @param continuation the initial continuation for the iteration
     * @param executor executor that will be returned by {@link #getExecutor()}
     */
    public ChainedCursor(
            @Nonnull Function<Optional<T>, CompletableFuture<Optional<T>>> nextGenerator,
            @Nonnull Function<T, byte[]> continuationEncoder,
            @Nonnull Function<byte[], T> continuationDecoder,
            @Nullable byte[] continuation,
            @Nonnull Executor executor) {
        this(null, nextGenerator, continuationEncoder, continuationDecoder, continuation, null, executor);
    }

    /**
     * Creates a <code>ChainedCursor</code>.
     *
     * <p>The {@link ScanProperties#isReverse()}} cannot be honored by the <code>ChainedCursor</code> as the
     * direction of iteration is strictly controlled by the <code>nextGenerator</code>.
     *
     * @param context a record context
     * @param nextGenerator a function that that is called to perform iteration
     * @param continuationEncoder a function that takes a value that was returned by <code>nextGenerator</code> and
     *     encodes it as a continuation value
     * @param continuationDecoder a function that takes a continuation that was produced by <code>continuationEncoder</code>
     *     and decodes it back to its original value as returned by <code>nextGenerator</code>
     * @param continuation the initial continuation for the iteration
     * @param scanProperties properties used to control the scanning behavior
     */
    public ChainedCursor(
            @Nonnull FDBRecordContext context,
            @Nonnull Function<Optional<T>, CompletableFuture<Optional<T>>> nextGenerator,
            @Nonnull Function<T, byte[]> continuationEncoder,
            @Nonnull Function<byte[], T> continuationDecoder,
            @Nullable byte[] continuation,
            @Nonnull ScanProperties scanProperties) {
        this(context, nextGenerator, continuationEncoder, continuationDecoder, continuation, scanProperties, context.getExecutor());
    }

    private ChainedCursor(
            @Nullable FDBRecordContext context,
            @Nonnull Function<Optional<T>, CompletableFuture<Optional<T>>> nextGenerator,
            @Nonnull Function<T, byte[]> continuationEncoder,
            @Nonnull Function<byte[], T> continuationDecoder,
            @Nullable byte[] continuation,
            @Nullable ScanProperties scanProperties,
            @Nonnull Executor executor) {
        this.nextGenerator = nextGenerator;
        this.continuationEncoder = continuationEncoder;
        this.executor = executor;
        if (continuation != null) {
            this.lastValue = Optional.of(continuationDecoder.apply(continuation));
        } else {
            lastValue = Optional.empty();
        }

        // If we didn't have a context, then we couldn't have any passed in limits, so create a
        // manager in which it will impose no limits.
        if (context == null) {
            limitManager = new CursorLimitManager(ScanProperties.FORWARD_SCAN);
        } else {
            // Due to our public constructors this scenario should never be able to happen, but this
            // is here most to get the code analysis tools to shut up.
            if (scanProperties == null) {
                throw new IllegalStateException("scanProperties cannot be null if context is not null");
            }
            if (scanProperties.isReverse()) {
                throw new RecordCoreArgumentException("ChainedCursor does not support reverse scans")
                        .addLogInfo(LogMessageKeys.SCAN_PROPERTIES, scanProperties);
            }
            limitManager = new CursorLimitManager(context, scanProperties);
        }

        this.maxReturnedRows = scanProperties == null
                               ? Integer.MAX_VALUE
                               : scanProperties.getExecuteProperties().getReturnedRowLimitOrMax();
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (lastResult != null && !lastResult.hasNext()) {
            return CompletableFuture.completedFuture(lastResult);
        }

        if (returnedRowCount == maxReturnedRows) {
            lastResult = RecordCursorResult.withoutNextValue(
                    new Continuation<>(lastValue, continuationEncoder), NoNextReason.RETURN_LIMIT_REACHED);
            return CompletableFuture.completedFuture(lastResult);
        }

        if (!limitManager.tryRecordScan()) {
            lastResult = RecordCursorResult.withoutNextValue(
                    new Continuation<>(lastValue, continuationEncoder), limitManager.getStoppedReason().get());
            return CompletableFuture.completedFuture(lastResult);
        }

        return nextGenerator.apply(lastValue).thenApply(nextValue -> {
            if (nextValue.isPresent()) {
                ++returnedRowCount;
                lastValue = nextValue;
                lastResult = RecordCursorResult.withNextValue(nextValue.get(), new Continuation<>(nextValue, continuationEncoder));
            } else {
                lastValue = nextValue;
                lastResult = RecordCursorResult.exhausted();
            }
            return lastResult;
        });
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        visitor.visitEnter(this);
        return visitor.visitLeave(this);
    }

    private static class Continuation<T> implements RecordCursorContinuation {
        @Nonnull
        private final Optional<T> lastValue;
        @Nonnull
        private final Function<T, byte[]> continuationEncoder;
        @Nullable
        private byte[] cachedBytes;

        public Continuation(@Nonnull Optional<T> lastValue, @Nonnull Function<T, byte[]> continuationEncoder) {
            this.lastValue = lastValue;
            this.continuationEncoder = continuationEncoder;
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            byte[] bytes = toBytes();
            return bytes == null ? ByteString.EMPTY : ZeroCopyByteString.wrap(bytes);
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (cachedBytes == null) {
                cachedBytes = lastValue.map(continuationEncoder).orElse(null);
            }
            return cachedBytes;
        }

        @Override
        public boolean isEnd() {
            return toBytes() == null;
        }
    }
}
