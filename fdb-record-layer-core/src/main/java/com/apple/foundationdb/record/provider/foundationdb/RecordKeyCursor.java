/*
 * RecordKeyCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.BaseCursor;
import com.apple.foundationdb.record.cursors.CursorLimitManager;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * This cursor may exceed out-of-band limits in order to ensure that it only ever stops in between (split) records.
 * It is therefore unusual since it may exceed the record scan limit arbitrarily based on the size of the record
 * and the maximum value size.
 */
public class RecordKeyCursor implements BaseCursor<Tuple> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordKeyCursor.class);

    @Nonnull
    private final FDBRecordContext context;
    @Nonnull
    private final RecordCursor<KeyValue> inner;
    private final boolean oldVersionFormat;
    @Nonnull
    private final SplitHelper.SizeInfo sizeInfo;
    @Nonnull
    private final Subspace subspace;
    @Nullable
    private Tuple next;
    @Nullable
    private Tuple nextKey;
    @Nullable
    private Subspace nextSubspace;
    @Nullable
    private FDBRecordVersion nextVersion;
    @Nullable
    private byte[] nextPrefix;
    private long nextIndex;
    @Nullable
    private NoNextReason innerNoNextReason;
    @Nullable
    private RecordCursorResult<KeyValue> pending;
    @Nullable
    private RecordCursorContinuation continuation;
    @Nonnull
    private final CursorLimitManager limitManager;
    private long readLastKeyNanos = 0L; // for logging purposes

    // for supporting old cursor API
    @Nullable
    private RecordCursorResult<Tuple> nextResult;

    public RecordKeyCursor(@Nonnull FDBRecordContext context, @Nonnull final Subspace subspace,
                              @Nonnull final RecordCursor<KeyValue> inner, final boolean oldVersionFormat,
                              @Nullable final SplitHelper.SizeInfo sizeInfo, @Nonnull ScanProperties scanProperties) {
        this(context, subspace, inner, oldVersionFormat, sizeInfo, new CursorLimitManager(scanProperties));
    }

    public RecordKeyCursor(@Nonnull FDBRecordContext context, @Nonnull final Subspace subspace,
                              @Nonnull final RecordCursor<KeyValue> inner, final boolean oldVersionFormat,
                              @Nullable final SplitHelper.SizeInfo sizeInfo,
                           @Nonnull CursorLimitManager limitManager) {
        this.context = context;
        this.subspace = subspace;
        this.inner = inner;
        this.oldVersionFormat = oldVersionFormat;
        this.sizeInfo = sizeInfo == null ? new SplitHelper.SizeInfo() : sizeInfo;
        this.limitManager = limitManager;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<Tuple>> onNext() {
        if (nextResult != null && !nextResult.hasNext()) {
            return CompletableFuture.completedFuture(nextResult);
        }
        if (limitManager.isStopped()) {
            final NoNextReason noNextReason = mergeNoNextReason();
            if (noNextReason.isSourceExhausted()) {
                // Can happen if the limit is reached while reading the final record, so the inner cursor
                // completes with SOURCE_EXHAUSTED
                nextResult = RecordCursorResult.exhausted();
            } else {
                nextResult = RecordCursorResult.withoutNextValue(continuation, mergeNoNextReason());
            }
            return CompletableFuture.completedFuture(nextResult);
        } else {
            return appendUntilNewKey().thenApply(vignore -> {
                if (nextVersion != null && next == null) {
                    throw new SplitHelper.FoundSplitWithoutStartException(SplitHelper.RECORD_VERSION, false)
                            .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey)
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()))
                            .addLogInfo(LogMessageKeys.VERSION, nextVersion);
                }
                if (!oldVersionFormat && nextKey != null) {
                    // Account for incomplete version
                    final byte[] versionKey = subspace.subspace(nextKey).pack(SplitHelper.RECORD_VERSION);
                    context.getLocalVersion(versionKey).ifPresent(localVersion -> {
                        nextVersion = FDBRecordVersion.incomplete(localVersion);
                        sizeInfo.setVersionedInline(true);
                        sizeInfo.setKeyCount(sizeInfo.getKeyCount() + 1);
                        sizeInfo.setKeySize(sizeInfo.getKeySize() + versionKey.length);
                        sizeInfo.setValueSize(sizeInfo.getValueSize() + 1 + FDBRecordVersion.VERSION_LENGTH);
                    });
                }

                if (next == null) { // no next result
                    nextResult = RecordCursorResult.withoutNextValue(continuation, mergeNoNextReason());
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(KeyValueLogMessage.of("unsplitter stopped",
                                LogMessageKeys.NEXT_CONTINUATION, continuation == null ? "null" : ByteArrayUtil2.loggable(continuation.toBytes()),
                                LogMessageKeys.NO_NEXT_REASON, nextResult.getNoNextReason(),
                                LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey())));
                    }
                } else { // has next result
                    sizeInfo.setVersionedInline(nextVersion != null);
                    final Tuple result = nextKey;
                    next = null;
                    nextKey = null;
                    nextVersion = null;
                    nextPrefix = null;
                    nextResult = RecordCursorResult.withNextValue(result, continuation);
                    if (LOGGER.isTraceEnabled()) {
                        KeyValueLogMessage msg = KeyValueLogMessage.build("unsplitter assembled new record",
                                LogMessageKeys.NEXT_CONTINUATION, continuation == null ? "null" : ByteArrayUtil2.loggable(continuation.toBytes()),
                                LogMessageKeys.KEY_TUPLE, result,
                                LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey()));
                        LOGGER.trace(msg.toString());
                    }
                }
                return nextResult;
            });
        }
    }

    @Nonnull
    @Override
    public RecordCursorResult<Tuple> getNext() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, onNext());
    }

    @Nonnull
    public NoNextReason mergeNoNextReason() {
        if (innerNoNextReason == NoNextReason.SOURCE_EXHAUSTED) {
            return innerNoNextReason;
        }
        return limitManager.getStoppedReason().orElse(innerNoNextReason);
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return inner.isClosed();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return inner.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inner.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    // Process all elements from the scan until we get a new primary key
    @SuppressWarnings("PMD.UnnecessaryLocalBeforeReturn") // Name and negation make it much clearer as is
    private CompletableFuture<Void> appendUntilNewKey() {
        return AsyncUtil.whileTrue(() -> {
            if (pending != null) {
                boolean complete = append(pending);
                pending = null;
                if (complete) {
                    // Split followed by unsplit; available right away.
                    return AsyncUtil.READY_FALSE;
                }
            }
            return inner.onNext().thenApply(innerResult -> {
                if (!innerResult.hasNext()) {
                    innerNoNextReason = innerResult.getNoNextReason();
                    // If we already built up some values, then we already cached an appropriate continuation.
                    // If we haven't the continuation might have changed so we need to refresh it.
                    if (next == null) {
                        continuation = innerResult.getContinuation();
                    }
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(KeyValueLogMessage.of("unsplitter inner cursor stopped",
                                LogMessageKeys.NEXT_CONTINUATION, continuation == null ? "null" : ByteArrayUtil2.loggable(continuation.toBytes()),
                                LogMessageKeys.NO_NEXT_REASON, innerNoNextReason,
                                LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey())
                        ));
                    }
                    return false;
                } else {
                    innerNoNextReason = null; // currently, we have a next value
                    limitManager.tryRecordScan();
                    boolean complete = append(innerResult);
                    return !complete;
                }
            });
        }, inner.getExecutor());
    }

    // Process the next key-value pair from the inner cursor; return whether unsplit complete.
    protected boolean append(@Nonnull RecordCursorResult<KeyValue> resultWithKv) {
        @Nonnull KeyValue kv = resultWithKv.get(); // KeyValue is non-null since we only pass in a result that has one
        limitManager.reportScannedBytes(kv.getKey().length + kv.getValue().length);
        if (nextPrefix == null) {
            continuation = resultWithKv.getContinuation();
            return appendFirst(kv);
        } else if (ByteArrayUtil.startsWith(kv.getKey(), nextPrefix)) {
            continuation = resultWithKv.getContinuation();
            return appendNext(kv);
        } else {
            pending = resultWithKv;
            logEndFound();
            return true;
        }
    }

    // Process the first key-value pair for a given record; return whether the record is complete
    private boolean appendFirst(@Nonnull KeyValue kv) {
        final Tuple keyTuple = subspace.unpack(kv.getKey());
        nextKey = keyTuple.popBack(); // Remove index item
        nextSubspace = subspace.subspace(nextKey);
        nextPrefix = nextSubspace.pack();
        next = Tuple.fromBytes(nextPrefix);
        nextIndex = keyTuple.getLong(keyTuple.size() - 1);
        sizeInfo.set(kv);
        boolean done;
        if (nextIndex == SplitHelper.UNSPLIT_RECORD) {
            // First key is an unsplit record key. Since this is going
            // in the forward direction, this is the only key
            sizeInfo.setSplit(false);
            done = true;
        } else if (nextIndex == SplitHelper.RECORD_VERSION) {
            if (oldVersionFormat) {
                throw new RecordCoreException("Found record version when old format specified")
                        .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                        .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple);
            }
            // First key is a record version. This should only happen in
            // the forward scan direction, so if this happens in
            // the reverse direction, it means that there isn't any
            // data associated with the record, but there is a version.
            sizeInfo.setVersionedInline(true);
            nextVersion = SplitHelper.unpackVersion(kv.getValue());
            next = null;
            done = false;
        } else if (nextIndex == SplitHelper.START_SPLIT_RECORD) {
            // The data is at the beginning of the split
            sizeInfo.setSplit(true);
            done = false;
        } else {
            throw new SplitHelper.FoundSplitWithoutStartException(nextIndex, false)
                    .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                    .addLogInfo(LogMessageKeys.KEY_TUPLE, keyTuple);
        }
        logFirstKey(done);
        return done;
    }

    // Process the key-value pair (other than the first one) for a given record; return whether the record is complete
    private boolean appendNext(@Nonnull KeyValue kv) {
        long index = nextSubspace.unpack(kv.getKey()).getLong(0);
        sizeInfo.add(kv);
        boolean done;
        if (nextIndex == SplitHelper.RECORD_VERSION && (index == SplitHelper.UNSPLIT_RECORD || index == SplitHelper.START_SPLIT_RECORD)) {
            // The first key (in a forward) scan was a version. Set the key (so far) to be
            // just what has been read from this key. If it is the beginning of
            // a split record, we have more to do. Otherwise, we know this is the
            // end of the record.
            next = Tuple.fromBytes(nextPrefix);
            nextIndex = index;
            sizeInfo.setSplit(index == SplitHelper.START_SPLIT_RECORD);
            done = nextIndex == SplitHelper.UNSPLIT_RECORD;
        } else if (index == nextIndex + 1) {
            // This is the second or later key (not counting a possible version key)
            // in the forward scan. Append its value to the end of the current
            // key-value pair being accumulated. Return false because there is
            // no way to know if this is the last key or not.
            next = Tuple.fromBytes(nextPrefix);
            nextIndex = index;
            done = false;
        } else {
            if (nextIndex == SplitHelper.RECORD_VERSION) {
                throw new SplitHelper.FoundSplitWithoutStartException(index, false)
                        .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                        .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey)
                        .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()));
            } else {
                throw new RecordCoreException("Split record segments out of order")
                        .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(kv.getKey()))
                        .addLogInfo(LogMessageKeys.KEY_TUPLE, nextKey)
                        .addLogInfo(LogMessageKeys.EXPECTED_INDEX, nextIndex +  1)
                        .addLogInfo(LogMessageKeys.FOUND_INDEX, index)
                        .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.pack()));
            }
        }
        logNextKey(done);
        return done;
    }

    private void logFirstKey(boolean done) {
        logKey("found first key in new split record", done);
    }

    private void logNextKey(boolean done) {
        logKey("found next key in split record", done);
    }

    private void logEndFound() {
        logKey("end key found for split record", true);
    }

    private void logKey(@Nonnull String staticMessage, boolean done) {
        if (LOGGER.isTraceEnabled()) {
            KeyValueLogMessage msg = KeyValueLogMessage.build(staticMessage,
                    LogMessageKeys.KEY_TUPLE, nextKey,
                    LogMessageKeys.SPLIT_REVERSE, false,
                    LogMessageKeys.SPLIT_NEXT_INDEX, nextIndex,
                    LogMessageKeys.KNOWN_LAST_KEY, done,
                    LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspace.getKey()));
            sizeInfo.addSizeLogInfo(msg);
            long currentNanos = System.nanoTime();
            if (readLastKeyNanos != 0) {
                msg.addKeyAndValue(LogMessageKeys.READ_LAST_KEY_MICROS, TimeUnit.NANOSECONDS.toMicros(currentNanos - readLastKeyNanos));
            }
            readLastKeyNanos = currentNanos;
            LOGGER.trace(msg.toString());
        }
    }
}
