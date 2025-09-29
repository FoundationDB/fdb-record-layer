/*
 * SizeStatisticsGroupingContinuation.java
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A continuation for the {@link SizeStatisticsGroupingCursor}.
 */
class SizeStatisticsGroupingContinuation implements RecordCursorContinuation {
    /**
     * A continuation for the case where we have one last result to send, after which the cursor would be done.
      */
    public static final SizeStatisticsGroupingContinuation LAST_RESULT_CONTINUATION = new SizeStatisticsGroupingContinuation();

    /** Whether the continuation is a marker for the last result being sent before the cursor is done. */
    private final boolean lastResultContinuation;
    @Nullable
    private RecordCursorContinuation innerContinuation;
    @Nullable
    private SizeStatisticsResults partialResults;
    private Tuple currentGroupingKey;
    @Nullable
    private byte[] cachedBytes;
    @Nullable
    private ByteString cachedByteString;

    /**
     * The inner cursor is done, and we sent the last result, cannot continue afterward.
     */
    private SizeStatisticsGroupingContinuation() {
        lastResultContinuation = true;
    }

    /**
     * The inner cursor still has results, we can continue (e.g. group break).
     */
    SizeStatisticsGroupingContinuation(@Nonnull RecordCursorResult<KeyValue> currentKvResult,
                                       @Nonnull SizeStatisticsResults partialResults,
                                       @Nonnull Tuple currentGroupingKey) {
        lastResultContinuation = false;
        this.innerContinuation = currentKvResult.getContinuation();
        this.partialResults = partialResults.copy(); //cache an immutable snapshot of the partial aggregate state
        this.currentGroupingKey = currentGroupingKey;
    }

    /**
     * Whether the given protobuf is the same as the "LAST_RESULT_CONTINUATION".
     * @param statsContinuation the incoming protobuf
     * @return TRUE if the protobuf marks a continuation with last result
     */
    public static boolean isLastResultContinuation(final RecordCursorProto.SizeStatisticsGroupingContinuation statsContinuation) {
        // Default to false
        return (statsContinuation.hasLastResultContinuation() &&
                statsContinuation.getLastResultContinuation());
    }

    public boolean isLastResultContinuation() {
        return lastResultContinuation;
    }

    @Nullable
    @Override
    public byte[] toBytes() {
        // form bytes exactly once
        if (this.cachedBytes == null) {
            this.cachedBytes = toByteString().toByteArray();
        }
        return cachedBytes;
    }

    @Override
    @Nonnull
    public ByteString toByteString() {
        if (cachedByteString == null) {
            final RecordCursorProto.SizeStatisticsGroupingContinuation.Builder builder = RecordCursorProto.SizeStatisticsGroupingContinuation.newBuilder();
            builder.setLastResultContinuation(lastResultContinuation);
            if (innerContinuation != null) {
                builder.setUnderlyingContinuation(innerContinuation.toByteString());
            }
            if (partialResults != null) {
                builder.setPartialResults(partialResults.toProto());
            }
            if (currentGroupingKey != null) {
                builder.setCurrentGroupingKey(ByteString.copyFrom(currentGroupingKey.pack()));
            }
            cachedByteString = builder.build().toByteString();
        }
        return cachedByteString;
    }

    /**
     * Whether this continuation is the end of the stream.
     * This is always false, since the {@link RecordCursorEndContinuation#END} would be used once the cursor is exhausted.
     * @return TRUE if the cursor is exhausted
     */
    @Override
    public boolean isEnd() {
        return false;
    }
}
