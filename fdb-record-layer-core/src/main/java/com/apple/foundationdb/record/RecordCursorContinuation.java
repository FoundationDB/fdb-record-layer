/*
 * RecordCursorContinuation.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.planprotos.PartialAggregationResult;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An interface for types that represent the continuation of a {@link RecordCursor}.
 *
 * <p>
 * A {@code RecordCursorContinuation} represents the current position of a cursor and can be used to restart a cursor
 * at a point immediately after the record returned with the continuation. As a result, it must include all the state
 * essential to the operation of the cursor tree. The continuation can also be serialized to an opaque byte array that
 * can be passed to a client.
 * </p>
 *
 * <p>
 * For historical reasons, there is a subtle relationship between the {@link #isEnd()} and {@link #toBytes()} methods.
 * If {@code isEnd()} returns {@code true}, then {@code toBytes()} must return {@code null}. However, the converse need
 * not be true: {@code toBytes()} may return {@code null} even if {@code isEnd()} returns {@code false}. For an example
 * of why this necessary, see {@link RecordCursorStartContinuation} and {@link RecordCursorEndContinuation}.
 * </p>
 *
 * <p>
 * When implementing a {@code RecordCursorContinuation}, great care must be taken to ensure that the continuation does
 * not share any objects with its cursor that might be changed while the cursor executes. Once initialized, a
 * continuation must remain unchanged and valid even after the cursor has advanced further or even been closed. To see
 * an example of how this can be done, see the implementation of the
 * {@link com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor}'s continuation.
 * </p>
 */
@API(API.Status.UNSTABLE)
public interface RecordCursorContinuation {
    /**
     * Serialize this continuation to a byte array. This method must always return the same array contents (but not
     * necessarily the same array). If {@link #isEnd()} returns {@code true}, then {@code toBytes()} must return
     * {@code null}.
     * @return a (possibly null) byte array containing a binary serialization of this continuation
     */
    @Nullable
    byte[] toBytes();

    /**
     * Serialize this continuation to a ByteString object.
     * If {@link #toBytes()} returns null, then {@code toByteString()} is supposed to return EMPTY.
     * @return a (possibly EMPTY) ByteString containing a binary serialization of this continuation
     */
    @Nonnull
    default ByteString toByteString() {
        final byte[] bytes = toBytes();
        return bytes == null ? ByteString.EMPTY : ByteString.copyFrom(bytes);
    }

    @Nullable
    default PartialAggregationResult getPartialAggregationResult() {
        return null;
    }

    /**
     * Return whether this continuation is an "end continuation", i.e., represents that the iteration has reached
     * its end and would not produce more results even if restarted. If {@code isEnd()} returns {@code true}, then
     * {@link #toBytes()} must return {@code null}.
     * @return whether this continuation represents the end of a cursor's iteration
     */
    boolean isEnd();
}
