/*
 * ContinuationImpl.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.continuation.grpc.ContinuationProto;

import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ContinuationImpl implements Continuation {
    public static final int CURRENT_VERSION = 1;

    public static final Continuation BEGIN = new ContinuationImpl((byte[]) null);

    public static final Continuation END = new ContinuationImpl(new byte[0]);

    @Nonnull
    private final ContinuationProto proto;

    // TODO(yhatem) remove semantic nulls.
    private ContinuationImpl(@Nullable byte[] continuationBytes) {
        ContinuationProto.Builder builder = ContinuationProto.newBuilder().setVersion(CURRENT_VERSION);
        if (continuationBytes != null) {
            builder.setUnderlyingBytes(ByteString.copyFrom(continuationBytes));
        }
        proto = builder.build();
    }

    private ContinuationImpl(@Nonnull ContinuationProto proto) {
        this.proto = proto;
    }

    public int getVersion() {
        return proto.getVersion();
    }

    @Override
    public byte[] serialize() {
        return proto.toByteArray();
    }

    @Nullable
    @Override
    public byte[] getUnderlyingBytes() {
        if (!proto.hasUnderlyingBytes()) {
            return null;
        } else {
            return proto.getUnderlyingBytes().toByteArray();
        }
    }

    // factory methods.

    /**
     * Create a new continuation from a given (inner) continuation bytes.
     * @param bytes the inner (cursor continuation) to be placed inside the newly created continuation
     * @return a continuation that holds the given cursor continuation
     */
    public static Continuation fromUnderlyingBytes(@Nullable byte[] bytes) {
        if (bytes == null) {
            return BEGIN;
        } else if (bytes.length == 0) {
            return END;
        }
        return new ContinuationImpl(bytes);
    }

    /**
     * Create a new continuation from a given (inner) continuation Integer offset.
     * @param offset the offset to be placed inside the newly created continuation
     * @return a continuation that holds the given offset
     */
    public static Continuation fromInt(int offset) {
        assert offset >= 0;
        return new ContinuationImpl(Ints.toByteArray(offset));
    }

    /**
     * Create a new continuation from a given cursor continuation.
     * @param cursorContinuation the inner cursor continuation to be placed inside the newly created continuation
     * @return a continuation that holds the given cursor continuation
     */
    public static Continuation fromRecordCursorContinuation(RecordCursorContinuation cursorContinuation) {
        return cursorContinuation.isEnd() ? END : new ContinuationImpl(cursorContinuation.toBytes());
    }

    /**
     * Deserialize and parse a continuation. This would create a continuation from a previously serialized byte array.
     * @param bytes the serialized continuation
     * @return the deserialized continuation
     * @throws InvalidProtocolBufferException in case the continuation cannot be deserialized
     */
    public static Continuation parseContinuation(byte[] bytes) throws InvalidProtocolBufferException {
        if (bytes == null) {
            return BEGIN;
        } else {
            return new ContinuationImpl(ContinuationProto.parseFrom(bytes));
        }
    }

    public static Continuation copyOf(@Nonnull Continuation other) throws RelationalException {
        if (other instanceof ContinuationImpl) {
            // ContinuationImpl is immutable, no need to actually copy
            return other;
        } else if (other.atBeginning()) {
            return BEGIN;
        } else if (other.atEnd()) {
            return END;
        } else {
            String message = String.format("programming error, extra logic required for copy-constructing from %s", other.getClass());
            assert false : message;
            throw new RelationalException(message, ErrorCode.INTERNAL_ERROR); // -ea safety net.
        }
    }
}
