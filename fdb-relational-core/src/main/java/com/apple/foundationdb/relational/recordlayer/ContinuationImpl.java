/*
 * ContinuationImpl.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.continuation.CompiledStatement;
import com.apple.foundationdb.relational.continuation.ContinuationProto;

import com.apple.foundationdb.relational.continuation.CopyPlan;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Locale;
import java.util.Objects;

@API(API.Status.EXPERIMENTAL)
public final class ContinuationImpl implements Continuation {
    public static final int CURRENT_VERSION = 1;

    public static final ContinuationImpl BEGIN = new ContinuationImpl((byte[]) null);

    public static final ContinuationImpl END = new ContinuationImpl(new byte[0]);

    @Nonnull
    private final ContinuationProto proto;

    // TODO(yhatem) remove semantic nulls.
    private ContinuationImpl(@Nullable byte[] continuationBytes) {
        ContinuationProto.Builder builder = ContinuationProto.newBuilder().setVersion(CURRENT_VERSION);
        if (continuationBytes != null) {
            builder.setExecutionState(ByteString.copyFrom(continuationBytes));
        }
        proto = builder.build();
    }

    ContinuationImpl(@Nonnull ContinuationProto proto) {
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
    public byte[] getExecutionState() {
        if (!proto.hasExecutionState()) {
            return null;
        } else {
            return proto.getExecutionState().toByteArray();
        }
    }

    @Override
    public Reason getReason() {
        if (proto.hasReason()) {
            return Reason.valueOf(proto.getReason().name());
        }
        return null;
    }

    public boolean hasCompiledStatement() {
        return proto.hasCompiledStatement();
    }

    public boolean hasCopyPlan() {
        return proto.hasCopyPlan();
    }

    @Nonnull
    public CopyPlan getCopyPlan() {
        return proto.getCopyPlan();
    }

    /**
     * Hash code for the parameter binding for the continuation.
     * Once the query gets a continuation, a stable hash of the parameter binding  (both explicit - from the customer and
     * implicit - added by the system) is stored in the continuation. Once another query request is attempted with the
     * continuation, the binding hash can be compared to verify that the query matches the continuation. The request will
     * be rejected if the hashes do not match.
     * @return a stable hash for the parameter bindings for the continuation
     */
    @Nullable
    public Integer getBindingHash() {
        if (proto.hasBindingHash()) {
            return proto.getBindingHash();
        } else {
            return null;
        }
    }

    /**
     * Hash code for the plan for the continuation.
     * The plan hash represents the plan that was used to execute the query. This would detect changes to the plan (either
     * because the query changed or because the environment changed). Once a query is attempted with a continuation where
     * the plan hash does not match that of the continuation, the query gets rejected.
     * @return the hash of the plan
     */
    @Nullable
    public Integer getPlanHash() {
        if (proto.hasPlanHash()) {
            return proto.getPlanHash();
        } else {
            return null;
        }
    }

    /**
     * Return the compiled statement proto if a package is contained inside the continuation.

import com.apple.foundationdb.annotation.API;
     * @return the {@link CompiledStatement}.
     */
    @Nullable
    public CompiledStatement getCompiledStatement() {
        if (proto.hasCompiledStatement()) {
            return proto.getCompiledStatement();
        } else {
            return null;
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
    public static ContinuationImpl parseContinuation(byte[] bytes) throws InvalidProtocolBufferException {
        if (bytes == null) {
            return BEGIN;
        } else {
            return new ContinuationImpl(ContinuationProto.parseFrom(bytes));
        }
    }

    public static ContinuationImpl copyOf(@Nonnull Continuation other) throws RelationalException {
        if (other instanceof ContinuationImpl) {
            // ContinuationImpl is immutable, no need to actually copy
            return (ContinuationImpl) other;
        } else if (other.atBeginning()) {
            return BEGIN;
        } else if (other.atEnd()) {
            return END;
        } else {
            String message = String.format(Locale.ROOT, "programming error, extra logic required for copy-constructing from %s", other.getClass());
            assert false : message;
            throw new RelationalException(message, ErrorCode.INTERNAL_ERROR); // -ea safety net.
        }
    }

    /**
     * Factory method to create a new {@link ContinuationBuilder}.
     * @return a newly created continuation builder
     */
    public static ContinuationBuilder newBuilder() {
        return new ContinuationBuilder();
    }

    /**
     * Create a {@link ContinuationBuilder} initialized to the state of this continuation. This can be used as a mutation
     * method to modify an existing continuation.
     * @return a builder initialized to the state of this continuation
     */
    public ContinuationBuilder asBuilder() {
        return new ContinuationBuilder(proto);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ContinuationImpl)) {
            return false;
        }
        ContinuationImpl that = (ContinuationImpl) o;
        return proto.equals(that.proto);
    }

    @Override
    public int hashCode() {
        return Objects.hash(proto);
    }
}
