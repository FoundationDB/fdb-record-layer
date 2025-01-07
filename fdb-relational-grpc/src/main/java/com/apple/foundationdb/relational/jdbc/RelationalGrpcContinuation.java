/*
 * RelationalGrpcContinuation.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetContinuation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class RelationalGrpcContinuation implements Continuation {
    public static final int CURRENT_VERSION = 1;

    public static final RelationalGrpcContinuation BEGIN = new RelationalGrpcContinuation((byte[]) null);
    public static final RelationalGrpcContinuation END = new RelationalGrpcContinuation(new byte[0]);

    @Nonnull
    private final ResultSetContinuation proto;

    public RelationalGrpcContinuation(@Nonnull ResultSetContinuation proto) {
        this.proto = proto;
    }

    private RelationalGrpcContinuation(@Nullable byte[] continuationBytes) {
        ResultSetContinuation.Builder builder = ResultSetContinuation.newBuilder().setVersion(CURRENT_VERSION);
        if (continuationBytes != null) {
            builder.setInternalState(ByteString.copyFrom(continuationBytes));
        }
        // This continuation has no Reason set (usage is internal to create BEGIN/END continuations)
        proto = builder.build();
    }

    /**
     * Deserialize and parse a continuation. This would create a continuation from a previously serialized byte array.
     * @param bytes the serialized continuation
     * @return the deserialized continuation
     * @throws InvalidProtocolBufferException in case the continuation cannot be deserialized
     */
    public static RelationalGrpcContinuation parseContinuation(byte[] bytes) throws InvalidProtocolBufferException {
        if (bytes == null) {
            return BEGIN; // TODO: Is this right?
        } else {
            return new RelationalGrpcContinuation(ResultSetContinuation.parseFrom(bytes));
        }
    }

    @Override
    public byte[] serialize() {
        return proto.toByteArray();
    }

    @Nullable
    @Override
    public byte[] getExecutionState() {
        return proto.getInternalState().toByteArray();
    }

    @Override
    public Reason getReason() {
        return TypeConversion.toReason(proto.getReason());
    }

    @Nonnull
    public ResultSetContinuation getProto() {
        return proto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RelationalGrpcContinuation)) {
            return false;
        }
        RelationalGrpcContinuation that = (RelationalGrpcContinuation) o;
        return Objects.equals(proto, that.proto);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(proto);
    }
}
