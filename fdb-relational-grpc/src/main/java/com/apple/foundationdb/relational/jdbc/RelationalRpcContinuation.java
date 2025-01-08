/*
 * RelationalRpcContinuation.java
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
import com.apple.foundationdb.relational.jdbc.grpc.v1.RpcContinuation;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class RelationalRpcContinuation implements Continuation {
    public static final int CURRENT_VERSION = 1;

    public static final RelationalRpcContinuation BEGIN = new RelationalRpcContinuation((byte[]) null);
    public static final RelationalRpcContinuation END = new RelationalRpcContinuation(new byte[0]);

    @Nonnull
    private final RpcContinuation proto;

    public RelationalRpcContinuation(@Nonnull RpcContinuation proto) {
        this.proto = proto;
    }

    private RelationalRpcContinuation(@Nullable byte[] continuationBytes) {
        RpcContinuation.Builder builder = RpcContinuation.newBuilder().setVersion(CURRENT_VERSION);
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
    public static RelationalRpcContinuation parseContinuation(byte[] bytes) throws InvalidProtocolBufferException {
        if (bytes == null) {
            return BEGIN; // TODO: Is this right?
        } else {
            return new RelationalRpcContinuation(RpcContinuation.parseFrom(bytes));
        }
    }

    @Override
    public byte[] serialize() {
        // Serialize the INTERNAL STATE. This is done since on the client side, when the query is modified to include an
        // encoded version of the continuation as a string, we have to use the INTERNAL flavor of the continuation to have
        // it properly deserialized by the server. The server only accepts the internal continuation as part of the query.
        return proto.getInternalState().toByteArray();
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
    public RpcContinuation getProto() {
        return proto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RelationalRpcContinuation)) {
            return false;
        }
        RelationalRpcContinuation that = (RelationalRpcContinuation) o;
        return Objects.equals(proto, that.proto);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(proto);
    }
}
