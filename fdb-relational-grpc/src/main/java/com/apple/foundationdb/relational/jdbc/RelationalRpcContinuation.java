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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

public class RelationalRpcContinuation implements Continuation {
    public static final int CURRENT_VERSION = 1;

    @Nonnull
    private final RpcContinuation proto;

    public RelationalRpcContinuation(@Nonnull RpcContinuation proto) {
        this.proto = proto;
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
        if (proto.hasInternalState()) {
            return proto.getInternalState().toByteArray();
        } else {
            return null;
        }
    }

    @Override
    public Reason getReason() {
        if (proto.hasReason()) {
            return TypeConversion.toReason(proto.getReason());
        } else {
            return null;
        }
    }

    @Override
    public boolean atBeginning() {
        return proto.getAtBeginning();
    }

    @Override
    public boolean atEnd() {
        return proto.getAtEnd();
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
