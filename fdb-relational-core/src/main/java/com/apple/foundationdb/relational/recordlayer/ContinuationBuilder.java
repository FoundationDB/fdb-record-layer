/*
 * ContinuationBuilder.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.continuation.grpc.ContinuationProto;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;

/**
 * A Builder class for an implementation of a {@link com.apple.foundationdb.relational.api.Continuation}.
 * The {@link ContinuationImpl} is immutable. This builder class allows a continuation to be built from scratch as well
 * mutated (via creating a builder from an existing continuation and then modifying).
 */
public class ContinuationBuilder {
    @Nonnull
    private final ContinuationProto.Builder proto;

    public ContinuationBuilder() {
        this.proto = ContinuationProto.newBuilder();
    }

    public ContinuationBuilder(@Nonnull ContinuationProto proto) {
        this.proto = proto.toBuilder();
    }

    public ContinuationBuilder withUnderlyingBytes(byte[] underlyingBytes) {
        this.proto.setUnderlyingBytes(ByteString.copyFrom(underlyingBytes));
        return this;
    }

    public ContinuationBuilder withBindingHash(int hash) {
        proto.setBindingHash(hash);
        return this;
    }

    public ContinuationBuilder withPlanHash(int hash) {
        proto.setPlanHash(hash);
        return this;
    }

    public Continuation build() {
        return new ContinuationImpl(proto.build());
    }
}
