/*
 * ContinuationBuilder.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.continuation.CompiledStatement;
import com.apple.foundationdb.relational.continuation.ContinuationProto;

import com.apple.foundationdb.relational.continuation.CopyPlan;
import com.google.protobuf.ByteString;


/**
 * A Builder class for an implementation of a {@link com.apple.foundationdb.relational.api.Continuation}.
 * The {@link ContinuationImpl} is immutable. This builder class allows a continuation to be built from scratch as well
 * mutated (via creating a builder from an existing continuation and then modifying).
 */
@API(API.Status.EXPERIMENTAL)
public class ContinuationBuilder {
    private final ContinuationProto.Builder proto;

    public ContinuationBuilder() {
        this.proto = ContinuationProto.newBuilder();
    }

    public ContinuationBuilder(ContinuationProto proto) {
        this.proto = proto.toBuilder();
    }

    public ContinuationBuilder withExecutionState(byte[] executionState) {
        this.proto.setExecutionState(ByteString.copyFrom(executionState));
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

    public ContinuationBuilder withCompiledStatement(final CompiledStatement compiledStatementProto) {
        proto.setCompiledStatement(compiledStatementProto);
        return this;
    }

    public ContinuationBuilder withCopyPlan(final CopyPlan copyPlan) {
        proto.setCopyPlan(copyPlan);
        return this;
    }

    public ContinuationBuilder withReason(final Continuation.Reason reason) {
        proto.setReason(ContinuationProto.Reason.valueOf(reason.name()));
        return this;
    }

    public Continuation build() {
        return new ContinuationImpl(proto.build());
    }
}
