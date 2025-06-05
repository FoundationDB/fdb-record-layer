/*
 * QueryHasherContext.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

@API(API.Status.EXPERIMENTAL)
public final class QueryHasherContext implements QueryExecutionContext {

    @Nonnull
    private final Literals literals;

    @Nullable
    private final byte[] continuation;

    private final boolean isForExplain;

    private final int parameterHash;

    @Nonnull
    private final PlanHashable.PlanHashMode planHashMode;

    private QueryHasherContext(@Nonnull Literals literals,
                               @Nullable byte[] continuation,
                               int parameterHash,
                               boolean isForExplain,
                               @Nonnull final PlanHashable.PlanHashMode planHashMode) {
        this.literals = literals;
        this.continuation = continuation;
        this.isForExplain = isForExplain;
        this.parameterHash = parameterHash;
        this.planHashMode = planHashMode;
    }

    @Nonnull
    @Override
    public Literals getLiterals() {
        return literals;
    }

    @Nonnull
    @Override
    public EvaluationContext getEvaluationContext(@Nonnull TypeRepository typeRepository) {
        if (literals.isEmpty()) {
            return EvaluationContext.forTypeRepository(typeRepository);
        }
        final var builder = EvaluationContext.newBuilder();
        builder.setConstant(Quantifier.constant(), literals.asMap());
        return builder.build(typeRepository);
    }

    @Nonnull
    @Override
    public ExecuteProperties.Builder getExecutionPropertiesBuilder() {
        return ExecuteProperties.newBuilder();
    }

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP", justification = "Intentional")
    @Nullable
    @Override
    public byte[] getContinuation() {
        return continuation;
    }

    @Override
    public int getParameterHash() {
        return parameterHash;
    }

    @Override
    public boolean isForExplain() {
        return isForExplain;
    }

    @Nonnull
    @Override
    public PlanHashable.PlanHashMode getPlanHashMode() {
        return planHashMode;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class Builder {
        @Nonnull
        private final Literals.Builder literalsBuilder;

        private boolean isForExplain;

        @Nullable
        private byte[] continuation;

        private int parameterHash;

        @Nullable
        private PlanHashable.PlanHashMode planHashMode;

        public Builder() {
            this.literalsBuilder = Literals.newBuilder();
            this.isForExplain = false;
            this.continuation = null;
            this.planHashMode = null;
        }

        @Nonnull
        public Builder setParameterHash(int parameterHash) {
            this.parameterHash = parameterHash;
            return this;
        }

        @Nonnull
        public Builder setOffset(@Nonnull Optional<ConstantObjectValue> offset) {
            // TODO
            Assert.thatUnchecked(offset.isEmpty(), "OFFSET clause is not supported.");
            return this;
        }

        @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "Intentional")
        @Nonnull
        public Builder setContinuation(@Nullable final byte[] continuation) {
            this.continuation = continuation; // copy to be safe?
            return this;
        }

        @Nonnull
        public Literals.Builder getLiteralsBuilder() {
            return literalsBuilder;
        }

        @Nonnull
        public Builder setForExplain(boolean isForExplain) {
            this.isForExplain = isForExplain;
            return this;
        }

        @Nonnull
        public Builder setPlanHashMode(@Nonnull PlanHashable.PlanHashMode planHashMode) {
            this.planHashMode = planHashMode;
            return this;
        }

        @Nonnull
        public QueryHasherContext build() {
            return new QueryHasherContext(literalsBuilder.build(), continuation,
                    parameterHash, isForExplain,
                    Objects.requireNonNull(planHashMode));
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }
}
