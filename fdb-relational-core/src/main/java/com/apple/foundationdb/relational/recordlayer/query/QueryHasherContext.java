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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public final class QueryHasherContext implements QueryExecutionParameters {

    @Nonnull
    private final List<Object> literals;

    @Nullable
    private final byte[] continuation;

    @Nonnull
    private final PreparedStatementParameters preparedStatementParameters;

    private final boolean isForExplain; // todo(yhatem) remove.

    private final int limit;

    private final int parameterHash;

    private final int offset;

    private QueryHasherContext(@Nonnull List<Object> literals,
                               @Nullable byte[] continuation,
                               @Nonnull PreparedStatementParameters preparedStatementParameters,
                               int limit,
                               int parameterHash,
                               int offset,
                               boolean isForExplain) {
        this.literals = literals;
        this.continuation = continuation;
        this.preparedStatementParameters = preparedStatementParameters;
        this.isForExplain = isForExplain;
        this.limit = limit;
        this.parameterHash = parameterHash;
        this.offset = offset;
    }

    @Nonnull
    @Override
    public EvaluationContext getEvaluationContext(@Nonnull TypeRepository typeRepository) {
        if (literals.isEmpty()) {
            return EvaluationContext.forTypeRepository(typeRepository);
        }
        final var builder = EvaluationContext.newBuilder();
        builder.setConstant(Quantifier.constant(), literals);
        return builder.build(typeRepository);
    }

    @Nonnull
    @Override
    public ExecuteProperties.Builder getExecutionPropertiesBuilder() {
        return ExecuteProperties.newBuilder().setReturnedRowLimit(limit).setSkip(offset);
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

    @Nonnull
    @Override
    public PreparedStatementParameters getPreparedStatementParameters() {
        return preparedStatementParameters;
    }

    @Override
    public boolean isForExplain() {
        return isForExplain;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class Builder {
        @Nonnull
        private final LiteralsBuilder literals;

        private boolean isForExplain;

        @Nullable
        private byte[] continuation;

        @Nonnull
        private Optional<Integer> limit;

        private int parameterHash;

        @Nonnull
        private PreparedStatementParameters preparedStatementParameters;

        public Builder() {
            this.literals = LiteralsBuilder.newBuilder();
            this.isForExplain = false;
            this.continuation = null;
            this.limit = Optional.empty();
            this.preparedStatementParameters = PreparedStatementParameters.empty();
        }

        @Nonnull
        public Builder setLimit(final int limit) {
            Assert.thatUnchecked(this.limit.isEmpty(), "setting multiple limits is not supported");
            ParserUtils.verifyIntegerBounds(limit, 1, null);
            this.limit = Optional.of(limit);
            return this;
        }

        @Nonnull
        public Builder setParameterHash(@Nonnull final int parameterHash) {
            this.parameterHash = parameterHash;
            return this;
        }

        @Nonnull
        public Builder setOffset(@Nonnull final Optional<ConstantObjectValue> offset) {
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

        public int addLiteral(@Nonnull final Object object) {
            return literals.addLiteral(object);
        }

        public int startArrayLiteral() {
            return literals.startArrayLiteral();
        }

        public void finishArrayLiteral() {
            literals.finishArrayLiteral();
        }

        // todo (yhatem) remove.
        @Nonnull
        public Builder setForExplain(boolean isForExplain) {
            this.isForExplain = isForExplain;
            return this;
        }

        @Nonnull
        public Builder setPreparedStatementParameters(@Nonnull final PreparedStatementParameters preparedStatementParameters) {
            this.preparedStatementParameters = preparedStatementParameters;
            return this;
        }

        @Nonnull
        public QueryHasherContext build() {
            return new QueryHasherContext(literals.getLiterals(), continuation, preparedStatementParameters, limit.orElse(ReadTransaction.ROW_LIMIT_UNLIMITED),
                    parameterHash, 0, isForExplain);
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    public int addStrippedLiteral(@Nullable final Object literal) {
        literals.add(literal);
        return literals.size() - 1;
    }
}
