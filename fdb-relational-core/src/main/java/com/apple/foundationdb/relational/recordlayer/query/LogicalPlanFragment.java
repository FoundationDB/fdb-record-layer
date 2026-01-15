/*
 * LogicalPlanFragment.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This represents an SQL fragment that comprises one or more {@link LogicalOperator}(s).
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public final class LogicalPlanFragment {

    @Nonnull
    private final Optional<LogicalPlanFragment> parent;

    @Nonnull
    private LogicalOperators operators;

    @Nonnull
    private Optional<State> state;

    @Nonnull
    private final List<Expression> innerJoinExpressions;

    private LogicalPlanFragment(@Nonnull Optional<LogicalPlanFragment> parent,
                                @Nonnull LogicalOperators operators,
                                @Nonnull Optional<State> state) {
        this.parent = parent;
        this.operators = operators;
        this.state = state;
        this.innerJoinExpressions = new ArrayList<>();
    }

    public void addInnerJoinExpression(@Nonnull Expression joinExpression) {
        this.innerJoinExpressions.add(joinExpression);
    }

    @Nonnull
    public List<Expression> getInnerJoinExpressions() {
        return Collections.unmodifiableList(innerJoinExpressions);
    }

    @Nonnull
    public LogicalOperators getLogicalOperators() {
        return operators;
    }

    @Nonnull
    public Optional<LogicalPlanFragment> getParentMaybe() {
        return parent;
    }

    public boolean hasParent() {
        return parent.isPresent();
    }

    @Nonnull
    public LogicalPlanFragment getParent() {
        Assert.thatUnchecked(parent.isPresent());
        return parent.get();
    }

    @Nonnull
    public LogicalOperators getLogicalOperatorsIncludingOuter() {
        final ImmutableList.Builder<LogicalOperator> resultBuilder = ImmutableList.builder();
        resultBuilder.addAll(getLogicalOperators());
        var current = parent;
        while (current.isPresent()) {
            resultBuilder.addAll(current.get().getLogicalOperators());
            current = current.get().getParentMaybe();
        }
        return LogicalOperators.of(resultBuilder.build());
    }

    public void addOperator(@Nonnull LogicalOperator logicalOperator) {
        this.operators = operators.concat(logicalOperator);
    }

    public void setOperator(@Nonnull LogicalOperator logicalOperator) {
        this.operators = LogicalOperators.ofSingle(logicalOperator);
    }

    public void setState(@Nonnull State state) {
        this.state = Optional.of(state);
    }

    public void setStateMaybe(@Nonnull Optional<State> state) {
        this.state = state;
    }

    @Nonnull
    public State getState() {
        Assert.thatUnchecked(state.isPresent());
        return state.get();
    }

    @Nonnull
    public Optional<State> getStateMaybe() {
        return state;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getOuterCorrelations() {
        final ImmutableSet.Builder<CorrelationIdentifier> resultBuilder = ImmutableSet.builder();
        var current = parent;
        while (current.isPresent()) {
            resultBuilder.addAll(current.get().getLogicalOperators().getCorrelations());
            current = current.get().parent;
        }
        return resultBuilder.build();
    }

    @Nonnull
    public LogicalPlanFragment addChild() {
        return LogicalPlanFragment.ofParent(Optional.of(this));
    }

    @Nonnull
    private static LogicalPlanFragment ofParent(@Nonnull Optional<LogicalPlanFragment> parent) {
        return new LogicalPlanFragment(parent, LogicalOperators.empty(), Optional.empty());
    }

    @Nonnull
    public static LogicalPlanFragment ofRoot() {
        return new LogicalPlanFragment(Optional.empty(), LogicalOperators.empty(), Optional.empty());
    }

    public static final class State {

        @Nonnull
        private final Optional<Type> targetType;

        @Nonnull
        private final Optional<StringTrieNode> targetTypeReorderings;

        @Nonnull
        private final Optional<Quantifier.ForEach> updateQuantifier;

        private State(@Nonnull Optional<Type> targetType,
                      @Nonnull Optional<StringTrieNode> targetTypeReorderings,
                      @Nonnull Optional<Quantifier.ForEach> updateQuantifier) {
            this.targetType = targetType;
            this.targetTypeReorderings = targetTypeReorderings;
            this.updateQuantifier = updateQuantifier;
        }

        @Nonnull
        public static Builder newBuilder() {
            return new Builder();
        }

        @Nonnull
        public Optional<Type> getTargetType() {
            return targetType;
        }

        @Nonnull
        public Optional<StringTrieNode> getTargetTypeReorderings() {
            return targetTypeReorderings;
        }

        @Nonnull
        public Optional<Quantifier.ForEach> getUpdateQuantifier() {
            return updateQuantifier;
        }

        public static final class Builder {

            @Nullable
            private Type targetType;

            @Nullable
            StringTrieNode targetTypeReorderings;

            @Nullable
            private Quantifier.ForEach updateQuantifier;

            private Builder() {
            }

            @Nonnull
            public Builder withTargetType(@Nonnull Type targetType) {
                this.targetType = targetType;
                return this;
            }

            @Nonnull
            public Builder withTargetTypeReorderings(@Nonnull StringTrieNode targetTypeReorderings) {
                this.targetTypeReorderings = targetTypeReorderings;
                return this;
            }

            @Nonnull
            public Builder withUpdateQuantifier(@Nonnull Quantifier.ForEach updateQuantifier) {
                this.updateQuantifier = updateQuantifier;
                return this;
            }

            @Nonnull
            public State build() {
                return new State(Optional.ofNullable(targetType), Optional.ofNullable(targetTypeReorderings), Optional.ofNullable(updateQuantifier));
            }
        }
    }
}
