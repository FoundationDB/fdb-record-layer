/*
 * LogicalPlanFragment.java
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This represents an SQL fragment that comprises one or more {@link LogicalOperator}(s).
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class LogicalPlanFragment {

    @Nonnull
    private final Optional<LogicalPlanFragment.Builder> parentBuilder;

    @Nonnull
    private final LogicalOperators operators;

    @Nonnull
    private final Optional<State> state;

    private LogicalPlanFragment(@Nonnull Optional<LogicalPlanFragment.Builder> parent,
                                @Nonnull LogicalOperators operators,
                                @Nonnull Optional<State> state) {
        this.parentBuilder = parent;
        this.operators = operators;
        this.state = state;
    }

    @Nonnull
    public Optional<LogicalPlanFragment.Builder> getParentBuilder() {
        return parentBuilder;
    }

    @Nonnull
    public LogicalOperators getOperators() {
        return operators;
    }

    public boolean hasState() {
        return state.isPresent();
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
        var current = parentBuilder;
        while (current.isPresent()) {
            resultBuilder.addAll(current.get().getLogicalOperators().getCorrelations());
            current = current.get().parent;
        }
        return resultBuilder.build();
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        @Nonnull
        private Optional<LogicalPlanFragment.Builder> parent;

        @Nonnull
        private final List<LogicalOperator> logicalOperators;

        @Nullable
        private State state;

        private Builder() {
            parent = Optional.empty();
            logicalOperators = new LinkedList<>();
        }

        @Nonnull
        public Builder withParent(@Nonnull Optional<LogicalPlanFragment.Builder> parent) {
            this.parent = parent;
            return this;
        }

        @Nonnull
        public Builder addLogicalOperator(@Nonnull LogicalOperator logicalOperator) {
            logicalOperators.add(logicalOperator);
            return this;
        }

        @Nonnull
        public Builder withState(@Nonnull State state) {
            this.state = state;
            return this;
        }

        @Nonnull
        public Builder clearState() {
            this.state = null;
            return this;
        }

        @Nonnull
        public Builder withState(@Nonnull Optional<State> stateMaybe) {
            this.state = stateMaybe.orElse(null);
            return this;
        }

        @Nonnull
        public Optional<State> getState() {
            return Optional.ofNullable(state);
        }

        @Nonnull
        public Builder addAllLogicalOperators(@Nonnull Collection<LogicalOperator> logicalOperators) {
            this.logicalOperators.addAll(logicalOperators);
            return this;
        }

        @Nonnull
        public LogicalOperators getLogicalOperators() {
            return LogicalOperators.of(logicalOperators);
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

        public boolean hasParent() {
            return parent.isPresent();
        }

        @Nonnull
        public LogicalPlanFragment.Builder getParent() {
            Assert.thatUnchecked(parent.isPresent());
            return parent.get();
        }

        @Nonnull
        public Optional<LogicalPlanFragment.Builder> getParentMaybe() {
            return parent;
        }

        @Nonnull
        public LogicalPlanFragment build() {
            Assert.thatUnchecked(!logicalOperators.isEmpty());
            return new LogicalPlanFragment(parent, LogicalOperators.of(logicalOperators), Optional.ofNullable(state));
        }

        //        void show() {
        //            final var dot = exportToDot();
        //            BrowserHelper.browse("/showPlannerExpression.html", ImmutableMap.of("$DOT", dot));
        //        }
        //
        //        @Nonnull
        //        public String exportToDot() {
        //            var stringBuilder = new StringBuilder();
        //            stringBuilder.append("digraph G {\n")
        //                    .append("compound=true;\n")
        //                    .append("fontname=courier;\n")
        //                    .append("splines=polyline;\n")
        //                    .append("rankdir=BT;\n");
        //            int counter = 0;
        //            for (final var logicalOperator : logicalOperators) {
        //                var dot = logicalOperator.exportSubgraphToDot(counter);
        //                stringBuilder.append(dot);
        //                ++counter;
        //            }
        //            stringBuilder.append("}");
        //            return stringBuilder.toString();
        //        }
    }

    public static final class State {

        @Nonnull
        private Optional<Type> targetType;

        @Nonnull
        private Optional<StringTrieNode> targetTypeReorderings;

        private State(@Nonnull Optional<Type> targetType,
                      @Nonnull Optional<StringTrieNode> targetTypeReorderings) {
            this.targetType = targetType;
            this.targetTypeReorderings = targetTypeReorderings;
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

        public static final class Builder {

            @Nullable
            private Type targetType;

            @Nullable
            StringTrieNode targetTypeReorderings;

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
            public State build() {
                return new State(Optional.ofNullable(targetType), Optional.ofNullable(targetTypeReorderings));
            }
        }
    }
}
