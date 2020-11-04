/*
 * ValueComparisonRangePredicate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.view.SourceEntry;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A special predicate used to represent a parameterized tuple range.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class ValueComparisonRangePredicate implements PredicateWithValue {
    @Nonnull
    private final Value value;

    public ValueComparisonRangePredicate(@Nonnull final Value value) {
        this.value = value;
    }

    @Override
    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nonnull final SourceEntry sourceEntry) {
        return null;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return value.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindTo(@Nonnull final ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(this, ImmutableList.of());
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final ValueComparisonRangePredicate that = (ValueComparisonRangePredicate)other;
        return value.semanticEquals(that.value, aliasMap);
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(value.semanticHashCode());
    }

    @Override
    public int planHash() {
        return Objects.hash(PlanHashable.planHash(value));
    }

    public static ValueComparisonRangePredicate placeholder(@Nonnull Value value, @Nonnull CorrelationIdentifier parameterAlias) {
        return new Placeholder(value, parameterAlias);
    }

    public static ValueComparisonRangePredicate sargable(@Nonnull Value value, @Nonnull ComparisonRange comparisonRange) {
        return new Sargable(value, comparisonRange);
    }

    /**
     * A place holder predicate solely used for index matching.
     */
    public static class Placeholder extends ValueComparisonRangePredicate {
        private final CorrelationIdentifier parameterAlias;

        public Placeholder(@Nonnull final Value value, @Nonnull CorrelationIdentifier parameterAlias) {
            super(value);
            this.parameterAlias = parameterAlias;
        }

        public CorrelationIdentifier getParameterAlias() {
            return parameterAlias;
        }

        @Override
        @SuppressWarnings({"ConstantConditions", "java:S2259"})
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (!super.semanticEquals(other, aliasMap)) {
                return false;
            }

            return Objects.equals(parameterAlias, ((Placeholder)other).parameterAlias);
        }

        @Nonnull
        @Override
        public Placeholder rebase(@Nonnull final AliasMap translationMap) {
            return new Placeholder(getValue().rebase(translationMap), parameterAlias);
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), parameterAlias);
        }

        @Override
        public String toString() {
            return "(" + getValue() + " -> " + parameterAlias.toString() + ")";
        }
    }

    /**
     * A query predicate that can be used as a (s)earch (arg)ument for an index scan.
     */
    public static class Sargable extends ValueComparisonRangePredicate {
        @Nonnull
        private final ComparisonRange comparisonRange;

        public Sargable(@Nonnull final Value value, @Nonnull final ComparisonRange comparisonRange) {
            super(value);
            this.comparisonRange = comparisonRange;
        }

        @Nonnull
        public ComparisonRange getComparisonRange() {
            return comparisonRange;
        }

        @Override
        public int planHash() {
            return Objects.hash(super.planHash(), comparisonRange.getRangeType());
        }

        @Override
        @SuppressWarnings({"ConstantConditions", "java:S2259"})
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (!super.semanticEquals(other, aliasMap)) {
                return false;
            }

            return Objects.equals(comparisonRange, ((Sargable)other).comparisonRange);
        }

        @Nonnull
        @Override
        public Sargable rebase(@Nonnull final AliasMap translationMap) {
            return new Sargable(getValue().rebase(translationMap), comparisonRange);
        }

        @Override
        public int semanticHashCode() {
            return Objects.hash(super.semanticHashCode(), comparisonRange);
        }

        @Override
        public String toString() {
            return getValue() + " " + comparisonRange;
        }
    }
}
