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
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A special predicate used to represent a parameterized tuple range.
 * Notably, it is mutable until "frozen" with a particular comparison range type.
 * @see com.apple.foundationdb.record.query.plan.temp.RelationalExpression#fromIndexDefinition
 */
@API(API.Status.EXPERIMENTAL)
public class ValueComparisonRangePredicate implements PredicateWithValue {
    @Nonnull
    private final Value value;
    @Nullable
    private final ComparisonRange.Type type;
    @Nullable
    private final ComparisonRange comparisonRange;

    public ValueComparisonRangePredicate(@Nonnull final Value value, @Nullable final ComparisonRange.Type type, @Nullable final ComparisonRange comparisonRange) {
        this.value = value;
        this.type = type;
        this.comparisonRange = comparisonRange;
    }

    @Override
    @Nonnull
    public Value getValue() {
        return value;
    }

    @Nullable
    public ComparisonRange getComparisonRange() {
        return comparisonRange;
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
    public QueryPredicate rebase(@Nonnull final AliasMap translationMap) {
        return new ValueComparisonRangePredicate(value.rebase(translationMap), type, comparisonRange);
    }

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindTo(@Nonnull final ExpressionMatcher<? extends Bindable> matcher) {
        return matcher.matchWith(this);
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final ValueComparisonRangePredicate that = (ValueComparisonRangePredicate)other;
        return value.semanticEquals(that.value, equivalenceMap) &&
               type == that.type &&
               Objects.equals(comparisonRange, that.comparisonRange);
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(value.semanticHashCode(), type, comparisonRange);
    }

    @Override
    public int planHash() {
        return Objects.hash(PlanHashable.planHash(value), type);
    }

    public static ValueComparisonRangePredicate withRequiredType(@Nonnull Value value, @Nonnull ComparisonRange.Type type) {
        return new ValueComparisonRangePredicate(value, type, null);
    }

    public static ValueComparisonRangePredicate withComparisonRange(@Nonnull Value value, @Nonnull ComparisonRange comparisonRange) {
        return new ValueComparisonRangePredicate(value, null, comparisonRange);
    }

    @Override
    public String toString() {
        return value + " " + type + " " + comparisonRange;
    }
}
