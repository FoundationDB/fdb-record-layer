/*
 * RecordQueryIntersectionOnValuePlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Intersection plan that compares using a {@link Value}.
 */
@SuppressWarnings("java:S2160")
public class RecordQueryIntersectionOnValuePlan extends RecordQueryIntersectionPlan {
    @Nonnull
    private final CorrelationIdentifier baseAlias;

    private RecordQueryIntersectionOnValuePlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                               @Nonnull final CorrelationIdentifier baseAlias,
                                               @Nonnull final Value comparisonKeyValue,
                                               final boolean reverse) {
        super(quantifiers,
                new ComparisonKeyFunction.OnValue(baseAlias, comparisonKeyValue),
                reverse);
        this.baseAlias = baseAlias;
    }

    @Nonnull
    @Override
    public ComparisonKeyFunction.OnValue getComparisonKeyFunction() {
        return (ComparisonKeyFunction.OnValue)super.getComparisonKeyFunction();
    }

    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        throw new RecordCoreException("this plan does not support this getRequiredFields()");
    }

    @Nonnull
    public Value getComparisonKeyValue() {
        return getComparisonKeyFunction().getComparisonKeyValue();
    }

    @Nonnull
    @Override
    public RecordQueryIntersectionOnValuePlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<Quantifier> translatedQuantifiers) {
        return new RecordQueryIntersectionOnValuePlan(Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers),
                baseAlias,
                getComparisonKeyValue(),
                isReverse());
    }

    @Nonnull
    @Override
    public RecordQueryIntersectionOnValuePlan withChildrenReferences(@Nonnull final List<? extends ExpressionRef<? extends RecordQueryPlan>> newChildren) {
        return new RecordQueryIntersectionOnValuePlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                baseAlias,
                getComparisonKeyValue(),
                isReverse());
    }

    @Override
    public RecordQueryIntersectionOnValuePlan strictlySorted() {
        final var quantifiers =
                Quantifiers.fromPlans(getChildren()
                        .stream()
                        .map(p -> GroupExpressionRef.of((RecordQueryPlan)p.strictlySorted())).collect(Collectors.toList()));
        return new RecordQueryIntersectionOnValuePlan(quantifiers, baseAlias, getComparisonKeyValue(), reverse);
    }

    @Nonnull
    public static RecordQueryIntersectionOnValuePlan intersection(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                                  @Nonnull final Function<CorrelationIdentifier, Value> comparisonKeyValueFunction,
                                                                  final boolean reverse) {
        final var baseAlias = CorrelationIdentifier.uniqueID();
        return new RecordQueryIntersectionOnValuePlan(quantifiers,
                baseAlias,
                comparisonKeyValueFunction.apply(baseAlias),
                reverse);
    }
}
