/*
 * RecordQueryIntersectionOnKeyExpressionPlan.java
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Intersection plan that compares using a {@link KeyExpression}.
 */
public class RecordQueryIntersectionOnKeyExpressionPlan extends RecordQueryIntersectionPlan {
    public RecordQueryIntersectionOnKeyExpressionPlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                                      @Nonnull final KeyExpression comparisonKey,
                                                      final boolean reverse) {
        super(quantifiers,
                new ComparisonKeyFunction.OnKeyExpression(comparisonKey),
                reverse);
    }

    @Nonnull
    @Override
    public ComparisonKeyFunction.OnKeyExpression getComparisonKeyFunction() {
        return (ComparisonKeyFunction.OnKeyExpression)super.getComparisonKeyFunction();
    }

    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        return ImmutableSet.copyOf(getComparisonKeyExpression().normalizeKeyForPositions());
    }

    @Nonnull
    public KeyExpression getComparisonKeyExpression() {
        return getComparisonKeyFunction().getComparisonKey();
    }

    @Nonnull
    @Override
    public RecordQueryIntersectionOnKeyExpressionPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<Quantifier> translatedQuantifiers) {
        return new RecordQueryIntersectionOnKeyExpressionPlan(Quantifiers.narrow(Quantifier.Physical.class, translatedQuantifiers),
                getComparisonKeyExpression(),
                isReverse());
    }

    @Nonnull
    @Override
    public RecordQueryIntersectionOnKeyExpressionPlan withChildrenReferences(@Nonnull final List<? extends ExpressionRef<? extends RecordQueryPlan>> newChildren) {
        return new RecordQueryIntersectionOnKeyExpressionPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                getComparisonKeyExpression(),
                isReverse());
    }

    @Override
    public RecordQueryIntersectionOnKeyExpressionPlan strictlySorted() {
        final var quantifiers =
                Quantifiers.fromPlans(getChildren()
                        .stream()
                        .map(p -> GroupExpressionRef.of((RecordQueryPlan)p.strictlySorted())).collect(Collectors.toList()));
        return new RecordQueryIntersectionOnKeyExpressionPlan(quantifiers, getComparisonKeyExpression(), reverse);
    }
}
