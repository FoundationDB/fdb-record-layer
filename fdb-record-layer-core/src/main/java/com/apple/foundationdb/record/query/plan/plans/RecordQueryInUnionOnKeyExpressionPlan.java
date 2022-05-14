/*
 * RecordQueryUnionOnKeyExpressionPlan.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Union plan that compares using a {@link KeyExpression}.
 */
public class RecordQueryInUnionOnKeyExpressionPlan extends RecordQueryInUnionPlan {
    public RecordQueryInUnionOnKeyExpressionPlan(@Nonnull final Quantifier.Physical inner,
                                                 @Nonnull final List<? extends InSource> inSources,
                                                 @Nonnull final KeyExpression comparisonKeyExpression,
                                                 final boolean reverse,
                                                 final int maxNumberOfValuesAllowed) {
        super(inner,
                inSources,
                new ComparisonKeyFunction.OnKeyExpression(comparisonKeyExpression) {
                    @Override
                    public <M extends Message> Function<QueryResult, List<Object>> apply(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext evaluationContext) {
                        return queryResult -> comparisonKeyExpression.evaluateSingleton(queryResult.getQueriedRecord()).toTupleAppropriateList();
                    }
                },
                reverse,
                maxNumberOfValuesAllowed);
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
    public RecordQueryInUnionOnKeyExpressionPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                              @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryInUnionOnKeyExpressionPlan(Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class),
                getInSources(),
                getComparisonKeyExpression(),
                reverse,
                maxNumberOfValuesAllowed);
    }

    @Nonnull
    @Override
    public RecordQueryInUnionOnKeyExpressionPlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryInUnionOnKeyExpressionPlan(Quantifier.physical(GroupExpressionRef.of(child)),
                getInSources(),
                getComparisonKeyExpression(),
                reverse,
                maxNumberOfValuesAllowed);
    }
}
