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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Union plan that compares using a {@link Value}.
 */
public class RecordQueryUnionOnValuePlan extends RecordQueryUnionPlan {
    @Nonnull
    private final CorrelationIdentifier baseAlias;
    
    public RecordQueryUnionOnValuePlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                       @Nonnull final Function<CorrelationIdentifier, Value> correlationIdentifierValueFunction,
                                       final boolean reverse,
                                       final boolean showComparisonKey) {
        this(quantifiers, CorrelationIdentifier.uniqueID(), correlationIdentifierValueFunction, reverse, showComparisonKey);
    }

    private RecordQueryUnionOnValuePlan(@Nonnull final List<Quantifier.Physical> quantifiers,
                                        @Nonnull final CorrelationIdentifier baseAlias,
                                        @Nonnull final Function<CorrelationIdentifier, Value> comparisonKeyValueFunction,
                                        final boolean reverse,
                                        final boolean showComparisonKey) {
        super(quantifiers,
                RecordQuerySetPlan.comparisonKeyValueFunction(baseAlias, comparisonKeyValueFunction.apply(baseAlias)),
                reverse,
                showComparisonKey);
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
    public RecordQueryUnionOnValuePlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                    @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryUnionOnValuePlan(Quantifiers.narrow(Quantifier.Physical.class, rebasedQuantifiers),
                baseAlias,
                alias -> getComparisonKeyValue(),
                isReverse(),
                showComparisonKey);
    }

    @Nonnull
    @Override
    public RecordQueryUnionOnValuePlan withChildrenReferences(@Nonnull final List<? extends ExpressionRef<? extends RecordQueryPlan>> newChildren) {
        return new RecordQueryUnionOnValuePlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()),
                baseAlias,
                alias -> getComparisonKeyValue(),
                isReverse(),
                showComparisonKey);
    }
}
