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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Union plan that compares using a {@link Value}.
 */
@SuppressWarnings("java:S2160")
public class RecordQueryInUnionOnValuePlan extends RecordQueryInUnionPlan {
    @Nonnull
    private final CorrelationIdentifier baseAlias;

    private RecordQueryInUnionOnValuePlan(@Nonnull final Quantifier.Physical inner,
                                          @Nonnull final List<? extends InSource> inSources,
                                          @Nonnull final CorrelationIdentifier baseAlias,
                                          @Nonnull final Value comparisonKeyValue,
                                          final boolean reverse,
                                          final int maxNumberOfValuesAllowed) {
        super(inner,
                inSources,
                new ComparisonKeyFunction.OnValue(baseAlias, comparisonKeyValue),
                reverse,
                maxNumberOfValuesAllowed);
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
    public RecordQueryInUnionOnValuePlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryInUnionOnValuePlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getInSources(),
                baseAlias,
                getComparisonKeyValue(),
                reverse,
                maxNumberOfValuesAllowed);
    }

    @Nonnull
    @Override
    public RecordQueryInUnionOnValuePlan withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryInUnionOnValuePlan(Quantifier.physical(GroupExpressionRef.of(child)),
                getInSources(),
                baseAlias,
                getComparisonKeyValue(),
                reverse,
                maxNumberOfValuesAllowed);
    }

    @Nonnull
    public static RecordQueryInUnionOnValuePlan inUnion(@Nonnull final Quantifier.Physical inner,
                                                        @Nonnull final List<? extends InSource> inSources,
                                                        @Nonnull final Function<CorrelationIdentifier, Value> comparisonKeyValueFunction,
                                                        final boolean reverse,
                                                        final int maxNumberOfValuesAllowed) {
        final var baseAlias = CorrelationIdentifier.uniqueID();
        return new RecordQueryInUnionOnValuePlan(inner,
                inSources,
                CorrelationIdentifier.uniqueID(),
                comparisonKeyValueFunction.apply(baseAlias),
                reverse,
                maxNumberOfValuesAllowed);
    }
}
