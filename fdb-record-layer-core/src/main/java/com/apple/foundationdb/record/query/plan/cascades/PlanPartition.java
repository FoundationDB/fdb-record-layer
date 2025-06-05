/*
 * PlanPartition.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A plan partition used for matching.
 */
public class PlanPartition extends ExpressionPartition<RecordQueryPlan> {
    private PlanPartition(@Nonnull final Map<ExpressionProperty<?>, ?> propertyValuesMap,
                          @Nonnull final Map<RecordQueryPlan, Map<ExpressionProperty<?>, ?>> planPropertyMap) {
        super(propertyValuesMap, planPropertyMap);
    }

    @Nonnull
    public Set<RecordQueryPlan> getPlans() {
        return getExpressions();
    }

    @Nonnull
    @Override
    public PlanPartition filter(@Nonnull final Predicate<RecordQueryPlan> expressionPredicate) {
        return with(getGroupingPropertyMap(), filterGroupedPropertyMap(expressionPredicate));
    }

    @Nonnull
    @Override
    protected PlanPartition with(@Nonnull final Map<ExpressionProperty<?>, ?> groupingPropertyMap,
                                 @Nonnull final Map<RecordQueryPlan, Map<ExpressionProperty<?>, ?>> groupedPropertyMap) {
        return new PlanPartition(groupingPropertyMap, groupedPropertyMap);
    }

    @Nonnull
    public static PlanPartition ofPlans(@Nonnull final Map<ExpressionProperty<?>, ?> propertyValuesMap,
                                        @Nonnull final Map<RecordQueryPlan, Map<ExpressionProperty<?>, ?>> planPropertyMap) {
        return new PlanPartition(propertyValuesMap, planPropertyMap);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static PlanPartition ofExpressions(@Nonnull final Map<ExpressionProperty<?>, ?> propertyValuesMap,
                                              @Nonnull final Map<? extends RelationalExpression, Map<ExpressionProperty<?>, ?>> expressionPropertyMap) {
        Debugger.sanityCheck(() ->
                Verify.verify(expressionPropertyMap.keySet()
                        .stream()
                        .allMatch(plan -> plan instanceof RecordQueryPlan)));
        return new PlanPartition(propertyValuesMap, (Map<RecordQueryPlan, Map<ExpressionProperty<?>, ?>>)expressionPropertyMap);
    }
}
