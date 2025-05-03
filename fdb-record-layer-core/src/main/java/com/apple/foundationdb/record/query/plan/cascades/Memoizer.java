/*
 * Memoizer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;

/**
 * A memoization interface.
 *
 */
@API(API.Status.EXPERIMENTAL)
public interface Memoizer {
    @Nonnull
    Reference memoizeExploratoryExpression(@Nonnull RelationalExpression expression);

    @Nonnull
    Reference memoizeMemberExpressions(@Nonnull Reference reference,
                                       @Nonnull Collection<? extends RelationalExpression> expressions);

    @Nonnull
    Reference memoizeFinalExpression(@Nonnull RelationalExpression expression);

    @Nonnull
    Reference memoizePlannedExpression(@Nonnull RelationalExpression expression);

    @Nonnull
    Reference memoizeMemberPlans(@Nonnull Reference reference,
                                 @Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    Reference memoizePlan(@Nonnull RecordQueryPlan plan);

    @Nonnull
    ReferenceBuilder memoizeExploratoryExpressionBuilder(@Nonnull RelationalExpression expression);

    @Nonnull
    ReferenceBuilder memoizeFinalExpressionsBuilder(@Nonnull Collection<? extends RelationalExpression> expressions);

    @Nonnull
    ReferenceOfPlansBuilder memoizeMemberPlansBuilder(@Nonnull Reference reference,
                                                      @Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    ReferenceOfPlansBuilder memoizePlanBuilder(@Nonnull RecordQueryPlan plan);

    @Nonnull
    ReferenceOfPlansBuilder memoizePlansBuilder(@Nonnull Collection<? extends RecordQueryPlan> recordQueryPlans);

    /**
     * Builder for references.
     */
    interface ReferenceBuilder {
        @Nonnull
        Reference reference();

        @Nonnull
        Set<? extends RelationalExpression> members();
    }

    /**
     * Builder for references.
     */
    interface ReferenceOfPlansBuilder extends ReferenceBuilder {
        @Nonnull
        Set<? extends RecordQueryPlan> members();
    }
}
