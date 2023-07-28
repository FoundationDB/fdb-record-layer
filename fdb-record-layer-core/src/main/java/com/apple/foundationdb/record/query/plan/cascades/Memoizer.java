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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 * A memoization interface.
 *
 */
@API(API.Status.EXPERIMENTAL)
public interface Memoizer  {
    Memoizer NO_MEMO = new Memoizer() {
        @Nonnull
        @Override
        public ExpressionRef<? extends RelationalExpression> memoizeExpression(@Nonnull final RelationalExpression expression) {
            return GroupExpressionRef.of(expression);
        }

        @Nonnull
        @Override
        public ExpressionRef<? extends RelationalExpression> memoizeLeafExpression(@Nonnull final RelationalExpression expression) {
            return GroupExpressionRef.of(expression);
        }

        @Nonnull
        @Override
        public ExpressionRef<? extends RelationalExpression> memoizeMemberPlans(@Nonnull final ExpressionRef<? extends RelationalExpression> reference, @Nonnull final Collection<? extends RecordQueryPlan> plans) {
            return GroupExpressionRef.from(plans);
        }

        @Nonnull
        @Override
        public ExpressionRef<? extends RecordQueryPlan> memoizePlans(@Nonnull final RecordQueryPlan... plans) {
            return GroupExpressionRef.from(plans);
        }

        @Nonnull
        @Override
        public ExpressionRef<? extends RecordQueryPlan> memoizePlans(@Nonnull final Collection<? extends RecordQueryPlan> plans) {
            return GroupExpressionRef.from(plans);
        }

        @Nonnull
        @Override
        public ExpressionRef<? extends RelationalExpression> memoizeReference(@Nonnull final ExpressionRef<? extends RelationalExpression> reference) {
            return reference;
        }

        @Nonnull
        @Override
        public ReferenceBuilder memoizeExpressionBuilder(@Nonnull final RelationalExpression expression) {
            return memoizeBuilder(ImmutableList.of(expression));
        }

        @Nonnull
        private ReferenceBuilder memoizeBuilder(@Nonnull final Collection<? extends RelationalExpression> expressions) {
            final var expressionsAsSet = new LinkedIdentitySet<>(expressions);
            return new ReferenceBuilder() {
                @Nonnull
                @Override
                public ExpressionRef<? extends RelationalExpression> reference() {
                    return GroupExpressionRef.from(expressions);
                }

                @Nonnull
                @Override
                public Set<? extends RelationalExpression> members() {
                    return expressionsAsSet;
                }
            };
        }

        @Nonnull
        @Override
        public ReferenceBuilder memoizeMemberPlansBuilder(@Nonnull final ExpressionRef<? extends RelationalExpression> reference, @Nonnull final Collection<? extends RecordQueryPlan> plans) {
            return memoizeBuilder(plans);
        }

        @Nonnull
        @Override
        public ReferenceBuilder memoizePlansBuilder(@Nonnull final RecordQueryPlan... plans) {
            return memoizeBuilder(Arrays.asList(plans));
        }

        @Nonnull
        @Override
        public ReferenceBuilder memoizePlansBuilder(@Nonnull final Collection<? extends RecordQueryPlan> plans) {
            return memoizeBuilder(plans);
        }
    };

    @Nonnull
    ExpressionRef<? extends RelationalExpression> memoizeExpression(@Nonnull RelationalExpression expression);

    @Nonnull
    ExpressionRef<? extends RelationalExpression> memoizeLeafExpression(@Nonnull RelationalExpression expression);


    @Nonnull
    ExpressionRef<? extends RelationalExpression> memoizeMemberPlans(@Nonnull ExpressionRef<? extends RelationalExpression> reference,
                                                                     @Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    ExpressionRef<? extends RecordQueryPlan> memoizePlans(@Nonnull RecordQueryPlan... plans);

    @Nonnull
    ExpressionRef<? extends RecordQueryPlan> memoizePlans(@Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    ExpressionRef<? extends RelationalExpression> memoizeReference(@Nonnull ExpressionRef<? extends RelationalExpression> reference);

    @Nonnull
    ReferenceBuilder memoizeExpressionBuilder(@Nonnull RelationalExpression expression);

    @Nonnull
    ReferenceBuilder memoizeMemberPlansBuilder(@Nonnull ExpressionRef<? extends RelationalExpression> reference,
                                               @Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    ReferenceBuilder memoizePlansBuilder(@Nonnull RecordQueryPlan... plans);

    @Nonnull
    ReferenceBuilder memoizePlansBuilder(@Nonnull Collection<? extends RecordQueryPlan> plans);

    static Memoizer noMemo() {
        return NO_MEMO;
    }

    /**
     * Builder for references.
     */
    interface ReferenceBuilder {
        @Nonnull
        ExpressionRef<? extends RelationalExpression> reference();

        @Nonnull
        Set<? extends RelationalExpression> members();
    }
}
