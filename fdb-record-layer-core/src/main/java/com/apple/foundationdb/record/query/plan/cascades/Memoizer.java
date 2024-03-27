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
        public Reference memoizeExpression(@Nonnull final RelationalExpression expression) {
            return Reference.of(expression);
        }

        @Nonnull
        @Override
        public Reference memoizeLeafExpression(@Nonnull final RelationalExpression expression) {
            return Reference.of(expression);
        }

        @Nonnull
        @Override
        public Reference memoizeMemberPlans(@Nonnull final Reference reference, @Nonnull final Collection<? extends RecordQueryPlan> plans) {
            return Reference.from(plans);
        }

        @Nonnull
        @Override
        public Reference memoizePlans(@Nonnull final RecordQueryPlan... plans) {
            return Reference.from(plans);
        }

        @Nonnull
        @Override
        public Reference memoizePlans(@Nonnull final Collection<? extends RecordQueryPlan> plans) {
            return Reference.from(plans);
        }

        @Nonnull
        @Override
        public Reference memoizeReference(@Nonnull final Reference reference) {
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
                public Reference reference() {
                    return Reference.from(expressions);
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
        public ReferenceBuilder memoizeMemberPlansBuilder(@Nonnull final Reference reference, @Nonnull final Collection<? extends RecordQueryPlan> plans) {
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
    Reference memoizeExpression(@Nonnull RelationalExpression expression);

    @Nonnull
    Reference memoizeLeafExpression(@Nonnull RelationalExpression expression);


    @Nonnull
    Reference memoizeMemberPlans(@Nonnull Reference reference,
                                 @Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    Reference memoizePlans(@Nonnull RecordQueryPlan... plans);

    @Nonnull
    Reference memoizePlans(@Nonnull Collection<? extends RecordQueryPlan> plans);

    @Nonnull
    Reference memoizeReference(@Nonnull Reference reference);

    @Nonnull
    ReferenceBuilder memoizeExpressionBuilder(@Nonnull RelationalExpression expression);

    @Nonnull
    ReferenceBuilder memoizeMemberPlansBuilder(@Nonnull Reference reference,
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
        Reference reference();

        @Nonnull
        Set<? extends RelationalExpression> members();
    }
}
