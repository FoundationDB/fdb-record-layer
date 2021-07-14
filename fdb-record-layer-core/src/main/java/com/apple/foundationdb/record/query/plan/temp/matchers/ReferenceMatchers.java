/*
 * ReferenceMatchers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TypedMatcher.typed;

/**
 * Matchers for {@link ExpressionRef}s.
 */
@API(API.Status.EXPERIMENTAL)
public class ReferenceMatchers {
    private ReferenceMatchers() {
        // do not instantiate
    }

    @SuppressWarnings("unchecked")
    public static <R extends ExpressionRef<? extends RelationalExpression>> BindingMatcher<R> anyRef() {
        return typed((Class<R>)(Class<?>)ExpressionRef.class);
    }

    public static BindingMatcher<? extends ExpressionRef<? extends RelationalExpression>> anyRefOverOnlyPlans() {
        return references(all(RelationalExpressionMatchers.ofType(RecordQueryPlan.class)));
    }

    @SuppressWarnings("unchecked")
    public static <R extends ExpressionRef<? extends RelationalExpression>, E extends RelationalExpression> BindingMatcher<R> references(@Nonnull final CollectionMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<R>)(Class<?>)ExpressionRef.class,
                Extractor.of(r -> r.getMembers(), name -> "members(" + name + ")"),
                downstream);
    }
}
