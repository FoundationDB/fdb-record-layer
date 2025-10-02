/*
 * ReferenceMatchers.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;

/**
 * Matchers for {@link Reference}s.
 */
@API(API.Status.EXPERIMENTAL)
public class ReferenceMatchers {
    @Nonnull
    private static final BindingMatcher<Reference> topReferenceMatcher = BindingMatcher.instance();
    @Nonnull
    private static final BindingMatcher<Reference> currentReferenceMatcher = BindingMatcher.instance();


    private ReferenceMatchers() {
        // do not instantiate
    }


    @Nonnull
    public static BindingMatcher<Reference> getTopReferenceMatcher() {
        return topReferenceMatcher;
    }

    @Nonnull
    public static BindingMatcher<Reference> getCurrentReferenceMatcher() {
        return currentReferenceMatcher;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<Reference> anyRef() {
        return typed(Reference.class);
    }

    @Nonnull
    public static BindingMatcher<Reference> anyRefOverOnlyPlans() {
        return members(all(RelationalExpressionMatchers.ofType(RecordQueryPlan.class)));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <E extends RelationalExpression> BindingMatcher<Reference> members(@Nonnull final CollectionMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::getAllMemberExpressions, name -> "allMembers(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <E extends RelationalExpression> BindingMatcher<Reference> exploratoryMembers(@Nonnull final CollectionMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::getExploratoryExpressions, name -> "exploratoryMember(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static <E extends RelationalExpression> BindingMatcher<Reference> exploratoryMember(@Nonnull final BindingMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::getExploratoryExpressions, name -> "exploratoryMember(" + name + ")"),
                AnyMatcher.any(downstream));
    }

    @Nonnull
    public static <E extends RelationalExpression> BindingMatcher<Reference> finalMember(@Nonnull final BindingMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::getFinalExpressions, name -> "finalMember(" + name + ")"),
                AnyMatcher.any(downstream));
    }
}
