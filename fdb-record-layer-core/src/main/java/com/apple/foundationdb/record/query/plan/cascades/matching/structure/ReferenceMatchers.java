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


import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;

/**
 * Matchers for {@link Reference}s.
 */
@API(API.Status.EXPERIMENTAL)
public class ReferenceMatchers {
    private static final BindingMatcher<Reference> topReferenceMatcher = BindingMatcher.instance();
    private static final BindingMatcher<Reference> currentReferenceMatcher = BindingMatcher.instance();


    private ReferenceMatchers() {
        // do not instantiate
    }


    public static BindingMatcher<Reference> getTopReferenceMatcher() {
        return topReferenceMatcher;
    }

    public static BindingMatcher<Reference> getCurrentReferenceMatcher() {
        return currentReferenceMatcher;
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<Reference> anyRef() {
        return typed(Reference.class);
    }

    public static BindingMatcher<Reference> anyRefOverOnlyPlans() {
        return members(all(RelationalExpressionMatchers.ofType(RecordQueryPlan.class)));
    }

    @SuppressWarnings("unchecked")
    public static <E extends RelationalExpression> BindingMatcher<Reference> members(final CollectionMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::getAllMemberExpressions, name -> "allMembers(" + name + ")"),
                downstream);
    }

    public static <E extends RelationalExpression> BindingMatcher<Reference> exploratoryMember(final BindingMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::getExploratoryExpressions, name -> "exploratoryMember(" + name + ")"),
                AnyMatcher.any(downstream));
    }

    public static <E extends RelationalExpression> BindingMatcher<Reference> finalMember(final BindingMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::getFinalExpressions, name -> "finalMember(" + name + ")"),
                AnyMatcher.any(downstream));
    }
}
