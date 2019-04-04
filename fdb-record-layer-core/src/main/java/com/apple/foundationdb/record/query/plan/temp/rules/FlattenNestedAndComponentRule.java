/*
 * FlattenNestedAndComponentRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.matchers.AllChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildWithRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple rule that performs some basic Boolean normalization by flattening a nested {@link AndComponent} into a single,
 * wider AND. This rule only attempts to remove a single {@code AndComponent}; it may be repeated if necessary.
 * For example, it would transform:
 * <code>
 *     Query.and(
 *         Query.and(Query.field("a").equals("foo"), Query.field("b").equals("bar")),
 *         Query.field(c").equals("baz"),
 *         Query.and(Query.field("d").equals("food"), Query.field("e").equals("bare"))
 * </code>
 * to
 * <code>
 *     Query.and(
 *         Query.field("a").equals("foo"),
 *         Query.field("b").equals("bar"),
 *         Query.field("c").equals("baz")),
 *         Query.and(Query.field("d").equals("food"), Query.field("e").equals("bare"))
 * </code>
 */
@API(API.Status.EXPERIMENTAL)
public class FlattenNestedAndComponentRule extends PlannerRule<AndComponent> {
    private static final ExpressionMatcher<ExpressionRef<QueryComponent>> andChildrenMatcher = ReferenceMatcher.anyRef();
    private static final ReferenceMatcher<QueryComponent> otherInnerComponentsMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<AndComponent> root = TypeMatcher.of(AndComponent.class,
            AnyChildWithRestMatcher.anyMatchingWithRest(
                    TypeMatcher.of(AndComponent.class, AllChildrenMatcher.allMatching(andChildrenMatcher)),
                    otherInnerComponentsMatcher));

    public FlattenNestedAndComponentRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        List<ExpressionRef<QueryComponent>> innerAndChildren = call.getBindings().getAll(andChildrenMatcher);
        List<ExpressionRef<QueryComponent>> otherOuterAndChildren = call.getBindings().getAll(otherInnerComponentsMatcher);
        List<ExpressionRef<QueryComponent>> allConjuncts = new ArrayList<>(innerAndChildren);
        allConjuncts.addAll(otherOuterAndChildren);

        call.yield(call.ref(new AndComponent(allConjuncts)));
        return ChangesMade.MADE_CHANGES;
    }
}
