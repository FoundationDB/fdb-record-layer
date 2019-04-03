/*
 * RewriteRuleCall.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

/**
 * A rule call implementation for the {@link com.apple.foundationdb.record.query.plan.temp.RewritePlanner}.
 * When a new expression is yieled by the rule's {@link PlannerRule#onMatch} method, the rule call substitutes the
 * existing contents of the root with the yielded expression.
 */
@API(API.Status.EXPERIMENTAL)
public class RewriteRuleCall implements PlannerRuleCall {
    @Nonnull
    private final PlannerRule<? extends PlannerExpression> rule;
    @Nonnull
    private final SingleExpressionRef<PlannerExpression> root;
    @Nonnull
    private final PlannerBindings bindings;
    @Nonnull
    private final PlanContext context;

    private RewriteRuleCall(@Nonnull PlanContext context,
                            @Nonnull PlannerRule<? extends PlannerExpression> rule,
                            @Nonnull SingleExpressionRef<PlannerExpression> root,
                            @Nonnull PlannerBindings bindings) {
        this.context = context;
        this.rule = rule;
        this.root = root;
        this.bindings = bindings;
    }

    /**
     * Run this rule call by calling the rule's {@link PlannerRule#onMatch(PlannerRuleCall)} method.
     * @return a {@link PlannerRule.ChangesMade} that indicates whether the running the rule yielded a new expression
     */
    public PlannerRule.ChangesMade run() {
        return rule.onMatch(this);
    }

    @Override
    @Nonnull
    public PlannerBindings getBindings() {
        return bindings;
    }

    @Override
    @Nonnull
    public PlanContext getContext() {
        return context;
    }

    /**
     * Replace the expression held by the {@code root} reference with the given expression.
     * @param expression the expression produced by the rule
     */
    @Override
    public void yield(@Nonnull ExpressionRef<? extends PlannerExpression> expression) {
        if (expression instanceof SingleExpressionRef) {
            root.insert(expression.get());
        } else {
            throw new RecordCoreArgumentException("a rule returned an incompatible reference to the rewrite planner");
        }
    }

    @Override
    public <U extends PlannerExpression> ExpressionRef<U> ref(U expression) {
        return SingleExpressionRef.of(expression);
    }

    /**
     * Attempt to match the given {@link PlannerRule} to the planner expression {@code root}. If the rule's expression
     * matcher matches {@code root}, create a new {@code RewriteRuleCall} with the given {@code root} and the
     * given rule, and return it. If the rule's expression matcher does not match {@code root} then return
     * {@code Optional.empty()}.
     * @param context a plan context with various metadata that could affect planning
     * @param rule a rule to attempt to apply
     * @param root a single expression reference containing a planner expression to apply the rule to
     * @return an {@code Optional} containing a rewrite rule call if the rule's matcher matched or {@code Optional.empty()} otherwise
     */
    public static Stream<RewriteRuleCall> tryMatchRule(
            @Nonnull PlanContext context,
            @Nonnull PlannerRule<? extends PlannerExpression> rule,
            @Nonnull SingleExpressionRef<PlannerExpression> root) {
        return root.bindTo(rule.getMatcher()).map(bindings -> new RewriteRuleCall(context, rule, root, bindings));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RewriteRuleCall{");
        sb.append("rule=").append(rule);
        sb.append(", root=").append(root);
        sb.append(", bindings=").append(bindings);
        sb.append(", context=").append(context);
        sb.append('}');
        return sb.toString();
    }
}
