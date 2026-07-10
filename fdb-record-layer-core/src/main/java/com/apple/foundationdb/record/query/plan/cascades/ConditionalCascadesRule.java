/*
 * ConditionalCascadesRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * A {@link CascadesRule} that groups together an ordered sequence of related inner rules for improved planning
 * efficiency.
 *
 * <p>Instead of letting the planner fire each of the grouped rules independently—with every matching rule
 * producing new expressions that then have to be explored and costed—a {@code ConditionalCascadesRule} asks the
 * planner to try the inner rules in order and to stop as soon as one of them makes progress, only falling through to
 * the next rule when the current one yields nothing. By arranging strictly-prioritized or mutually-exclusive
 * transformations in such a conditional chain we can prune the planner’s search space. The alternatives that the later
 * rules would have produced are never generated, which keeps the memo smaller and makes planning faster.
 *
 * <p>A conditional rule is never applied directly; attempting to call {@link #onMatch} results in a
 * {@link RecordCoreException}. Instead, the planner treats conditional rules specially: When it encounters one, it
 * selects the subset of inner rules that are currently enabled (via the planner configuration) and schedules those,
 * in order, for the conditional application just described.
 *
 * <p>All inner rules must agree on the root class of their root binding matcher and on their root operator.
 * The wrapping conditional rule exposes a single binding matcher and root operator that stands in for the whole group.
 *
 * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
 * @param <R> the kind of inner {@link CascadesRule} grouped by this rule
 */
@API(API.Status.EXPERIMENTAL)
public class ConditionalCascadesRule<T, R extends CascadesRule<T>> extends AbstractCascadesRule<T> {
    /**
     * The grouped inner rules, as an immutable list.
     */
    @Nonnull
    final List<R> rules;

    /**
     * The root operator common to all inner rules, or {@code null} if the inner rules do not declare a root operator.
     * All inner rules are required to agree on this value (verified at construction), so this single class stands in
     * for the whole group.
     */
    @Nullable
    final Class<?> rootOperator;

    /**
     * Creates a rule that groups the given inner rules. The list of rules must be non-empty. All rules must agree on
     * their root operator and on the root class of their binding matcher.
     */
    public ConditionalCascadesRule(@Nonnull final List<R> rules) {
        super(deriveBindingMatcher(rules), ImmutableSet.of());
        this.rules = ImmutableList.copyOf(rules);
        this.rootOperator = deriveRootOperator(rules);
    }

    /**
     * Creates a rule that groups the given inner rules. This is a convenience overload of
     * {@link #ConditionalCascadesRule(List)}.
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public ConditionalCascadesRule(@Nonnull final R... rules) {
        this(ImmutableList.copyOf(rules));
    }

    /**
     * Returns the inner rules grouped by this rule as an immutable list.
     */
    @Nonnull
    public List<R> getRules() {
        return rules;
    }

    /**
     * Returns the inner rules grouped by this rule that satisfy the given predicate, preserving their order.
     */
    @Nonnull
    public List<R> getRules(@Nonnull final Predicate<? super R> rulePredicate) {
        return rules.stream()
                .filter(rulePredicate)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns the root operator shared by all inner rules. If the inner rules do not declare a root operator, returns
     * an empty {@link Optional}.
     */
    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.ofNullable(rootOperator);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        throw new RecordCoreException("cannot call this method directly");
    }

    /**
     * Returns a string representation of this rule. The string consists of its simple class name followed by its inner
     * rules; for example, {@code ConditionalExplorationCascadesRule[DecorrelateValuesRule, …]}.
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + rules;
    }

    /**
     * Derives the common binding matcher for a group of inner rules. All rules are expected to share a binding matcher
     * with the same root class; this is verified, and the first matcher is returned as the representative.
     */
    @Nonnull
    private static <T, R extends PlannerRule<CascadesRuleCall, T>> BindingMatcher<T> deriveBindingMatcher(@Nonnull final List<R> rules) {
        Verify.verify(!rules.isEmpty(), "`ConditionalCascadesRule` must contain at least one rule");
        final BindingMatcher<T> matcher = rules.get(0).getMatcher();
        for (final R rule : rules) {
            Verify.verify(rule.getMatcher().getRootClass() == matcher.getRootClass());
        }
        return matcher;
    }

    /**
     * Derives the common root operator for a group of inner rules. All rules are expected to agree on their root
     * operator (either the same class, or all empty). This is verified, and the common value is returned.
     */
    @Nullable
    private static <T, R extends PlannerRule<CascadesRuleCall, T>> Class<?> deriveRootOperator(@Nonnull final List<R> rules) {
        Verify.verify(!rules.isEmpty(), "`ConditionalCascadesRule` must contain at least one rule");
        final Class<?> op = rules.get(0).getRootOperator().orElse(null);
        for (final R rule : rules) {
            Verify.verify(rule.getRootOperator().orElse(null) == op);
        }
        return op;
    }

    /**
     * A {@link ConditionalCascadesRule} that groups exploration rules and is itself an {@link ExplorationCascadesRule}.
     *
     * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
     */
    public static class ConditionalExplorationCascadesRule<T extends RelationalExpression> extends ConditionalCascadesRule<T, ExplorationCascadesRule<T>> implements ExplorationCascadesRule<T> {
        /**
         * Creates a rule that groups the given inner exploration rules.
         */
        public ConditionalExplorationCascadesRule(@Nonnull final List<ExplorationCascadesRule<T>> rules) {
            super(rules);
        }

        /**
         * Creates a rule that groups the given inner exploration rules.
         */
        @SafeVarargs
        @SuppressWarnings("varargs")
        public ConditionalExplorationCascadesRule(@Nonnull final ExplorationCascadesRule<T>... rules) {
            super(rules);
        }

        @Override
        public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
            throw new RecordCoreException("cannot call this method directly");
        }
    }

    /**
     * A {@link ConditionalCascadesRule} that groups implementation rules and is itself an
     * {@link ImplementationCascadesRule}.
     *
     * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
     */
    public static class ConditionalImplementationCascadesRule<T extends RelationalExpression> extends ConditionalCascadesRule<T, ImplementationCascadesRule<T>> implements ImplementationCascadesRule<T> {
        /**
         * Creates a rule that groups the given inner implementation rules.
         */
        public ConditionalImplementationCascadesRule(@Nonnull final List<ImplementationCascadesRule<T>> rules) {
            super(rules);
        }

        /**
         * Creates a rule that groups the given inner implementation rules.
         */
        @SafeVarargs
        @SuppressWarnings("varargs")
        public ConditionalImplementationCascadesRule(@Nonnull final ImplementationCascadesRule<T>... rules) {
            super(rules);
        }

        @Override
        public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
            throw new RecordCoreException("cannot call this method directly");
        }
    }
}
