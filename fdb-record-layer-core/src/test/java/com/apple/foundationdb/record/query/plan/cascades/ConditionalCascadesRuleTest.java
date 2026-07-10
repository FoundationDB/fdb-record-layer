/*
 * ConditionalCascadesRuleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.ConditionalCascadesRule.ConditionalExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ConditionalCascadesRule.ConditionalImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ConditionalCascadesRule}.
 */
class ConditionalCascadesRuleTest {

    @Test
    void constructorDerivesCommonRootOperator() {
        final StubRule first = stubRule(SelectExpression.class);
        final StubRule second = stubRule(SelectExpression.class);
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule = conditionalRuleOf(first, second);
        assertThat(rule.getRootOperator()).contains(SelectExpression.class);
        assertThat(rule.getRules()).containsExactly(first, second);
    }

    @Test
    void varargsConstructorDerivesCommonRootOperator() {
        // Exercises the varargs constructor directly, rather than the list constructor used by `conditionalRuleOf()`.
        final StubRule first = stubRule(SelectExpression.class);
        final StubRule second = stubRule(SelectExpression.class);
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule = new ConditionalCascadesRule<>(first, second);
        assertThat(rule.getRules()).containsExactly(first, second);
        assertThat(rule.getRootOperator()).contains(SelectExpression.class);
    }

    @Test
    void constructorWithSingleRuleSucceeds() {
        final StubRule only = stubRule(SelectExpression.class);
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule = conditionalRuleOf(only);
        assertThat(rule.getRules()).containsExactly(only);
        assertThat(rule.getRootOperator()).contains(SelectExpression.class);
    }

    @Test
    void constructorWithEmptyListThrows() {
        assertThatThrownBy(() -> new ConditionalCascadesRule<>(ImmutableList.<StubRule>of()))
                .isInstanceOf(VerifyException.class)
                .hasMessageContaining("must contain at least one rule");
    }

    @Test
    void constructorWithMismatchedRootClassThrows() {
        final StubRule selectRule = stubRule(SelectExpression.class);
        final StubRule filterRule = stubRule(LogicalFilterExpression.class);
        assertThatThrownBy(() -> conditionalRuleOf(selectRule, filterRule)).isInstanceOf(VerifyException.class);
    }

    @Test
    void constructorWithMismatchedRootOperatorThrows() {
        // Both rules match the same root class, so the binding-matcher check passes, but they advertise different
        // root operators, which the root-operator check must reject.
        final StubRule first = stubRule(SelectExpression.class, Optional.of(SelectExpression.class));
        final StubRule second = stubRule(SelectExpression.class, Optional.of(LogicalFilterExpression.class));
        assertThatThrownBy(() -> conditionalRuleOf(first, second)).isInstanceOf(VerifyException.class);
    }

    @Test
    void constructorWithPresentAndEmptyRootOperatorThrows() {
        final StubRule present = stubRule(SelectExpression.class, Optional.of(SelectExpression.class));
        final StubRule empty = stubRule(SelectExpression.class, Optional.empty());
        assertThatThrownBy(() -> conditionalRuleOf(present, empty)).isInstanceOf(VerifyException.class);
    }

    @Test
    void constructorWithAllEmptyRootOperatorsHasEmptyRootOperator() {
        final StubRule first = stubRule(SelectExpression.class, Optional.empty());
        final StubRule second = stubRule(SelectExpression.class, Optional.empty());
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule = conditionalRuleOf(first, second);
        assertThat(rule.getRootOperator()).isEmpty();
    }

    @Test
    void getRulesReturnsRulesInOrder() {
        final StubRule first = stubRule(SelectExpression.class);
        final StubRule second = stubRule(SelectExpression.class);
        final StubRule third = stubRule(SelectExpression.class);
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule = conditionalRuleOf(first, second, third);
        assertThat(rule.getRules()).containsExactly(first, second, third);
    }

    @Test
    void getRulesWithPredicateFiltersInOrder() {
        final StubRule first = stubRule(SelectExpression.class);
        final StubRule second = stubRule(SelectExpression.class);
        final StubRule third = stubRule(SelectExpression.class);
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule = conditionalRuleOf(first, second, third);
        assertThat(rule.getRules(candidate -> candidate == first || candidate == third)).containsExactly(first, third);
    }

    @Test
    void getRulesWithNeverMatchingPredicateReturnsEmpty() {
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule =
                conditionalRuleOf(stubRule(SelectExpression.class), stubRule(SelectExpression.class));
        assertThat(rule.getRules(candidate -> false)).isEmpty();
    }

    @Test
    void onMatchThrows() {
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule =
                conditionalRuleOf(stubRule(SelectExpression.class));
        assertThatThrownBy(() -> rule.onMatch((CascadesRuleCall) null))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("cannot call this method directly");
    }

    @Test
    void explorationVariantOnMatchThrows() {
        final ConditionalExplorationCascadesRule<RelationalExpression> rule =
                new ConditionalExplorationCascadesRule<>(ImmutableList.of(stubExplorationRule(SelectExpression.class)));
        assertThat(rule.getRootOperator()).contains(SelectExpression.class);
        assertThatThrownBy(() -> rule.onMatch((ExplorationCascadesRuleCall) null))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("cannot call this method directly");
        assertThatThrownBy(() -> rule.onMatch((CascadesRuleCall) null))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("cannot call this method directly");
    }

    @Test
    void explorationVariantVarargsConstructor() {
        final StubExplorationRule first = stubExplorationRule(SelectExpression.class);
        final StubExplorationRule second = stubExplorationRule(SelectExpression.class);
        final ConditionalExplorationCascadesRule<RelationalExpression> rule =
                new ConditionalExplorationCascadesRule<>(first, second);
        assertThat(rule.getRules()).containsExactly(first, second);
        assertThat(rule.getRootOperator()).contains(SelectExpression.class);
    }

    @Test
    void implementationVariantListConstructorAndOnMatchThrows() {
        final ConditionalImplementationCascadesRule<RelationalExpression> rule =
                new ConditionalImplementationCascadesRule<>(ImmutableList.of(stubImplementationRule(SelectExpression.class)));
        assertThat(rule.getRootOperator()).contains(SelectExpression.class);
        assertThatThrownBy(() -> rule.onMatch((ImplementationCascadesRuleCall) null))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("cannot call this method directly");
        assertThatThrownBy(() -> rule.onMatch((CascadesRuleCall) null))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("cannot call this method directly");
    }

    @Test
    void implementationVariantVarargsConstructor() {
        final StubImplementationRule first = stubImplementationRule(SelectExpression.class);
        final StubImplementationRule second = stubImplementationRule(SelectExpression.class);
        final ConditionalImplementationCascadesRule<RelationalExpression> rule =
                new ConditionalImplementationCascadesRule<>(first, second);
        assertThat(rule.getRules()).containsExactly(first, second);
        assertThat(rule.getRootOperator()).contains(SelectExpression.class);
    }

    @Test
    void toStringContainsClassNameAndRules() {
        final ConditionalCascadesRule<RelationalExpression, StubRule> rule =
                conditionalRuleOf(stubRule(SelectExpression.class));
        assertThat(rule.toString())
                .startsWith("ConditionalCascadesRule[")
                .contains(StubRule.class.getSimpleName());
    }

    @Nonnull
    private static ConditionalCascadesRule<RelationalExpression, StubRule> conditionalRuleOf(@Nonnull final StubRule... rules) {
        return new ConditionalCascadesRule<>(ImmutableList.copyOf(rules));
    }

    @Nonnull
    private static StubRule stubRule(@Nonnull final Class<? extends RelationalExpression> rootClass) {
        return stubRule(rootClass, Optional.of(rootClass));
    }

    @Nonnull
    private static StubRule stubRule(@Nonnull final Class<? extends RelationalExpression> rootClass,
                                     @Nonnull final Optional<Class<?>> rootOperator) {
        return new StubRule(matcherFor(rootClass), rootOperator);
    }

    @Nonnull
    private static StubExplorationRule stubExplorationRule(@Nonnull final Class<? extends RelationalExpression> rootClass) {
        return new StubExplorationRule(matcherFor(rootClass));
    }

    @Nonnull
    private static StubImplementationRule stubImplementationRule(@Nonnull final Class<? extends RelationalExpression> rootClass) {
        return new StubImplementationRule(matcherFor(rootClass));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    private static BindingMatcher<RelationalExpression> matcherFor(@Nonnull final Class<? extends RelationalExpression> rootClass) {
        return (BindingMatcher<RelationalExpression>) (BindingMatcher<?>) RelationalExpressionMatchers.ofType(rootClass);
    }

    /**
     * A minimal {@link CascadesRule} whose matcher root class and advertised root operator can be set independently,
     * so the construction-time invariants of {@link ConditionalCascadesRule} can be exercised in isolation.
     */
    private static final class StubRule extends AbstractCascadesRule<RelationalExpression> {
        @Nonnull
        private final Optional<Class<?>> rootOperator;

        private StubRule(@Nonnull final BindingMatcher<RelationalExpression> matcher,
                         @Nonnull final Optional<Class<?>> rootOperator) {
            super(matcher);
            this.rootOperator = rootOperator;
        }

        @Nonnull
        @Override
        public Optional<Class<?>> getRootOperator() {
            return rootOperator;
        }

        @Override
        public void onMatch(@Nonnull final CascadesRuleCall call) {
            throw new UnsupportedOperationException("stub rule should not be executed");
        }
    }

    /**
     * A minimal {@link ExplorationCascadesRule} used to exercise the {@link ConditionalExplorationCascadesRule}
     * variant. Its root operator is derived from its matcher, like an ordinary rule.
     */
    private static final class StubExplorationRule extends AbstractCascadesRule<RelationalExpression>
            implements ExplorationCascadesRule<RelationalExpression> {
        private StubExplorationRule(@Nonnull final BindingMatcher<RelationalExpression> matcher) {
            super(matcher);
        }

        @Override
        public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
            throw new UnsupportedOperationException("stub rule should not be executed");
        }
    }

    /**
     * A minimal {@link ImplementationCascadesRule} used to exercise the {@link ConditionalImplementationCascadesRule}
     * variant. Its root operator is derived from its matcher, like an ordinary rule.
     */
    private static final class StubImplementationRule extends AbstractCascadesRule<RelationalExpression>
            implements ImplementationCascadesRule<RelationalExpression> {
        private StubImplementationRule(@Nonnull final BindingMatcher<RelationalExpression> matcher) {
            super(matcher);
        }

        @Override
        public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
            throw new UnsupportedOperationException("stub rule should not be executed");
        }
    }
}
