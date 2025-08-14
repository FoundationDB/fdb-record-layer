/*
 * ConditionalCascadesRule.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * TODO.
 * @param <T> a parent planner expression type of all possible root planner expressions that this rule could match
 * @param <R> the kind of
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public class ConditionalCascadesRule<T, R extends CascadesRule<T>> extends AbstractCascadesRule<T> {
    @Nonnull
    final List<R> rules;
    @Nonnull
    Optional<Class<?>> rootOperatorOptional;

    @SafeVarargs
    @SuppressWarnings("varargs")
    public ConditionalCascadesRule(@Nonnull final R... rules) {
        this(ImmutableList.copyOf(rules));
    }

    public ConditionalCascadesRule(@Nonnull final List<R> rules) {
        super(bindingMatcher(rules), ImmutableSet.of());
        this.rules = ImmutableList.copyOf(rules);
        this.rootOperatorOptional = rootOperatorMaybe(rules);
    }

    @Nonnull
    public List<R> getRules() {
        return rules;
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return rootOperatorOptional;
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        throw new RecordCoreException("cannot call this method directly");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + rules;
    }

    @Nonnull
    private static <T, R extends PlannerRule<CascadesRuleCall, T>> BindingMatcher<T> bindingMatcher(@Nonnull final List<R> rules) {
        BindingMatcher<T> bindingMatcher = null;
        for (final var rule : rules) {
            if (bindingMatcher == null) {
                bindingMatcher = rule.getMatcher();
            } else {
                final var currentBindingMatcher = rule.getMatcher();
                Verify.verify(currentBindingMatcher.getRootClass() == bindingMatcher.getRootClass());
            }
        }
        return Objects.requireNonNull(bindingMatcher, "need to have at least one rule");
    }

    @Nonnull
    @SuppressWarnings("OptionalAssignedToNull")
    private static <T, R extends PlannerRule<CascadesRuleCall, T>> Optional<Class<?>> rootOperatorMaybe(@Nonnull final List<R> rules) {
        Optional<Class<?>> rootOperatorOptional = null;
        for (final var rule : rules) {
            if (rootOperatorOptional == null) {
                rootOperatorOptional = rule.getRootOperator();
            } else {
                final var currentRootOperatorOptional = rule.getRootOperator();
                Verify.verify(rootOperatorOptional.isPresent() || currentRootOperatorOptional.isEmpty());
                Verify.verify(rootOperatorOptional.isEmpty() ||
                        (currentRootOperatorOptional.isPresent() && rootOperatorOptional.get() == currentRootOperatorOptional.get()));
            }
        }
        return Objects.requireNonNull(rootOperatorOptional, "need to have at least one rule");
    }

    public static class ConditionalExplorationCascadesRule<T extends RelationalExpression> extends ConditionalCascadesRule<T, ExplorationCascadesRule<T>> implements ExplorationCascadesRule<T> {
        @SafeVarargs
        @SuppressWarnings("varargs")
        public ConditionalExplorationCascadesRule(@Nonnull final ExplorationCascadesRule<T>... rules) {
            super(rules);
        }

        public ConditionalExplorationCascadesRule(@Nonnull final List<ExplorationCascadesRule<T>> rules) {
            super(rules);
        }

        @Override
        public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
            throw new RecordCoreException("cannot call this method directly");
        }
    }

    public static class ConditionalImplementationCascadesRule<T extends RelationalExpression> extends ConditionalCascadesRule<T, ImplementationCascadesRule<T>> implements ImplementationCascadesRule<T> {
        @SafeVarargs
        @SuppressWarnings("varargs")
        public ConditionalImplementationCascadesRule(@Nonnull final ImplementationCascadesRule<T>... rules) {
            super(rules);
        }

        public ConditionalImplementationCascadesRule(@Nonnull final List<ImplementationCascadesRule<T>> rules) {
            super(rules);
        }

        @Override
        public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
            throw new RecordCoreException("cannot call this method directly");
        }
    }
}
