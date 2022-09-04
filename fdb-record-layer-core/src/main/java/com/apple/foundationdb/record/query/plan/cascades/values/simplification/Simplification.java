/*
 * Simplification.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Main class of a mini rewrite engine to simplify
 * {@link com.apple.foundationdb.record.query.plan.cascades.values.Value} trees.
 */
public class Simplification {
    @Nonnull
    public static Value simplify(@Nonnull Value root,
                                 @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                 @Nonnull final AbstractValueRuleSet<Value, ValueSimplificationRuleCall> ruleSet) {
        return root.<Value>mapMaybe((current, mappedChildren) -> {
            current = computeCurrent(current, mappedChildren);
            return Simplification.executeRuleSet(current,
                    ruleSet,
                    (rule, self, plannerBindings) -> new ValueSimplificationRuleCall(rule, self, plannerBindings, constantAliases),
                    Iterables::getOnlyElement);
        }).orElseThrow(() -> new RecordCoreException("expected a mapped tree"));
    }

    @Nonnull
    public static <R> ValueComputationRuleCall.ValueWithResult<R> compute(@Nonnull Value root,
                                                                          @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                                                          @Nonnull final ValueComputationRuleSet<R> ruleSet) {
        final var resultsMap = new LinkedIdentityMap<Value, ValueComputationRuleCall.ValueWithResult<R>>();

        root = root.<Value>mapMaybe((current, mappedChildren) -> {
            current = computeCurrent(current, mappedChildren);
            return executeRuleSet(current,
                    ruleSet,
                    (rule, self, plannerBindings) -> new ValueComputationRuleCall<>(rule, self, plannerBindings, constantAliases, v -> Objects.requireNonNull(resultsMap.get(v))),
                    results -> onResultsFunction(resultsMap, results));
        }).orElseThrow(() -> new RecordCoreException("expected a mapped tree"));
        return Objects.requireNonNull(resultsMap.get(root));
    }

    @Nonnull
    private static Value computeCurrent(@Nonnull final Value current, @Nonnull final Iterable<? extends Value> mappedChildren) {
        final var children = current.getChildren();
        final var childrenIterator = children.iterator();
        final var mappedChildrenIterator = mappedChildren.iterator();
        boolean isSame = true;
        while (childrenIterator.hasNext() && mappedChildrenIterator.hasNext()) {
            final Value child = childrenIterator.next();
            final Value mappedChild = mappedChildrenIterator.next();
            if (child != mappedChild) {
                isSame = false;
                break;
            }
        }
        // make sure they are both exhausted or both are not exhausted
        Verify.verify(childrenIterator.hasNext() == mappedChildrenIterator.hasNext());
        return isSame ? current : current.withChildren(mappedChildren);
    }

    @Nonnull
    private static <R> Value onResultsFunction(@Nonnull final Map<Value, ValueComputationRuleCall.ValueWithResult<R>> resultsMap,
                                               @Nonnull final Collection<ValueComputationRuleCall.ValueWithResult<R>> results) {
        Verify.verify(results.size() <= 1);

        final var valueWithResult = Iterables.getOnlyElement(results);
        final var value = valueWithResult.getValue();
        resultsMap.put(value, valueWithResult);
        return value;
    }

    @Nonnull
    private static <R, C extends AbstractValueRuleCall<R, C>> Value executeRuleSet(@Nonnull Value self,
                                                                                   @Nonnull final AbstractValueRuleSet<R, C> ruleSet,
                                                                                   @Nonnull final RuleCallCreator<R, C> ruleCallCreator,
                                                                                   @Nonnull final Function<Collection<R>, Value> onResultsFunction) {
        boolean madeProgress;
        do {
            madeProgress = false;
            final var ruleIterator =
                    ruleSet.getValueRules(self).iterator();

            while (ruleIterator.hasNext()) {
                final var rule = ruleIterator.next();
                final BindingMatcher<? extends Value> matcher = rule.getMatcher();

                final var matchIterator = matcher.bindMatches(PlannerBindings.empty(), self).iterator();

                while (matchIterator.hasNext()) {
                    final var plannerBindings = matchIterator.next();
                    final var ruleCall = ruleCallCreator.create(rule, self, plannerBindings);

                    //
                    // Run the rule. See if the rule yielded a simplification.
                    //
                    rule.onMatch(ruleCall);
                    final var results = ruleCall.getResults();

                    if (!results.isEmpty()) {
                        self = onResultsFunction.apply(results);

                        //
                        // We made progress. Make sure we exit the inner while loops and restart with the first rule
                        // for the new `self` again.
                        //
                        madeProgress = true;
                        break;
                    }
                }

                if (madeProgress) {
                    break;
                }
            }
        } while (madeProgress);

        return self;
    }

    /**
     * Functional interface to create a specific rule call.
     * @param <R> the type parameter representing the type of result that is handed to {@link PlannerRuleCall#yield(Object)}
     * @param <C> the type of {@link PlannerRuleCall}
     */
    @FunctionalInterface
    public interface RuleCallCreator<R, C extends AbstractValueRuleCall<R, C>> {
        C create(@Nonnull final AbstractValueRule<R, C, ? extends Value> rule,
                 @Nonnull final Value self,
                 @Nonnull final PlannerBindings plannerBindings);
    }
}

