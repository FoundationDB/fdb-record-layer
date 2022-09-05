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
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
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
            return Simplification.executeRuleSet(root,
                    current,
                    ruleSet,
                    (rule, r, c, plannerBindings) -> new ValueSimplificationRuleCall(rule, r, c, plannerBindings, constantAliases),
                    Iterables::getOnlyElement);
        }).orElseThrow(() -> new RecordCoreException("expected a mapped tree"));
    }

    @Nullable
    public static <A, R> ValueComputationRuleCall.ValueWithResult<R> compute(@Nonnull final Value root,
                                                                             @Nonnull A argument,
                                                                             @Nonnull final Set<CorrelationIdentifier> constantAliases,
                                                                             @Nonnull final ValueComputationRuleSet<A, R> ruleSet) {
        final var resultsMap = new LinkedIdentityMap<Value, ValueComputationRuleCall.ValueWithResult<R>>();

        final var newRoot = root.<Value>mapMaybe((current, mappedChildren) -> {
            current = computeCurrent(current, mappedChildren);
            return executeRuleSet(root,
                    current,
                    ruleSet,
                    (rule, r, c, plannerBindings) -> new ValueComputationRuleCall<>(rule, r, c, argument, plannerBindings, constantAliases, resultsMap::get),
                    results -> onResultsFunction(resultsMap, results));
        }).orElseThrow(() -> new RecordCoreException("expected a mapped tree"));
        return resultsMap.get(newRoot);
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
    private static <R, C extends AbstractValueRuleCall<R, C>> Value executeRuleSet(@Nonnull final Value root,
                                                                                   @Nonnull Value current,
                                                                                   @Nonnull final AbstractValueRuleSet<R, C> ruleSet,
                                                                                   @Nonnull final RuleCallCreator<R, C> ruleCallCreator,
                                                                                   @Nonnull final Function<Collection<R>, Value> onResultsFunction) {
        final boolean isRoot = current == root;
        Value newCurrent = current;
        do {
            current = newCurrent;
            final var ruleIterator =
                    ruleSet.getValueRules(current).iterator();

            while (ruleIterator.hasNext()) {
                final var rule = ruleIterator.next();
                final BindingMatcher<? extends Value> matcher = rule.getMatcher();

                final var matchIterator = matcher.bindMatches(PlannerBindings.empty(), current).iterator();

                while (matchIterator.hasNext()) {
                    final var plannerBindings = matchIterator.next();
                    final var ruleCall = ruleCallCreator.create(rule, isRoot ? current : root, current, plannerBindings);

                    //
                    // Run the rule. See if the rule yielded a simplification.
                    //
                    rule.onMatch(ruleCall);
                    final var results = ruleCall.getResults();

                    if (!results.isEmpty()) {
                        newCurrent = onResultsFunction.apply(results);

                        if (current != newCurrent) {
                            //
                            // We made progress. Make sure we exit the inner while loops and restart with the first rule
                            // for the new `current` again.
                            //
                            break;
                        }
                    }
                }

                if (current != newCurrent) {
                    break;
                }
            }
        } while (current != newCurrent);

        return current;
    }

    /**
     * Functional interface to create a specific rule call.
     * @param <R> the type parameter representing the type of result that is handed to {@link PlannerRuleCall#yield(Object)}
     * @param <C> the type of {@link PlannerRuleCall}
     */
    @FunctionalInterface
    public interface RuleCallCreator<R, C extends AbstractValueRuleCall<R, C>> {
        C create(@Nonnull final AbstractValueRule<R, C, ? extends Value> rule,
                 @Nonnull final Value root,
                 @Nonnull final Value current,
                 @Nonnull final PlannerBindings plannerBindings);
    }
}

