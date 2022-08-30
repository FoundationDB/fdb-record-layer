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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Main class of a mini rewrite engine to simplify
 * {@link com.apple.foundationdb.record.query.plan.cascades.values.Value} trees.
 */
public class Simplification {
    @Nonnull
    public static Value simplify(@Nonnull Value root,
                                 @Nonnull Set<CorrelationIdentifier> constantAliases,
                                 @Nonnull final ValueSimplificationRuleSet ruleSet,
                                 @Nonnull final Predicate<ValueSimplificationRule<? extends Value>> rulePredicate) {

        //
        // First, simplify all children separately. Keep track if they return the same, so we can save a copy for
        // `this`.
        //
        final var newChildrenBuilder = ImmutableList.<Value>builder();
        boolean isSame = true;
        for (final var child : root.getChildren()) {
            final var newChild = simplify(child, constantAliases, ruleSet, rulePredicate);
            newChildrenBuilder.add(newChild);
            if (child != newChild) {
                isSame = false;
            }
        }

        //
        // `self` tracks the most recent simplification
        //
        var self = isSame ? root : root.withChildren(newChildrenBuilder.build());
        boolean madeProgress;
        do {
            madeProgress = false;
            final var ruleIterator =
                    ruleSet.getValueRules(self, rulePredicate).iterator();

            while (ruleIterator.hasNext()) {
                final var rule = ruleIterator.next();
                final BindingMatcher<? extends Value> matcher = rule.getMatcher();

                final var matchIterator = matcher.bindMatches(PlannerBindings.empty(), self).iterator();

                while (matchIterator.hasNext()) {
                    final var plannerBindings = matchIterator.next();
                    final var ruleCall = new ValueSimplificationRuleCall(rule, self, plannerBindings, constantAliases);

                    //
                    // Run the rule. See if the rule yielded a simplification.
                    //
                    ruleCall.run();
                    final var newValues = ruleCall.getNewValues();
                    Verify.verify(newValues.size() <= 1);

                    if (!newValues.isEmpty()) {
                        self = Iterables.getOnlyElement(newValues);

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
}

