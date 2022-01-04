/*
 * PushInterestingOrderingThroughInLikeSelectRule.java
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
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.OrderingAttribute;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.CollectionMatcher;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.explodeExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.selectExpression;

/**
 * A rule that pushes an interesting {@link OrderingAttribute} through a specific {@link SelectExpression}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PushInterestingOrderingThroughInLikeSelectRule extends PlannerRule<SelectExpression> implements PreOrderRule {
    private static final BindingMatcher<ExplodeExpression> explodeExpressionMatcher = explodeExpression();
    private static final CollectionMatcher<Quantifier.ForEach> explodeQuantifiersMatcher = some(forEachQuantifier(explodeExpressionMatcher));

    private static final BindingMatcher<SelectExpression> root =
            selectExpression(explodeQuantifiersMatcher);

    public PushInterestingOrderingThroughInLikeSelectRule() {
        super(root, ImmutableSet.of(OrderingAttribute.ORDERING));
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var requestedOrderingsOptional = call.getInterestingProperty(OrderingAttribute.ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }
        final var requestedOrderings = requestedOrderingsOptional.get();

        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);

        final Collection<? extends Quantifier.ForEach> explodeQuantifiers = bindings.get(explodeQuantifiersMatcher);

        //
        // Find the one quantifier that is not ranging over an explode expression.
        //
        final var innerForEachQuantifierOptional =
                findInnerQuantifier(selectExpression,
                        explodeQuantifiers,
                        Quantifiers.aliases(explodeQuantifiers));
        if (innerForEachQuantifierOptional.isEmpty()) {
            return;
        }
        final var innerForEachQuantifier = innerForEachQuantifierOptional.get();

        final var lowerReference = innerForEachQuantifier.getRangesOver();

        //
        // Push down the existing requested orderings verbatim. This is both applicable for possible in-joins
        // and in-unions.
        //
        call.pushRequirement(lowerReference,
                OrderingAttribute.ORDERING,
                requestedOrderings);
    }

    @Nonnull
    public static Optional<Quantifier.ForEach> findInnerQuantifier(@Nonnull final SelectExpression selectExpression,
                                                                   @Nonnull final Collection<? extends Quantifier> explodeQuantifiers,
                                                                   @Nonnull final Set<CorrelationIdentifier> explodeAliases) {
        final List<? extends Quantifier> quantifiers = selectExpression.getQuantifiers();

        //
        // There should be n quantifiers ranging over explodes and exactly one that is not over an explode expression.
        //
        if (explodeQuantifiers.size() + 1 != quantifiers.size()) {
            return Optional.empty();
        }

        //
        // Find the one quantifier that is not ranging over an explode expression.
        //
        final var innerQuantifierOptional = quantifiers
                .stream()
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach && !explodeAliases.contains(quantifier.getAlias()))
                .map(quantifier -> quantifier.narrow(Quantifier.ForEach.class))
                .findAny();

        return innerQuantifierOptional.flatMap(innerQuantifier -> {
            //
            // For now, we have to insist that this quantifier is correlated to all explode quantifiers.
            //
            final var correlatedTo = innerQuantifier.getCorrelatedTo();
            return correlatedTo.containsAll(explodeAliases) ? Optional.of(innerQuantifier) : Optional.empty();
        });
    }
}
