/*
 * OrToLogicalUnionRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;

/**
 * Convert a filter on an {@linkplain OrPredicate or} expression into a plan on the union. In particular, this will
 * produce a {@link LogicalUnionExpression} with simple filter plans on each child.
 *
 * <pre>
 * {@code
 *     +----------------------------+                 +-----------------------------------+
 *     |                            |                 |                                   |
 *     |  SelectExpression          |                 |  LogicalUnionExpression           |
 *     |       p1 v p2 v ... v pn   |                 |                                   |
 *     |                            |                 +-----------------------------------+
 *     +-------------+--------------+                        /        |               \
 *                   |                    +-->              /         |                \
 *                   | qun                                 /          |                 \
 *                   |                                    /           |                  \
 *                   |                                   /            |                   \
 *                   |                             +--------+    +--------+          +--------+
 *                   |                             |        |    |        |          |        |
 *                   |                             |  SEL   |    |  SEL   |          |  SEL   |
 *                   |                             |    p1' |    |    p2' |   ....   |    pn' |
 *                   |                             |        |    |        |          |        |
 *                   |                             +--------+    +--------+          +--------+
 *                   |                                /              /                   /
 *                   |                               / qun          / qun               / qun
 *            +------+------+  ---------------------+              /                   /
 *            |             |                                     /                   /
 *            |   any ref   |  ----------------------------------+                   /
 *            |             |                                                       /
 *            +-------------+  ----------------------------------------------------+
 * }
 * </pre>
 * Where p1, p2, ..., pn are the or terms of the predicate in the original {@link SelectExpression}.
 *        
 */
@API(API.Status.EXPERIMENTAL)
public class OrToLogicalUnionRule extends CascadesRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<Quantifier> qunMatcher = anyQuantifier();
    @Nonnull
    private static final BindingMatcher<QueryPredicate> orTermPredicateMatcher = anyPredicate();
    @Nonnull
    private static final BindingMatcher<OrPredicate> orMatcher = QueryPredicateMatchers.ofTypeWithChildren(OrPredicate.class, all(orTermPredicateMatcher));
    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            RelationalExpressionMatchers.selectExpression(exactly(orMatcher), all(qunMatcher));

    public OrToLogicalUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        final var resultValue = selectExpression.getResultValue();
        final var quantifiers = bindings.getAll(qunMatcher);
        final var orTermPredicates = bindings.getAll(orTermPredicateMatcher);

        final var ownedForEachAliases =
                quantifiers.stream()
                        .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                        .map(Quantifier::getAlias)
                        .collect(ImmutableSet.toImmutableSet());

        final var isSimpleResultValue = (resultValue instanceof QuantifiedObjectValue) &&
                                        (ownedForEachAliases.contains(((QuantifiedObjectValue)resultValue).getAlias()));

        final var resultValueCorrelatedTo = resultValue.getCorrelatedTo();
        final var referredOwnedForEachAliases =
                Sets.intersection(resultValueCorrelatedTo, ownedForEachAliases);

        if (referredOwnedForEachAliases.isEmpty()) {
            // There is no point in creating a union as the or terms are somehow correlated and cannot be used for
            // index matching and other good stuff.
            return;
        }
        
        final Optional<CorrelationIdentifier> referredAliasOptional;
        if (!isSimpleResultValue) {
            if (referredOwnedForEachAliases.size() > 1) {
                return;
            }
            referredAliasOptional = referredOwnedForEachAliases.stream().findFirst();
        } else {
            referredAliasOptional = Optional.of(((QuantifiedObjectValue)resultValue).getAlias());
        }

        final var relationalExpressionReferences = Lists.<ExpressionRef<RelationalExpression>>newArrayListWithCapacity(orTermPredicates.size());
        for (final var orPredicate : orTermPredicates) {
            final var orCorrelatedTo = orPredicate.getCorrelatedTo();

            //
            // Subset the quantifiers to only those that are actually needed by this or term. Needed quantifiers are
            // quantifiers that contribute (in positive or negative ways) to the cardinality, i.e. all for-each quantifiers
            // and existential quantifiers that are predicated by means of an exists() predicate. As existential
            // quantifier by itself just creates a true or false but never removes a record or contributes in a meaningful
            // way to the result set.
            // TODO This optimization can be done for all quantifiers that are not referred to by the term that also have a
            //      cardinality of one.
            //
            final ImmutableList<? extends Quantifier> neededQuantifiers =
                    quantifiers
                            .stream()
                            .filter(quantifier -> quantifier instanceof Quantifier.ForEach ||
                                                  (quantifier instanceof Quantifier.Existential && orCorrelatedTo.contains(quantifier.getAlias())))
                            .collect(ImmutableList.toImmutableList());

            final Value lowerResultValue;
            if (!isSimpleResultValue) {
                if (neededQuantifiers.size() > 1) {
                    // We cannot do this rewrite as it becomes impossible (for now) to reason about the duplicates
                    // that need to be preserved without introducing additional ones
                    return;
                }

                lowerResultValue = neededQuantifiers
                        .stream()
                        .findFirst()
                        .map(onlyNeededQuantifier -> (Value)Iterables.getOnlyElement(neededQuantifiers).getFlowedObjectValue())
                        .orElseGet(() -> new LiteralValue<>(1));
            } else {
                lowerResultValue = resultValue;
            }

            relationalExpressionReferences.add(GroupExpressionRef.of(new SelectExpression(lowerResultValue, neededQuantifiers, ImmutableList.of(orPredicate))));
        }

        var resultReference = GroupExpressionRef.<RelationalExpression>of(new LogicalUnionExpression(Quantifiers.forEachQuantifiers(relationalExpressionReferences)));

        if (!isSimpleResultValue) {
            final var unionQuantifier = Quantifier.forEach(resultReference);
            final var rebasedResultValue = referredAliasOptional.map(referredAlias -> resultValue.rebase(AliasMap.of(referredAlias, unionQuantifier.getAlias()))).orElse(resultValue);
            resultReference = GroupExpressionRef.of(new SelectExpression(rebasedResultValue, ImmutableList.of(unionQuantifier), ImmutableList.of()));
        }

        call.yield(resultReference);
    }
}
