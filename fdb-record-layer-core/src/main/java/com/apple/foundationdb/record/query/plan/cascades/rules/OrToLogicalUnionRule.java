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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.QueryPredicateWithDnfRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher.combinations;
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
    private static final CollectionMatcher<QueryPredicate> combinationPredicateMatcher = all(anyPredicate());
    @Nonnull
    private static final BindingMatcher<SelectExpression> root =
            RelationalExpressionMatchers.selectExpression(combinations(combinationPredicateMatcher), all(qunMatcher));

    public OrToLogicalUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        final var resultValue = selectExpression.getResultValue();
        final var quantifiers = bindings.getAll(qunMatcher);

        //
        // There are some complications arising from the fact that we can (under certain) circumstances do this
        // transformation even for joins.
        // 1. All cardinality-modifying, i.e. for-each quantifiers need to be moved into the legs of the
        //    newly formed union.
        // 2. The result value (which must stay above the newly formed union for duplicate-preserving reasons can
        //    only ever refer to at most one for each quantifier from the owned set of for-each quantifiers.
        // 3. TODO For now we only allow exactly one for-each quantifier.

        final var ownedForEachAliases =
                quantifiers.stream()
                        .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                        .map(Quantifier::getAlias)
                        .collect(ImmutableSet.toImmutableSet());
        if (ownedForEachAliases.size() != 1) {
            return;
        }

        final var isSimpleResultValue = (resultValue instanceof QuantifiedObjectValue) &&
                                        (ownedForEachAliases.contains(((QuantifiedObjectValue)resultValue).getAlias()));

        final var resultValueCorrelatedTo = resultValue.getCorrelatedTo();

        final Optional<CorrelationIdentifier> referredAliasByResultOptional;
        final var referredOwnedForEachAliases =
                Sets.intersection(resultValueCorrelatedTo, ownedForEachAliases);
        Verify.verify(referredOwnedForEachAliases.size() <= 1);

        if (referredOwnedForEachAliases.isEmpty()) {
            referredAliasByResultOptional = Optional.empty();
        } else {
            referredAliasByResultOptional = Optional.of(Iterables.getOnlyElement(referredOwnedForEachAliases));
        }

        final var fixedPredicates = LinkedIdentitySet.copyOf(bindings.get(combinationPredicateMatcher));
        final var toBeDnfPredicates =
                selectExpression.getPredicates()
                        .stream()
                        .filter(predicate -> !fixedPredicates.contains(predicate))
                        .collect(LinkedIdentitySet.toLinkedIdentitySet());
        if (toBeDnfPredicates.isEmpty()) {
            return;
        }

        final var conjunctedPredicate = AndPredicate.and(toBeDnfPredicates);
        final var constantAliases = Sets.difference(conjunctedPredicate.getCorrelatedTo(), Quantifiers.aliases(selectExpression.getQuantifiers()));

        final var dnfPredicate =
                Simplification.optimize(conjunctedPredicate, EvaluationContext.empty(), AliasMap.emptyMap(),
                        constantAliases, QueryPredicateWithDnfRuleSet.ofComputationRules()).getLeft();
        if (!(dnfPredicate instanceof OrPredicate)) {
            // it can be that the dnf-predicate is trivial, i.e. it is only an AND of boolean variables
            return;
        }

        final var aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(quantifiers);
        // there is definitely exactly one quantifier in the needed list
        final var onlyNeededForEachQuantifier = aliasToQuantifierMap.get(Iterables.getOnlyElement(ownedForEachAliases));
        final Value lowerResultValue = onlyNeededForEachQuantifier.getFlowedObjectValue();
        final var fixedPredicatesCorrelatedTo = fixedPredicates.stream().flatMap(p -> p.getCorrelatedTo().stream()).collect(ImmutableSet.toImmutableSet());
        final var orTermPredicates = dnfPredicate.getChildren();
        final var relationalExpressionReferences = Lists.<ExpressionRef<? extends RelationalExpression>>newArrayList();
        for (final var orTermPredicate : orTermPredicates) {
            final var orTermCorrelatedTo = orTermPredicate.getCorrelatedTo();

            //
            // Subset the quantifiers to only those that are actually needed by this or term. Needed quantifiers are
            // quantifiers that contribute (in positive or negative ways) to the cardinality, i.e. all for-each quantifiers
            // and existential quantifiers that are predicated by means of an exists() predicate. As existential
            // quantifier by itself just creates a true or false but never removes a record or contributes in a meaningful
            // way to the result set.
            // TODO This optimization can be done for all quantifiers that are not referred to by the term that also have a
            //      cardinality of one.
            //
            final var neededAdditionalQuantifiers =
                    quantifiers
                            .stream()
                            .filter(quantifier -> quantifier instanceof Quantifier.Existential &&
                                                  (orTermCorrelatedTo.contains(quantifier.getAlias()) ||
                                                   fixedPredicatesCorrelatedTo.contains(quantifier.getAlias())))
                            .map(quantifier -> Quantifier.existentialBuilder().withAlias(quantifier.getAlias()).build(aliasToQuantifierMap.get(quantifier.getAlias()).getRangesOver()))
                            .collect(ImmutableList.toImmutableList());

            final var neededForEachQuantifiers =
                    ownedForEachAliases.stream().map(alias -> Quantifier.forEachBuilder().withAlias(alias).build(aliasToQuantifierMap.get(alias).getRangesOver())).collect(ImmutableList.toImmutableList());

            final var unionLegExpression =
                    new SelectExpression(lowerResultValue,
                            ImmutableList.copyOf(Iterables.concat(neededForEachQuantifiers, neededAdditionalQuantifiers)),
                            ImmutableList.<QueryPredicate>builder().addAll(fixedPredicates).add(orTermPredicate).build());
            relationalExpressionReferences.add(call.memoizeExpression(unionLegExpression));
        }

        var unionReferenceBuilder = call.memoizeExpressionBuilder(new LogicalUnionExpression(Quantifiers.forEachQuantifiers(relationalExpressionReferences)));

        if (!isSimpleResultValue) {
            final ExpressionRef<? extends RelationalExpression> unionReference = unionReferenceBuilder.reference();
            final var unionQuantifier = referredAliasByResultOptional.map(alias -> Quantifier.forEachBuilder().withAlias(alias).build(unionReference)).orElse(Quantifier.forEach(unionReference));
            unionReferenceBuilder = call.memoizeExpressionBuilder(new SelectExpression(resultValue, ImmutableList.of(unionQuantifier), ImmutableList.of()));
        }

        call.yield(unionReferenceBuilder.members());
    }
}
