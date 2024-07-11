/*
 * PredicateToLogicalUnionRule.java
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
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUniqueExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.Extractor;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.LeafQueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.QueryPredicateWithDnfRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.Simplification;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MatchPartitionMatchers.ofExpressionAndMatches;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.anyPartialMatch;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.anyQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;

/**
 * Transform a {@link SelectExpression} with a set of {@link QueryPredicate}s into a union operation.
 * <br>
 * The basic idea around this transformation: First, the {@link QueryPredicate}s in a {@link SelectExpression}
 * can be viewed as one conjuncted predicate. This predicate can be assumed to have been transformed into a CNF using
 * distribute low, deMorgan's law, etc. by {@link NormalizePredicatesRule}. That technically means that the
 * {@link QueryPredicate}s in the current {@link SelectExpression} only contain
 * <ol>
 *     <li> {@link OrPredicate}s, </li>
 *     <li> boolean variables aka {@link LeafQueryPredicate}s, and</li>
 *     <li> {@link com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate}s over boolean
 *          variables</li>
 * </ol>
 *
 * Note that this rule currently also attempts the best effort on predicates in non-normal forms (i.e. as written in the
 * query), but the outcome may not be exhaustive. This is important if the CNF is too complex.
 * <br>
 * Let's first assume the CNF is simple, i.e. the number of conjuncts is within the limit given by
 * {@link RecordQueryPlannerConfiguration#getOrToUnionMaxNumConjuncts()} (defaulted by
 * {@link #DEFAULT_MAX_NUM_CONJUNCTS}).
 * The CNF can be repeatedly multiplied out to eventually obtain the DNF. It is, however, beneficial to create all
 * intermediate forms as well.
 * <br>
 * In particular, a CNF represented by {@code a1 ^ a2 ^ ... ^ an} can be transformed into another form by fixing any
 * combination of {@code k} factors, transforming the remaining factors into their DNF and leaving the fixed factors
 * untouched.
 * <br>
 * For instance, the first three factors in {@code a1 ^ a2 ^ a3 ^ a4 ^ a5} can be fixed to obtain
 * {@code a1 ^ a2 ^ a3 ^ DNF(a4, a5)}.
 * <br>
 * Note that the DNF of the remaining factors (in the example {@code DNF(a4, a5)}) is an {@link OrPredicate} as long as
 * the DNF is not trivial. The DNF thus can usually be written as {@code o1 v o2 v ... v om}.
 * <br>
 * At this stage of the transformation the predicate is of the shape:
 * {@code AND(fixed factors) ^ OR(transformed (DNF) remaining factors} or written out
 * {@code af1 ^ af2 ^ ... ^ afk ^ (o1 v o2 v ... v om)}}.
 * <br>
 * It is now useful to multiply out the OR against the fixed factors:
 * {@code (af1 ^ (o1 v o2 v ... v om)) v (af2 ^ (o1 v o2 v ... v om)) v ... v (afk ^ (o1 v o2 v ... v om))}}.
 * Note that in this term all fixed factors remain intact; they just happen to be repeated per OR term.
 * <br>
 * As a final step this term is used to create a {@link LogicalUnionExpression} over {@link SelectExpression}s that
 * contain each contain a term {@code (ai ^ (o1 v o2 v ... v om))}.
 * <br>
 * In order to create all meaningful forms starting with the CNF and ending at the DNF (all factors have been multiplied
 * out) we need to consider all combinations of factors in the CNF. Note that for a given combination we are not
 * interested in different permutations of the fixed factors, i.e. it does not matter if we first multiply out some
 * factor {@code f1} and then {@code f2} or vice versa. We do not want to find all n-ary UNIONs (including all shapes
 * of UNION-trees).
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class PredicateToLogicalUnionRule extends CascadesRule<MatchPartition> {
    public static final int DEFAULT_MAX_NUM_CONJUNCTS = 9; // 510 combinations

    @Nonnull
    private static final BindingMatcher<Quantifier> qunMatcher = anyQuantifier();
    @Nonnull
    private static final CollectionMatcher<QueryPredicate> combinationPredicateMatcher = all(anyPredicate());
    @Nonnull
    private static final BindingMatcher<SelectExpression> expressionMatcher =
            RelationalExpressionMatchers.selectExpression(nonTrivialPredicates(limitedPredicateCombinations(combinationPredicateMatcher)), all(qunMatcher));

    private static final BindingMatcher<PartialMatch> anyPartialMatchMatcher = anyPartialMatch();

    private static final BindingMatcher<MatchPartition> rootMatcher =
            ofExpressionAndMatches(expressionMatcher, all(anyPartialMatchMatcher));

    public PredicateToLogicalUnionRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(expressionMatcher);
        final var resultValue = selectExpression.getResultValue();
        final var quantifiers = bindings.getAll(qunMatcher);
        final var matches = bindings.getAll(anyPartialMatchMatcher);

        //
        // There are some complications arising from the fact that we can (under certain) circumstances do this
        // transformation even for joins.
        // 1. All cardinality-modifying, i.e. for-each quantifiers need to be moved into the legs of the
        //    newly formed union.
        // 2. The result value (which must stay above the newly formed union for duplicate-preserving reasons can
        //    only ever refer to at most one for each quantifier from the owned set of for-each quantifiers.
        // 3. TODO For now we only allow exactly one for-each quantifier.
        //

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

        final var partiallyMatchedOrs = new LinkedIdentitySet<QueryPredicate>();
        for (final var match : matches) {
            final var matchInfo = match.getMatchInfo();
            final var predicateMap = matchInfo.getPredicateMap();
            predicateMap.values()
                    .stream()
                    .flatMap(predicateMapping ->
                            predicateMapping.getMappingKind() == PredicateMultiMap.PredicateMapping.MappingKind.OR_TERM_IMPLIES_CANDIDATE
                            ? Stream.of(predicateMapping.getQueryPredicate())
                            : Stream.empty())
                    .forEach(partiallyMatchedOrs::add);
        }
        if (toBeDnfPredicates.stream().anyMatch(predicate -> predicate instanceof OrPredicate && !partiallyMatchedOrs.contains(predicate))) {
            return;
        }
        
        final var conjunctedPredicate = AndPredicate.and(toBeDnfPredicates);
        final var constantAliases = Sets.difference(conjunctedPredicate.getCorrelatedTo(),
                Quantifiers.aliases(selectExpression.getQuantifiers()));

        final var dnfPredicate =
                Simplification.optimize(conjunctedPredicate, EvaluationContext.empty(), AliasMap.emptyMap(),
                        constantAliases, QueryPredicateWithDnfRuleSet.ofComputationRules()).getLeft();
        if (dnfPredicate.isAtomic() || !(dnfPredicate instanceof OrPredicate)) {
            // it can be that the dnf-predicate is trivial, i.e. it is only an AND of boolean variables
            return;
        }

        final var aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(quantifiers);
        // there is definitely exactly one quantifier in the needed list
        final var onlyNeededForEachQuantifier = aliasToQuantifierMap.get(Iterables.getOnlyElement(ownedForEachAliases));
        final Value lowerResultValue = onlyNeededForEachQuantifier.getFlowedObjectValue();
        final var fixedPredicatesCorrelatedTo = fixedPredicates.stream().flatMap(p -> p.getCorrelatedTo().stream()).collect(ImmutableSet.toImmutableSet());
        final var fixedAtomicPredicates =
                fixedPredicates.stream()
                        .map(predicate -> predicate.withAtomicity(true))
                        .collect(LinkedIdentitySet.toLinkedIdentitySet());

        final var references = Lists.<Reference>newArrayList();
        for (final var orTermPredicate : dnfPredicate.getChildren()) {
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

            final var selectExpressionLeg =
                    new SelectExpression(lowerResultValue,
                            ImmutableList.copyOf(Iterables.concat(neededForEachQuantifiers, neededAdditionalQuantifiers)),
                            ImmutableList.<QueryPredicate>builder().addAll(fixedAtomicPredicates).add(orTermPredicate).build());
            final Reference memoizedSelectExpressionLeg = call.memoizeExpression(selectExpressionLeg);
            final var uniqueExpressionLeg = new LogicalUniqueExpression(Quantifier.forEach(memoizedSelectExpressionLeg));
            final var memoizedUniqueExpressionLeg = call.memoizeExpression(uniqueExpressionLeg);
            references.add(memoizedUniqueExpressionLeg);
        }
        
        var unionReferenceBuilder = call.memoizeExpressionBuilder(new LogicalUnionExpression(Quantifiers.forEachQuantifiers(references)));

        unionReferenceBuilder = call.memoizeExpressionBuilder(new LogicalDistinctExpression(Quantifier.forEach(unionReferenceBuilder.reference())));

        if (!isSimpleResultValue) {
            final Reference unionReference = unionReferenceBuilder.reference();
            final var unionQuantifier = referredAliasByResultOptional.map(alias -> Quantifier.forEachBuilder().withAlias(alias).build(unionReference)).orElse(Quantifier.forEach(unionReference));
            unionReferenceBuilder = call.memoizeExpressionBuilder(new SelectExpression(resultValue, ImmutableList.of(unionQuantifier), ImmutableList.of()));
        }

        call.yieldExpression(unionReferenceBuilder.members());
    }

    @SuppressWarnings("unchecked")
    private static CollectionMatcher<QueryPredicate> nonTrivialPredicates(@Nonnull final CollectionMatcher<? extends QueryPredicate> downstream) {
        //
        // We want to subset the predicates in the SelectExpression to only use the factors of the normal form that
        // are non-trivial, i.e. real ORs as opposed to boolean variables, i.e. comparisons and other leaves.
        // Note that by doing this we still exhaustively create all desired forms. Any trivial factor remains unfixed
        // in the DNF of the remaining factors and is repeated for each term in the DNF which is the identical result
        // to fixing those trivial predicate. In other words, filtering the predicates here eliminates duplicates that
        // would otherwise be generated by this rule.
        //
        return CollectionMatcher.fromBindingMatcher(
                TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<Collection<QueryPredicate>>)(Class<?>)Collection.class,
                        Extractor.of(predicates -> predicates.stream()
                                .filter(predicate -> !predicate.isAtomic() && !(predicate instanceof LeafQueryPredicate))
                                .collect(ImmutableList.toImmutableList()), name -> "nonTrivialPredicates(" + name + ")"),
                        downstream));
    }

    @SuppressWarnings("SameParameterValue")
    private static CollectionMatcher<QueryPredicate> limitedPredicateCombinations(@Nonnull final CollectionMatcher<? extends QueryPredicate> downstream) {
        //
        // We create a regular combinations() matcher that is limited on the number of predicates in a way that
        // it will only create a combination of size 0, that is we will only transform an existing OR into a UNION
        // without any fixed predicates.
        //
        return CollectionMatcher.combinations(downstream,
                (plannerConfiguration, queryPredicates) -> 0,
                (plannerConfiguration, queryPredicates) ->
                        queryPredicates.size() > plannerConfiguration.getOrToUnionMaxNumConjuncts()
                        ? Math.min(queryPredicates.size(), 1)
                        : queryPredicates.size());
    }
}
