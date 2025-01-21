/*
 * QueryPredicate.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.Narrowable;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateCompensation;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.apple.foundationdb.record.query.plan.cascades.UsesValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

/**
 * Class to model the concept of a predicate. A predicate is a construct that can be evaluated using
 * three-values logic for a set of given inputs. The caller can then use that result to take appropriate action,
 * e.g. filter a record out of a set of records, etc.
 */
@API(API.Status.EXPERIMENTAL)
public interface QueryPredicate extends Correlated<QueryPredicate>, TreeLike<QueryPredicate>, UsesValueEquivalence<QueryPredicate>, PlanHashable, Narrowable<QueryPredicate>, PlanSerializable {
    @Nonnull
    @Override
    default QueryPredicate getThis() {
        return this;
    }

    /**
     * Determines if this predicate implies some other predicate.
     * <p>
     * Let's say that {@code EVAL(p)} denotes the outcome of the evaluation of a predicate. A predicate {@code p1}
     * implies some other predicate {@code p2} if
     *
     * <pre>
     *     {@code
     *     (EVAL(p1, recordBindings) == true) -> (EVAL(p2, recordBindings) == true)
     *     }
     * </pre>
     * <p>
     * for all {@code recordBindings} possibly contained in a stream of records that are potentially being flowed at
     * execution time.
     * <p>
     * If {@code p1} implies {@code p2}, this method returns an instance of class {@link PredicateMapping} which should
     * give the caller all necessary info to change {@code p2} to {@code COMP(p2)} in a way make the opposite also
     * true:
     *
     * <pre>
     *     {@code
     *     (EVAL(p1, recordBindings) == true) <-> (EVAL(COMP(p2), recordBindings) == true)
     *     }
     * </pre>
     * <p>
     * Note that this method takes special care when placeholders are involved as this method is called during index
     * matching with candidates graphs. A placeholder by itself cannot be executed. In order for the place holder to
     * match it has to partake in a relationship with a query predicate that tells the placeholder the specific
     * comparison
     * and bounds it operates over. In some sends this expresses a kind of polymorphism of the placeholder that is
     * bound
     * to a specific predicate only in the presence of a sargable predicate
     * ({@link PredicateWithValueAndRanges}) on the query side.
     *
     * <h2>Examples:</h2>
     *
     * <h2>Example 1</h2>
     * <pre>
     *     {@code
     *     p1: x = 5
     *     p2: true (tautology predicate)
     *
     *     result: optional of PredicateMapping(COMP(true) => x = 5)
     *     }
     *     {@code p1} implies {@code p2} but, i.e., {@code x = 5} implies {@code true} but in order for {@code true} to
     *     imply {@code x = 5}, the compensation has to be applied such that {@code COMP(p2)} becomes {@code true ^ x = 5}.
     * </pre>
     *
     * <h2>Example 2</h2>
     * <pre>
     *     {@code
     *     p1: x = 5
     *     p2: x COMPARISONRANGE (placeholder)
     *
     *     result: optional of PredicateMapping(COMP(x COMPARISONRANGE) => x = 5, binding b to indicate
     *     COMPARISONRANGE should be [5, 5])
     *     }
     *     {@code p1} implies {@code p2} but, i.e., {@code x = 5} implies {@code x COMPARISONRANGE} but only if
     *     {@code COMPARISONRANGE} is bound to {@code [5, 5]} but in order for {@code x COMPARISONRANGE} to
     *     imply {@code x = 5}, the compensation has to be applied such that {@code COMP(p2)} becomes {@code x = 5}.
     * </pre>
     *
     * <h2>Example 3</h2>
     * <pre>
     *     {@code
     *     p1: x = 5
     *     p2: y COMPARISONRANGE (placeholder)
     *
     *     result: Optional.empty()
     *     }
     *     {@code p1} does not imply {@code p2}, i.e., {@code x = 5} does not imply {@code y COMPARISONRANGE}.
     * </pre>
     * <p>
     * Note: This method is expected to return a meaningful non-empty result if called with a candidate predicate that
     * also represents a tautology.
     *
     * @param valueEquivalence the current value equivalence
     * @param originalQueryPredicate original predicate on the query side
     * @param candidatePredicate another predicate (usually in a match candidate)
     * @param evaluationContext the evaluation context used to evaluate any compile-time constants when examining predicate
     * implication.
     *
     * @return {@code Optional(predicateMapping)} if {@code this} implies {@code candidatePredicate} where
     * {@code predicateMapping} is a new instance of {@link PredicateMapping} that captures potential bindings
     * and compensation for {@code candidatePredicate}
     * such that {@code candidatePredicate} to also imply {@code this}, {@code Optional.empty()} otherwise
     */
    @Nonnull
    @SuppressWarnings("unused")
    default Optional<PredicateMapping> impliesCandidatePredicateMaybe(@NonNull final ValueEquivalence valueEquivalence,
                                                                      @Nonnull final QueryPredicate originalQueryPredicate,
                                                                      @Nonnull final QueryPredicate candidatePredicate,
                                                                      @Nonnull final EvaluationContext evaluationContext) {
        if (candidatePredicate instanceof Placeholder) {
            return Optional.empty();
        }

        if (candidatePredicate.isTautology()) {
            return Optional.of(
                    PredicateMapping.regularMappingBuilder(originalQueryPredicate, this, candidatePredicate)
                            .setPredicateCompensation(getDefaultPredicateCompensation(originalQueryPredicate))
                            .build());
        }

        final var semanticEquals = this.semanticEquals(candidatePredicate, valueEquivalence);
        return semanticEquals
                .mapToOptional(queryPlanConstraint ->
                        PredicateMapping.regularMappingBuilder(originalQueryPredicate, this,
                                        candidatePredicate)
                                .setPredicateCompensation(getDefaultPredicateCompensation(originalQueryPredicate))
                                .setConstraint(queryPlanConstraint)
                                .build());
    }

    /**
     * Return a {@link PredicateCompensation} that reapplies this predicate.
     * @param originalQueryPredicate the original query predicate that was used to create this predicate
     * @return a new {@link PredicateCompensation} that reapplies this predicate.
     */
    @Nonnull
    default PredicateCompensation getDefaultPredicateCompensation(@Nonnull final QueryPredicate originalQueryPredicate) {
        return (partialMatch, boundParameterPrefixMap, pullUp) ->
                computeCompensationFunction(partialMatch, originalQueryPredicate, boundParameterPrefixMap, pullUp);
    }

    /**
     * Return a {@link PredicateCompensationFunction} that reapplies this predicate.
     * @param partialMatch the partial match that was established when this predicate was matched
     * @param originalQueryPredicate the original query predicate that was used to when this predicate was translated
     * @param boundParameterPrefixMap the bound parameter prefix map
     * @param pullUp the pull-up structure used during compensation
     * @return a new {@link PredicateCompensation} that reapplies this predicate.
     */
    @Nonnull
    default PredicateCompensationFunction computeCompensationFunction(@Nonnull final PartialMatch partialMatch,
                                                                      @Nonnull final QueryPredicate originalQueryPredicate,
                                                                      @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                      @Nonnull final PullUp pullUp) {
        Debugger.sanityCheck(() ->
                Verify.verify(Iterables.size(getChildren()) ==
                        Iterables.size(originalQueryPredicate.getChildren())));

        final var originalChildrenIterator =
                originalQueryPredicate.getChildren().iterator();
        final var childPredicateCompensations =
                ImmutableList.<PredicateCompensationFunction>builder();
        for (final var childPredicate : getChildren()) {
            final var originalChildPredicate = originalChildrenIterator.next();
            childPredicateCompensations.add(
                    childPredicate.computeCompensationFunction(partialMatch, originalChildPredicate,
                            boundParameterPrefixMap, pullUp));
        }
        return computeCompensationFunction(partialMatch, originalQueryPredicate, boundParameterPrefixMap,
                childPredicateCompensations.build(), pullUp);
    }

    @Nonnull
    default PredicateCompensationFunction computeCompensationFunction(@Nonnull final PartialMatch partialMatch,
                                                                      @Nonnull final QueryPredicate originalQueryPredicate,
                                                                      @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                                                      @Nonnull final List<PredicateCompensationFunction> childrenResults,
                                                                      @Nonnull final PullUp pullUp) {
        Verify.verify(this instanceof LeafQueryPredicate);
        Verify.verify(childrenResults.isEmpty());

        return toResidualPredicate()
                .replaceValuesMaybe(pullUp::pullUpMaybe)
                .map(queryPredicate ->
                        PredicateCompensationFunction.of(baseAlias ->
                                LinkedIdentitySet.of(queryPredicate.translateCorrelations(
                                        TranslationMap.ofAliases(pullUp.getTopAlias(), baseAlias), false))))
                .orElse(PredicateCompensationFunction.impossibleCompensation());
    }

    /**
     * Create a {@link QueryPredicate} that is equivalent to {@code this} but which is evaluated as a residual
     * predicate (cannot function as an index search argument).
     * @return a {@link QueryPredicate} (which may be {@code this}) that can be evaluated as a residual predicate.
     */
    @Nonnull
    default QueryPredicate toResidualPredicate() {
        if (Iterables.isEmpty(getChildren())) {
            return this;
        }
        return withChildren(
                StreamSupport.stream(getChildren().spliterator(), false)
                        .map(QueryPredicate::toResidualPredicate)
                        .collect(ImmutableList.toImmutableList()));
    }

    /**
     * Method to find all mappings of this predicate in an {@link Iterable} of candidate predicates. If no mapping can
     * be found at all, this method will then call
     * {@link #impliesCandidatePredicateMaybe(ValueEquivalence, QueryPredicate, QueryPredicate, EvaluationContext)} using
     * a tautology predicate as candidate which should by contract should return a {@link PredicateMapping}.
     * @param valueEquivalence the current alias map together with some other known equalities
     * @param candidatePredicates an {@link Iterable} of candiate predicates
     * @param evaluationContext the evaluation context used to examine predicate implication.
     * @return a non-empty collection of {@link PredicateMapping}s
     */
    default Collection<PredicateMapping> findImpliedMappings(@NonNull final ValueEquivalence valueEquivalence,
                                                             @Nonnull final QueryPredicate originalQueryPredicate,
                                                             @Nonnull final Iterable<? extends QueryPredicate> candidatePredicates,
                                                             @Nonnull final EvaluationContext evaluationContext) {
        final Set<PredicateMapping.MappingKey> mappingKeys = Sets.newHashSet();
        final ImmutableList.Builder<PredicateMapping> mappingBuilder = ImmutableList.builder();

        for (final QueryPredicate candidatePredicate : candidatePredicates) {
            final Optional<PredicateMapping> predicateMappingOptional =
                    impliesCandidatePredicateMaybe(valueEquivalence, originalQueryPredicate, candidatePredicate,
                            evaluationContext);
            predicateMappingOptional.ifPresent(predicateMapping -> {
                final var mappingKey = predicateMapping.getMappingKey();
                if (!mappingKeys.contains(mappingKey)) {
                    mappingKeys.add(mappingKey);
                    mappingBuilder.add(predicateMapping);
                }
            });
        }

        final ImmutableList<PredicateMapping> result = mappingBuilder.build();
        if (mappingKeys.isEmpty()) {
            //
            // Note that for uniqueness constraints during matching it is important to create a new instance of the
            // constant predicate here.
            //
            final ConstantPredicate tautologyPredicate = new ConstantPredicate(true);
            return impliesCandidatePredicateMaybe(valueEquivalence, originalQueryPredicate, tautologyPredicate,
                    evaluationContext)
                    .map(ImmutableSet::of)
                    .orElseThrow(() -> new RecordCoreException("should have found at least one mapping"));
        }
        return result;
    }
    
    /**
     * Method that indicates whether this predicate is filtering at all.
     * @return {@code true} if this predicate always evaluates to true, {@code false} otherwise
     */
    default boolean isTautology() {
        return false;
    }

    default boolean isContradiction() {
        return false;
    }

    @Nullable
    @SpotBugsSuppressWarnings(value = {"NP_NONNULL_PARAM_VIOLATION"}, justification = "compile-time evaluations take their value from the context only")
    default Boolean compileTimeEval(@Nonnull final EvaluationContext context) {
        return eval(null, context);
    }

    @Nullable
    default <M extends Message> Boolean evalWithoutStore(@Nonnull final EvaluationContext context) {
        return eval(null, context);
    }

    @Nullable
    <M extends Message> Boolean eval(@Nullable FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context);

    @Nonnull
    Set<CorrelationIdentifier> getCorrelatedToWithoutChildren();

    @Override
    int semanticHashCode();

    int hashCodeWithoutChildren();

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    default boolean semanticEquals(@Nullable final Object other,
                                   @Nonnull final AliasMap aliasMap) {
        if (other == null) {
            return false;
        }

        if (this == other) {
            return true;
        }

        if (this.getClass() != other.getClass()) {
            return false;
        }

        return semanticEquals(other, ValueEquivalence.fromAliasMap(aliasMap)).isTrue();
    }

    @Nonnull
    @Override
    default BooleanWithConstraint semanticEqualsTyped(@Nonnull final QueryPredicate other,
                                                      @Nonnull final ValueEquivalence valueEquivalence) {
        final var equalsWithoutChildren =
                equalsWithoutChildren(other, valueEquivalence);
        if (equalsWithoutChildren.isFalse()) {
            return BooleanWithConstraint.falseValue();
        }

        return equalsWithoutChildren.composeWithOther(equalsForChildren(other, valueEquivalence));
    }

    @Nonnull
    default BooleanWithConstraint equalsForChildren(@Nonnull final QueryPredicate otherPred,
                                                    @Nonnull final ValueEquivalence valueEquivalence) {
        final Iterator<? extends QueryPredicate> preds = getChildren().iterator();
        final Iterator<? extends QueryPredicate> otherPreds = otherPred.getChildren().iterator();

        var constraint = BooleanWithConstraint.alwaysTrue();
        while (preds.hasNext()) {
            if (!otherPreds.hasNext()) {
                return BooleanWithConstraint.falseValue();
            }

            final var semanticEqualsOptional =
                    preds.next().semanticEquals(otherPreds.next(), valueEquivalence);
            if (semanticEqualsOptional.isFalse()) {
                return BooleanWithConstraint.falseValue();
            }
            constraint = constraint.composeWithOther(semanticEqualsOptional);
        }

        if (otherPreds.hasNext()) {
            return BooleanWithConstraint.falseValue();
        }
        return constraint;
    }

    @SuppressWarnings({"squid:S1172", "unused", "PMD.CompareObjectsWithEquals"})
    @Nonnull
    default BooleanWithConstraint equalsWithoutChildren(@Nonnull final QueryPredicate other,
                                                        @Nonnull final ValueEquivalence valueEquivalence) {
        if (this == other) {
            return BooleanWithConstraint.alwaysTrue();
        }

        if (other.getClass() != getClass()) {
            return BooleanWithConstraint.falseValue();
        }

        return other.isAtomic() == isAtomic() ? BooleanWithConstraint.alwaysTrue() : BooleanWithConstraint.falseValue();
    }

    boolean isAtomic();

    @Nonnull
    QueryPredicate withAtomicity(boolean isAtomic);

    /**
     * Returns a human-friendly textual representation of this {@link QueryPredicate}.
     *
     * @return a human-friendly textual representation of this {@link QueryPredicate}.
     */
    @Nonnull
    default ExplainTokensWithPrecedence explain() {
        final var explainFunctions =
                Streams.stream(getChildren())
                        .map(child -> (Supplier<ExplainTokensWithPrecedence>)child::explain)
                        .collect(ImmutableList.toImmutableList());
        return explain(explainFunctions);
    }

    @Nonnull
    ExplainTokensWithPrecedence explain(@Nonnull Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers);

    @Nonnull
    @Override
    default QueryPredicate rebase(@Nonnull final AliasMap aliasMap) {
        final var translationMap = TranslationMap.rebaseWithAliasMap(aliasMap);
        return translateCorrelations(translationMap, false);
    }

    @Nonnull
    default QueryPredicate translateCorrelations(@Nonnull final TranslationMap translationMap, final boolean shouldSimplify) {
        return replaceLeavesMaybe(predicate -> predicate.translateLeafPredicate(translationMap, shouldSimplify))
                .orElseThrow(() -> new RecordCoreException("unable to map tree"));
    }

    @Nullable
    @SuppressWarnings("unused")
    default QueryPredicate translateLeafPredicate(@Nonnull final TranslationMap translationMap, final boolean shouldSimplify) {
        throw new RecordCoreException("implementor must override");
    }

    @Nonnull
    default Optional<PredicateWithValueAndRanges> toValueWithRangesMaybe(@Nonnull final EvaluationContext evaluationContext) {
        return Optional.empty();
    }

    @Nonnull
    default Optional<QueryPredicate> replaceValuesMaybe(@Nonnull final Function<Value, Optional<Value>> replacementFunction) {
        return replaceLeavesMaybe(leafPredicate -> {
            if (leafPredicate instanceof PredicateWithValue) {
                final var predicateWithValue = (PredicateWithValue)leafPredicate;

                return predicateWithValue.translateValueAndComparisonsMaybe(replacementFunction,
                        comparison -> comparison.replaceValuesMaybe(replacementFunction)).orElse(null);
            }
            return leafPredicate;
        });
    }

    @Nonnull
    PQueryPredicate toQueryPredicateProto(@Nonnull PlanSerializationContext serializationContext);

    @Nonnull
    static QueryPredicate fromQueryPredicateProto(@Nonnull final PlanSerializationContext serializationContext,
                                                  @Nonnull final PQueryPredicate queryPredicateProto) {
        return (QueryPredicate)PlanSerialization.dispatchFromProtoContainer(serializationContext, queryPredicateProto);
    }

    @Nonnull
    static List<QueryPredicate> translatePredicates(@Nonnull final TranslationMap translationMap,
                                                    @Nonnull final List<QueryPredicate> predicates,
                                                    final boolean shouldSimplifyValues) {
        final var resultPredicatesBuilder = ImmutableList.<QueryPredicate>builder();
        for (final var predicate : predicates) {
            final var newOuterInnerPredicate =
                    predicate.replaceLeavesMaybe(leafPredicate -> leafPredicate.translateLeafPredicate(translationMap, shouldSimplifyValues))
                            .orElseThrow(() -> new RecordCoreException("unable to translate predicate"));
            resultPredicatesBuilder.add(newOuterInnerPredicate);
        }
        return resultPredicatesBuilder.build();
    }
}
