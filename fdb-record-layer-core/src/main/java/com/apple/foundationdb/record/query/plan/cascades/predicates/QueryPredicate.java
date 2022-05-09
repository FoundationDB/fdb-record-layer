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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Correlated;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.TreeLike;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.checkerframework.checker.nullness.qual.NonNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

/**
 * Class to model the concept of a predicate. A predicate is a construct that can be evaluated using
 * three-values logic for a set of given inputs. The caller can then use that result to take appropriate action,
 * e.g. filter a record out of a set of records, etc.
 */
@API(API.Status.EXPERIMENTAL)
public interface QueryPredicate extends Correlated<QueryPredicate>, TreeLike<QueryPredicate>, PlanHashable {

    @Nonnull
    @Override
    default QueryPredicate getThis() {
        return this;
    }

    /**
     * Determines if this predicate implies some other predicate.
     *
     * Let's say that {@code EVAL(p)} denotes the outcome of the evaluation of a predicate. A predicate {@code p1}
     * implies some other predicate {@code p2} if
     *
     * <pre>
     *     {@code
     *     (EVAL(p1, recordBindings) == true) -> (EVAL(p2, recordBindings) == true)
     *     }
     * </pre>
     *
     * for all {@code recordBindings} possibly contained in a stream of records that are potentially being flowed at
     * execution time.
     *
     * If {@code p1} implies {@code p2}, this method returns an instance of class {@link PredicateMapping} which should
     * give the caller all necessary info to change {@code p2} to {@code COMP(p2)} in a way make the opposite also true:
     *
     * <pre>
     *     {@code
     *     (EVAL(p1, recordBindings) == true) <-> (EVAL(COMP(p2), recordBindings) == true)
     *     }
     * </pre>
     *
     * Note that this method takes special care when placeholders are involved as this method is called during index
     * matching with candidates graphs. A placeholder by itself cannot be executed. In order for the place holder to
     * match it has to partake in a relationship with a query predicate that tells the placeholder the specific comparison
     * and bounds it operates over. In some sends this expresses a kind of polymorphism of the placeholder that is bound
     * to a specific predicate only in the presence of a sargable predicate
     * ({@link ValueComparisonRangePredicate.Sargable}) on the query side.
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
     *
     * Note: This method is expected to return a meaningful non-empty result if called with a candidate predicate that
     * also represents a tautology.
     *
     * @param aliasMap the current alias map
     * @param candidatePredicate another predicate (usually in a match candidate)
     * @return {@code Optional(predicateMapping)} if {@code this} implies {@code candidatePredicate} where
     *         {@code predicateMapping} is an new instance of {@link PredicateMapping} that captures potential bindings
     *         and compensation for {@code candidatePredicate}
     *         such that {@code candidatePredicate} to also imply {@code this}, {@code Optional.empty()} otherwise
     */
    @Nonnull
    @SuppressWarnings("unused")
    default Optional<PredicateMapping> impliesCandidatePredicate(@NonNull AliasMap aliasMap,
                                                                 @Nonnull final QueryPredicate candidatePredicate) {
        if (candidatePredicate.isTautology()) {
            return Optional.of(new PredicateMapping(this, candidatePredicate, ((matchInfo, boundParameterPrefixMap) -> Optional.of(toResidualPredicate()))));
        }
        
        return Optional.empty();
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
     * be found at all, this method will then call {@link #impliesCandidatePredicate(AliasMap, QueryPredicate)} using
     * a tautology predicate as candidate which should by contract should return a {@link PredicateMapping}.
     * @param aliasMap the current alias map
     * @param candidatePredicates an {@link Iterable} of candiate predicates
     * @return a non-empty set of {@link PredicateMapping}s
     */
    default Set<PredicateMapping> findImpliedMappings(@NonNull AliasMap aliasMap,
                                                      @Nonnull Iterable<? extends QueryPredicate> candidatePredicates) {
        final ImmutableSet.Builder<PredicateMapping> mappingBuilder = ImmutableSet.builder();

        for (final QueryPredicate candidatePredicate : candidatePredicates) {
            final Optional<PredicateMapping> impliedByQueryPredicateOptional =
                    impliesCandidatePredicate(aliasMap, candidatePredicate);
            impliedByQueryPredicateOptional.ifPresent(mappingBuilder::add);
        }

        final ImmutableSet<PredicateMapping> result = mappingBuilder.build();
        if (result.isEmpty()) {
            final ConstantPredicate tautologyPredicate = new ConstantPredicate(true);
            return impliesCandidatePredicate(aliasMap, tautologyPredicate)
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

    @Nullable
    <M extends Message> Boolean eval(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context);

    @Nonnull
    @Override
    default Set<CorrelationIdentifier> getCorrelatedTo() {
        return fold(QueryPredicate::getCorrelatedToWithoutChildren,
                (correlatedToWithoutChildren, childrenCorrelatedTo) -> {
                    ImmutableSet.Builder<CorrelationIdentifier> correlatedToBuilder = ImmutableSet.builder();
                    correlatedToBuilder.addAll(correlatedToWithoutChildren);
                    childrenCorrelatedTo.forEach(correlatedToBuilder::addAll);
                    return correlatedToBuilder.build();
                });
    }

    @Nonnull
    default Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

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

        if (!(other instanceof QueryPredicate)) {
            return false;
        }

        final QueryPredicate otherAndOrPred = (QueryPredicate)other;
        if (!equalsWithoutChildren(otherAndOrPred, aliasMap)) {
            return false;
        }

        final Iterator<? extends QueryPredicate> preds = getChildren().iterator();
        final Iterator<? extends QueryPredicate> otherPreds = otherAndOrPred.getChildren().iterator();

        while (preds.hasNext()) {
            if (!otherPreds.hasNext()) {
                return false;
            }

            if (!preds.next().semanticEquals(otherPreds.next(), aliasMap)) {
                return false;
            }
        }

        return !otherPreds.hasNext();
    }

    @SuppressWarnings({"squid:S1172", "unused", "PMD.CompareObjectsWithEquals"})
    default boolean equalsWithoutChildren(@Nonnull final QueryPredicate other,
                                          @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }

        return other.getClass() == getClass();
    }

    @Nonnull
    @Override
    default QueryPredicate rebase(@Nonnull final AliasMap aliasMap) {
        final var translationMap = TranslationMap.rebaseWithAliasMap(aliasMap);
        return translateCorrelations(translationMap);
    }

    @Nonnull
    default QueryPredicate translateCorrelations(@Nonnull final TranslationMap translationMap) {
        return replaceLeavesMaybe(predicate -> predicate.translateLeafPredicate(translationMap)).orElseThrow(() -> new RecordCoreException("unable to map tree"));
    }

    @Nullable
    @SuppressWarnings("unused")
    default QueryPredicate translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        throw new RecordCoreException("implementor must override");
    }

    @Nonnull
    default Optional<QueryPredicate> translateValues(@Nonnull final UnaryOperator<Value> translationOperator) {
        return replaceLeavesMaybe(t -> {
            if (!(t instanceof PredicateWithValue)) {
                return this;
            }
            final PredicateWithValue predicateWithValue = (PredicateWithValue)t;
            return predicateWithValue.getValue()
                            .replaceLeavesMaybe(translationOperator)
                    .map(predicateWithValue::withValue)
                    .orElse(null);
        });
    }
}
