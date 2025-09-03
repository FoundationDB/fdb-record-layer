/*
 * Compensation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ResultCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Interface for all kinds of compensation. A compensation is the byproduct of expression DAG matching.
 * Matching two graphs {@code Q} and {@code M} may yield two sub graphs {@code Q_s} and {@code M_s} that match.
 * {@code Q_s} and {@code M_s} may be completely equivalent to each other and {@code Q_s} can be substituted with
 * {@code M_s} freely and vice versa. The requirement that {@code Q_s} and {@code M_s} have to be semantically
 * equivalent for such a substitution, however, is not very useful. Normally, we try to find a materialized data set
 * such as an index that can be utilized by the planner directly using a scan
 * (requiring a complete match on the {@code M}-side, that is {@code M_s} equals {@code M}) instead of computing the
 * result from the raw base data set. For those purposes, it makes more sense to relax the matching requirement to just
 * require that the materialized view side of matching {@code Q} can subsume the query side {@code M} which means that
 * executing {@code M} at least contains the result that executing {@code Q} would yield. That execution, however,
 * may produce also extraneous records. Compensation corrects for that problem by applying certain post-operations
 * such as filtering, distinct-ing or resorting.
 *
 * <p>
 * Example in SQL terms:
 * </p>
 *
 * <p>
 * Query:
 * <pre>
 * {@code
 *   SELECT *
 *   FROM recordType
 *   WHERE a = 3 AND b = 5
 * }
 * </pre>
 *
 * <p>
 * Index:
 * <pre>
 * {@code
 *   SELECT *
 *   FROM recordType
 *   WHERE a <comparison parameter p0>
 *   ORDER BY a
 * }
 * </pre>
 *
 * <p>
 * A match for the two graphs created for both query and index side is:
 * <pre>
 * {@code
 *   SELECT *
 *   FROM recordType
 *   WHERE a <p0 -> "= 3">
 *   ORDER BY a
 * }
 * </pre>
 * Using this graph we can now substitute the scan over the index but we will have to account for the unmatched
 * second predicate in {@code Q}:
 * <pre>
 * {@code
 *   SELECT *
 *   FROM index
 *   WHERE b = 5
 * }
 * </pre>
 * The query fragment that needs to be inserted to correct for extra records the index scan produces, that is
 * the {@code WHERE x = 5} is compensation.
 * <br>
 * Compensation is computed either during the matching process or is computed after a complete match has been found
 * utilizing helper structures such as {@link PartialMatch} and {@link MatchInfo}, which are themselves
 * built during matching. Logic in the data access rules computes and applies compensation as needed when a complete
 * index match has been found.
 * <br>
 * A query sub graph can have multiple matches that could be utilized. In the example above, another index on {@code b}
 * would also match but use {@code b} for the index scan and a predicate {@code WHERE a = 3}. Both match the query,
 * and in fact both indexes can be utilized together by intersecting the index scans. For those cases, it becomes
 * important to be able to intersect compensations as well such that only the minimally required combined
 * compensation is required. In this example, no compensation is required as the individual indexes being intersected
 * complement each other nicely.
 */
public interface Compensation {
    /**
     * Named object to indicate that this compensation is in fact no compensation, that is no additional operators
     * need to be injected to compensate for a match.
     */
    Compensation NO_COMPENSATION = new Compensation() {
        @Override
        public boolean isNeeded() {
            return false;
        }

        @Override
        public boolean isFinalNeeded() {
            return false;
        }

        @Override
        public boolean isNeededForFiltering() {
            return false;
        }

        @Nonnull
        @Override
        public Compensation intersect(@Nonnull final Compensation otherCompensation) {
            return this;
        }

        @Nonnull
        @Override
        public RelationalExpression apply(@Nonnull final Memoizer memoizer,
                                          @Nonnull final RelationalExpression relationalExpression,
                                          @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
            throw new RecordCoreException("this method should not be called");
        }

        @Nonnull
        @Override
        public RelationalExpression applyFinal(@Nonnull final Memoizer memoizer,
                                               @Nonnull final RelationalExpression relationalExpression,
                                               @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
            throw new RecordCoreException("this method should not be called");
        }

        @Override
        public String toString() {
            return "no-compensation";
        }
    };

    /**
     * Identity element for the intersection monoid defined by {@link #intersect(Compensation)}.
     * Example for the usage pattern for that monoid:
     * <br>
     * Let {@code compensations} be a {@link Collection} of {@link Compensation}.
     * You can use {@link java.util.stream.Stream#reduce} to create an intersection of
     * all compensations in that collection.
     * <pre>
     * {code
     * final Compensations intersectedCompensations =
     *   compensations
     *     .stream()
     *     .reduce(Compensation.impossibleCompensation(), Compensation::intersect);
     * }
     * </pre>
     * Note that if {@code compensations} is empty, the result of the intersection is the impossible compensation.
     */
    Compensation IMPOSSIBLE_COMPENSATION = new Compensation() {
        @Override
        public boolean isImpossible() {
            return true;
        }

        @Nonnull
        @Override
        public Compensation intersect(@Nonnull final Compensation otherCompensation) {
            return otherCompensation;
        }

        @Nonnull
        @Override
        public RelationalExpression apply(@Nonnull final Memoizer memoizer,
                                          @Nonnull final RelationalExpression relationalExpression,
                                          @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
            throw new RecordCoreException("this method should not be called");
        }

        @Nonnull
        @Override
        public RelationalExpression applyFinal(@Nonnull final Memoizer memoizer,
                                               @Nonnull final RelationalExpression relationalExpression,
                                               @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
            throw new RecordCoreException("this method should not be called");
        }
    };

    @Nonnull
    default RelationalExpression applyAllNeededCompensations(@Nonnull final Memoizer memoizer,
                                                             @Nonnull RelationalExpression relationalExpression,
                                                             @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
        if (isNeededForFiltering()) {
            relationalExpression = apply(memoizer, relationalExpression, matchedToRealizedTranslationMapFunction);
        }
        if (isFinalNeeded()) {
            relationalExpression = applyFinal(memoizer, relationalExpression, matchedToRealizedTranslationMapFunction);
        }

        return relationalExpression;
    }

    /**
     * When applied to a reference this method returns a {@link RelationalExpression} consuming the
     * reference passed in that applies additional predicates as expressed by e.g. the predicate compensation map in
     * {@link WithSelectCompensation}.
     * @param memoizer the memoizer for new {@link Reference}s
     * @param relationalExpression root of graph to apply compensation to
     * @param matchedToRealizedTranslationMapFunction a function that given an alias for the quantifier over the
     *        realized compensation, returns a {@link TranslationMap} that is then used to translate
     *        from {@link QueryPredicate}s and {@link Value}s using matched aliases to the realized alias
     * @return a new relational expression that corrects the result of {@code reference} by applying appropriate
     *         filters and/or transformations
     */
    @Nonnull
    RelationalExpression apply(@Nonnull Memoizer memoizer, @Nonnull RelationalExpression relationalExpression,
                               @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction);

    /**
     * When applied to a reference this method returns a {@link RelationalExpression} consuming the
     * reference passed in that applies a final shape correction as needed by e.g. the result compensation function in
     * {@link WithSelectCompensation}.
     * @param memoizer the memoizer for new {@link Reference}s
     * @param relationalExpression root of graph to apply compensation to
     * @param matchedToRealizedTranslationMapFunction a function that given an alias for the quantifier over the
     *        realized compensation, returns a {@link TranslationMap} that is then used to translate
     *        from {@link QueryPredicate}s and {@link Value}s using matched aliases to the realized alias
     * @return a new relational expression that corrects the result of {@code reference} by applying a final shape
     *         correction of the resulting records.
     */
    @Nonnull
    RelationalExpression applyFinal(@Nonnull Memoizer memoizer, @Nonnull RelationalExpression relationalExpression,
                                    @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction);

    /**
     * Returns if this compensation object needs to be applied in order to correct the result of a match.
     * @return {@code true} if this compensation must be applied, {@code false} if this compensation is not needed.
     *         Note, by contract it is illegal to call {@link #apply} on this compensation if this method returns
     *         {@code false}.
     */
    default boolean isNeeded() {
        return true;
    }

    /**
     * Returns if this compensation object needs to be applied in order to filter to the correct records. This is
     * important when a situation calls for the correct filtering but does not care about the actual result.
     * For instance, the {@link com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate}
     * needs to reapply if its child compensation is needed for filtering but that condition only cares for the
     * existence but not the actual value.
     * Note that it holds that if this method implied {@link #isNeeded()}.
     * @return {@code true} if this compensation must be applied, {@code false} if this compensation is not needed.
     *         Note, by contract it is illegal to call {@link #apply} on this compensation if this method returns
     *         {@code false}.
     */
    default boolean isNeededForFiltering() {
        return true;
    }

    /**
     * Method that indicates if this compensation needs to be applied if it is the top compensation in the chain of
     * compensations. Just like the regular usage of a compensation might not be needed (as for instance all predicates
     * have already been used as sargables), it might also be unnecessary to apply the final compensation part.
     * @return {@code true} if the caller must call {@code applyFinal()} if this compensation is the top compensation
     *         of a compensation chain.
     */
    default boolean isFinalNeeded() {
        return true;
    }

    /**
     * Returns if this compensation can be applied in a way to yield the correct result.
     *
     * @return {@code true} is compensation is not possible, {@code false} otherwise
     */
    default boolean isImpossible() {
        return false;
    }

    /**
     * Returns if this compensation can further be combined with subsequent compensations further up the graph
     * or whether this compensation would actually have to be applied exactly at the position in the graph that
     * created it. This property is used for compensations which only can be applied on the top level expression
     * that is matched.
     * @return {@code true} is compensation can be deferred,, {@code false} otherwise.
     */
    default boolean canBeDeferred() {
        return true;
    }

    /**
     * Union this compensation with another one passed in.
     * @param otherCompensation other compensation to union this compensation with
     * @return the new compensation representing the union of both compensations
     */
    @Nonnull
    default Compensation union(@Nonnull Compensation otherCompensation) {
        if (!isNeeded() && !otherCompensation.isNeeded()) {
            return noCompensation();
        }

        if (!isNeeded()) {
            return otherCompensation;
        }

        if (!otherCompensation.isNeeded()) {
            return this;
        }

        return new Compensation() {
            @Nonnull
            @Override
            public RelationalExpression apply(@Nonnull final Memoizer memoizer,
                                              @Nonnull final RelationalExpression relationalExpression,
                                              @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
                return Compensation.this.apply(memoizer, otherCompensation.apply(memoizer, relationalExpression,
                        matchedToRealizedTranslationMapFunction), matchedToRealizedTranslationMapFunction);
            }

            @Nonnull
            @Override
            public RelationalExpression applyFinal(@Nonnull final Memoizer memoizer,
                                                   @Nonnull final RelationalExpression relationalExpression,
                                                   @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
                return Compensation.this.applyFinal(memoizer, otherCompensation.applyFinal(memoizer,
                        relationalExpression, matchedToRealizedTranslationMapFunction),
                        matchedToRealizedTranslationMapFunction);
            }
        };
    }

    /**
     * Intersect this compensation with another one passed in.
     * @param otherCompensation other compensation to intersect this compensation with
     * @return the new compensation representing the intersection of both compensations
     */
    @Nonnull
    default Compensation intersect(@Nonnull Compensation otherCompensation) {
        if (!isNeeded() || !otherCompensation.isNeeded()) {
            return noCompensation();
        }

        return new Compensation() {
            @Nonnull
            @Override
            public RelationalExpression apply(@Nonnull final Memoizer memoizer,
                                              @Nonnull final RelationalExpression relationalExpression,
                                              @Nonnull final Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
                return Compensation.this.apply(memoizer,
                        otherCompensation.apply(memoizer, relationalExpression,
                                matchedToRealizedTranslationMapFunction), matchedToRealizedTranslationMapFunction);
            }

            @Nonnull
            @Override
            public RelationalExpression applyFinal(@Nonnull final Memoizer memoizer,
                                                   @Nonnull final RelationalExpression relationalExpression,
                                                   @Nonnull final Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
                return Compensation.this.applyFinal(memoizer,
                        otherCompensation.applyFinal(memoizer, relationalExpression,
                                matchedToRealizedTranslationMapFunction), matchedToRealizedTranslationMapFunction);
            }
        };
    }

    /**
     * Returns a compensation which represents no compensation at all, i.e. it returns an object where
     * {@link #isNeeded()} returns {@code false}. That object cannot be applied.
     * @return a compensation object that represents the absence of the need for compensation
     */
    @Nonnull
    static Compensation noCompensation() {
        return NO_COMPENSATION;
    }

    /**
     * Returns a compensation which represents the <em>impossible</em> compensation, i.e. it returns an object where
     * {@link #isNeeded()} returns {@code true} but that cannot be applied. This object is only needed to define
     * the identity of the intersection monoid on compensations. One can imagine the impossible compensation to stand
     * for the fact that compensation is needed (that is the match subsumes the query) but that the compensation itself
     * cannot be computed.
     * @return a compensation object that represents an impossible compensation
     */
    @Nonnull
    static Compensation impossibleCompensation() {
        return IMPOSSIBLE_COMPENSATION;
    }

    @Nonnull
    default ForMatch derived(final boolean isImpossible,
                             @Nonnull final LinkedIdentityMap<QueryPredicate, PredicateCompensationFunction> predicateCompensationMap,
                             @Nonnull final Collection<? extends Quantifier> matchedQuantifiers,
                             @Nonnull final Set<? extends Quantifier> unmatchedQuantifiers,
                             @Nonnull final Set<CorrelationIdentifier> compensatedAliases,
                             @Nonnull final ResultCompensationFunction resultCompensationFunction,
                             @Nonnull final GroupByMappings groupByMappings) {
        //
        // At least one of these conditions must be true:
        // - it is an impossible compensation (in which case the predicate compensation map may be empty)
        // - we need to change the shape of the record
        // - there are predicates that need to be compensated
        // - there may be unmatched quantifiers that we need to deal with
        // - any of this compensation's children need to compensate for something -- tested in isNeededForFiltering()
        //
        Verify.verify(isImpossible || !unmatchedQuantifiers.isEmpty() ||
                !predicateCompensationMap.isEmpty() || resultCompensationFunction.isNeeded() || isNeededForFiltering());

        return new ForMatch(isImpossible, this, predicateCompensationMap, matchedQuantifiers,
                unmatchedQuantifiers, compensatedAliases, resultCompensationFunction, groupByMappings);
    }

    /**
     * Compute the necessary compensation for the result of the top operation's {@link PartialMatch}.
     * @param partialMatch the partial match
     * @param rootOfMatchPullUp pull up to get expressions to top level
     * @return a {@link CompensatedResult}
     */
    @Nonnull
    static Optional<CompensatedResult> computeResultCompensation(@Nonnull final PartialMatch partialMatch,
                                                                 @Nullable final PullUp rootOfMatchPullUp) {
        final var matchInfo = partialMatch.getMatchInfo();
        boolean isCompensationImpossible = false;
        final PredicateMultiMap.ResultCompensationFunction resultCompensationFunction;
        final GroupByMappings groupByMappings;

        if (rootOfMatchPullUp == null) {
            // It is possible for the pull up to be null. If that is the case, we do not need compensation.
            resultCompensationFunction = PredicateMultiMap.ResultCompensationFunction.noCompensationNeeded();
            groupByMappings = GroupByMappings.empty();
        } else {
            //
            // Pull up the result of the query as translated to the candidate side. If that can be done,
            // we then attempt to avoid straightforward identity compensations. If that's not possible, we'll
            // create a result compensation.
            //
            final var maxMatchMap = matchInfo.getMaxMatchMap();
            final var pulledUpTranslatedResultValueOptional =
                    rootOfMatchPullUp.pullUpValueMaybe(maxMatchMap.getQueryValue());
            if (pulledUpTranslatedResultValueOptional.isEmpty()) {
                return Optional.empty();
            }

            final var pulledUpTranslatedResultValue = pulledUpTranslatedResultValueOptional.get();

            if (QuantifiedObjectValue.isSimpleQuantifiedObjectValueOver(pulledUpTranslatedResultValue,
                    rootOfMatchPullUp.getCandidateAlias())) {
                resultCompensationFunction = PredicateMultiMap.ResultCompensationFunction.noCompensationNeeded();
            } else {
                resultCompensationFunction =
                        PredicateMultiMap.ResultCompensationFunction.ofValue(pulledUpTranslatedResultValue);
            }
            isCompensationImpossible = resultCompensationFunction.isImpossible();

            // Fix up the group my mappings as well.
            groupByMappings =
                    MatchInfo.RegularMatchInfo.pullUpAggregateCandidateMappings(partialMatch, rootOfMatchPullUp);
        }
        return Optional.of(new CompensatedResult(isCompensationImpossible, resultCompensationFunction,
                groupByMappings));
    }

    class CompensatedResult {
        private final boolean isCompensationImpossible;
        @Nonnull
        private final PredicateMultiMap.ResultCompensationFunction resultCompensationFunction;
        @Nonnull
        private final GroupByMappings groupByMappings;

        public CompensatedResult(final boolean isCompensationImpossible,
                                 @Nonnull final PredicateMultiMap.ResultCompensationFunction resultCompensationFunction,
                                 @Nonnull final GroupByMappings groupByMappings) {
            this.isCompensationImpossible = isCompensationImpossible;
            this.resultCompensationFunction = resultCompensationFunction;
            this.groupByMappings = groupByMappings;
        }

        public boolean isCompensationImpossible() {
            return isCompensationImpossible;
        }

        @Nonnull
        public PredicateMultiMap.ResultCompensationFunction getResultCompensationFunction() {
            return resultCompensationFunction;
        }

        @Nonnull
        public GroupByMappings getGroupByMappings() {
            return groupByMappings;
        }
    }

    /**
     * Interface for {@link Compensation}s that map original {@link QueryPredicate}s to compensating
     * {@link QueryPredicate}s.
     */
    interface WithSelectCompensation extends Compensation {
        @Override
        default boolean isNeeded() {
            return getChildCompensation().isNeeded() ||
                   !getUnmatchedForEachQuantifiers().isEmpty() ||
                   !getPredicateCompensationMap().isEmpty() ||
                   getResultCompensationFunction().isNeeded();
        }

        @Override
        default boolean isNeededForFiltering() {
            return getChildCompensation().isNeededForFiltering() ||
                   !getUnmatchedForEachQuantifiers().isEmpty() ||
                   !getPredicateCompensationMap().isEmpty();
        }

        @Override
        default boolean isFinalNeeded() {
            return getResultCompensationFunction().isNeeded();
        }

        @Nonnull
        Compensation getChildCompensation();

        @Nonnull
        Set<Quantifier> getMatchedQuantifiers();


        @Nonnull
        Set<Quantifier> getUnmatchedQuantifiers();

        @Nonnull
        Set<CorrelationIdentifier> getCompensatedAliases();

        @Nonnull
        Set<Quantifier> getUnmatchedForEachQuantifiers();

        @Nonnull
        Map<QueryPredicate, PredicateCompensationFunction> getPredicateCompensationMap();

        @Nonnull
        ResultCompensationFunction getResultCompensationFunction();

        @Nonnull
        GroupByMappings getGroupByMappings();

        /**
         * Specific implementation of union-ing two compensations both of type {@link WithSelectCompensation}.
         * This implementation delegates to its super method if {@code otherCompensation} is not of type
         * {@link WithSelectCompensation}. If it is, it creates a new compensation of type
         * {@link WithSelectCompensation} that contains the mappings of both this compensation
         * and {@code otherCompensation}.
         * @param otherCompensation other compensation to union this compensation with
         * @return a new compensation object representing the logical union between {@code this} and
         *         {@code otherCompensation}
         */
        @Nonnull
        @Override
        default Compensation union(@Nonnull Compensation otherCompensation) {
            if (!(otherCompensation instanceof WithSelectCompensation)) {
                return otherCompensation.union(this);
            }

            final WithSelectCompensation otherWithSelectCompensation = (WithSelectCompensation)otherCompensation;

            final var unionedMatchedQuantifiers = Sets.union(getMatchedQuantifiers(), otherWithSelectCompensation.getMatchedQuantifiers());
            final var numberForEachInUnion = unionedMatchedQuantifiers.stream().filter(quantifier -> quantifier instanceof Quantifier.ForEach).collect(ImmutableList.toImmutableList()).size();
            if (numberForEachInUnion > 1) {
                // TODO This should be made better in the future. For now we just return the impossible compensation.
                //      What we need instead of using mapped and unmapped quantifiers, is sufficient info to create a
                //      translation map from quantified column values to compensating values instead of having to perform
                //      translation on a quantifier level which becomes ambiguous if the source query has more than one
                //      matched quantifier underneath the query expression but the replacing scan expression only has one
                //      quantifier. What that translation map should define is a mapping from quantifier on the
                //      query side to quantifier column value on the over scan expression that is created in the
                //      data access rules.
                return impossibleCompensation();
            }

            if (!getUnmatchedForEachQuantifiers().isEmpty() || !otherWithSelectCompensation.getUnmatchedForEachQuantifiers().isEmpty()) {
                return impossibleCompensation();
            }

            final Compensation unionedChildCompensation = getChildCompensation().union(otherWithSelectCompensation.getChildCompensation());
            if (unionedChildCompensation.isImpossible() || !unionedChildCompensation.canBeDeferred()) {
                return Compensation.impossibleCompensation();
            }

            final ResultCompensationFunction newResultResultCompensationFunction;
            final var resultCompensationFunction = getResultCompensationFunction();
            final var otherResultCompensationFunction = otherWithSelectCompensation.getResultCompensationFunction();
            if (!resultCompensationFunction.isNeeded() &&
                    !otherResultCompensationFunction.isNeeded()) {
                newResultResultCompensationFunction = ResultCompensationFunction.noCompensationNeeded();
            } else {
                Verify.verify(resultCompensationFunction.isNeeded());
                Verify.verify(otherResultCompensationFunction.isNeeded());
                // pick the one from this side -- it does not matter as both candidates have the same shape
                newResultResultCompensationFunction = resultCompensationFunction;
            }

            final var otherCompensationMap =
                    otherWithSelectCompensation.getPredicateCompensationMap();
            final var combinedPredicateMap = new LinkedIdentityMap<QueryPredicate, PredicateCompensationFunction>();

            combinedPredicateMap.putAll(getPredicateCompensationMap());

            for (final var otherEntry : otherCompensationMap.entrySet()) {
                // if the other side does not have compensation for this key, we don't need compensation
                if (combinedPredicateMap.containsKey(otherEntry.getKey())) {
                    // Both compensations have a compensation for this particular predicate which is essentially
                    // reapplying the predicate.
                    //
                    // TODO Remember that both sides are effectively asking for a specific compensation predicate
                    // (to post filtering, etc. of their side after the candidate scan). So in the most generic case
                    // you have a query predicate that is matched in both compensations differently (cannot happen today)
                    // and compensated differently.
                    // Even though it cannot happen today, we can still AND the compensations together. For now,
                    // we just bail!
                    //
                    throw new RecordCoreException("predicate is mapped more than once");
                }
                combinedPredicateMap.put(otherEntry.getKey(), otherEntry.getValue());
            }

            if (!unionedChildCompensation.isNeededForFiltering() &&
                    !newResultResultCompensationFunction.isNeeded() && combinedPredicateMap.isEmpty()) {
                return Compensation.noCompensation();
            }

            if (!newResultResultCompensationFunction.isNeeded() && combinedPredicateMap.isEmpty()) {
                return unionedChildCompensation;
            }

            return unionedChildCompensation.derived(false,
                    combinedPredicateMap,
                    unionedMatchedQuantifiers,
                    ImmutableSet.of(),
                    Sets.union(getCompensatedAliases(), otherWithSelectCompensation.getCompensatedAliases()),
                    newResultResultCompensationFunction,
                    GroupByMappings.empty());
        }

        /**
         * Specific implementation of intersecting two compensations both of type {@link WithSelectCompensation}.
         * This implementation delegates to its super method if {@code otherCompensation} is not of type
         * {@link WithSelectCompensation}. If it is, it creates a new compensation of type
         * {@link WithSelectCompensation} that contains only the mappings that are contained in both this compensation
         * and {@code otherCompensation}.
         * @param otherCompensation other compensation to intersect this compensation with
         * @return a new compensation object representing the logical intersection between {@code this} and
         *         {@code otherCompensation}
         */
        @Nonnull
        @Override
        default Compensation intersect(@Nonnull Compensation otherCompensation) {
            if (!(otherCompensation instanceof WithSelectCompensation)) {
                return otherCompensation.intersect(this);
            }
            final var otherWithSelectCompensation = (WithSelectCompensation)otherCompensation;

            final Compensation childCompensation = getChildCompensation();
            Verify.verify(!(childCompensation instanceof WithSelectCompensation) ||
                    ((WithSelectCompensation)childCompensation).getUnmatchedForEachQuantifiers().isEmpty());

            final Compensation intersectedChildCompensation =
                    childCompensation.intersect(otherWithSelectCompensation.getChildCompensation());
            if (intersectedChildCompensation.isImpossible() || !intersectedChildCompensation.canBeDeferred()) {
                return Compensation.impossibleCompensation();
            }

            final var newMatchedGroupingsMapBuilder = ImmutableBiMap.<Value, Value>builder();
            final var matchedGroupingsMap = getGroupByMappings().getMatchedGroupingsMap();
            newMatchedGroupingsMapBuilder.putAll(matchedGroupingsMap);
            for (final var entry : otherWithSelectCompensation.getGroupByMappings().getMatchedGroupingsMap().entrySet()) {
                if (!matchedGroupingsMap.containsKey(entry.getKey())) {
                    newMatchedGroupingsMapBuilder.put(entry);
                }
            }

            final var newMatchedAggregatesMapBuilder = ImmutableBiMap.<Value, Value>builder();
            final var matchedAggregatesMap = getGroupByMappings().getMatchedAggregatesMap();
            newMatchedAggregatesMapBuilder.putAll(matchedAggregatesMap);
            for (final var entry : otherWithSelectCompensation.getGroupByMappings().getMatchedAggregatesMap().entrySet()) {
                if (!matchedAggregatesMap.containsKey(entry.getKey())) {
                    newMatchedAggregatesMapBuilder.put(entry);
                }
            }
            final var newMatchedAggregatesMap = newMatchedAggregatesMapBuilder.build();
            final var newUnmatchedAggregatesMapBuilder =
                    ImmutableBiMap.<CorrelationIdentifier, Value>builder();
            final var unmatchedAggregateMap = getGroupByMappings().getUnmatchedAggregatesMap();
            for (final var entry : unmatchedAggregateMap.entrySet()) {
                if (!newMatchedAggregatesMap.containsKey(entry.getValue())) {
                    newUnmatchedAggregatesMapBuilder.put(entry);
                }
            }
            for (final var entry : otherWithSelectCompensation.getGroupByMappings().getUnmatchedAggregatesMap().entrySet()) {
                if (!newMatchedAggregatesMap.containsKey(entry.getValue()) &&
                        !unmatchedAggregateMap.inverse().containsKey(entry.getValue())) {
                    newUnmatchedAggregatesMapBuilder.put(entry);
                }
            }
            final var newGroupByMappings = GroupByMappings.of(newMatchedGroupingsMapBuilder.build(),
                    newMatchedAggregatesMap, newUnmatchedAggregatesMapBuilder.build());

            boolean isImpossible = false;

            final ResultCompensationFunction newResultResultCompensationFunction;
            final var resultCompensationFunction = getResultCompensationFunction();
            final var otherResultCompensationFunction = otherWithSelectCompensation.getResultCompensationFunction();
            if (!resultCompensationFunction.isNeeded() &&
                    !otherResultCompensationFunction.isNeeded()) {
                newResultResultCompensationFunction = ResultCompensationFunction.noCompensationNeeded();
            } else {
                Verify.verify(resultCompensationFunction.isNeeded());
                Verify.verify(otherResultCompensationFunction.isNeeded());
                // pick the one from this side -- it does not matter as both candidates have the same shape
                newResultResultCompensationFunction =
                        resultCompensationFunction.amend(unmatchedAggregateMap, newMatchedAggregatesMap);
                isImpossible |= newResultResultCompensationFunction.isImpossible();
            }

            final var otherCompensationMap =
                    otherWithSelectCompensation.getPredicateCompensationMap();
            final var combinedPredicateMap = new LinkedIdentityMap<QueryPredicate, PredicateCompensationFunction>();
            for (final var entry : getPredicateCompensationMap().entrySet()) {
                // if the other side does not have compensation for this key, we don't need compensation
                final var otherPredicateCompensationFunction = otherCompensationMap.get(entry.getKey());
                if (otherPredicateCompensationFunction != null) {
                    // Both compensations have a compensation for this particular predicate which is essentially
                    // reapplying the predicate. Two cases arise:
                    // 1. Both predicate compensation functions are needed and possible. At this point it doesn't
                    //    matter which side we take as both create the same compensating filter. If at any point in the
                    //    future one data access has a better reapplication we need to generate plan variants with
                    //    either compensation and let the planner figure out which one wins. We just pick one side here.
                    // 2. Either one or both compensation functions are impossible, but the intersection is possible.
                    //    We take the compensation function from this side and amend it with the compensation function
                    //    from the other side.
                    final var newPredicateCompensationFunction = entry.getValue().amend(unmatchedAggregateMap, newMatchedAggregatesMap);
                    combinedPredicateMap.put(entry.getKey(), newPredicateCompensationFunction);
                    isImpossible |= newPredicateCompensationFunction.isImpossible();
                }
            }

            if (!intersectedChildCompensation.isNeededForFiltering() &&
                    !newResultResultCompensationFunction.isNeeded() && combinedPredicateMap.isEmpty()) {
                return Compensation.noCompensation();
            }

            if (!newResultResultCompensationFunction.isNeeded() && combinedPredicateMap.isEmpty()) {
                return intersectedChildCompensation;
            }

            // Note that at the current time each side can only contribute at most one foreach quantifier, thus the
            // intersection should also only contain at most one for each quantifier.
            final Sets.SetView<Quantifier> intersectedMatchedQuantifiers =
                    Sets.union(getMatchedQuantifiers(), otherWithSelectCompensation.getMatchedQuantifiers());

            final Sets.SetView<Quantifier> intersectedUnmatchedQuantifiers =
                    Sets.intersection(getUnmatchedQuantifiers(), otherWithSelectCompensation.getUnmatchedQuantifiers());

            final var unmatchedQuantifiers = Sets.intersection(this.getUnmatchedQuantifiers(), otherWithSelectCompensation.getUnmatchedQuantifiers());
            final var unmatchedQuantifierAliases = unmatchedQuantifiers.stream().map(Quantifier::getAlias).collect(ImmutableList.toImmutableList());
            if (!isImpossible) {
                isImpossible = combinedPredicateMap.keySet()
                        .stream()
                        .flatMap(queryPredicate -> queryPredicate.getCorrelatedTo().stream())
                        .anyMatch(unmatchedQuantifierAliases::contains);
            }

            return intersectedChildCompensation.derived(isImpossible,
                    combinedPredicateMap,
                    intersectedMatchedQuantifiers,
                    intersectedUnmatchedQuantifiers,
                    getCompensatedAliases(), // both compensated aliases must be identical, but too expensive to check
                    newResultResultCompensationFunction,
                    newGroupByMappings);
        }
    }

    /**
     * Regular compensation class for matches based on query predicates.
     */
    class ForMatch implements WithSelectCompensation {
        final boolean isImpossible;

        @Nonnull
        private final Compensation childCompensation;
        @Nonnull
        final Map<QueryPredicate, PredicateCompensationFunction> predicateCompensationMap;
        @Nonnull
        private final Set<Quantifier> matchedQuantifiers;
        @Nonnull
        private final Set<Quantifier> unmatchedQuantifiers;

        /**
         * We keep track of compensated aliases which define a kind of responsibility for this compensation,
         * that is, when the compensation is applied, the caller can be ensured that the match replacement
         * together with the compensation can replace those quantifiers. Normally the set of compensated aliases
         * comprises all matched quantifiers and existential non-matched quantifiers.
         */
        @Nonnull
        private final Set<CorrelationIdentifier> compensatedAliases;
        @Nonnull
        private final ResultCompensationFunction resultCompensationFunction;
        @Nonnull
        private final GroupByMappings groupByMappings;

        @Nonnull
        private final Supplier<Set<Quantifier>> unmatchedForEachQuantifiersSupplier;

        private ForMatch(final boolean isImpossible,
                         @Nonnull final Compensation childCompensation,
                         @Nonnull final Map<QueryPredicate, PredicateCompensationFunction> predicateCompensationMap,
                         @Nonnull final Collection<? extends Quantifier> matchedQuantifiers,
                         @Nonnull final Collection<? extends Quantifier> unmatchedQuantifiers,
                         @Nonnull final Set<CorrelationIdentifier> compensatedAliases,
                         @Nonnull final ResultCompensationFunction resultCompensationFunction,
                         @Nonnull final GroupByMappings groupByMappings) {
            this.isImpossible = isImpossible;
            this.childCompensation = childCompensation;
            this.predicateCompensationMap = new LinkedIdentityMap<>();
            this.predicateCompensationMap.putAll(predicateCompensationMap);
            this.matchedQuantifiers = new LinkedIdentitySet<>();
            this.matchedQuantifiers.addAll(matchedQuantifiers);
            this.unmatchedQuantifiers = new LinkedIdentitySet<>();
            this.unmatchedQuantifiers.addAll(unmatchedQuantifiers);
            this.compensatedAliases = ImmutableSet.copyOf(compensatedAliases);
            this.resultCompensationFunction = resultCompensationFunction;
            this.groupByMappings = groupByMappings;
            this.unmatchedForEachQuantifiersSupplier = Suppliers.memoize(this::computeUnmatchedForEachQuantifiers);
        }

        @Override
        public boolean isImpossible() {
            return isImpossible;
        }

        @Override
        @Nonnull
        public Compensation getChildCompensation() {
            return childCompensation;
        }

        @Nonnull
        @Override
        public Set<Quantifier> getMatchedQuantifiers() {
            return matchedQuantifiers;
        }

        @Nonnull
        @Override
        public Set<Quantifier> getUnmatchedQuantifiers() {
            return unmatchedQuantifiers;
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCompensatedAliases() {
            return compensatedAliases;
        }

        @Nonnull
        @Override
        public Set<Quantifier> getUnmatchedForEachQuantifiers() {
            return unmatchedForEachQuantifiersSupplier.get();
        }

        @Nonnull
        public Set<Quantifier> computeUnmatchedForEachQuantifiers() {
            return unmatchedQuantifiers.stream()
                    .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                    .collect(LinkedIdentitySet.toLinkedIdentitySet());
        }

        @Nonnull
        @Override
        public Map<QueryPredicate, PredicateCompensationFunction> getPredicateCompensationMap() {
            return predicateCompensationMap;
        }

        @Nonnull
        @Override
        public ResultCompensationFunction getResultCompensationFunction() {
            return resultCompensationFunction;
        }

        @Nonnull
        @Override
        public GroupByMappings getGroupByMappings() {
            return groupByMappings;
        }

        /**
         * When applied to a reference this method returns a {@link RelationalExpression} consuming the
         * reference passed in that applies additional predicates as expressed by the predicate compensation map.
         *
         * @param memoizer the memoizer for new {@link Reference}s
         * @param relationalExpression root of graph to apply compensation to
         * @param matchedToRealizedTranslationMapFunction a function that given an alias for the quantifier over the
         *        realized compensation, returns a {@link TranslationMap} that is then used to translate
         *        from {@link QueryPredicate}s and {@link Value}s using matched aliases to the realized alias
         *
         * @return a new relational expression that corrects the result of {@code reference} by applying appropriate
         * filters and/or transformations
         */
        @Nonnull
        @Override
        public RelationalExpression apply(@Nonnull final Memoizer memoizer,
                                          @Nonnull RelationalExpression relationalExpression,
                                          @Nonnull final Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
            Verify.verify(!isImpossible());

            // apply the child as needed
            if (childCompensation.isNeededForFiltering()) {
                relationalExpression = childCompensation.apply(memoizer, relationalExpression,
                        matchedToRealizedTranslationMapFunction);
            }

            final var matchedForEachAlias = getMatchedForEachAlias();
            final var matchedToRealizedTranslationMap = matchedToRealizedTranslationMapFunction.apply(matchedForEachAlias);

            final var compensatedPredicates = new LinkedIdentitySet<QueryPredicate>();
            final var injectCompensationFunctions = predicateCompensationMap.values();
            for (final var predicateCompensationFunction : injectCompensationFunctions) {
                // TODO construct a translation map using matchedForEachAlias as target
                compensatedPredicates.addAll(
                        predicateCompensationFunction.applyCompensationForPredicate(matchedToRealizedTranslationMap));
            }

            final var compensatedPredicatesCorrelatedTo =
                    compensatedPredicates
                            .stream()
                            .flatMap(predicate -> predicate.getCorrelatedTo().stream())
                            .collect(ImmutableSet.toImmutableSet());

            final var toBePulledUpQuantifiersBuilder = ImmutableSet.<Quantifier>builder();

            for (final var matchedQuantifier : matchedQuantifiers) {
                if (matchedQuantifier instanceof Quantifier.Existential) {
                    if (compensatedPredicatesCorrelatedTo.contains(matchedQuantifier.getAlias())) {
                        //
                        // This quantifier is matched but since there is a predicate referring to it which
                        // can only be an EXISTS() and we only partially matched the EXISTS() (as it is in the
                        // compensation map) we need to pull up the corresponding matched quantifier.
                        //
                        toBePulledUpQuantifiersBuilder.add(matchedQuantifier);
                    }
                }
            }

            for (final var unmatchedQuantifier : unmatchedQuantifiers) {
                if (unmatchedQuantifier instanceof Quantifier.ForEach) {
                    //
                    // Even though the quantifier is unmatched it can still affect the cardinality of this SELECT
                    // which means that we need to retain (pull-up) that quantifier.
                    //
                    toBePulledUpQuantifiersBuilder.add(unmatchedQuantifier);
                } else {
                    Verify.verify(unmatchedQuantifier instanceof Quantifier.Existential);
                    //
                    // If the unmatched quantifier is existential but there is nothing
                    //
                    if (compensatedPredicatesCorrelatedTo.contains(unmatchedQuantifier.getAlias())) {
                        toBePulledUpQuantifiersBuilder.add(unmatchedQuantifier);
                    }
                }
            }
            final var toBePulledUpQuantifiers = toBePulledUpQuantifiersBuilder.build();

            if (compensatedPredicates.isEmpty() && toBePulledUpQuantifiers.isEmpty()) {
                return relationalExpression;
            }

            //
            // At this point we definitely need a new SELECT expression.
            //
            final var newBaseQuantifier =
                    Quantifier.forEach(memoizer.memoizeUnknownExpression(relationalExpression),
                            matchedForEachAlias);

            //
            // TODO In the vast majority of cases the then branch is taken where the compensation does not create
            //      a join. If the compensation, however, has to reapply the predicate, then in addition to using
            //      part of the predicate as a sargable we may actually enter the else branch here. This happens for the
            //      reapplication of EXISTS(). The to-do is Try to find a solution that allows us to create a select
            //      here without triggering a whole round of explorations for the newly-formed join. As this really
            //      only happens for existential predicates, we could just introduce a binary join expression that does
            //      not undergo e.g. or-to-union. Note that this is not a problem for the then case as the predicates
            //      in the logical filter expression are left alone by other rules.
            //
            if (toBePulledUpQuantifiers.isEmpty()) {
                return new LogicalFilterExpression(compensatedPredicates, newBaseQuantifier);
            } else {
                final var completeExpansionBuilder = GraphExpansion.builder();
                completeExpansionBuilder.addAllQuantifiers(toBePulledUpQuantifiersBuilder.build());

                // add base quantifier
                completeExpansionBuilder.addQuantifier(newBaseQuantifier);
                completeExpansionBuilder.addAllPredicates(compensatedPredicates);

                return completeExpansionBuilder.build().buildSimpleSelectOverQuantifier(newBaseQuantifier);
            }
        }

        @Nonnull
        private CorrelationIdentifier getMatchedForEachAlias() {
            final var matchedQuantifierMap =
                    Quantifiers.aliasToQuantifierMap(matchedQuantifiers);

            final var matchedAliases =
                    matchedQuantifierMap.keySet();
            Verify.verify(compensatedAliases.equals(matchedAliases));

            final var matchedForEachQuantifierAliases =
                    matchedAliases
                            .stream()
                            .filter(alias -> matchedQuantifierMap.get(alias) instanceof Quantifier.ForEach)
                            .collect(ImmutableSet.toImmutableSet());

            return Iterables.getOnlyElement(matchedForEachQuantifierAliases);
        }

        @Nonnull
        @Override
        public RelationalExpression applyFinal(@Nonnull final Memoizer memoizer,
                                               @Nonnull RelationalExpression relationalExpression,
                                               @Nonnull Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction) {
            Verify.verify(!isImpossible());
            Verify.verify(resultCompensationFunction.isNeeded());

            final var matchedForEachAlias = getMatchedForEachAlias();

            final var resultValue =
                    resultCompensationFunction.applyCompensationForResult(
                            matchedToRealizedTranslationMapFunction.apply(matchedForEachAlias));

            //
            // At this point we definitely need a new SELECT expression.
            //
            final var newBaseQuantifier =
                    Quantifier.forEach(memoizer.memoizeUnknownExpression(relationalExpression),
                            matchedForEachAlias);

            return GraphExpansion.builder()
                    .addQuantifier(newBaseQuantifier)
                    .build()
                    .buildSelectWithResultValue(resultValue);
        }

        @Nonnull
        @Override
        public String toString() {
            final var result = new StringBuilder();
            if (isNeeded()) {
                result.append("needed; ");
            } else {
                result.append("not needed; ");
            }
            if (isImpossible()) {
                result.append("impossible");
            } else {
                result.append("possible");
            }
            return result.toString();
        }
    }
}
