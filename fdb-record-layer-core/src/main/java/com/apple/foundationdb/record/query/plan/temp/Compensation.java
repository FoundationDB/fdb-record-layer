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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.rules.DataAccessRule;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

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
 * may produce also extraneous records. Compensation corrects for that problem by applying certain post-operations such as
 * filtering, distincting or resorting.
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
 *
 * Compensation is computed either during the matching process or is computed after a complete match has been found
 * utilizing helper structures such as {@link PartialMatch} and {@link MatchInfo}, which are themselves
 * built during matching. Logic in
 * {@link DataAccessRule} computes and applies compensation
 * as needed when a complete index match has been found.
 *
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

        @Nonnull
        @Override
        public Compensation intersect(@Nonnull final Compensation otherCompensation) {
            return this;
        }

        @Override
        public RelationalExpression apply(@Nonnull final ExpressionRef<RelationalExpression> reference) {
            throw new RecordCoreException("this method should not be called");
        }
    };

    /**
     * Identity element for the intersection monoid defined by {@link #intersect(Compensation)}.
     * Example for the usage pattern for that monoid:
     *
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
        public boolean isNeeded() {
            return true;
        }

        @Override
        public boolean isImpossible() {
            return true;
        }

        @Nonnull
        @Override
        public Compensation intersect(@Nonnull final Compensation otherCompensation) {
            return otherCompensation;
        }

        @Override
        public RelationalExpression apply(@Nonnull final ExpressionRef<RelationalExpression> reference) {
            throw new RecordCoreException("this method should not be called");
        }
    };

    RelationalExpression apply(@Nonnull final ExpressionRef<RelationalExpression> reference);

    /**
     * Returns if this compensation object needs to be applied in order to correct the result of a match.
     * @return {@code true} if this compensation must be applied, {@code false} if this compensation is not needed.
     *         Note, by contract it is illegal to call {@link #apply} on this compensation if this method returns
     *         {@code false}.
     */
    default boolean isNeeded() {
        return true;
    }

    default boolean isImpossible() {
        return false;
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
            public RelationalExpression apply(@Nonnull final ExpressionRef<RelationalExpression> reference) {
                return Compensation.this.apply(
                        GroupExpressionRef.of(otherCompensation
                                .apply(reference)));
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
            public RelationalExpression apply(@Nonnull final ExpressionRef<RelationalExpression> reference) {
                return Compensation.this.apply(
                        GroupExpressionRef.of(otherCompensation
                                .apply(reference)));
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

    /**
     * Returns a specific compensation object that uses a mapping between predicates from a query to predicates that
     * are used for the application of the compensation.
     * @param childCompensation a compensation that should be applied before the compensation being created in this
     *        method
     * @param predicateCompensationMap map that maps {@link QueryPredicate}s of the query to {@link QueryPredicate}s
     *        used for compensation
     * @param mappedQuantifiers a set of quantifiers that are mapped in the original {@link PartialMatch}.
     * @param unmappedForEachQuantifiers a set of for each quantifiers that are not mapped in the original {@link PartialMatch}
     * @return a new compensation
     */
    @Nonnull
    static Compensation ofChildCompensationAndPredicateMap(@Nonnull final Compensation childCompensation,
                                                           @Nonnull final Map<QueryPredicate, QueryPredicate> predicateCompensationMap,
                                                           @Nonnull final Set<Quantifier> mappedQuantifiers,
                                                           @Nonnull final Set<Quantifier.ForEach> unmappedForEachQuantifiers) {
        return predicateCompensationMap.isEmpty() ? noCompensation() : new ForMatch(childCompensation, predicateCompensationMap, mappedQuantifiers, unmappedForEachQuantifiers);
    }

    /**
     * Interface for {@link Compensation}s that map original {@link QueryPredicate}s to compensating
     * {@link QueryPredicate}s.
     */
    interface WithPredicateCompensation extends Compensation {
        @Nonnull
        Compensation getChildCompensation();

        @Nonnull
        Set<Quantifier> getMappedQuantifiers();

        @Nonnull
        Set<Quantifier> getUnmatchedForEachQuantifiers();

        @Nonnull
        Map<QueryPredicate, QueryPredicate> getPredicateCompensationMap();

        /**
         * Method to return a new compensation of at least type {@link WithPredicateCompensation} based on the current
         * compensation object. This method should be implemented by implementing classes and/or their sub classes.
         * @param childCompensation a compensation that should be applied before the compensation being created in this
         *        method
         * @param predicateCompensationMap map that maps {@link QueryPredicate}s of the query to {@link QueryPredicate}s
         *        used for compensation
         * @param unmappedForEachQuantifiers a set of unmapped quantifiers
         * @return a new compensation
         */
        @Nonnull
        WithPredicateCompensation derivedWithPredicateCompensationMap(@Nonnull Compensation childCompensation,
                                                                      @Nonnull IdentityHashMap<QueryPredicate, QueryPredicate> predicateCompensationMap,
                                                                      @Nonnull final Collection<? extends Quantifier> mappedQuantifiers,
                                                                      @Nonnull Set<? extends Quantifier> unmappedForEachQuantifiers);

        /**
         * Specific implementation of union-ing two compensations both of type {@link WithPredicateCompensation}.
         * This implementation delegates to its super method if {@code otherCompensation} is not of type
         * {@link WithPredicateCompensation}. If it is, it creates a new compensation of type
         * {@link WithPredicateCompensation} that contains the mappings of both this compensation
         * and {@code otherCompensation}.
         * @param otherCompensation other compensation to union this compensation with
         * @return a new compensation object representing the logical union between {@code this} and
         *         {@code otherCompensation}
         */
        @Nonnull
        @Override
        default Compensation union(@Nonnull Compensation otherCompensation) {
            if (!(otherCompensation instanceof WithPredicateCompensation)) {
                return Compensation.super.union(otherCompensation);
            }

            final WithPredicateCompensation otherWithPredicateCompensation = (WithPredicateCompensation)otherCompensation;

            final var unionedMappedQuantifiers = Sets.union(getMappedQuantifiers(), otherWithPredicateCompensation.getMappedQuantifiers());
            final var numberForEachInUnion = unionedMappedQuantifiers.stream().filter(quantifier -> quantifier instanceof Quantifier.ForEach).collect(ImmutableList.toImmutableList()).size();
            if (numberForEachInUnion > 1) {
                // TODO This should be made better in the future. For now we just return the impossible compensation.
                //      What we need instead of using mapped and unmapped quantifiers is sufficient info to create a
                //      translation map from quantified column values to compensating values instead of having to perform
                //      translation on a quantifier level which becomes ambiguous if the source query has more than one
                //      matched quantifier underneath the query expression but the replacing scan expression only has one
                //      quantifier. What that translation map should define is a mapping from quantifier on the
                //      query side to quantifier column value on the over scan expression that is created in the
                //      data access rules.
                return impossibleCompensation();
            }

            if (!getUnmatchedForEachQuantifiers().isEmpty() || !otherWithPredicateCompensation.getUnmatchedForEachQuantifiers().isEmpty()) {
                return impossibleCompensation();
            }

            final Map<QueryPredicate, QueryPredicate> otherCompensationMap = otherWithPredicateCompensation.getPredicateCompensationMap();
            final IdentityHashMap<QueryPredicate, QueryPredicate> combinedMap = Maps.newIdentityHashMap();

            combinedMap.putAll(getPredicateCompensationMap());

            for (final Map.Entry<QueryPredicate, QueryPredicate> otherEntry : otherCompensationMap.entrySet()) {
                // if the other side does not have compensation for this key, we don't need compensation
                if (combinedMap.containsKey(otherEntry.getKey())) {
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
                combinedMap.put(otherEntry.getKey(), otherEntry.getValue());
            }

            final Compensation unionedChildCompensation = getChildCompensation().union(otherWithPredicateCompensation.getChildCompensation());
            if (!unionedChildCompensation.isNeeded() && combinedMap.isEmpty()) {
                return Compensation.noCompensation();
            }

            if (combinedMap.isEmpty()) {
                return unionedChildCompensation;
            }

            return derivedWithPredicateCompensationMap(unionedChildCompensation, combinedMap, unionedMappedQuantifiers, ImmutableSet.of());
        }

        /**
         * Specific implementation of intersecting two compensations both of type {@link WithPredicateCompensation}.
         * This implementation delegates to its super method if {@code otherCompensation} is not of type
         * {@link WithPredicateCompensation}. If it is, it creates a new compensation of type
         * {@link WithPredicateCompensation} that contains only the mappings that are contained in both this compensation
         * and {@code otherCompensation}.
         * @param otherCompensation other compensation to intersect this compensation with
         * @return a new compensation object representing the logical intersection between {@code this} and
         *         {@code otherCompensation}
         */
        @Nonnull
        @Override
        default Compensation intersect(@Nonnull Compensation otherCompensation) {
            if (!(otherCompensation instanceof WithPredicateCompensation)) {
                return Compensation.super.intersect(otherCompensation);
            }
            final WithPredicateCompensation otherWithPredicateCompensation = (WithPredicateCompensation)otherCompensation;
            final Map<QueryPredicate, QueryPredicate> otherCompensationMap = otherWithPredicateCompensation.getPredicateCompensationMap();

            final IdentityHashMap<QueryPredicate, QueryPredicate> combinedMap = Maps.newIdentityHashMap();
            for (final Map.Entry<QueryPredicate, QueryPredicate> entry : getPredicateCompensationMap().entrySet()) {
                // if the other side does not have compensation for this key, we don't need compensation
                if (otherCompensationMap.containsKey(entry.getKey())) {
                    // Both compensations have a compensation for this particular predicate which is essentially
                    // reapplying the predicate. At this point it doesn't matter which side we take as both create
                    // the same compensating filter. If at any point in the future one data access has a better
                    // reapplication we need to generate plan variants with either compensation and let the planner
                    // figure out which one wins.
                    // We just pick one side here.
                    combinedMap.put(entry.getKey(), entry.getValue());
                }
            }

            final Compensation childCompensation = getChildCompensation();
            Verify.verify(!(childCompensation instanceof WithPredicateCompensation) ||
                          ((WithPredicateCompensation)childCompensation).getUnmatchedForEachQuantifiers().isEmpty());

            final Compensation intersectedChildCompensation = childCompensation.intersect(otherWithPredicateCompensation.getChildCompensation());
            if (!intersectedChildCompensation.isNeeded() && combinedMap.isEmpty()) {
                return Compensation.noCompensation();
            }

            if (combinedMap.isEmpty()) {
                return intersectedChildCompensation;
            }

            // Note that at the current time each side can only contribute at most one foreach quantifier, thus the
            // intersection should also only contain at most one for each quantifier.
            final Sets.SetView<Quantifier> intersectedMappedQuantifiers =
                    Sets.intersection(getMappedQuantifiers(), otherWithPredicateCompensation.getMappedQuantifiers());

            final Sets.SetView<Quantifier> intersectedUnmappedForEachQuantifiers =
                    Sets.intersection(getUnmatchedForEachQuantifiers(), otherWithPredicateCompensation.getUnmatchedForEachQuantifiers());

            return derivedWithPredicateCompensationMap(intersectedChildCompensation, combinedMap, intersectedMappedQuantifiers, intersectedUnmappedForEachQuantifiers);
        }
    }

    /**
     * Regular compensation class for matches based on query predicates.
     */
    class ForMatch implements WithPredicateCompensation {
        @Nonnull
        private final Compensation childCompensation;
        @Nonnull
        final Map<QueryPredicate, QueryPredicate> predicateCompensationMap;
        @Nonnull
        private final Set<Quantifier> mappedQuantifiers;
        @Nonnull
        private final Set<Quantifier> unmatchedForEachQuantifiers;

        private ForMatch(@Nonnull final Compensation childCompensation,
                         @Nonnull final Map<QueryPredicate, QueryPredicate> predicateCompensationMap,
                         @Nonnull final Collection<? extends Quantifier> mappedQuantifiers,
                         @Nonnull final Collection<? extends Quantifier> unmatchedForEachQuantifiers) {
            this.childCompensation = childCompensation;
            this.predicateCompensationMap = Maps.newIdentityHashMap();
            this.predicateCompensationMap.putAll(predicateCompensationMap);
            this.mappedQuantifiers = new LinkedIdentitySet<>();
            this.mappedQuantifiers.addAll(mappedQuantifiers);
            this.unmatchedForEachQuantifiers = new LinkedIdentitySet<>();
            this.unmatchedForEachQuantifiers.addAll(unmatchedForEachQuantifiers);
        }

        @Override
        @Nonnull
        public Compensation getChildCompensation() {
            return childCompensation;
        }

        @Nonnull
        @Override
        public Set<Quantifier> getMappedQuantifiers() {
            return mappedQuantifiers;
        }

        @Nonnull
        @Override
        public Set<Quantifier> getUnmatchedForEachQuantifiers() {
            return unmatchedForEachQuantifiers;
        }

        @Nonnull
        @Override
        public Map<QueryPredicate, QueryPredicate> getPredicateCompensationMap() {
            return predicateCompensationMap;
        }

        @Nonnull
        @Override
        public WithPredicateCompensation derivedWithPredicateCompensationMap(@Nonnull final Compensation childCompensation,
                                                                             @Nonnull final IdentityHashMap<QueryPredicate, QueryPredicate> predicateCompensationMap,
                                                                             @Nonnull final Collection<? extends Quantifier> mappedQuantifiers,
                                                                             @Nonnull final Set<? extends Quantifier> unmappedForEachQuantifiers) {
            Verify.verify(!predicateCompensationMap.isEmpty());
            return new ForMatch(childCompensation, predicateCompensationMap, mappedQuantifiers, unmappedForEachQuantifiers);
        }

        /**
         * When applied to a reference this method returns a {@link LogicalFilterExpression} consuming the
         * reference passed in that applies additional predicates as expressed by the predicate compensation map.
         * @param reference root of graph to apply compensation to
         * @return a new relational expression that corrects the result of {@code reference} by applying an additional
         *         filter
         */
        @Override
        public RelationalExpression apply(@Nonnull ExpressionRef<RelationalExpression> reference) {
            // apply the child as needed
            if (childCompensation.isNeeded()) {
                reference = GroupExpressionRef.of(childCompensation.apply(reference));
            }

            final var mappedForEachQuantifiers =
                    mappedQuantifiers
                            .stream()
                            .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                            .map(Quantifier::getAlias)
                            .collect(ImmutableList.toImmutableList());

            Verify.verify(mappedForEachQuantifiers.size() <= 1);
            final Quantifier quantifier = Quantifier.forEach(reference);

            final AliasMap translationMap;
            if (mappedQuantifiers.size() == 1) {
                translationMap = AliasMap.of(Iterables.getOnlyElement(mappedForEachQuantifiers), quantifier.getAlias());
            } else {
                translationMap = AliasMap.emptyMap();
            }

            final Collection<QueryPredicate> predicates = predicateCompensationMap.values();
            final ImmutableList<QueryPredicate> rebasedPredicates = predicates
                    .stream()
                    .map(queryPredicate -> queryPredicate.rebase(translationMap))
                    .collect(ImmutableList.toImmutableList());
            return new LogicalFilterExpression(rebasedPredicates, quantifier);
        }
    }
}
