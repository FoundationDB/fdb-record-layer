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
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Interface for all kinds of compensation. A compensation is the byproduct of expression DAG matching.
 * Matching two graphs {@code Q} and {@code M} may yield two sub graphs {@code Q_s} and {@code M_s} that match.
 * {@code A_s} and {@code B_s} may be completely equivalent to each other and {@code Q_s} can be substituted with
 * {@code M_s} freely and vice versa. The requirement that {@code Q_s} and {@code M_s} have to be semantically
 * equivalent for such a substitution, however, is not very useful. Normally, we try to find a materialized data set
 * such as an index that can be utilized by the planner directly using a scan
 * (requiring a complete match on the {@code M}-side, that is {@code M_s} equals {@code M}) instead of computing the
 * result from the raw base data set. For those purposes it makes more sense to relax the matching requirement to just
 * require that the materialized view side of matching {@code M} can subsume the query side {@code M} which means that
 * executing {@code M} at least contains the result result executing {@code Q} would result in. But it may produce
 * extraneous records. Compensation corrects for that problem by applying certain post-operations such as
 * filtering, distincting or resorting.
 *
 * Example in SQL terms:
 *
 * Query:
 * <pre>
 * {@code
 *   SELECT *
 *   FROM recordType
 *   WHERE a = 3 AND b = 5
 * }
 * </pre>
 *
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
public interface Compensation extends Function<ExpressionRef<RelationalExpression>, RelationalExpression> {
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
        public RelationalExpression apply(final ExpressionRef<RelationalExpression> reference) {
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

        @Nonnull
        @Override
        public Compensation intersect(@Nonnull final Compensation otherCompensation) {
            return otherCompensation;
        }

        @Override
        public RelationalExpression apply(final ExpressionRef<RelationalExpression> reference) {
            throw new RecordCoreException("this method should not be called");
        }
    };

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
     * {@link #isNeeded()} returns {@code true} however, that cannot be applied. This object is only needed to define
     * the identity of the intersection monoid on compensations. One can imagine the impossible compensation to stand
     * for the fact that compensation is needed (that is the match subsumes the query) but that the compensation itself
     * cannot be computed.
     * @return a compensation object that represents an impossible for compensation
     */
    @Nonnull
    static Compensation impossibleCompensation() {
        return IMPOSSIBLE_COMPENSATION;
    }

    /**
     * Returns a specific compensation object that uses a mapping between predicates from a query to predicates that
     * are used for the application of the compensation.
     * @param predicateCompensationMap map that maps {@link QueryPredicate}s of the query to {@link QueryPredicate}s
     *        used for compensation
     * @return a new compensation
     */
    @Nonnull
    static Compensation ofPredicateMap(final Map<QueryPredicate, QueryPredicate> predicateCompensationMap) {
        return predicateCompensationMap.isEmpty() ? noCompensation() : new ForMatch(predicateCompensationMap);
    }

    /**
     * Interface for {@link Compensation}s that map original {@link QueryPredicate}s to compensating
     * {@link QueryPredicate}s.
     */
    interface WithPredicateCompensation extends Compensation {
        @Nonnull
        Map<QueryPredicate, QueryPredicate> getPredicateCompensationMap();

        /**
         * Method to return a new compensation of at least type {@link WithPredicateCompensation} based on the current
         * compensation object. This method should be implemented by implementing classes and/or their sub classes.
         * TODO make this method protected in Java 11.
         * @param predicateCompensationMap  map that maps {@link QueryPredicate}s of the query to {@link QueryPredicate}s
         *        used for compensation
         * @return a new compensation
         */
        @Nonnull
        WithPredicateCompensation derivedWithPredicateCompensationMap(@Nonnull IdentityHashMap<QueryPredicate, QueryPredicate> predicateCompensationMap);

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
                    // we just pick one side
                    combinedMap.put(entry.getKey(), entry.getValue());
                }
            }

            return combinedMap.isEmpty() ? noCompensation() : derivedWithPredicateCompensationMap(combinedMap);
        }
    }

    /**
     * Regular compensation class for matches based on query predicates.
     */
    class ForMatch implements WithPredicateCompensation {
        @Nonnull
        final Map<QueryPredicate, QueryPredicate> predicateCompensationMap;

        public ForMatch(@Nonnull final Map<QueryPredicate, QueryPredicate> predicateCompensationMap) {
            this.predicateCompensationMap = Maps.newIdentityHashMap();
            this.predicateCompensationMap.putAll(predicateCompensationMap);
        }

        @Nonnull
        @Override
        public Map<QueryPredicate, QueryPredicate> getPredicateCompensationMap() {
            return predicateCompensationMap;
        }

        @Nonnull
        @Override
        public WithPredicateCompensation derivedWithPredicateCompensationMap(@Nonnull final IdentityHashMap<QueryPredicate, QueryPredicate> predicateCompensationMap) {
            Verify.verify(!predicateCompensationMap.isEmpty());
            return new ForMatch(predicateCompensationMap);
        }

        /**
         * When applied to a reference this method returns a {@link LogicalFilterExpression} consuming the
         * reference passed in that applies additional predicates as expressed by the predicate compensation map.
         * @param reference root of graph to apply compensation to
         * @return a new relational expression that corrects the result of {@code reference} by applying an additional
         *         filter
         */
        @Override
        public RelationalExpression apply(final ExpressionRef<RelationalExpression> reference) {
            final Quantifier quantifier = Quantifier.forEach(reference);
            final Collection<QueryPredicate> predicates = predicateCompensationMap.values();
            final ImmutableList<QueryPredicate> rebasedPredicates = predicates
                    .stream()
                    .map(queryPredicate -> {
                        final Set<CorrelationIdentifier> correlatedTo = queryPredicate.getCorrelatedTo();
                        Verify.verify(correlatedTo.size() == 1);
                        final AliasMap translationMap = AliasMap.of(Iterables.getOnlyElement(correlatedTo), quantifier.getAlias());
                        return queryPredicate.rebase(translationMap);
                    })
                    .collect(ImmutableList.toImmutableList());
            return new LogicalFilterExpression(AndPredicate.and(rebasedPredicates), quantifier);
        }
    }
}
