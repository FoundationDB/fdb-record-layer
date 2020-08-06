/*
 * Quantifiers.java
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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.Quantifier.Existential;
import com.apple.foundationdb.record.query.plan.temp.Quantifier.ForEach;
import com.apple.foundationdb.record.query.plan.temp.Quantifier.Physical;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Auxiliary class containing factory methods and helpers for {@link Quantifier}.
 */
public class Quantifiers {

    private Quantifiers() {
        // prevent instantiation
    }

    /**
     * Create a list of for-each quantifiers from a list of references to range over.
     * @param rangesOverExpressions iterable {@link ExpressionRef}s of {@link RelationalExpression}s
     * @param <E> type parameter to constrain expressions to {@link RelationalExpression}
     * @return a list of for-each quantifiers where each quantifier ranges over one of the given references
     */
    @Nonnull
    public static <E extends RelationalExpression> List<ForEach> forEachQuantifiers(@Nonnull final Iterable<ExpressionRef<E>> rangesOverExpressions) {
        return fromExpressions(rangesOverExpressions, Quantifier::forEach);
    }

    /**
     * Create a list of existential quantifiers from a list of expression references these quantifiers should range over.
     * @param rangesOverPlans iterable {@link ExpressionRef}s of of {@link RelationalExpression}s.
     * @param <E> type parameter to constrain expressions to {@link RelationalExpression}
     * @return a list of physical quantifiers where each quantifier ranges over one of the given references
     */
    @Nonnull
    public static <E extends RelationalExpression> List<Existential> existentialQuantifiers(@Nonnull final Iterable<? extends ExpressionRef<E>> rangesOverPlans) {
        return fromExpressions(rangesOverPlans, Quantifier::existential);
    }

    /**
     * Create a list of physical quantifiers given a list of references these quantifiers should range over.
     * @param rangesOverPlans iterable {@link ExpressionRef}s of {@link RecordQueryPlan}
     * @param <E> type parameter to constrain expressions to {@link RecordQueryPlan}
     * @return a list of physical quantifiers where each quantifier ranges over a reference contained in the given iterable
     */
    @Nonnull
    public static <E extends RecordQueryPlan> List<Physical> fromPlans(@Nonnull final Iterable<? extends ExpressionRef<E>> rangesOverPlans) {
        return fromExpressions(rangesOverPlans, Quantifier::physical);
    }

    /**
     * Create a list of quantifiers given a list of references these quantifiers should range over.
     * @param rangesOverExpressions iterable of {@link ExpressionRef}s the quantifiers will be created to range over
     * @param creator lambda to to be called for each expression reference contained in {@code rangesOverExpression}s
     *        to create the actual quantifier. This allows for callers to create different kinds of quantifier based
     *        on needs.
     * @param <E> type parameter to constrain expressions to {@link RelationalExpression}
     * @param <Q> the type of the quantifier to be created
     * @return a list of quantifiers where each quantifier ranges over an reference contained in the given iterable
     */
    @Nonnull
    public static <E extends RelationalExpression, Q extends Quantifier> List<Q> fromExpressions(@Nonnull final Iterable<? extends ExpressionRef<E>> rangesOverExpressions,
                                                                                                 @Nonnull final Function<ExpressionRef<E>, Q> creator) {
        return StreamSupport
                .stream(rangesOverExpressions.spliterator(), false)
                .map(creator)
                .collect(Collectors.toList());
    }

    /**
     * Convenience helper to create a single ID translation map from the alias of one quantifier to the alias
     * of another quantifier.
     * @param from quantifier
     * @param to quantifier
     * @return a new translation map mapping from {@code from.getAlias()} to {@code to.getAlias()}
     */
    @Nonnull
    public static AliasMap translate(@Nonnull final Quantifier from, @Nonnull final Quantifier to) {
        return AliasMap.of(from.getAlias(), to.getAlias());
    }

    /**
     * Convenience helper to create an alias translation map based on a translation map using quantifiers.
     * of another quantifier.
     * @param map quantifier to quantifier bi-map
     * @return a new {@link AliasMap} mapping from {@code from.getAlias()} to {@code to.getAlias()}
     */
    @Nonnull
    public static AliasMap toIDMap(@Nonnull final BiMap<Quantifier, Quantifier> map) {
        return AliasMap.of(map.entrySet()
                .stream()
                .collect(ImmutableBiMap.toImmutableBiMap(entry -> entry.getKey().getAlias(),
                        entry -> entry.getValue().getAlias())));
    }

    @Nonnull
    public static ImmutableMap<CorrelationIdentifier, ? extends Quantifier> toAliasToQuantifierMap(@Nonnull final Collection<? extends Quantifier> quantifiers) {
        return quantifiers.stream()
                .collect(ImmutableMap.toImmutableMap(Quantifier::getAlias, Function.identity()));
    }

    @Nonnull
    public static <Q extends Quantifier> List<Q> narrow(@Nonnull Class<Q> narrowedClass,
                                                        @Nonnull final List<? extends Quantifier> quantifiers) {
        return quantifiers.stream()
                .map(narrowedClass::cast)
                .collect(Collectors.toList());
    }

    @Nonnull
    public static <Q extends Quantifier> Set<Q> narrow(@Nonnull Class<Q> narrowedClass,
                                                       @Nonnull final Set<? extends Quantifier> quantifiers) {
        return quantifiers.stream()
                .map(narrowedClass::cast)
                .collect(Collectors.toSet());
    }

    /**
     * Method to match up the quantifiers this expression owns with the given set of other quantifiers using an
     * equivalence map.
     *
     * Two quantifiers can be considered a match if the given predicate returns {@code true}.
     * This method attempts to match up each quantifier from the set of quantifiers this expression owns to the given
     * other set of quantifiers such that the given predicate returns {@code true} for each mapping. Note that there
     * may be multiple distinct matches between the quantifier sets. This method returns an {@link Iterable} of such
     * matches where each match is only computed when it is requested by an iterator's {@link Iterator#hasNext()}.
     * Thus it can be assumed that this method never pre-computes all possible matches.
     *
     * This method returns an empty mapping if e.g. the number quantifiers in the respective sets doesn't match.
     *
     * For expressions that cannot introduce correlations, this method is simply {@code O(n!)} where is the
     * number of quantifiers (which is equal for both sets). This path is taken by UNIONs and other set operations.
     *
     * For expressions that introduce correlations, the matching process is more complicated. First, we determine one
     * topologically correct ordering. Then we iterate through all topologically correct orderings of the set of
     * quantifiers this expression owns. For each element in the permutation of quantifiers that is returned by
     * the {@link TopologicalSort.TopologicalOrderPermutationIterator}
     * we try to math the quantifiers between the sets topologically from left to right (using {@code predicate}).
     * For each match we find we add an equivalence between the alias of this and the other quantifier that matched
     * in an equivalence map that extends the given equivalence map.
     *
     * @param quantifiers collection of quantifiers
     * @param otherQuantifiers collection of _other_ quantifiers
     * @param canCorrelate indicator if the expression containing these quantifiers can actively introduce correlations
     * @param equivalencesMap equivalence map
     * @param predicate that tests if two quantifiers and their graph they range over can be considered equivalent
     * @return An {@link AliasMap} containing a mapping from the quantifiers this expression owns to the quantifiers in
     *         {@code otherQuantifiers}. Note that the mapping is bijective and can therefore be inverted.
     */
    @SuppressWarnings("squid:S135")
    @Nonnull
    public static Iterable<AliasMap> match(@Nonnull final Collection<? extends Quantifier> quantifiers,
                                           @Nonnull final Collection<? extends Quantifier> otherQuantifiers,
                                           final boolean canCorrelate,
                                           @Nonnull final AliasMap equivalencesMap,
                                           @Nonnull final MatchingPredicate predicate) {
        final ImmutableMap<CorrelationIdentifier, ? extends Quantifier> aliasToQuantifierMap = toAliasToQuantifierMap(quantifiers);
        final ImmutableMap<CorrelationIdentifier, ? extends Quantifier> otherAliasToQuantifierMap = toAliasToQuantifierMap(otherQuantifiers);

        return equivalencesMap.match(aliasToQuantifierMap.keySet(),
                alias -> aliasToQuantifierMap.get(alias).getCorrelatedTo(),
                otherAliasToQuantifierMap.keySet(),
                otherAlias -> otherAliasToQuantifierMap.get(otherAlias).getCorrelatedTo(),
                canCorrelate,
                (alias, otherAlias, eM) -> {
                    final Quantifier quantifier = aliasToQuantifierMap.get(alias);
                    final Quantifier otherQuantifier = otherAliasToQuantifierMap.get(otherAlias);

                    if (!quantifier.equalsOnKind(otherQuantifier)) {
                        return false;
                    }

                    return predicate.test(quantifier, otherQuantifier, eM);
                });
    }

    /**
     * An predicate that tests for a match between quantifiers also taking into account an equivalence maps between
     * {@link CorrelationIdentifier}s.
     */
    @FunctionalInterface
    public interface MatchingPredicate {
        boolean test(@Nonnull Quantifier quantifier,
                     @Nonnull Quantifier otherQuantifier,
                     @Nonnull final AliasMap equivalencesMap);
    }
}
