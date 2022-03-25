/*
 * RelationalExpression.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterable;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.temp.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalProjectionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matching.BoundMatch;
import com.apple.foundationdb.record.query.plan.temp.matching.MatchFunction;
import com.apple.foundationdb.record.query.plan.temp.matching.MatchPredicate;
import com.apple.foundationdb.record.query.plan.temp.rules.AdjustMatchRule;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * A relational expression is a {@link RelationalExpression} that represents a stream of records. At all times, the root
 * expression being planned must be relational. This interface acts as a common tag interface for
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}s, which can actually produce a stream of records,
 * and various logical relational expressions (not yet introduced), which represent an abstract stream of records but can't
 * be executed directly (such as an unimplemented sort). Other planner expressions such as {@link com.apple.foundationdb.record.query.expressions.QueryComponent}
 * and {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression} do not represent streams of records.
 *
 * The basic type that represents a part of the planner expression tree. An expression is generally an immutable
 * object with two different kinds of fields: regular Java fields and reference fields. The regular fields represent
 * "node information", which pertains only to this specific node in the tree. In contrast, the reference fields represent
 * this expression's children in the tree, such as its inputs and filter/sort expressions, and are always hidden behind
 * an {@link ExpressionRef}.
 *
 * Deciding whether certain fields constitute "node information" (and should therefore be a regular field) or
 * "hierarchical information" (and therefore should not be) is subtle and more of an art than a science. There are two
 * reasonable tests that can help make this decision:
 * <ol>
 *     <li>When writing a planner rule to manipulate this field, does it make sense to match it separately
 *     or access it as a getter on the matched operator? Will you ever want to match to just this field?</li>
 *     <li>Should the planner memoize (and therefore optimize) this field separately from its parent?</li>
 * </ol>
 *
 * For example, {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} has only regular fields, including the
 * index name and the comparisons to use when scanning it.
 * Applying the first rule, it wouldn't really make sense to match the index name or the comparisons being performed on
 * their own: they're what define an index scan, after all!
 * Applying the second rule, they're relatively small immutable objects that don't need to be memoized.
 *
 * In contrast, {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan} has no regular fields.
 * A filter plan has two important fields: the <code>Query.Component</code> used for the filter and a child plan that
 * provides input. Both of these might be matched by rules directly, in order to optimize them without regard for the
 * fact that there's a filter. Similarly, they should both be memoized separately, since there might be many possible
 * implementations of each.
 */
@API(API.Status.EXPERIMENTAL)
public interface RelationalExpression extends Correlated<RelationalExpression>, Typed, Narrowable<RelationalExpression> {
    @Nonnull
    static RelationalExpression fromRecordQuery(@Nonnull PlanContext context,
                                                @Nonnull RecordQuery query) {
        final var recordMetaData = context.getMetaData();
        query.validate(recordMetaData);
        final var recordTypes = context.getRecordTypes();

        final GroupExpressionRef<? extends RelationalExpression> baseRef;
        Quantifier.ForEach quantifier;
        if (recordTypes.isEmpty()) {
            baseRef = GroupExpressionRef.of(new FullUnorderedScanExpression(context.getMetaData().getRecordTypes().keySet()));
            quantifier = Quantifier.forEach(baseRef);
        } else {
            final var fuseRef = GroupExpressionRef.of(new FullUnorderedScanExpression(context.getMetaData().getRecordTypes().keySet()));
            baseRef = GroupExpressionRef.of(
                    new LogicalTypeFilterExpression(
                            new HashSet<>(recordTypes),
                            Quantifier.forEach(fuseRef),
                            Type.Record.fromFieldDescriptorsMap(recordMetaData.getFieldDescriptorMapFromNames(recordTypes))));
            quantifier = Quantifier.forEach(baseRef);
        }

        final SelectExpression selectExpression;
        if (query.getFilter() != null) {
            selectExpression =
                    GraphExpansion.ofOthers(GraphExpansion.builder().addQuantifier(quantifier).build(),
                                    query.getFilter()
                                            .expand(quantifier, () -> Quantifier.forEach(baseRef)))
                            .buildSimpleSelectOverQuantifier(quantifier);
        } else {
            selectExpression =
                    GraphExpansion.builder().addQuantifier(quantifier).build()
                            .buildSimpleSelectOverQuantifier(quantifier);
        }
        quantifier = Quantifier.forEach(GroupExpressionRef.of(selectExpression));

        if (query.removesDuplicates()) {
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalDistinctExpression(quantifier)));
        }

        if (query.getSort() != null) {
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalSortExpression(query.getSort(), query.isSortReverse(), quantifier)));
        } else {
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalSortExpression(null, false, quantifier)));
        }

        if (query.getRequiredResults() != null) {
            final List<? extends Value> projectedValues =
                    Value.fromKeyExpressions(
                            query.getRequiredResults()
                                    .stream()
                                    .flatMap(keyExpression -> keyExpression.normalizeKeyForPositions().stream())
                                    .collect(ImmutableList.toImmutableList()),
                            quantifier);
            quantifier = Quantifier.forEach(GroupExpressionRef.of(new LogicalProjectionExpression(projectedValues, quantifier)));
        }

        return quantifier.getRangesOver().get();
    }

    @Nonnull
    @Override
    default Type.Relation getResultType() {
        return new Type.Relation(getResultValue().getResultType());
    }

    @Nonnull
    Value getResultValue();

    @SuppressWarnings({"java:S3655", "UnstableApiUsage"})
    default boolean semanticEqualsForResults(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap aliasMap) {
        return getResultValue().semanticEquals(otherExpression.getResultValue(), aliasMap);
    }

    /**
     * Return an iterator of references to the children of this planner expression. The iterators returned by different
     * calls are guaranteed to be independent (i.e., advancing one will not advance another). However, they might point
     * to the same object, as when <code>Collections.emptyIterator()</code> is returned. The returned iterator should
     * be treated as an immutable object and may throw an exception if {@link Iterator#remove} is called.
     * The iterator must return its elements in a consistent order.
     * @return an iterator of references to the children of this planner expression
     */
    @Nonnull
    List<? extends Quantifier> getQuantifiers();

    /**
     * Returns if this expression can be the anchor of a correlation.
     *
     * A correlation is always formed between three entities:
     * <ol>
     * <li>the {@link Quantifier} that flows data</li>
     * <li>2. the anchor (which is a {@link RelationalExpression}) that ranges directly over the source</li>
     * <li>3. the consumers (or dependents) of the correlation which must be a descendant of the anchor.</li>
     * </ol>
     *
     * In order for a correlation to be meaningful, the anchor must define how data is bound and used by all
     * dependents. For most expressions it is not meaningful or even possible to define correlation in such a way.
     *
     * For instance, a {@link LogicalUnionExpression}
     * cannot correlate (this method returns {@code false}) because it is not meaningful to bind a record from one child
     * of the union while providing bound values to another.
     *
     * In another example, a logical select expression can correlate which means that one child of the SELECT expression
     * can be evaluated and the resulting records can bound individually one after another. For each bound record
     * flowing along that quantifier the other children of the SELECT expression can be evaluated, potentially causing
     * more correlation values to be bound, etc. These concepts follow closely to the mechanics of what SQL calls a query
     * block.
     *
     * The existence of a correlation between source, anchor, and dependents may adversely affect planning because
     * a correlation always imposes order between the evaluated of children of an expression. This may or may
     * not tie the hands of the planner to produce an optimal plan. In certain cases, queries written in a correlated
     * way can be <em>de-correlated</em> to allow for better optimization techniques.
     *
     * @return {@code true} if this expression can be the anchor of a correlation, {@code false} otherwise.
     */
    default boolean canCorrelate() {
        return false;
    }

    boolean equalsWithoutChildren(@Nonnull final RelationalExpression other,
                                  @Nonnull final AliasMap equivalences);

    int hashCodeWithoutChildren();

    /**
     * Overloaded method to call {@link #semanticEquals} with an empty alias map.
     * @param other object to compare to this expression
     * @return {@code true} if this object is semantically equal to {@code other} that is {@code this} and {@code other}
     *         produce the same result when invoked with no bindings, {@code false} otherwise.
     */
    default boolean semanticEquals(@Nullable final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    /**
     * Method to establish whether this relational expression is equal to another object under the bindings
     * given by the {@link AliasMap} passed in.
     * @param other the other object to establish equality with
     * @param aliasMap a map of {@link CorrelationIdentifier}s {@code ids} to {@code ids'}. A correlation
     *        identifier {@code id} used in {@code this} should be considered equal to another correlation identifier
     *        {@code id'} used in {@code other} if either they are the same by {@link Object#equals}
     *        of if there is a mapping from {@code id} to {@code id'}.
     * @return {@code true} if this is considered equal to {@code other}, false otherwise
     */
    @Override
    default boolean semanticEquals(@Nullable final Object other,
                                   @Nonnull final AliasMap aliasMap) {
        // check some early-outs
        if (this == other) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (!(other instanceof RelationalExpression)) {
            return false;
        }

        final RelationalExpression otherExpression = (RelationalExpression)other;

        // use matching logic to establish equality
        final Iterable<AliasMap> boundMatchIterable =
                findMatches(otherExpression,
                        aliasMap,
                        (quantifier, otherQuantifier, nestedEquivalenceMap) -> {
                            if (quantifier.semanticHashCode() != otherQuantifier.semanticHashCode()) {
                                return false;
                            }
                            return quantifier.semanticEquals(otherQuantifier, nestedEquivalenceMap);
                        },
                        ((boundCorrelatedToMap, boundMapIterable) -> {
                            if (getQuantifiers().isEmpty()) {
                                return equalsWithoutChildren(otherExpression, boundCorrelatedToMap);
                            }

                            return StreamSupport.stream(boundMapIterable.spliterator(), false)
                                    .anyMatch(boundMap -> equalsWithoutChildren(otherExpression, boundMap));
                        }));

        return !Iterables.isEmpty(boundMatchIterable);
    }

    @Nonnull
    default <E extends RelationalExpression> E narrow(@Nonnull Class<E> narrowedClass) {
        return narrowedClass.cast(this);
    }

    @Nonnull
    default <E extends RelationalExpression> Optional<E> narrowMaybe(@Nonnull Class<E> narrowedClass) {
        if (narrowedClass.isInstance(this)) {
            return Optional.of(narrowedClass.cast(this));
        }
        return Optional.empty();
    }

    /**
     * Find matches between this expression and another given expression under the bindings
     * given by the {@link AliasMap} passed in.
     *
     * @param otherExpression other expression
     * @param aliasMap alias map with external bindings
     * @param matchPredicate a predicate uses for matching a pair of {@link Quantifier}s
     * @param combinePredicate a predicate to accept or reject a match
     * @return an {@link Iterable} of {@link AliasMap}s where each alias map is a match.
     */
    @Nonnull
    default Iterable<AliasMap> findMatches(@Nonnull final RelationalExpression otherExpression,
                                           @Nonnull final AliasMap aliasMap,
                                           @Nonnull final MatchPredicate<Quantifier> matchPredicate,
                                           @Nonnull final CombinePredicate combinePredicate) {

        if (getClass() != otherExpression.getClass()) {
            return ImmutableList.of();
        }

        // We know this and otherExpression are of the same class.canCorrelate() needs to match as well.
        Verify.verify(canCorrelate() == otherExpression.canCorrelate());

        final Iterable<AliasMap> boundCorrelatedToIterable =
                enumerateUnboundCorrelatedTo(aliasMap,
                        otherExpression);

        return () ->
                StreamSupport.stream(boundCorrelatedToIterable.spliterator(), false)
                        .filter(boundCorrelatedToMap -> {
                            final Iterable<AliasMap> boundMapIterable =
                                    Quantifiers.findMatches(
                                            boundCorrelatedToMap,
                                            getQuantifiers(),
                                            otherExpression.getQuantifiers(),
                                            matchPredicate);

                            return combinePredicate.combine(boundCorrelatedToMap, boundMapIterable);
                        }).iterator();
    }

    /**
     * A functional interface to combine the matches computed over pairs of quantifiers during matching into a
     * boolean result (for the bound correlatedTo set handed into {@link #combine}).
     *
     */
    @FunctionalInterface
    interface CombinePredicate {
        /**
         * Combine the results of a {@link Quantifiers#findMatches} into a boolean result.
         * @param boundCorrelatedToMap the bound correlated to map
         * @param boundMapIterable an iterable of {@link AliasMap} for all the matches for a given
         *        {@code boundCorrelatedToMap}
         * @return {@code false} if the match should be dropped or {@code true} if it should be kept.
         */
        boolean combine(@Nonnull final AliasMap boundCorrelatedToMap,
                        @Nonnull final Iterable<AliasMap> boundMapIterable);
    }

    /**
     * Attempt to match this expression (this graph) with another expression (from another graph called the candidate
     * graph) to produce matches of some kind.
     *
     * This overload matches over all quantifiers owned by this expression respectively the {@code otherExpression}.
     * See {@link #match(RelationalExpression, AliasMap, List, List, Function, MatchFunction, CombineFunction)} for
     * more information about the matching process.
     *
     * @param otherExpression the expression to match this expression with
     * @param boundAliasMap alias map containing bound aliases
     * @param constraintsFunction function constraining the number of permutations to enumerate
     * @param matchFunction function producing a match result as iterable of type {@code M}
     * @param combineFunction function to produce an iterable of type {@code S} by combining on bound matches of type
     *        {@code M}
     * @param <M> intermediate type to represent match results
     * @param <S> final type to represent match results
     * @return an {@link Iterable} of type {@code S} of matches of {@code this} expression with {@code otherExpression}.
     */
    @Nonnull
    default <M, S> Iterable<S> match(@Nonnull final RelationalExpression otherExpression,
                                     @Nonnull final AliasMap boundAliasMap,
                                     @Nonnull final Function<Quantifier, Collection<AliasMap>> constraintsFunction,
                                     @Nonnull final MatchFunction<Quantifier, M> matchFunction,
                                     @Nonnull final CombineFunction<M, S> combineFunction) {
        final List<? extends Quantifier> quantifiers = getQuantifiers();
        final List<? extends Quantifier> otherQuantifiers = otherExpression.getQuantifiers();

        return match(otherExpression,
                boundAliasMap,
                quantifiers,
                otherQuantifiers,
                constraintsFunction,
                matchFunction,
                combineFunction);
    }

    /**
     * Attempt to match this expression (this graph) with another expression (from another graph called the candidate
     * graph) to produce matches of some kind.
     *
     * Two relational expressions can only match if the sub-graphs of the quantifiers they range over match
     * themselves under a bijective association (mapping between the quantifiers of this expression and
     * the quantifier of the candidate expression). To this end, the {@code matchFunction} passed in to this method is
     * used to determine the matches between two quantifiers: one from this graph and one from the candidate graph.
     * The {@code matchFunction} can produce zero, one or many matches which are returned as an {@link Iterable} of type
     * {@code M}.
     *
     * This method attempts to find that bijective mapping between the quantifiers contained by their respective
     * expressions. Naturally, if the expressions that are being matched own a different number of quantifiers we cannot
     * ever find a bijective mapping. In that case, the two expressions do not match at all. If, on the other hand the
     * expressions that are being matched do have the same number of quantifiers, we need to enumerate all possible
     * associations in order to potentially find matches. In a naive implementation, and not considering any other
     * constraints, such an enumeration produces a number of mappings that is equal to the enumeration of all permutations
     * of sets of size {@code n} which is {@code n!}. Fortunately, it is possible for most cases to impose strict
     * constraints on the enumeration of mappings and therefore reduce the degrees of freedom we seemingly have at first
     * considerably. For instance, if this expression can be the anchor of a correlation, there might be an implied
     * necessary order imposed by a correlation between two quantifiers {@code q1} and {@code q2} where {@code q2}
     * depends on {@code q1}, denoted by {@code q1 -> q2}.
     * This implies that every enumerated mapping must contain {@code q1} before {@code q2} which therefore decreases
     * the number of all mappings that need to be enumerated.
     *
     * One complicating factor are correlations to parts of the graph that are not contained in the sub-graphs
     * underneath {@code this} respectively the candidate expression. These correlations are enumerated and bound
     * prior to matching the quantifiers.
     *
     * @param otherExpression the expression to match this expression with
     * @param boundAliasMap alias map containing bound aliases
     * @param quantifiers the set of quantifiers owned by this expression that is matched over
     * @param otherQuantifiers the set of quantifiers owned by {@code otherExpression}  that is matched over
     * @param constraintsFunction function constraining the number of permutations to enumerate
     * @param matchFunction function producing a match result as iterable of type {@code M}
     * @param combineFunction function to produce an iterable of type {@code S} by combining on bound matches of type
     *        {@code M}
     * @param <M> intermediate type to represent match results
     * @param <S> final type to represent match results
     * @return an {@link Iterable} of type {@code S} of matches of {@code this} expression with {@code otherExpression}.
     */
    @Nonnull
    default <M, S> Iterable<S> match(@Nonnull final RelationalExpression otherExpression,
                                     @Nonnull final AliasMap boundAliasMap,
                                     @Nonnull final List<? extends Quantifier> quantifiers,
                                     @Nonnull final List<? extends Quantifier> otherQuantifiers,
                                     @Nonnull final Function<Quantifier, Collection<AliasMap>> constraintsFunction,
                                     @Nonnull final MatchFunction<Quantifier, M> matchFunction,
                                     @Nonnull final CombineFunction<M, S> combineFunction) {
        // This is a cheap and effective great filter that is prone to eliminate non-matching cases hopefully very
        // quickly -- removing this shouldn't change any semantics, just performance
        if (getClass() != otherExpression.getClass()) {
            return ImmutableList.of();
        }

        // If the class is the same ==> this.canCorrelate() == other.canCorrelate()  -- ensure that's the case.
        Verify.verify(canCorrelate() == otherExpression.canCorrelate());

        // We strive to find a binding for every alias in the grand union of all correlatedTo sets of all quantifiers.
        // There are three kinds of correlations on those quantifiers:
        //
        // 1. correlations to quantifiers outside of the sub-graph rooted at this expression
        // 2. correlations among each other, e.g. q2 is correlated to q1 and both are underneath this expression
        // 3. previously bound mappings -- we won't touch those here.
        //
        // We use the terminology "to bind an alias" to denote that we map an alias in this graph to an alias in
        // the other graph.
        // In order to produce an actual match we need to bind all aliases falling into (1) and (2) in a compatible way.
        //
        // For (1) we need to establish unbound deep correlations. We use the .getCorrelatedTo() sets of both expressions
        // and try to match these. That is O(n) where n is the number of unbound deep correlations. In order to restrict
        // the number of correlations we need to enumerate we use the following function to establish bindings absolutely
        // necessary given constraints. See .enumerateConstraintAliases() for further explanations. It's important that
        // a. this method attempts to reduce the degrees of freedom we have for (1)
        // b. calling this method here is not needed for semantic correctness, just for performance
        final Iterable<AliasMap> boundConstraintIterable =
                Quantifiers.enumerateConstraintAliases(boundAliasMap,
                        quantifiers,
                        constraintsFunction,
                        getCorrelatedTo(),
                        otherExpression.getCorrelatedTo());

        // We now attempt to bind all deep correlations that are not bound yet. This results in an iterable over all
        // possible bindings out of (1)
        final Iterable<AliasMap> boundCorrelatedToIterable =
                IterableHelpers.flatMap(boundConstraintIterable,
                        boundConstraintMap -> enumerateUnboundCorrelatedTo(boundConstraintMap, otherExpression));

        // Call the matching logic to compute the matches for each mapping for (1) (and to find bindings for (2)).
        return IterableHelpers.flatMap(boundCorrelatedToIterable,
                boundCorrelatedToMap -> {
                    final Iterable<BoundMatch<EnumeratingIterable<M>>> boundMatchIterable =
                            Quantifiers.match(
                                    boundCorrelatedToMap,
                                    getQuantifiers(),
                                    otherQuantifiers,
                                    matchFunction);
                    return combineFunction.combine(boundCorrelatedToMap, boundMatchIterable);
                });
    }

    /**
     * A functional interface to combine the matches computed over pairs of quantifiers during matching into a
     * result (for the bound correlatedTo set handed into {@link #combine}).
     *
     * Let's assume we have multiple bindings during matching on the deep correlations (.getCorrelatedTo() of this).
     * Let's call that the sets of outer bindings. We also attempt to match the quantifiers owned by this to the
     * quantifiers of other.
     *
     * For each set of outer bindings we enumerate bindings among the owned quantifiers of the respective expressions.
     * Let's call those sets the inner bindings. For each set out outer bindings there are many sets of inner bindings.
     *
     * At the end of matching we want to establish a match between this expression and some other expression and the
     * quantifiers owned by the respective expressions and their bindings among each other do not matter anymore in
     * terms of matching logic. Those bindings only matter for the result of the match under a set of outer bindings.
     *
     * The matching algorithm returns an iterable of some type {@code S} which is computed by a lambda passed in by
     * the caller. It is up to the caller what to do with the outer and inner bindings and how to compute a useful
     * result out of it. The signature of that lambda is defined by this interface.
     *
     * During matching the matching logic calls {@link #combine} for each set of outer bindings with and {@link Iterable}
     * over sets of inner bindings (and their match results).
     *
     * @param <R> type of the match result computed while matching quantifiers
     * @param <S> type of combined match result
     */
    @FunctionalInterface
    interface CombineFunction<R, S> {
        /**
         * Combine the sets of bindings (and their results) under the given set of outer bindings to an iterable of
         * combined results.
         * @param boundCorrelatedToMap set of outer bindings encoded in an {@link AliasMap}
         * @param boundMatches iterable of {@link BoundMatch}es
         * @return an iterable of type {@code S}
         */
        @Nonnull
        Iterable<S> combine(@Nonnull final AliasMap boundCorrelatedToMap,
                            @Nonnull final Iterable<BoundMatch<EnumeratingIterable<R>>> boundMatches);
    }

    /**
     * Method to enumerate all bindings of unbound correlations of this and some other expression.
     *
     * Example:
     *
     * <pre>
     * {@code
     *
     *   this  (correlated to a1, a2, a3)            other (correlated to aa, ab, ac)
     *   /|\                                          /|\
     *  .....                                        .....
     *
     *  }
     * </pre>
     * Example a:
     * <pre>
     * {@code
     *  boundAliasMap: (a2 -> ac)
     *  result:
     *    iterable of:
     *      (a1 -> aa, a3 -> ab)
     *      (a1 -> ab, a3 -> aa)
     * }
     * </pre>
     * Example b:
     * <pre>
     * {@code
     *  boundAliasMap: (empty)
     *  result:
     *    iterable of:
     *      (a1 -> aa, a2 -> ab, a3 -> ac)
     *      (a1 -> aa, a2 -> ac, a3 -> ab)
     *      (a1 -> ab, a2 -> aa, a3 -> ac)
     *      (a1 -> ab, a2 -> ac, a3 -> aa)
     *      (a1 -> ac, a2 -> aa, a3 -> ab)
     *      (a1 -> ac, a2 -> ab, a3 -> aa)
     * }
     * </pre>
     *
     * @param boundAliasMap alias map of bindings that should be considered pre-bound
     * @param otherExpression the other expression
     * @return an iterable of sets of bindings
     */
    @Nonnull
    default Iterable<AliasMap> enumerateUnboundCorrelatedTo(@Nonnull final AliasMap boundAliasMap,
                                                            @Nonnull final RelationalExpression otherExpression) {
        final Set<CorrelationIdentifier> correlatedTo = getCorrelatedTo();
        final Set<CorrelationIdentifier> otherCorrelatedTo = otherExpression.getCorrelatedTo();

        final AliasMap identitiesMap = bindIdentities(otherExpression, boundAliasMap);

        final AliasMap aliasMapWithIdentities = boundAliasMap.combine(identitiesMap);
        final Set<CorrelationIdentifier> unboundCorrelatedTo = Sets.difference(correlatedTo, aliasMapWithIdentities.sources());
        final Set<CorrelationIdentifier> unboundOtherCorrelatedTo = Sets.difference(otherCorrelatedTo, aliasMapWithIdentities.targets());

        return aliasMapWithIdentities
                .findMatches(
                        unboundCorrelatedTo,
                        alias -> ImmutableSet.of(),
                        unboundOtherCorrelatedTo,
                        otherAlias -> ImmutableSet.of(),
                        (alias, otherAlias, nestedEquivalencesMap) -> true);
    }

    /**
     * Given the correlatedTo sets {@code c1} and {@code c2} of this expression and some other expression compute a set
     * of bindings that contains identity bindings ({@code a -> a}) for the intersection of {@code c1} and {@code c2}.
     * @param otherExpression other expression
     * @param boundAliasMap alias map of bindings that should be considered pre-bound meaning that this method does
     *        not include aliases participating in this map into the identities bindings.
     * @return an {@link AliasMap} for containing only identity bindings for the intersection of the correlatedTo set
     *         of this expression and the other expression
     */
    @Nonnull
    default AliasMap bindIdentities(@Nonnull final RelationalExpression otherExpression,
                                    @Nonnull final AliasMap boundAliasMap) {
        final Set<CorrelationIdentifier> correlatedTo = getCorrelatedTo();
        final Set<CorrelationIdentifier> otherCorrelatedTo = otherExpression.getCorrelatedTo();

        Sets.SetView<CorrelationIdentifier> unboundCorrelatedTo = Sets.difference(correlatedTo, boundAliasMap.sources());
        Sets.SetView<CorrelationIdentifier> unboundOtherCorrelatedTo = Sets.difference(otherCorrelatedTo, boundAliasMap.targets());

        final Sets.SetView<CorrelationIdentifier> commonUnbound = Sets.intersection(unboundCorrelatedTo, unboundOtherCorrelatedTo);
        return AliasMap.identitiesFor(commonUnbound);
    }

    /**
     * Try to establish if {@code otherExpression} subsumes this one. If two expression are semantically equal
     * (e.g. in structure or by other means of reasoning) they should exactly return the same records. There are
     * use cases where semantic equality is too strict and not that useful. During index matching we don't necessarily
     * need to know if two expressions produce the same result, we just need to know that the candidate scan produces
     * a (non-proper) super multiset of records (the candidate therefore includes all records warranted by the query)
     * and we can match query against that candidate. The difference between result set produced by the candidate and
     * the query then must be corrected by applying compensation. The following tautologies apply:
     * <ul>
     *     <li>
     *         If query and candidate are semantically equivalent, the query side should match to the candidate side
     *         without any compensation. In other words the query expression can simply be replaced by the candidate
     *         expression.
     *     </li>
     *     <li>
     *         If the candidate is matched and we decide to rewrite this query expression with the appropriate top
     *         expression on the candidate side then it holds that the query expression is equivalent to
     *         the computed compensation of the match over the candidate scan.
     *     </li>
     *     <li>
     *         A query cannot match to a candidate if it cannot be proven that the candidate cannot at least produce
     *         all the records that the query may produce.
     *     </li>
     * </ul>
     * @param candidateExpression the candidate expression
     * @param aliasMap a map of alias defining the equivalence between aliases and therefore quantifiers
     * @param partialMatchMap a map from quantifier to a {@link PartialMatch} that pulled up along that quantifier
     *        from one of the expressions below that quantifier
     * @return an iterable of {@link MatchInfo}s if subsumption between this expression and the candidate expression
     *         can be established
     */
    @Nonnull
    default Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                           @Nonnull final AliasMap aliasMap,
                                           @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        // we don't match by default -- end
        return ImmutableList.of();
    }

    /**
     * Helper method that can be called by sub classes to defer subsumption in a way that the particular expression
     * only matches if it is semantically equivalent.
     * @param candidateExpression the candidate expression
     * @param aliasMap a map of alias defining the equivalence between aliases and therefore quantifiers
     * @param partialMatchMap a map from quantifier to a {@link PartialMatch} that pulled up along that quantifier
     *        from one of the expressions below that quantifier
     * @return an iterable of {@link MatchInfo}s if semantic equivalence between this expression and the candidate
     *         expression can be established
     */
    @Nonnull
    default Iterable<MatchInfo> exactlySubsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                                  @Nonnull final AliasMap aliasMap,
                                                  @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        if (hasUnboundQuantifiers(aliasMap) || hasIncompatibleBoundQuantifiers(aliasMap, candidateExpression.getQuantifiers())) {
            return ImmutableList.of();
        }

        if (equalsWithoutChildren(candidateExpression, aliasMap)) {
            return MatchInfo.tryFromMatchMap(partialMatchMap)
                    .map(ImmutableList::of)
                    .orElse(ImmutableList.of());
        } else {
            return ImmutableList.of();
        }
    }

    /**
     * Override that is called by {@link AdjustMatchRule} to improve an already existing {@link PartialMatch}.
     * @param partialMatch the partial match already existing between {@code expression} and {@code this}
     * @return {@code Optional.empty()} if the match could not be adjusted, Optional.of(matchInfo) for a new adjusted
     *         match, otherwise.
     */
    @Nonnull
    default Optional<MatchInfo> adjustMatch(@Nonnull final PartialMatch partialMatch) {
        return Optional.empty();
    }

    default boolean hasUnboundQuantifiers(final AliasMap aliasMap) {
        return getQuantifiers()
                .stream()
                .map(Quantifier::getAlias)
                .anyMatch(alias -> !aliasMap.containsSource(alias));
    }

    default boolean hasIncompatibleBoundQuantifiers(final AliasMap aliasMap, final Collection<? extends Quantifier> otherQuantifiers) {
        final BiMap<CorrelationIdentifier, Quantifier> otherAliasToQuantifierMap = Quantifiers.toBiMap(otherQuantifiers);
        return getQuantifiers()
                .stream()
                .filter(quantifier -> aliasMap.containsSource(quantifier.getAlias())) // must be bound on this side
                .anyMatch(quantifier -> {
                    final Quantifier otherQuantifier =
                            Objects.requireNonNull(otherAliasToQuantifierMap.get(aliasMap.getTarget(quantifier.getAlias())));
                    return !quantifier.equalsOnKind(otherQuantifier);
                });
    }

    default Compensation compensate(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        throw new RecordCoreException("expression matched but no compensation logic implemented");
    }

    default Set<Quantifier.ForEach> computeUnmatchedForEachQuantifiers(@Nonnull final PartialMatch partialMatch) {
        return ImmutableSet.of();
    }

    /**
     * Compute the semantic hash code of this expression. The logic computing the hash code is agnostic to the order
     * of owned quantifiers.
     * @return the semantic hash code
     */
    @Override
    default int semanticHashCode() {
        return Objects.hash(getQuantifiers()
                        .stream()
                        .map(Quantifier::semanticHashCode)
                        .collect(ImmutableSet.toImmutableSet()),
                hashCodeWithoutChildren());
    }

    /**
     * Apply the given property visitor to this planner expression and its children. Returns {@code null} if
     * {@link PlannerProperty#shouldVisit(RelationalExpression)} called on this expression returns {@code false}.
     * @param visitor a {@link PlannerProperty} visitor to evaluate
     * @param <U> the type of the evaluated property
     * @return the result of evaluating the property on the subtree rooted at this expression
     */
    @Nullable
    default <U> U acceptPropertyVisitor(@Nonnull PlannerProperty<U> visitor) {
        if (visitor.shouldVisit(this)) {
            final List<U> quantifierResults = new ArrayList<>();
            final List<? extends Quantifier> quantifiers = getQuantifiers();
            for (final Quantifier quantifier : quantifiers) {
                quantifierResults.add(quantifier.acceptPropertyVisitor(visitor));
            }

            return visitor.evaluateAtExpression(this, quantifierResults);
        }
        return null;
    }

    /**
     * This is needed for graph integration into IntelliJ as IntelliJ only ever evaluates selfish methods. Add this
     * method as a custom renderer for the type {@link RelationalExpression}. During debugging you can then for instance
     * click show() on an instance and enjoy the query graph it represents rendered in your standard browser.
     * @param renderSingleGroups whether to render group references with just one member
     * @return the String "done"
     */
    @Nonnull
    default String show(final boolean renderSingleGroups) {
        return PlannerGraphProperty.show(renderSingleGroups, this);
    }
}
