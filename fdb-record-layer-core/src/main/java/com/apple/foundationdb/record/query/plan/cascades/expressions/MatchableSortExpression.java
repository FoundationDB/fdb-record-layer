/*
 * MatchableSortExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.rules.AdjustMatchRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.RemoveSortRule;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A relational planner expression that represents an unimplemented sort on the records produced by its inner
 * relational planner expression.
 *
 * TODO BEGIN
 * This class is somewhat flawed. There is a cousin of this class {@link LogicalSortExpression} which serves a similar
 * purpose conceptually but is still technically quite different. {@link LogicalSortExpression} serves as the logical
 * precursor to a physical implementation of a sort (which we do not (yet) have). So in order for the planner to produce
 * an executable plan, that sort expression has to be optimized away by e.g. {@link RemoveSortRule}. This class
 * is different in a way that it an only be used as an expression to be matched against. In other words this class is
 * only ever allowed to appear in a match candidate on the expression side. The reason why that is comes down to the
 * simple question of how to express the thing(s) to sort on in general and what to do with nested repeated value in
 * particular.
 *
 * What should we be able to express when we express order? One thing we definitely want to sort by is a scalar
 * value that can be computed from the record or an index key that flows into the operator. On the contrary we should
 * not be able to sort by something like {@code field("a", FanOut)} as it really does not make sense
 * in this context. What does make sense is to say that you want the stream to be ordered by {@code field("a", FanOut)},
 * but you actually are expressing a requirement on data computed on a record before it even flows into the sort, i.e.
 * it cannot be computed (conceptually) in this operator. The nested repeated field {@code a} needs to come from either
 * a physical version of an {@link ExplodeExpression} (which we don't have (yet)) or from an index access as index keys
 * can be constructed in this way. The expressiveness that is fundamentally lacking here is to model what data flows,
 * how it flows and what operations have been applied to it since it was generated. There are other places where exactly
 * this is a problem as well. Specifically, for the sort expression, however, this problem manifests itself as:
 *
 * (1) Either the sort is expressed by using {@link KeyExpression}s. In that case, expressing a nested repeated value
 *     in the sort expression itself does not make sense. Again, we should model such a case as a sort over an value
 *     that has been pre-produced by a cardinality-changing operator such as a physical variant of explode or by
 *     explicitly referring to and modelling the index keys that can flow along with the fetched record. Note that this
 *     of course has far-reaching implications for optimizations that attempt to defer the fetch of a record.
 *     Essentially, we would like to say
 *
 *     <pre>
 *     index: {@code field(a, FanOut)}
 *     plan: {@code Sort(IndexScan(index), a [5, 10]), indexkey.a)}
 *     </pre>
 *
 *     i.e. we want to sort by the index key. We do not want to model that plan like
 *     <pre>
 *     plan: {@code Sort(IndexScan(index), a [5, 10]), record.field("a", FanOut))}
 *     </pre>
 *
 *     where the sort expression (even only if conceptually) extracts the {@code a}s from the base records as the base
 *     record at that moment already may contain duplicates of the base record due to using that index in the underlying
 *     scan.
 * (2) Alternatively, we can express the sort by using explicit index scan-provided values. You simply cannot sort by
 *     anything that does not come from an index or a primary scan (the latter cannot really sort by anything that's
 *     repeated due to the key being a primary key). That works for the current landscape as there is no physical sort
 *     (yet) and it overcomes the problem explained above in a way that we can express to sort by anything the
 *     index scan (or primary scan) produces as opposed to something that can be computed on-the-fly based upon the
 *     base record. This also removes the requirement of the base record to be fetched and present at the the time the
 *     sort happens (conceptually) and allows for deferred-fetch optimizations.
 *
 * As a direct result of this we now have two different logical sort expressions. One, {@link LogicalSortExpression}
 * which expresses order by using {@link KeyExpression}s and which has the problems layed out in (1), and  one
 * {@link MatchableSortExpression} which expresses order by explicitly naming the constituent parts of an index.
 * In the future, we should strive to unify these two classes to one logical sort expression. For now, we have
 * a logical sort expression ({@link LogicalSortExpression}) on the query side and a matchable sort expression
 * ({@link MatchableSortExpression}) on the match candidate side.
 * TODO END
 *
 * When it comes to matching this class does not (yet) subsume a {@link LogicalSortExpression} although it probably
 * should be able to just for orthogonality of how rules can be applied and the search space for transformations
 * and matching is explored. As an expression of this class never transforms into a physical sort (even if
 * we had one, this expression would not be transformed into a a physical operator). In a sense this expression
 * expresses a property of the flowing data stream (which ultimately comes from an ordered scan). This property
 * is picked up by {@link AdjustMatchRule} meaning that this operator can match "in-between" expressions on the query
 * expression side (homomorphic matching) by improving the existing match memo by adorning it using the correct order-by
 * property.
 */
@API(API.Status.EXPERIMENTAL)
public class MatchableSortExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {
    /**
     * A list of {@link CorrelationIdentifier}s that refer to parameter ids of this match candidate. This
     * restricts the expressiveness of this operator to only use index keys (or non-repeateds in primary scans) to
     * express order.
     */
    @Nonnull
    private final List<CorrelationIdentifier> sortParameterIds;

    /**
     * Indicator whether this expression should conceptually flow data according to {@link #sortParameterIds} ascending
     * (forward) or descending (backward or reverse).
     */
    private final boolean isReverse;

    /**
     * Quantifier over expression dag that produces the stream this expression sorts.
     */
    @Nonnull
    private final Quantifier inner;

    /**
     * Overloaded constructor. Creates a new {@link MatchableSortExpression}
     * @param sortParameterIds a list of parameter ids defining the order
     * @param isReverse an indicator whether this expression should conceptually flow data according to
     *        {@link #sortParameterIds} ascending (forward) or descending (backward or reverse).
     * @param innerExpression expression dag that produces the stream this expression sorts
     */
    public MatchableSortExpression(@Nonnull final List<CorrelationIdentifier> sortParameterIds,
                                   final boolean isReverse,
                                   @Nonnull final RelationalExpression innerExpression) {
        this(sortParameterIds, isReverse, Quantifier.forEach(Reference.of(innerExpression)));
    }

    /**
     * Overloaded constructor. Creates a new {@link MatchableSortExpression}
     * @param sortParameterIds a list of parameter ids defining the order
     * @param isReverse an indicator whether this expression should conceptually flow data according to
     *        {@link #sortParameterIds} ascending (forward) or descending (backward or reverse).
     * @param inner quantifier ranging over an expression dag that produces the stream this expression sorts
     */
    public MatchableSortExpression(@Nonnull final List<CorrelationIdentifier> sortParameterIds,
                                   final boolean isReverse,
                                   @Nonnull final Quantifier inner) {
        this.sortParameterIds = ImmutableList.copyOf(sortParameterIds);
        this.isReverse = isReverse;
        this.inner = inner;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(getInner());
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    public List<CorrelationIdentifier> getSortParameterIds() {
        return sortParameterIds;
    }

    public boolean isReverse() {
        return isReverse;
    }

    @Nonnull
    private Quantifier getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public MatchableSortExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                         @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new MatchableSortExpression(getSortParameterIds(),
                isReverse(),
                Iterables.getOnlyElement(translatedQuantifiers));
    }

    @Nonnull
    @Override
    public Optional<MatchInfo> adjustMatch(@Nonnull final PartialMatch partialMatch) {
        final var matchInfo = partialMatch.getMatchInfo();
        return Optional.of(matchInfo.withOrderingInfo(forPartialMatch(partialMatch)));
    }

    /**
     * This synthesizes a list of {@link MatchedOrderingPart}s from the current partial match and the ordering information
     * contained in this expression. It delegates to {@link MatchCandidate#computeMatchedOrderingParts} to do this work as
     * while there is a lot of commonality across different index kinds, special indexes may need to define and declare
     * their order in a specific unique way.
     * @param partialMatch the pre-existing partial match on {@code (expression, this)} that the caller wants to adjust.
     * @return a list of bound key parts that express the order of the outgoing data stream and their respective mappings
     *         between query and match candidate
     */
    @Nonnull
    private List<MatchedOrderingPart> forPartialMatch(@Nonnull PartialMatch partialMatch) {
        final var matchCandidate = partialMatch.getMatchCandidate();
        return matchCandidate.computeMatchedOrderingParts(partialMatch.getMatchInfo(), getSortParameterIds(), isReverse());
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }

        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        final var other = (MatchableSortExpression) otherExpression;

        return isReverse == other.isReverse && !sortParameterIds.equals(other.sortParameterIds);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getSortParameterIds(), isReverse());
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.SORT_OPERATOR,
                        ImmutableList.of("BY {{expression}}"),
                        ImmutableMap.of("expression", Attribute.gml(sortParameterIds.toString()))),
                childGraphs);
    }
}
