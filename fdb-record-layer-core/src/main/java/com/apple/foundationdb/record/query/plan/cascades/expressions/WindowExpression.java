/*
 * WindowExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.GroupByMappings;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.WithIndentationsExplainFormatter;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A logical window expression that represents applying a window function over partitioned and ordered input tuples.
 *
 * <p>The expression computes exactly one {@link WindowValue} and flows it alongside the additional result values the rest
 * of the query still needs. The {@code PARTITION BY} columns are kept separately from those because they describe how
 * the input has to be clustered rather than what the expression flows, and the requested ordering states how the input
 * has to be sorted within a partition; together they are the arrangement the planner has to satisfy for the window
 * function to be computable.
 *
 * <p>Partitioning by a column does <em>not</em> flow it: the partitioning values are stated over the input quantifier, so
 * the operator can read them from its input whether or not they appear in its output. A partitioning column that
 * something above the window expression still needs has to be listed as an additional result value like any other column
 * -- which is what the match candidate does, because its partitioning columns are sargable and so have to be reachable
 * further up.
 */
@API(API.Status.EXPERIMENTAL)
public class WindowExpression extends AbstractRelationalExpressionWithChildren implements InternalPlannerGraphRewritable {

    @Nonnull
    private final WindowValue windowValue;

    @Nonnull
    private final List<Value> partitioningValues;

    @Nonnull
    private final List<Value> additionalResultValues;

    @Nonnull
    private final List<WindowOrderingPart> windowOrderingParts;

    @Nonnull
    private final Supplier<RequestedOrdering> requestedOrderingSupplier;

    @Nonnull
    private final Quantifier innerQuantifier;

    @Nonnull
    private final Supplier<Value> resultValueSupplier;

    public WindowExpression(@Nonnull final WindowValue windowValue,
                            @Nonnull final List<Value> partitioningValues,
                            @Nonnull final List<Value> additionalResultValues,
                            @Nonnull final List<WindowOrderingPart> windowOrderingParts,
                            @Nonnull final Quantifier innerQuantifier) {
        this.windowValue = windowValue;
        this.partitioningValues = ImmutableList.copyOf(partitioningValues);
        this.additionalResultValues = ImmutableList.copyOf(additionalResultValues);
        this.windowOrderingParts = ImmutableList.copyOf(windowOrderingParts);
        this.requestedOrderingSupplier = this::computeRequestedOrderingParts;
        this.innerQuantifier = innerQuantifier;
        this.resultValueSupplier = Suppliers.memoize(this::computeResultValue);
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        final var builder = ImmutableSet.<CorrelationIdentifier>builder();
        builder.addAll(windowValue.getCorrelatedTo());
        for (final var partitioningValue : partitioningValues) {
            builder.addAll(partitioningValue.getCorrelatedTo());
        }
        for (final var additionalResultValue : additionalResultValues) {
            builder.addAll(additionalResultValue.getCorrelatedTo());
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValueSupplier.get();
    }

    @Nonnull
    private Value computeResultValue() {
        final var columns = ImmutableList.<Column<? extends Value>>builder();
        final var takenNames = Sets.<String>newHashSet();
        for (final var partitioningValue : partitioningValues) {
            columns.add(Column.of(columnName(partitioningValue, takenNames), partitioningValue));
        }
        for (final var additionalResultValue : additionalResultValues) {
            columns.add(Column.of(columnName(additionalResultValue, takenNames), additionalResultValue));
        }
        columns.add(Column.unnamedOf(windowValue));
        return RecordConstructorValue.ofColumns(columns.build());
    }

    @Nonnull
    private static Optional<String> columnName(@Nonnull final Value value, @Nonnull final Set<String> takenNames) {
        if (!(value instanceof FieldValue)) {
            return Optional.empty();
        }
        // two columns reading the same field name by different paths would make the record type ambiguous
        return ((FieldValue)value).getLastFieldName().filter(takenNames::add);
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(innerQuantifier);
    }

    @Nonnull
    public Quantifier getInnerQuantifier() {
        return innerQuantifier;
    }

    @Nonnull
    public WindowValue getWindowValue() {
        return windowValue;
    }

    @Nonnull
    public List<Value> getPartitioningValues() {
        return partitioningValues;
    }

    @Nonnull
    public List<Value> getAdditionalResultValues() {
        return additionalResultValues;
    }

    @Nonnull
    public RequestedOrdering getRequestedOrdering() {
        return requestedOrderingSupplier.get();
    }

    @Nonnull
    private RequestedOrdering computeRequestedOrderingParts() {
        if (partitioningValues.isEmpty() && getWindowOrderingParts().isEmpty()) {
            return RequestedOrdering.preserve();
        }

        // get correlations of all values relevant for ordering.
        final var correlatedTos = Streams.concat(getWindowOrderingParts().stream().map(WindowOrderingPart::getValue),
                        partitioningValues.stream())
                .flatMap(value -> value.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
        Verify.verify(correlatedTos.size() <= 1, "window ordering parts must be correlated to at most one alias");

        if (correlatedTos.isEmpty()) {
            return RequestedOrdering.preserve();
        }

        final var toCurrentAliasMap = AliasMap.ofAliases(Iterables.getOnlyElement(correlatedTos), Quantifier.current());

        final var requestedPartitioningOrderingPartsStream = partitioningValues.stream()
                .map(partitioningValue -> {
                    final var rebasedPartitioningValue = partitioningValue.rebase(toCurrentAliasMap);
                    return new OrderingPart.RequestedOrderingPart(rebasedPartitioningValue,
                            OrderingPart.RequestedSortOrder.ANY); // no specific requested direction for partitioning columns.
                });

        final var requestedWindowOrderingPartsStream = getWindowOrderingParts().stream()
                .map(windowOrderingPart -> {
                    final var rebasedWindowOrderingValue = windowOrderingPart.getValue().rebase(toCurrentAliasMap);
                    return new OrderingPart.RequestedOrderingPart(rebasedWindowOrderingValue,
                            windowOrderingPart.getSortOrder());
                });

        final var requestedOrderingParts = Streams.concat(requestedPartitioningOrderingPartsStream,
                requestedWindowOrderingPartsStream).collect(ImmutableList.toImmutableList());

        return RequestedOrdering.ofParts(requestedOrderingParts, RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS,
                false, innerQuantifier.getCorrelatedTo());
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        if (this == other) {
            return true;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        final var otherWindowExpr = (WindowExpression)other;

        if (!getResultValue().semanticEquals(otherWindowExpr.getResultValue(), equivalences)) {
            return false;
        }

        if (getWindowOrderingParts().size() != otherWindowExpr.getWindowOrderingParts().size()) {
            return false;
        }

        return Streams.zip(getWindowOrderingParts().stream(), otherWindowExpr.getWindowOrderingParts().stream(),
                (thisOrderingPart, thatOrderingPart) ->
                        thisOrderingPart.semanticEquals(thatOrderingPart, equivalences)).allMatch(b -> b);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getResultValue(), getWindowOrderingParts());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object other) {
        return semanticEquals(other);
    }

    /**
     * A window expression is subsumed by a candidate that computes the very same window function over the very same
     * partitions. There is no room for compensation: a {@link WindowValue} is index-only, so if the candidate does not
     * materialize exactly this value there is nowhere else for it to come from.
     *
     * <p>Partitioning is compared as a set rather than as a list -- the columns bound the partitions, and their order
     * within the {@code PARTITION BY} does not change which rows share one. The ordering inside a partition, on the
     * other hand, is what the ranking is over, so it has to be the ordering the candidate provides.
     */
    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                         @Nonnull final AliasMap bindingAliasMap,
                                         @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                         @Nonnull final EvaluationContext evaluationContext) {
        if (candidateExpression.getClass() != getClass()) {
            return ImmutableList.of();
        }
        final var candidateWindowExpression = (WindowExpression)candidateExpression;
        final var candidateInnerQuantifier = candidateWindowExpression.getInnerQuantifier();

        if (!(innerQuantifier instanceof Quantifier.ForEach innerForEachQun)
                || !(candidateInnerQuantifier instanceof Quantifier.ForEach candidateInnerForEachQun)) {
            return ImmutableList.of();
        }

        final var candidateAlias = bindingAliasMap.getTarget(innerQuantifier.getAlias());
        if (candidateAlias == null) {
            return ImmutableList.of();
        }
        Verify.verify(candidateAlias.equals(candidateInnerQuantifier.getAlias()));

        if (innerForEachQun.isNullOnEmpty() != candidateInnerForEachQun.isNullOnEmpty()) {
            return ImmutableList.of();
        }

        final var translationMapOptional = RelationalExpression.pullUpAndComposeTranslationMapsMaybe(candidateExpression,
                bindingAliasMap, partialMatchMap);
        if (translationMapOptional.isEmpty()) {
            return ImmutableList.of();
        }
        final var translationMap = translationMapOptional.get();

        final var valueEquivalence = ValueEquivalence.fromAliasMap(bindingAliasMap)
                        .then(ValueEquivalence.constantEquivalenceWithEvaluationContext(evaluationContext));

        final var translatedWindowValue = windowValue.translateCorrelations(translationMap, true);
        final var windowValuesMatch = translatedWindowValue.semanticEquals(candidateWindowExpression.getWindowValue(),
                valueEquivalence);
        if (windowValuesMatch.isFalse()) {
            return ImmutableList.of();
        }

        final var partitioningMatches = partitioningSubsumedBy(candidateWindowExpression, translationMap, valueEquivalence);
        if (partitioningMatches.isFalse()) {
            return ImmutableList.of();
        }

        final var translatedResultValue = getResultValue().translateCorrelations(translationMap, true);
        final var maxMatchMap =
                MaxMatchMap.compute(translatedResultValue, candidateExpression.getResultValue(),
                        Quantifiers.aliases(candidateExpression.getQuantifiers()), valueEquivalence);
        final var queryPlanConstraint =
                windowValuesMatch.getConstraint()
                        .compose(partitioningMatches.getConstraint())
                        .compose(maxMatchMap.getQueryPlanConstraint());

        return MatchInfo.RegularMatchInfo.tryMerge(bindingAliasMap, partialMatchMap, ImmutableMap.of(),
                        PredicateMap.empty(), maxMatchMap, GroupByMappings.empty(), null, queryPlanConstraint)
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    /**
     * Whether this expression partitions by the same columns as the given candidate, in any order. Every partitioning
     * column has to find a distinct counterpart, so the two sides bound exactly the same partitions.
     */
    @Nonnull
    private ConstrainedBoolean partitioningSubsumedBy(@Nonnull final WindowExpression candidateWindowExpression,
                                                      @Nonnull final TranslationMap translationMap,
                                                      @Nonnull final ValueEquivalence valueEquivalence) {
        final var candidatePartitioningValues = candidateWindowExpression.getPartitioningValues();
        if (partitioningValues.size() != candidatePartitioningValues.size()) {
            return ConstrainedBoolean.falseValue();
        }
        final var unmatchedCandidateValues = new LinkedHashSet<>(candidatePartitioningValues);
        var result = ConstrainedBoolean.alwaysTrue();
        for (final var partitioningValue : partitioningValues) {
            final var translatedPartitioningValue = partitioningValue.translateCorrelations(translationMap, true);
            var found = false;
            for (final var iterator = unmatchedCandidateValues.iterator(); iterator.hasNext(); ) {
                final var semanticEquals =
                        translatedPartitioningValue.semanticEquals(iterator.next(), valueEquivalence);
                if (semanticEquals.isTrue()) {
                    iterator.remove();
                    result = result.composeWithOther(semanticEquals);
                    found = true;
                    break;
                }
            }
            if (!found) {
                return ConstrainedBoolean.falseValue();
            }
        }
        return unmatchedCandidateValues.isEmpty() ? result : ConstrainedBoolean.falseValue();
    }

    /**
     * Compensation for a matched window expression can only ever reshape the records the match produces, never change
     * which ones there are. The window value is read straight out of the index, so it has already been computed over
     * whatever rows the index holds by the time any compensation runs: a predicate that still has to be applied below
     * the window would filter rows the window function was computed over, and reapplying it above the match cannot undo
     * that. Such a match is rejected rather than compensated.
     */
    @Nonnull
    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch,
                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                   @Nullable final PullUp pullUp,
                                   @Nonnull final CorrelationIdentifier candidateAlias) {
        final var regularMatchInfo = partialMatch.getRegularMatchInfo();

        final var nestedPullUpPair = partialMatch.nestPullUp(pullUp, candidateAlias);
        final var rootOfMatchPullUp = nestedPullUpPair.getKey();
        final var adjustedPullUp = Objects.requireNonNull(nestedPullUpPair.getRight());

        final var childPartialMatchOptional =
                regularMatchInfo.getChildPartialMatchMaybe(innerQuantifier);
        if (childPartialMatchOptional.isEmpty()) {
            return Compensation.impossibleCompensation();
        }

        final var bindingAliasMap = regularMatchInfo.getBindingAliasMap();
        final var childCompensation =
                childPartialMatchOptional.get()
                        .compensate(boundParameterPrefixMap, adjustedPullUp,
                                Objects.requireNonNull(bindingAliasMap.getTarget(innerQuantifier.getAlias())));

        //
        // Anything the child still has to filter would have to be applied on top of the window value the index already
        // computed, which is not the same query. Note that a predicate on a partitioning column would in fact be safe
        // to apply either way, but such a predicate is sargable on the candidate and hence never lands here.
        //
        if (childCompensation.isImpossible() ||
                !childCompensation.canBeDeferred() ||
                childCompensation.isNeededForFiltering()) {
            return Compensation.impossibleCompensation();
        }

        final var compensatedResultOptional =
                Compensation.computeResultCompensation(partialMatch, rootOfMatchPullUp);
        if (compensatedResultOptional.isEmpty()) {
            return Compensation.impossibleCompensation();
        }
        final var compensatedResult = compensatedResultOptional.get();
        if (!childCompensation.isNeeded() &&
                !compensatedResult.getResultCompensationFunction().isNeeded()) {
            return Compensation.noCompensation();
        }

        final var unmatchedQuantifiers = partialMatch.getUnmatchedQuantifiers();
        Verify.verify(unmatchedQuantifiers.isEmpty());

        return childCompensation.derived(compensatedResult.isCompensationImpossible(),
                new LinkedIdentityMap<>(),
                getMatchedQuantifiers(partialMatch),
                unmatchedQuantifiers,
                partialMatch.getCompensatedAliases(),
                compensatedResult.getResultCompensationFunction(),
                compensatedResult.getGroupByMappings());
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                     final boolean shouldSimplifyValues,
                                                     @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 1);
        final var translatedWindowValue =
                (WindowValue)windowValue.translateCorrelations(translationMap, shouldSimplifyValues);
        final var translatedPartitioningValues = partitioningValues.stream()
                .map(value -> value.translateCorrelations(translationMap, shouldSimplifyValues))
                .collect(ImmutableList.toImmutableList());
        final var translatedPassThroughValues = additionalResultValues.stream()
                .map(value -> value.translateCorrelations(translationMap, shouldSimplifyValues))
                .collect(ImmutableList.toImmutableList());
        final var translatedWindowOrderingParts = getWindowOrderingParts().stream()
                .map(windowOrderingPart -> windowOrderingPart.translateCorrelations(translationMap, shouldSimplifyValues))
                .collect(ImmutableList.toImmutableList());
        return new WindowExpression(translatedWindowValue, translatedPartitioningValues, translatedPassThroughValues,
                translatedWindowOrderingParts, Iterables.getOnlyElement(translatedQuantifiers));
    }

    @Override
    public String toString() {
        return "Window(" + windowValue + ", partitioning: " + partitioningValues + ")";
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        final var formatter = WithIndentationsExplainFormatter.forDot(5);
        final var resultString = "WINDOW " + getResultValue().explain().getExplainTokens().render(formatter);

        final var detailsBuilder = ImmutableList.<String>builder();
        if (!partitioningValues.isEmpty()) {
            detailsBuilder.add("PARTITION BY " + partitioningValues);
        }
        final var requestedOrdering = getRequestedOrdering();
        if (!requestedOrdering.isPreserve()) {
            detailsBuilder.add("ORDER BY " + requestedOrdering);
        }

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        resultString,
                        detailsBuilder.build(),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Nonnull
    public List<WindowOrderingPart> getWindowOrderingParts() {
        return windowOrderingParts;
    }
}
