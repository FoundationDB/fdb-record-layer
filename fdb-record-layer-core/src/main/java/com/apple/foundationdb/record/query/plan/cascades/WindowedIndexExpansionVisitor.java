/*
 * ScalagAggIndexExpansionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.debug.SymbolDebugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RankValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Class to expand a by-rank index access into a candidate graph. The visitation methods are left unchanged from the super
 * class {@link KeyExpressionExpansionVisitor}, this class merely provides a specific {@link #expand} method.
 */
public class WindowedIndexExpansionVisitor extends KeyExpressionExpansionVisitor implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private final Index index;
    @Nonnull
    private final List<RecordType> recordTypes;

    public WindowedIndexExpansionVisitor(@Nonnull Index index, @Nonnull Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(IndexTypes.RANK.equals(index.getType()));
        this.index = index;
        this.recordTypes = ImmutableList.copyOf(recordTypes);
    }

    /**
     * We expand a rank index into a QGM representing the nature of the index as relational-algebra.
     *
     * <pre>
     * {@code
     * SELECT g1, g2, ..., score
     * FROM T AS outerBase,
     *      (SELECT g1, g2, ..., score, primaryKey
     *       FROM (SELECT FROM(SELECT g1, g2, ..., score, RANK(score PARTITION BY g1, g2) AS rank
     *             FROM T as innerBase) AS rankSelect
     *       WHERE [rankPlaceholder] outerBase.primaryKey = rankSelect.primaryKey)
     * WHERE [g1, g2, ...]
     * }
     *
     * The notation {@code [identifier]} is used to represent an index parameter (or also called a placeholder)
     * which is used for matching.
     *
     * Note that the pseudo-SQL above can vary (and become significantly more complex) in the case where the grouping
     * or the ranking is done over a (nested) repeated. In such a case the {@code innerBase} is the cross product of the
     * explosions of all grouping expressions and the score field.
     * </pre>
     *
     * @param baseQuantifierSupplier a quantifier supplier to create base data access
     * @param primaryKey the primary key of the data object the caller wants to access
     * @param isReverse an indicator whether the result set is expected to be returned in reverse order
     * @return a match candidate for this rank index
     */
    @Nonnull
    @Override
    public MatchCandidate expand(@Nonnull final Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nullable final KeyExpression primaryKey,
                                 final boolean isReverse) {
        var rootExpression = index.getRootExpression();
        Verify.verify(rootExpression instanceof GroupingKeyExpression);

        SymbolDebugger.updateIndex(PredicateWithValueAndRanges.class, old -> 0);
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();

        final var baseQuantifier = baseQuantifierSupplier.get();

        final var baseExpansion =
                GraphExpansion.builder()
                        .pullUpQuantifier(baseQuantifier)
                        .build();

        // add the value for the flow of records
        allExpansionsBuilder.add(baseExpansion);

        final var baseAlias = baseQuantifier.getAlias();

        final var groupingAndArgumentValues = Lists.<Value>newArrayList();
        final var groupingKeyExpression = (GroupingKeyExpression)rootExpression;
        // TODO verify if there is only ever going to be a grouped count of 1, for now assert on it
        Verify.verify(groupingKeyExpression.getGroupedCount() == 1);

        final var innerBaseQuantifier = baseQuantifierSupplier.get();
        final var innerBaseAlias = innerBaseQuantifier.getAlias();
        final var expandGroupingsAndArgumentsResult =
                expandGroupingsAndArguments(baseQuantifier, innerBaseQuantifier, groupingKeyExpression,
                        groupingAndArgumentValues);
        final var rankSelectExpression = expandGroupingsAndArgumentsResult.getExpansion().buildSelect();
        final var rankAlias = expandGroupingsAndArgumentsResult.getRankAlias();
        final var rankQuantifier = Quantifier.forEach(Reference.initialOf(rankSelectExpression));

        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(rankQuantifier));

        final var duplicatedPlaceholders =
                duplicateSimpleGroupingPlaceholders(baseAlias, innerBaseAlias, groupingKeyExpression,
                        expandGroupingsAndArgumentsResult.getGroupingsAndArgumentsPlaceholders(), rankSelectExpression);
        allExpansionsBuilder.add(duplicatedPlaceholders);

        final var primaryKeyAliasesBuilder = ImmutableList.<CorrelationIdentifier>builder();
        final var primaryKeyValues = Lists.<Value>newArrayList();
        if (primaryKey != null) {
            // unfortunately we must copy as the returned list is not guaranteed to be mutable which is needed for the
            // trimPrimaryKey() function as it is causing a side effect
            final List<KeyExpression> trimmedPrimaryKeys = Lists.newArrayList(primaryKey.normalizeKeyForPositions());
            index.trimPrimaryKey(trimmedPrimaryKeys);

            for (final var primaryKeyPart : trimmedPrimaryKeys) {
                final var initialStateForKeyPart =
                        VisitorState.of(primaryKeyValues,
                                Lists.newArrayList(),
                                baseQuantifier,
                                ImmutableList.of(),
                                -1,
                                0,
                                false,
                                false);
                final var primaryKeyPartExpansion =
                        pop(primaryKeyPart.expand(push(initialStateForKeyPart)))
                                .toBuilder()
                                .removeAllResultColumns()
                                .build();
                allExpansionsBuilder.add(primaryKeyPartExpansion);
                primaryKeyAliasesBuilder.addAll(primaryKeyPartExpansion.getPlaceholderAliases());
            }
        }
        final var primaryKeyAliases = primaryKeyAliasesBuilder.build();

        final var indexKeyValues = computeIndexKeyValues(baseAlias, innerBaseAlias, groupingAndArgumentValues, primaryKeyValues);

        final var completeExpansion = GraphExpansion.ofOthers(allExpansionsBuilder.build());
        final var groupingAndArgumentAliases = expandGroupingsAndArgumentsResult.getGroupingsAndArgumentsAliases();
        final var groupingAliases = groupingAndArgumentAliases.subList(0, groupingKeyExpression.getGroupingCount());
        final var scoreAlias = groupingAndArgumentAliases.get(groupingAndArgumentAliases.size() - 1);
        final var matchableSortExpression = new MatchableSortExpression(WindowedIndexScanMatchCandidate.orderingAliases(groupingAliases, scoreAlias, primaryKeyAliases), isReverse, completeExpansion.buildSelect());

        return new WindowedIndexScanMatchCandidate(
                index,
                recordTypes,
                Traversal.withRoot(Reference.initialOf(matchableSortExpression)),
                baseQuantifier.getFlowedObjectType(),
                baseAlias,
                groupingAliases,
                scoreAlias,
                rankAlias,
                primaryKeyAliases,
                indexKeyValues,
                ValueIndexExpansionVisitor.fullKey(index, primaryKey),
                primaryKey);
    }

    @Nonnull
    private List<Value> computeIndexKeyValues(@Nonnull CorrelationIdentifier baseAlias,
                                              @Nonnull CorrelationIdentifier innerBaseAlias,
                                              @Nonnull final List<Value> groupingAndArgumentValues,
                                              @Nonnull final List<Value> primaryKeyValues) {
        final var rebasedGroupingAndArgumentValues =
                groupingAndArgumentValues
                        .stream()
                        .map(value -> value.rebase(AliasMap.ofAliases(innerBaseAlias, baseAlias)))
                        .collect(ImmutableList.toImmutableList());
        return ImmutableList.<Value>builder()
                .addAll(rebasedGroupingAndArgumentValues)
                .addAll(primaryKeyValues)
                .build();
    }

    @Nonnull
    private GraphExpansion duplicateSimpleGroupingPlaceholders(@Nonnull final CorrelationIdentifier baseAlias,
                                                               @Nonnull final CorrelationIdentifier innerBaseAlias,
                                                               @Nonnull final GroupingKeyExpression groupingKeyExpression,
                                                               @Nonnull final List<Placeholder> groupingsAndArgumentsPlaceholders,
                                                               @Nonnull final SelectExpression rankSelectExpression) {
        final var expansions = Lists.<GraphExpansion>newArrayList();

        //
        // Duplicate the simple placeholders of the groupings to the top level select expression
        //
        final var groupingPlaceholders =
                ImmutableSet.copyOf(groupingsAndArgumentsPlaceholders.subList(0, groupingKeyExpression.getGroupingCount()));
        final var rankOtherLocalAliases =
                rankSelectExpression.getQuantifiers()
                        .stream()
                        .map(Quantifier::getAlias)
                        .filter(alias -> !alias.equals(innerBaseAlias))
                        .collect(ImmutableSet.toImmutableSet());

        for (final var predicate : rankSelectExpression.getPredicates()) {
            if (!(predicate instanceof Placeholder)) {
                continue;
            }

            final var placeholder = (PredicateWithValueAndRanges)predicate;

            if (!groupingPlaceholders.contains(placeholder)) {
                continue;
            }

            final var placeHolderCorrelatedTo = placeholder.getCorrelatedTo();

            // placeholder must use the innerBaseAlias
            if (!placeHolderCorrelatedTo.contains(innerBaseAlias)) {
                continue;
            }

            // placeholder must not use any other local aliases as that would prevent us from moving them
            if (!Sets.intersection(placeHolderCorrelatedTo, rankOtherLocalAliases).isEmpty()) {
                continue;
            }

            final var rebasedPlaceholder = (Placeholder)placeholder.rebase(AliasMap.ofAliases(innerBaseAlias, baseAlias));
            expansions.add(GraphExpansion.ofPlaceholder(rebasedPlaceholder));
        }

        return GraphExpansion.ofOthers(expansions);
    }

    @Nonnull
    private ExpandGroupingsAndArgumentsResults expandGroupingsAndArguments(@Nonnull final Quantifier.ForEach baseQuantifier,
                                                                           @Nonnull final Quantifier.ForEach innerBaseQuantifier,
                                                                           @Nonnull final GroupingKeyExpression groupingKeyExpression,
                                                                           @Nonnull final List<Value> groupingAndArgumentValues) {
        final var wholeKeyExpression = groupingKeyExpression.getWholeKey();

        final VisitorState initialState =
                VisitorState.of(groupingAndArgumentValues,
                        Lists.newArrayList(),
                        innerBaseQuantifier,
                        ImmutableList.of(),
                        -1,
                        0,
                        false,
                        false);

        final var partitioningAndArgumentExpansion =
                pop(wholeKeyExpression.expand(push(initialState)));
        final var sealedPartitioningAndArgumentExpansion = partitioningAndArgumentExpansion.seal();

        //
        // Construct a select expression that uses a windowed value to express the rank.
        //
        final var partitioningSize = groupingKeyExpression.getGroupingCount();
        final var partitioningExpressions = sealedPartitioningAndArgumentExpansion.getResultValues().subList(0, partitioningSize);
        final var argumentExpressions = sealedPartitioningAndArgumentExpansion.getResultValues().subList(partitioningSize, groupingKeyExpression.getColumnSize());
        final var rankValue = new RankValue(partitioningExpressions, argumentExpressions);
        final var rankAlias = newParameterAlias();
        final var rankPlaceholder = Placeholder.newInstanceWithoutRanges(rankValue, rankAlias);
        final var selfJoinPredicate =
                innerBaseQuantifier.getFlowedObjectValue()
                        .withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                                QuantifiedObjectValue.of(baseQuantifier.getAlias(), baseQuantifier.getFlowedObjectType())));

        final var expansionBuilder =
                partitioningAndArgumentExpansion.toBuilder()
                        .addPredicate(rankPlaceholder)
                        .addPredicate(selfJoinPredicate)
                        .addPlaceholder(rankPlaceholder)
                        .removeAllResultColumns();

        expansionBuilder.pullUpQuantifier(innerBaseQuantifier);
        partitioningAndArgumentExpansion.getQuantifiers()
                .forEach(quantifier -> expansionBuilder.addAllResultColumns(quantifier.getFlowedColumns()));

        return new ExpandGroupingsAndArgumentsResults(
                expansionBuilder.build(),
                rankAlias,
                partitioningAndArgumentExpansion.getPlaceholderAliases(),
                partitioningAndArgumentExpansion.getPlaceholders());
    }

    private static class ExpandGroupingsAndArgumentsResults {
        @Nonnull
        private final GraphExpansion expansion;
        @Nonnull
        private final CorrelationIdentifier rankAlias;
        @Nonnull
        private final List<CorrelationIdentifier> groupingsAndArgumentsAliases;
        @Nonnull
        private final List<Placeholder> groupingsAndArgumentsPlaceholders;

        public ExpandGroupingsAndArgumentsResults(@Nonnull final GraphExpansion expansion,
                                                  @Nonnull final CorrelationIdentifier rankAlias,
                                                  @Nonnull final List<CorrelationIdentifier> groupingsAndArgumentsAliases,
                                                  @Nonnull final List<Placeholder> groupingsAndArgumentsPlaceholders) {
            this.expansion = expansion;
            this.rankAlias = rankAlias;
            this.groupingsAndArgumentsAliases = groupingsAndArgumentsAliases;
            this.groupingsAndArgumentsPlaceholders = groupingsAndArgumentsPlaceholders;
        }

        @Nonnull
        public GraphExpansion getExpansion() {
            return expansion;
        }

        @Nonnull
        public CorrelationIdentifier getRankAlias() {
            return rankAlias;
        }

        @Nonnull
        public List<CorrelationIdentifier> getGroupingsAndArgumentsAliases() {
            return groupingsAndArgumentsAliases;
        }

        @Nonnull
        public List<Placeholder> getGroupingsAndArgumentsPlaceholders() {
            return groupingsAndArgumentsPlaceholders;
        }
    }
}
