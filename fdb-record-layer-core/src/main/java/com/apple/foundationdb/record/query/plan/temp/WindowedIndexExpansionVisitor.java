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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.RankValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;

/**
 * Class to expand value index access into a candidate graph. The visitation methods are left unchanged from the super
 * class {@link KeyExpressionExpansionVisitor}, this class merely provides a specific {@link #expand} method.
 */
public class WindowedIndexExpansionVisitor extends KeyExpressionExpansionVisitor implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private final Index index;
    @Nonnull
    private final List<RecordType> recordTypes;

    public WindowedIndexExpansionVisitor(@Nonnull Index index, @Nonnull Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(index.getType().equals(IndexTypes.RANK));
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
     * or the ranking is done over a nested repeated. In such a case the {@code innerBase} is the cross product of the
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

        Debugger.updateIndex(Placeholder.class, old -> 0);
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();

        final var baseQuantifier = baseQuantifierSupplier.get();

        // add the value for the flow of records
        final var recordValue = QuantifiedColumnValue.of(baseQuantifier.getAlias(), 0);
        allExpansionsBuilder.add(GraphExpansion.ofResultValueAndQuantifier(recordValue, baseQuantifier));

        final var baseAlias = baseQuantifier.getAlias();

        final List<Value> groupingAndArgumentValues = Lists.newArrayList();
        final var groupingKeyExpression = (GroupingKeyExpression)rootExpression;
        final var innerBaseQuantifier = baseQuantifierSupplier.get();
        final var innerBaseAlias = innerBaseQuantifier.getAlias();
        final var rankGroupingsAndArgumentsExpansion =
                expandGroupingsAndArguments(innerBaseQuantifier, groupingKeyExpression, groupingAndArgumentValues);
        final var rankSelectExpression = rankGroupingsAndArgumentsExpansion.buildSelect();
        final var rankQuantifier = Quantifier.forEach(GroupExpressionRef.of(rankSelectExpression));

        //
        // Construct another select expression that applies the predicate on the rank value as well as adds a join
        // predicate between the outer and the inner base as we model this index access as a semi join
        //
        // predicate on rank is expressed as an index placeholder
        final var rankColumnValue = QuantifiedColumnValue.of(rankQuantifier.getAlias(), rankSelectExpression.getResultValues().size() - 1);
        final var rankAndJoiningPredicateExpansion = buildRankComparisonSelectExpression(baseAlias, rankQuantifier, rankColumnValue);
        final var rankAndJoiningPredicateSelectExpression = rankAndJoiningPredicateExpansion.buildSelect();
        final var rankComparisonQuantifier =
                Quantifier.forEach(GroupExpressionRef.of(rankAndJoiningPredicateSelectExpression));

        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(rankComparisonQuantifier));

        final var duplicatedPlaceholders =
                duplicateSimpleGroupingPlaceholders(baseAlias, innerBaseAlias, groupingKeyExpression, rankGroupingsAndArgumentsExpansion.getPlaceholders(), rankSelectExpression);
        allExpansionsBuilder.add(duplicatedPlaceholders);

        if (primaryKey != null) {
            // unfortunately we must copy as the returned list is not guaranteed to be mutable which is needed for the
            // trimPrimaryKey() function as it is causing a side-effect
            final List<KeyExpression> trimmedPrimaryKeys = Lists.newArrayList(primaryKey.normalizeKeyForPositions());
            index.trimPrimaryKey(trimmedPrimaryKeys);

            for (int i = 0; i < trimmedPrimaryKeys.size(); i++) {
                final KeyExpression primaryKeyPart = trimmedPrimaryKeys.get(i);

                final VisitorState initialStateForKeyPart =
                        VisitorState.of(groupingAndArgumentValues,
                                Lists.newArrayList(),
                                baseQuantifier.getAlias(),
                                ImmutableList.of(),
                                -1,
                                0);
                final GraphExpansion primaryKeyPartExpansion =
                        pop(primaryKeyPart.expand(push(initialStateForKeyPart)));
                allExpansionsBuilder.add(primaryKeyPartExpansion);
            }
        }

        final var completeExpansion = GraphExpansion.ofOthers(allExpansionsBuilder.build());
        final var parameters = completeExpansion.getPlaceholderAliases();
        final var matchableSortExpression = new MatchableSortExpression(parameters, isReverse, completeExpansion.buildSelect());
        return new WindowedIndexScanMatchCandidate(index,
                recordTypes,
                ExpressionRefTraversal.withRoot(GroupExpressionRef.of(matchableSortExpression)),
                parameters,
                recordValue,
                groupingAndArgumentValues,
                ImmutableList.of(),
                fullKey(index, primaryKey));
    }

    @Nonnull
    private GraphExpansion duplicateSimpleGroupingPlaceholders(final CorrelationIdentifier baseAlias,
                                                               final CorrelationIdentifier innerBaseAlias,
                                                               final GroupingKeyExpression groupingKeyExpression,
                                                               final List<Placeholder> groupingsAndArgumentsPlaceholders,
                                                               final SelectExpression rankSelectExpression) {
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

            final var placeholder = (Placeholder)predicate;

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

            final var rebasedPlaceholder = (Placeholder)placeholder.rebase(AliasMap.of(innerBaseAlias, baseAlias));
            expansions.add(GraphExpansion.ofResultValueAndPlaceholder(rebasedPlaceholder.getValue(), rebasedPlaceholder));
        }

        return GraphExpansion.ofOthers(expansions);
    }

    @Nonnull
    private GraphExpansion buildRankComparisonSelectExpression(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final Quantifier.ForEach rankQuantifier, @Nonnull final QuantifiedColumnValue rankColumnValue) {
        // hold on to the placeholder for later
        final var rankPlaceholder = rankColumnValue.asPlaceholder(newParameterAlias());

        final var rankComparisonExpansion =
                GraphExpansion.ofPredicateAndQuantifier(rankPlaceholder, rankQuantifier);

        // join predicate
        // TODO I do not like the way this predicate is currently built. We should instead create individual predicates
        //      for each part of the identifying common parts of the record (i.e. a key on both sides). That can be
        //      the common primary key of the outer and inner but we don't know that here as it's not yet part of
        //      the information accessible on the quantifier and taking the common primary key of this query from
        //      the plan context is not necessarily correct in the future when we project and mutate things within
        //      the query.
        final var selfJoinPredicate =
                new QuantifiedObjectValue(rankQuantifier.getAlias())
                        .withComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS,
                                Bindings.Internal.CORRELATION.bindingName(baseAlias.toString()), Bindings.Internal.CORRELATION));
        final var selfJoinPredicateExpansion = GraphExpansion.ofPredicate(selfJoinPredicate);

        return GraphExpansion.ofOthers(ImmutableList.of(rankComparisonExpansion, selfJoinPredicateExpansion));
    }

    @Nonnull
    private GraphExpansion expandGroupingsAndArguments(@Nonnull final Quantifier.ForEach innerBaseQuantifier,
                                                       @Nonnull final GroupingKeyExpression groupingKeyExpression,
                                                       @Nonnull final List<Value> groupingAndArgumentValues) {
        final var wholeKeyExpression = groupingKeyExpression.getWholeKey();

        final VisitorState initialState =
                VisitorState.of(groupingAndArgumentValues,
                        Lists.newArrayList(),
                        innerBaseQuantifier.getAlias(),
                        ImmutableList.of(),
                        -1,
                        0);

        final var partitioningAndArgumentExpansion =
                pop(wholeKeyExpression.expand(push(initialState)));

        //
        // Construct a select expression that uses a windowed value to express the rank.
        //
        final var partitioningSize = groupingKeyExpression.getGroupingCount();
        final var partitioningExpressions = partitioningAndArgumentExpansion.getResultValues().subList(0, partitioningSize);
        final var argumentExpressions = partitioningAndArgumentExpansion.getResultValues().subList(partitioningSize, groupingKeyExpression.getColumnSize());
        final var rankValue = new RankValue(partitioningExpressions, argumentExpressions);
        return GraphExpansion.ofOthers(ImmutableList.of(partitioningAndArgumentExpansion,
                        GraphExpansion.ofResultValueAndQuantifier(rankValue, innerBaseQuantifier)));
    }

    /**
     * Compute the full key of an index (given that the index is a value index).
     *
     * @param index index to be expanded
     * @param primaryKey primary key of the records the index ranges over. The primary key is used to determine
     *        parts in the index definition that already contain parts of the primary key. All primary key components
     *        that are not already part of the index key are appended to the index key.
     * @return a {@link KeyExpression} describing the <em>full</em> index key as stored
     */
    @Nonnull
    public static KeyExpression fullKey(@Nonnull Index index, @Nullable final KeyExpression primaryKey) {
        if (primaryKey == null) {
            return index.getRootExpression();
        }
        final ArrayList<KeyExpression> trimmedPrimaryKeyComponents = new ArrayList<>(primaryKey.normalizeKeyForPositions());
        index.trimPrimaryKey(trimmedPrimaryKeyComponents);
        final ImmutableList.Builder<KeyExpression> fullKeyListBuilder = ImmutableList.builder();
        fullKeyListBuilder.add(index.getRootExpression());
        fullKeyListBuilder.addAll(trimmedPrimaryKeyComponents);
        return concat(fullKeyListBuilder.build());
    }
}
