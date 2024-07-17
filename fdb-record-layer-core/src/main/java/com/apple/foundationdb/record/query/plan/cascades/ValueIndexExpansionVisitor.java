/*
 * ValueIndexExpansionVisitor.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;

/**
 * Class to expand value index access into a candidate graph. The visitation methods are left unchanged from the super
 * class {@link KeyExpressionExpansionVisitor}, this class merely provides a specific {@link #expand} method.
 */
public class ValueIndexExpansionVisitor extends KeyExpressionExpansionVisitor implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private static final Set<String> SUPPORTED_INDEX_TYPES = Set.of(
            IndexTypes.VALUE,
            IndexTypes.VERSION,
            IndexTypes.RANK,
            IndexTypes.PERMUTED_MAX,
            IndexTypes.PERMUTED_MIN
    );

    @Nonnull
    private static final Set<String> GROUPED_INDEX_TYPES = Set.of(
            IndexTypes.RANK,
            IndexTypes.PERMUTED_MAX,
            IndexTypes.PERMUTED_MIN
    );

    @Nonnull
    private final Index index;
    @Nonnull
    private final List<RecordType> queriedRecordTypes;

    public ValueIndexExpansionVisitor(@Nonnull Index index, @Nonnull Collection<RecordType> queriedRecordTypes) {
        Preconditions.checkArgument(SUPPORTED_INDEX_TYPES.contains(index.getType()));
        this.index = index;
        this.queriedRecordTypes = ImmutableList.copyOf(queriedRecordTypes);
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public MatchCandidate expand(@Nonnull final Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nullable final KeyExpression primaryKey,
                                 final boolean isReverse) {
        Debugger.updateIndex(PredicateWithValueAndRanges.class, old -> 0);

        final var baseQuantifier = baseQuantifierSupplier.get();
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();

        // add the value for the flow of records
        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        var rootExpression = index.getRootExpression();

        if (rootExpression instanceof GroupingKeyExpression) {
            if (GROUPED_INDEX_TYPES.contains(index.getType())) {
                rootExpression = ((GroupingKeyExpression)rootExpression).getWholeKey();
            } else {
                throw new UnsupportedOperationException("cannot create match candidate on grouping expression for unknown index type");
            }
        }

        final int keyValueSplitPoint;
        if (rootExpression instanceof KeyWithValueExpression) {
            final KeyWithValueExpression keyWithValueExpression = (KeyWithValueExpression)rootExpression;
            keyValueSplitPoint = keyWithValueExpression.getSplitPoint();
            rootExpression = keyWithValueExpression.getInnerKey();
        } else {
            keyValueSplitPoint = -1;
        }

        final var keyValues = Lists.<Value>newArrayList();
        final var valueValues = Lists.<Value>newArrayList();

        final var initialState =
                VisitorState.of(keyValues,
                        valueValues,
                        baseQuantifier,
                        ImmutableList.of(),
                        keyValueSplitPoint,
                        0);

        final var keyValueExpansion =
                pop(rootExpression.expand(push(initialState)));

        allExpansionsBuilder.add(keyValueExpansion);

        if (index.hasPredicate()) {
            final var filteredIndexPredicate = Objects.requireNonNull(index.getPredicate()).toPredicate(baseQuantifier.getFlowedObjectValue());
            final var valueRangesMaybe = IndexPredicateExpansion.dnfPredicateToRanges(filteredIndexPredicate);
            final var predicateExpansionBuilder = GraphExpansion.builder();
            if (valueRangesMaybe.isEmpty()) { // could not create DNF, store the predicate as-is.
                allExpansionsBuilder.add(GraphExpansion.ofPredicate(filteredIndexPredicate));
            } else {
                final var valueRanges = valueRangesMaybe.get();
                for (final var value : valueRanges.keySet()) {
                    // we check if the predicate value is a placeholder, if so, create a placeholder, otherwise, add it as a constraint.
                    final var maybePlaceholder = keyValueExpansion.getPlaceholders()
                            .stream()
                            .filter(existingPlaceholder -> existingPlaceholder.getValue().semanticEquals(value, AliasMap.emptyMap()))
                            .findFirst();
                    if (maybePlaceholder.isEmpty()) {
                        predicateExpansionBuilder.addPredicate(PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.copyOf(valueRanges.get(value))));
                    } else {
                        predicateExpansionBuilder.addPlaceholder(maybePlaceholder.get().withExtraRanges(ImmutableSet.copyOf(valueRanges.get(value))));
                    }
                }
            }
            allExpansionsBuilder.add(predicateExpansionBuilder.build());
        }

        final var keySize = keyValues.size();

        if (primaryKey != null) {
            // unfortunately we must copy as the returned list is not guaranteed to be mutable which is needed for the
            // trimPrimaryKey() function as it is causing a side-effect
            final var trimmedPrimaryKeys = Lists.newArrayList(primaryKey.normalizeKeyForPositions());
            index.trimPrimaryKey(trimmedPrimaryKeys);

            for (int i = 0; i < trimmedPrimaryKeys.size(); i++) {
                final KeyExpression primaryKeyPart = trimmedPrimaryKeys.get(i);

                final var initialStateForKeyPart =
                        VisitorState.of(keyValues,
                                Lists.newArrayList(),
                                baseQuantifier,
                                ImmutableList.of(),
                                -1,
                                keySize + i);
                final var primaryKeyPartExpansion =
                        pop(primaryKeyPart.expand(push(initialStateForKeyPart)));
                allExpansionsBuilder
                        .add(primaryKeyPartExpansion);
            }
        }

        final var completeExpansion = GraphExpansion.ofOthers(allExpansionsBuilder.build());
        final var sealedExpansion = completeExpansion.seal();
        final var parameters = sealedExpansion.getPlaceholders().stream().map(Placeholder::getParameterAlias).collect(ImmutableList.toImmutableList());
        final var matchableSortExpression = new MatchableSortExpression(parameters, isReverse, sealedExpansion.buildSelect());
        return new ValueIndexScanMatchCandidate(index,
                queriedRecordTypes,
                Traversal.withRoot(Reference.of(matchableSortExpression)),
                parameters,
                baseQuantifier.getFlowedObjectType(),
                baseQuantifier.getAlias(),
                keyValues,
                valueValues,
                fullKey(index, primaryKey),
                primaryKey);
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
        final KeyExpression rootExpression = index.getRootExpression() instanceof KeyWithValueExpression
                                             ? ((KeyWithValueExpression)index.getRootExpression()).getKeyExpression()
                                             : index.getRootExpression();
        if (primaryKey == null) {
            return rootExpression;
        }
        final var trimmedPrimaryKeyComponents = new ArrayList<>(primaryKey.normalizeKeyForPositions());
        index.trimPrimaryKey(trimmedPrimaryKeyComponents);
        if (trimmedPrimaryKeyComponents.isEmpty()) {
            return rootExpression;
        }
        final var fullKeyListBuilder = ImmutableList.<KeyExpression>builder();
        fullKeyListBuilder.add(rootExpression);
        fullKeyListBuilder.addAll(trimmedPrimaryKeyComponents);
        return concat(fullKeyListBuilder.build());
    }
}
