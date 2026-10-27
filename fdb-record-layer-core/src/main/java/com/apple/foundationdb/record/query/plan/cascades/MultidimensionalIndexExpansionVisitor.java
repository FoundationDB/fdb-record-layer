/*
 * MultidimensionalIndexExpansionVisitor.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Expand a multidimensional (R-tree) index into a Cascades match candidate graph.
 *
 * <p>The root of the index key expression is a {@link DimensionsKeyExpression} (optionally wrapped by a
 * {@link KeyWithValueExpression} for covering values), which cannot be expanded through the ordinary
 * {@link KeyExpressionExpansionVisitor} dispatch path: the {@link KeyExpressionVisitor}
 * interface has no {@code visitExpression(DimensionsKeyExpression)} overload, so
 * {@code DimensionsKeyExpression.expand(visitor)} always lands on the throwing fallback
 * {@link KeyExpressionExpansionVisitor#visitExpression(KeyExpression)}. This visitor works around that by
 * decomposing the root into its <em>prefix</em>, <em>dimensions</em>, and <em>suffix</em> subkeys and expanding
 * each subkey separately, threading a single {@link VisitorState} across all three so
 * {@code keyValues} / {@code valueValues} / {@code currentOrdinal} accumulate as if the three had been
 * concatenated into one {@code ThenKeyExpression}. The result is one ordinary placeholder per index column,
 * in index order &mdash; no new {@link Value} type is required.
 *
 * <p>Only the prefix segment's placeholders feed the candidate's
 * {@link MatchableSortExpression}: the Hilbert-curve traversal order within the dimensions segment is not a
 * meaningful external sort, mirroring the legacy planner's {@code planGeospatial} rejecting any
 * {@code sort != null}.
 *
 * @see MultidimensionalIndexScanMatchCandidate for the candidate produced by {@code expand}.
 */
public class MultidimensionalIndexExpansionVisitor extends KeyExpressionExpansionVisitor implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private static final Set<String> SUPPORTED_INDEX_TYPES = Set.of(
            IndexTypes.MULTIDIMENSIONAL
    );

    @Nonnull
    private final Index index;
    @Nonnull
    private final List<RecordType> queriedRecordTypes;

    public MultidimensionalIndexExpansionVisitor(@Nonnull Index index, @Nonnull Collection<RecordType> queriedRecordTypes) {
        Preconditions.checkArgument(SUPPORTED_INDEX_TYPES.contains(index.getType()));
        this.index = index;
        this.queriedRecordTypes = ImmutableList.copyOf(queriedRecordTypes);
    }

    @Nonnull
    @Override
    public MatchCandidate expand(@Nonnull final Set<String> availableRecordTypeNames,
                                 @Nonnull final Set<String> queriedRecordTypeNames,
                                 @Nonnull final Type.Record baseType,
                                 @Nonnull final AccessHint accessHint,
                                 @Nullable final KeyExpression primaryKey,
                                 final boolean isReverse) {
        Debugger.updateIndex(PredicateWithValueAndRanges.class, old -> 0);

        final var baseQuantifier = Quantifier.forEach(ExpansionVisitor.createBaseRef(availableRecordTypeNames,
                queriedRecordTypeNames, baseType, null, accessHint));
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();

        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        var rootExpression = index.getRootExpression();

        final int keyValueSplitPoint;
        if (rootExpression instanceof KeyWithValueExpression) {
            final KeyWithValueExpression keyWithValueExpression = (KeyWithValueExpression)rootExpression;
            keyValueSplitPoint = keyWithValueExpression.getSplitPoint();
            rootExpression = keyWithValueExpression.getInnerKey();
        } else {
            keyValueSplitPoint = -1;
        }

        // The factory's validator guarantees a DimensionsKeyExpression (optionally wrapped by
        // KeyWithValueExpression above). If it isn't there, the index is structurally malformed.
        if (!(rootExpression instanceof DimensionsKeyExpression)) {
            throw new RecordCoreException("multidimensional index root must be a DimensionsKeyExpression");
        }
        final DimensionsKeyExpression dimensionsKeyExpression = (DimensionsKeyExpression)rootExpression;
        final int prefixSize = dimensionsKeyExpression.getPrefixSize();
        final int dimensionsSize = dimensionsKeyExpression.getDimensionsSize();
        final KeyExpression wholeKey = dimensionsKeyExpression.getWholeKey();
        final int columnSize = dimensionsKeyExpression.getColumnSize();

        final var keyValues = Lists.<Value>newArrayList();
        final var valueValues = Lists.<Value>newArrayList();

        // Expand prefix / dimensions / suffix subkeys as if they were the children of a ThenKeyExpression
        // (see KeyExpressionExpansionVisitor#visitExpression(ThenKeyExpression) for the analogous child-loop
        // that threads `currentOrdinal` and shares the `keyValues`/`valueValues` accumulators).
        final var segmentExpansionsBuilder = ImmutableList.<GraphExpansion>builder();
        int currentOrdinal = 0;

        if (prefixSize > 0) {
            final KeyExpression prefixSubKey = wholeKey.getSubKey(0, prefixSize);
            final var prefixState = VisitorState.of(keyValues,
                    valueValues,
                    baseQuantifier,
                    ImmutableList.of(),
                    keyValueSplitPoint,
                    currentOrdinal,
                    false,
                    true);
            segmentExpansionsBuilder.add(pop(prefixSubKey.expand(push(prefixState))));
            currentOrdinal += prefixSize;
        }

        final KeyExpression dimensionsSubKey = wholeKey.getSubKey(prefixSize, prefixSize + dimensionsSize);
        final var dimensionsState = VisitorState.of(keyValues,
                valueValues,
                baseQuantifier,
                ImmutableList.of(),
                keyValueSplitPoint,
                currentOrdinal,
                false,
                true);
        segmentExpansionsBuilder.add(pop(dimensionsSubKey.expand(push(dimensionsState))));
        currentOrdinal += dimensionsSize;

        if (currentOrdinal < columnSize) {
            final KeyExpression suffixSubKey = wholeKey.getSubKey(currentOrdinal, columnSize);
            final var suffixState = VisitorState.of(keyValues,
                    valueValues,
                    baseQuantifier,
                    ImmutableList.of(),
                    keyValueSplitPoint,
                    currentOrdinal,
                    false,
                    true);
            segmentExpansionsBuilder.add(pop(suffixSubKey.expand(push(suffixState))));
        }

        final var keyValueExpansion = GraphExpansion.ofOthers(segmentExpansionsBuilder.build())
                .toBuilder()
                .removeAllResultColumns()
                .build();
        allExpansionsBuilder.add(keyValueExpansion);

        if (index.hasPredicate()) {
            final var filteredIndexPredicate = Objects.requireNonNull(index.getPredicate()).toPredicate(baseQuantifier.getFlowedObjectValue());
            if (!filteredIndexPredicate.isTautology()) {
                final var valueRangesMaybe = IndexPredicateExpansion.dnfPredicateToRanges(filteredIndexPredicate);
                final var predicateExpansionBuilder = GraphExpansion.builder();
                if (valueRangesMaybe.isEmpty()) {
                    allExpansionsBuilder.add(GraphExpansion.ofPredicate(filteredIndexPredicate));
                } else {
                    final var valueRanges = valueRangesMaybe.get();
                    for (final var value : valueRanges.keySet()) {
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
        }

        final var keySize = keyValues.size();

        if (primaryKey != null) {
            final var trimmedPrimaryKeys = Lists.newArrayList(primaryKey.normalizeKeyForPositions());
            index.trimPrimaryKey(trimmedPrimaryKeys);

            for (int i = 0; i < trimmedPrimaryKeys.size(); i++) {
                final KeyExpression primaryKeyPart = trimmedPrimaryKeys.get(i);

                final var primaryKeyState =
                        VisitorState.of(keyValues,
                                Lists.newArrayList(),
                                baseQuantifier,
                                ImmutableList.of(),
                                -1,
                                keySize + i,
                                false,
                                true);
                final var primaryKeyPartExpansion =
                        pop(primaryKeyPart.expand(push(primaryKeyState)))
                                .toBuilder()
                                .removeAllResultColumns()
                                .build();
                allExpansionsBuilder.add(primaryKeyPartExpansion);
            }
        }

        final var completeExpansion = GraphExpansion.ofOthers(allExpansionsBuilder.build());
        final var sealedExpansion = completeExpansion.seal();
        final var parameters = sealedExpansion.getPlaceholders()
                .stream()
                .map(Placeholder::getParameterAlias)
                .collect(ImmutableList.toImmutableList());
        // An R-tree scan cannot be materialized unless every prefix and every dimension is bound (see
        // MultidimensionalIndexScanMatchCandidate.computePrefixMap for the all-or-nothing invariant). Declaring
        // those aliases as required-for-binding lets AbstractDataAccessRule.prepareMatchesAndCompensations prune
        // partial matches that miss any of them, so the concrete toEquivalentPlan is never invoked on an
        // under-bound match.
        final var keyValueAliases = keyValueExpansion.getPlaceholderAliases();
        final var requiredAliasesBuilder = ImmutableSet.<CorrelationIdentifier>builder();
        requiredAliasesBuilder.addAll(keyValueAliases.subList(0, Math.min(prefixSize + dimensionsSize, keyValueAliases.size())));
        sealedExpansion.getPlaceholders().stream()
                .filter(placeholder -> placeholder.getValue().isIndexOnly())
                .map(Placeholder::getParameterAlias)
                .forEach(requiredAliasesBuilder::add);
        final var parametersRequiredForBinding = requiredAliasesBuilder.build();

        // Restrict the ordering aliases to the prefix segment. Placeholders in `keyValueExpansion` were
        // added in index order (prefix, then dimensions, then suffix), so the first `prefixSize` are the
        // prefix aliases; dimensions and suffix are deliberately excluded because Hilbert-curve traversal
        // order is not a meaningful external sort.
        final List<CorrelationIdentifier> orderingAliases = prefixSize == 0
                ? ImmutableList.of()
                : ImmutableList.copyOf(keyValueAliases.subList(0, prefixSize));

        final var selectExpression = sealedExpansion.buildSelectWithResultValue(baseQuantifier.getFlowedObjectValue());
        final var maybeWithSort = orderingAliases.isEmpty()
                                  ? Reference.initialOf(selectExpression)
                                  : Reference.initialOf(new MatchableSortExpression(orderingAliases, isReverse, selectExpression));

        return new MultidimensionalIndexScanMatchCandidate(index,
                queriedRecordTypes,
                Traversal.withRoot(maybeWithSort),
                parameters,
                orderingAliases,
                parametersRequiredForBinding,
                baseQuantifier.getFlowedObjectType().narrowRecordMaybe()
                        .orElseThrow(() -> new RecordCoreException("cannot create match candidate with non-record type")),
                baseQuantifier.getAlias(),
                ValueIndexExpansionVisitor.fullKey(index, primaryKey),
                primaryKey,
                prefixSize,
                dimensionsSize);
    }
}
