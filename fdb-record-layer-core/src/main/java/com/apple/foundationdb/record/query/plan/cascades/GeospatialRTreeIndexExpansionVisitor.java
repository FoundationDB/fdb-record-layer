/*
 * GeospatialRTreeIndexExpansionVisitor.java
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
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexes.GeospatialRTreeIndexHelper;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.HaversineDistanceValue;
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
 * Expand a geospatial R-tree index into a Cascades match candidate graph.
 *
 * <p>The index has an optional {@link GroupingKeyExpression} root wrapping the two coordinate columns
 * {@code (latitude, longitude)}; any leading grouping columns partition the index into a distinct R-tree per grouping
 * tuple. This visitor unwraps the grouping (via {@link GeospatialRTreeIndexHelper#getGroupingCount(KeyExpression)}),
 * emits an ordinary placeholder per grouping column, and emits a single coordinates placeholder built from
 * {@link HaversineDistanceValue}'s 2-argument reduced form — the query-independent shape a
 * {@code Comparisons.WithinDistanceComparison} binds against, produced by
 * {@link HaversineDistanceValue#transformComparisonMaybe} at query construction time.
 *
 * <p>Only the grouping placeholders feed the candidate's {@link MatchableSortExpression}. The Hilbert-curve traversal
 * within the R-tree is not a meaningful external sort, matching the legacy planner's
 * {@code RecordQueryPlanner.planGeospatial} rejecting any {@code sort != null}.
 *
 * @see HaversineDistanceValue for the coordinates placeholder shape.
 */
public class GeospatialRTreeIndexExpansionVisitor extends KeyExpressionExpansionVisitor
        implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private static final Set<String> SUPPORTED_INDEX_TYPES = Set.of(
            IndexTypes.GEOSPATIAL_RTREE
    );

    @Nonnull
    private final Index index;
    @Nonnull
    private final List<RecordType> queriedRecordTypes;

    public GeospatialRTreeIndexExpansionVisitor(@Nonnull Index index, @Nonnull Collection<RecordType> queriedRecordTypes) {
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

        final KeyExpression rootExpression = index.getRootExpression();
        final int prefixSize = GeospatialRTreeIndexHelper.getGroupingCount(rootExpression);
        final KeyExpression wholeKey = rootExpression instanceof GroupingKeyExpression
                                       ? ((GroupingKeyExpression)rootExpression).getWholeKey()
                                       : rootExpression;
        final int columnSize = wholeKey.getColumnSize();
        final int coordinatesStart = columnSize - GeospatialRTreeIndexHelper.COORDINATE_DIMENSIONS;
        if (coordinatesStart != prefixSize) {
            // Enforced by GeospatialRTreeIndexMaintainerFactory's validator; the visitor is defensive in case a caller
            // hands in an index built outside that path.
            throw new RecordCoreException("geospatial R-tree index root must end with exactly two coordinate columns");
        }

        final var keyValues = Lists.<Value>newArrayList();
        final var valueValues = Lists.<Value>newArrayList();

        final var segmentExpansionsBuilder = ImmutableList.<GraphExpansion>builder();
        int currentOrdinal = 0;

        if (prefixSize > 0) {
            final KeyExpression prefixSubKey = wholeKey.getSubKey(0, prefixSize);
            final var prefixState = VisitorState.of(keyValues,
                    valueValues,
                    baseQuantifier,
                    ImmutableList.of(),
                    -1,
                    currentOrdinal,
                    false,
                    true);
            segmentExpansionsBuilder.add(pop(prefixSubKey.expand(push(prefixState))));
            currentOrdinal += prefixSize;
        }

        // Expand the coordinate subkey with the internal/non-select-star state so each field emits a result column but
        // no per-column placeholder: the raw lat/lon columns are absorbed into a single HaversineDistanceValue
        // placeholder below, exactly the way FunctionKeyExpression's visitor extracts argument values before wrapping
        // them into a single function value.
        final KeyExpression coordinatesSubKey = wholeKey.getSubKey(prefixSize, columnSize);
        final var coordinatesState = VisitorState.of(keyValues,
                valueValues,
                baseQuantifier,
                ImmutableList.of(),
                -1,
                currentOrdinal,
                true,
                false);
        final GraphExpansion coordinatesFieldExpansion = pop(coordinatesSubKey.expand(push(coordinatesState)));
        final var coordinateColumns = coordinatesFieldExpansion.getResultColumns();
        if (coordinateColumns.size() != GeospatialRTreeIndexHelper.COORDINATE_DIMENSIONS) {
            throw new RecordCoreException("expected two coordinate result columns from geospatial R-tree index root");
        }
        final Value latitudeValue = coordinateColumns.get(0).getValue();
        final Value longitudeValue = coordinateColumns.get(1).getValue();
        // Preserve the key-values invariant so downstream primary-key ordinals resolve correctly: the coordinate
        // columns contribute to the index-side key even though they land on the composite HaversineDistanceValue
        // placeholder rather than on per-column placeholders.
        keyValues.add(latitudeValue);
        keyValues.add(longitudeValue);

        final Placeholder coordinatesPlaceholder =
                new HaversineDistanceValue(latitudeValue, longitudeValue).asPlaceholder(newParameterAlias());
        segmentExpansionsBuilder.add(GraphExpansion.ofPlaceholder(coordinatesPlaceholder));

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
        // The coordinates placeholder must always be bound: a geospatial R-tree scan without (lat, lon, radius) has
        // no meaningful semantics. This mirrors what SargableAliasesRequiredForBinding is designed to enforce and,
        // critically, prevents SelectExpression.adjustMatch from promoting a leaf-level partial match into a spurious
        // "complete" match that would then trigger toEquivalentPlan with an empty parameter binding map.
        final var parametersRequiredForBindingBuilder = ImmutableSet.<CorrelationIdentifier>builder();
        sealedExpansion.getPlaceholders().stream()
                .filter(placeholder -> placeholder.getValue().isIndexOnly())
                .map(Placeholder::getParameterAlias)
                .forEach(parametersRequiredForBindingBuilder::add);
        parametersRequiredForBindingBuilder.add(coordinatesPlaceholder.getParameterAlias());
        final var parametersRequiredForBinding = parametersRequiredForBindingBuilder.build();

        // Restrict the ordering aliases to the grouping (prefix) segment. Placeholders in `keyValueExpansion` were
        // added grouping-then-coordinates, so the first `prefixSize` are the grouping aliases and the trailing
        // coordinates placeholder is intentionally excluded.
        final var keyValueAliases = keyValueExpansion.getPlaceholderAliases();
        final List<CorrelationIdentifier> orderingAliases = prefixSize == 0
                ? ImmutableList.of()
                : ImmutableList.copyOf(keyValueAliases.subList(0, prefixSize));

        final var selectExpression = sealedExpansion.buildSelectWithResultValue(baseQuantifier.getFlowedObjectValue());
        final var maybeWithSort = orderingAliases.isEmpty()
                                  ? Reference.initialOf(selectExpression)
                                  : Reference.initialOf(new MatchableSortExpression(orderingAliases, isReverse, selectExpression));

        return new GeospatialRTreeScanMatchCandidate(index,
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
                prefixSize);
    }
}
