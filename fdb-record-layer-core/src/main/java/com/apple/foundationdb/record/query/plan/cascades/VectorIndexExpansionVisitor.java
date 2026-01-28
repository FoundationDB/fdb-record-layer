/*
 * VectorIndexExpansionVisitor.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.hnsw.Config;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.values.CosineDistanceRowNumberValue;
import com.apple.foundationdb.record.query.plan.cascades.values.DotProductDistanceRowNumberValue;
import com.apple.foundationdb.record.query.plan.cascades.values.EuclideanDistanceRowNumberValue;
import com.apple.foundationdb.record.query.plan.cascades.values.EuclideanSquareDistanceRowNumberValue;
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
import java.util.function.Supplier;

/**
 * Class to expand vector index access into a candidate graph. The visitation methods are left unchanged from the super
 * class {@link KeyExpressionExpansionVisitor}, this class merely provides a specific {@link #expand} method.
 */
public class VectorIndexExpansionVisitor extends KeyExpressionExpansionVisitor implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private static final Set<String> SUPPORTED_INDEX_TYPES = Set.of(
            IndexTypes.VECTOR
    );

    @Nonnull
    private final Index index;
    @Nonnull
    private final List<RecordType> queriedRecordTypes;

    public VectorIndexExpansionVisitor(@Nonnull Index index, @Nonnull Collection<RecordType> queriedRecordTypes) {
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

        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        var rootExpression = index.getRootExpression();
        if (rootExpression instanceof GroupingKeyExpression) {
            throw new UnsupportedOperationException("cannot create match candidate on grouping expression for unknown index type");
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
                        0,
                        false,
                        true);

        final var keyValueExpansion =
                pop(rootExpression.expand(push(initialState)))
                        .toBuilder()
                        .removeAllResultColumns()
                        .build();

        final var orderingAliases = keyValueExpansion.getPlaceholderAliases();
        final var distanceValuePlaceholder = createDistanceValuePlaceholder(ImmutableList.copyOf(keyValues),
                ImmutableList.copyOf(valueValues));
        allExpansionsBuilder.add(keyValueExpansion);
        allExpansionsBuilder.add(GraphExpansion.ofPlaceholder(distanceValuePlaceholder));

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

        final var completeExpansion = GraphExpansion.ofOthers(allExpansionsBuilder.build());
        final var sealedExpansion = completeExpansion.seal();
        final var parameters = sealedExpansion.getPlaceholders()
                .stream()
                .map(Placeholder::getParameterAlias)
                .collect(ImmutableList.toImmutableList());
        final var parametersRequiredForBinding = sealedExpansion.getPlaceholders().stream()
                .filter(placeholder -> placeholder.getValue().isIndexOnly())
                .map(Placeholder::getParameterAlias)
                .collect(ImmutableSet.toImmutableSet());

        final var selectExpression = sealedExpansion.buildSelectWithResultValue(baseQuantifier.getFlowedObjectValue());

        final var maybeWithSort = orderingAliases.isEmpty()
                                  ? Reference.initialOf(selectExpression) // window is the entire table.
                                  : Reference.initialOf(new MatchableSortExpression(orderingAliases, isReverse, selectExpression));
        return new VectorIndexScanMatchCandidate(index,
                queriedRecordTypes,
                Traversal.withRoot(maybeWithSort),
                parameters,
                orderingAliases,
                parametersRequiredForBinding,
                baseQuantifier.getFlowedObjectType().narrowRecordMaybe().orElseThrow(() -> new RecordCoreException("cannot create match candidate with non-record type")),
                baseQuantifier.getAlias(),
                ValueIndexExpansionVisitor.fullKey(index, primaryKey),
                primaryKey);
    }

    @Nonnull
    private Placeholder createDistanceValuePlaceholder(@Nonnull Iterable<? extends Value> partitioningValues,
                                                       @Nonnull Iterable<? extends Value> argumentValues) {
        final var metric = index.getOptions().getOrDefault(IndexOptions.HNSW_METRIC, Config.DEFAULT_METRIC.name());

        if (metric.equals(Metric.EUCLIDEAN_METRIC.name())) {
            return new EuclideanDistanceRowNumberValue(partitioningValues, argumentValues).asPlaceholder(newParameterAlias());
        }

        if (metric.equals(Metric.EUCLIDEAN_SQUARE_METRIC.name())) {
            return new EuclideanSquareDistanceRowNumberValue(partitioningValues, argumentValues).asPlaceholder(newParameterAlias());
        }

        if (metric.equals(Metric.COSINE_METRIC.name())) {
            return new CosineDistanceRowNumberValue(partitioningValues, argumentValues).asPlaceholder(newParameterAlias());
        }

        if (metric.equals(Metric.DOT_PRODUCT_METRIC.name())) {
            return new DotProductDistanceRowNumberValue(partitioningValues, argumentValues).asPlaceholder(newParameterAlias());
        }

        throw new RecordCoreException("vector index does not support provided metric type " + metric);
    }
}
