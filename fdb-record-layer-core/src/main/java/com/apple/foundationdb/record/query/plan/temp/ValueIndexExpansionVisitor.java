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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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
public class ValueIndexExpansionVisitor extends KeyExpressionExpansionVisitor implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private final Index index;
    @Nonnull
    private final List<RecordType> recordTypes;

    public ValueIndexExpansionVisitor(@Nonnull Index index, @Nonnull Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(index.getType().equals(IndexTypes.VALUE) || index.getType().equals(IndexTypes.RANK));
        this.index = index;
        this.recordTypes = ImmutableList.copyOf(recordTypes);
    }

    @Nonnull
    @Override
    public MatchCandidate expand(@Nonnull final Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nullable final KeyExpression primaryKey,
                                 final boolean isReverse) {
        Debugger.updateIndex(ValueComparisonRangePredicate.Placeholder.class, old -> 0);

        final var baseQuantifier = baseQuantifierSupplier.get();
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();

        // add the value for the flow of records
        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        var rootExpression = index.getRootExpression();

        if (rootExpression instanceof GroupingKeyExpression) {
            if (index.getType().equals(IndexTypes.RANK)) {
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
                        baseQuantifier.getAlias(),
                        ImmutableList.of(),
                        keyValueSplitPoint,
                        0);

        final var keyValueExpansion =
                pop(rootExpression.expand(push(initialState)));

        allExpansionsBuilder.add(keyValueExpansion);

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
                                baseQuantifier.getAlias(),
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
        final var parameters = completeExpansion.getPlaceholderAliases();
        final var matchableSortExpression = new MatchableSortExpression(parameters, isReverse, completeExpansion.buildSelect());
        return new ValueIndexScanMatchCandidate(index,
                recordTypes,
                ExpressionRefTraversal.withRoot(GroupExpressionRef.of(matchableSortExpression)),
                parameters,
                baseQuantifier.getAlias(),
                keyValues,
                valueValues,
                fullKey(index, primaryKey));
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
        final var trimmedPrimaryKeyComponents = new ArrayList<>(primaryKey.normalizeKeyForPositions());
        index.trimPrimaryKey(trimmedPrimaryKeyComponents);
        final var fullKeyListBuilder = ImmutableList.<KeyExpression>builder();
        fullKeyListBuilder.add(index.getRootExpression());
        fullKeyListBuilder.addAll(trimmedPrimaryKeyComponents);
        return concat(fullKeyListBuilder.build());
    }
}
