/*
 * ValueIndexLikeExpansionVisitor.java
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;

/**
 * Class to expand value index access into a candidate graph. The visitation methods are left unchanged from the super
 * class {@link ValueIndexLikeExpansionVisitor}, this class merely provides a specific {@link #expand} method.
 */
public class ValueIndexExpansionVisitor extends ValueIndexLikeExpansionVisitor {
    private final Index index;

    public ValueIndexExpansionVisitor(@Nonnull Index index) {
        Preconditions.checkArgument(index.getType().equals(IndexTypes.VALUE));
        this.index = index;
    }

    @Nonnull
    @Override
    public MatchCandidate expand(@Nonnull final Quantifier.ForEach baseQuantifier,
                                 @Nullable final KeyExpression primaryKey,
                                 final boolean isReverse) {
        Debugger.updateIndex(ValueComparisonRangePredicate.Placeholder.class, old -> 0);
        final ImmutableList.Builder<GraphExpansion> allExpansionsBuilder = ImmutableList.builder();

        push(VisitorState.of(baseQuantifier.getAlias(), ImmutableList.of(), -1, 0));
        allExpansionsBuilder.add(index.getRootExpression().expand(this));
        pop();

        if (primaryKey != null) {
            // unfortunately we must copy as the returned list is not guaranteed to be mutable which is needed for the
            // trimPrimaryKey() function as it is causing a side-effect
            final List<KeyExpression> trimmedPrimaryKeys = Lists.newArrayList(primaryKey.normalizeKeyForPositions());
            index.trimPrimaryKey(trimmedPrimaryKeys);

            trimmedPrimaryKeys
                    .forEach(primaryKeyPart -> {
                        push(VisitorState.of(baseQuantifier.getAlias(), ImmutableList.of(), -1, 0));
                        allExpansionsBuilder.add(primaryKeyPart.expand(this));
                        pop();
                    });
        }

        final GraphExpansion completeExpansion = GraphExpansion.ofOthers(allExpansionsBuilder.build());
        final List<CorrelationIdentifier> parameters = completeExpansion.getPlaceholderAliases();
        final MatchableSortExpression matchableSortExpression = new MatchableSortExpression(parameters, isReverse, completeExpansion.buildSelectWithBase(baseQuantifier));
        return new ValueIndexScanMatchCandidate(index.getName(),
                ExpressionRefTraversal.withRoot(GroupExpressionRef.of(matchableSortExpression)),
                parameters,
                fullKey(primaryKey));
    }

    /**
     * Compute the full key of an index (given that the index is a value index).
     *
     * @param primaryKey primary key of the records the index ranges over. The primary key is used to determine
     *        parts in the index definition that already contain parts of the primary key. All primary key components
     *        that are not already part of the index key are appended to the index key.
     * @return a {@link KeyExpression} describing the <em>full</em> index key as stored
     */
    @Nonnull
    private KeyExpression fullKey(@Nullable final KeyExpression primaryKey) {
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
        return new ValueIndexExpansionVisitor(index).fullKey(primaryKey);
    }
}
