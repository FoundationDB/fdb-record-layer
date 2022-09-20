/*
 * AggregateIndexExpansionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Expands an aggregate index into a {@link MatchCandidate}.
 */
public class AggregateIndexExpansionVisitor extends KeyExpressionExpansionVisitor
                                            implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {

    @Nonnull
    private final Index index;

    @Nonnull
    private final Collection<RecordType> recordTypes;

    public AggregateIndexExpansionVisitor(@Nonnull final Index index, @Nonnull final Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(allowedIndexTypes.contains(index.getType()));
        Preconditions.checkArgument(index.getRootExpression() instanceof GroupingKeyExpression);
        this.index = index;
        this.recordTypes = recordTypes;
    }

    @Nonnull
    @Override
    public MatchCandidate expand(@Nonnull final Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nullable final KeyExpression primaryKey,
                                 final boolean isReverse) {

        final var baseQuantifier = baseQuantifierSupplier.get();
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();


        // add the value for the flow of records
        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        final var groupingKeyExpr = (GroupingKeyExpression)index.getRootExpression();

        final var groupedExpr = groupingKeyExpr.getGroupedSubKey(); // squashed together.
        final var groupingExpr = groupingKeyExpr.getGroupingSubKey(); // GROUP BY A <---

        final var keyValues = Lists.<Value>newArrayList();
        final var valueValues = Lists.<Value>newArrayList();
        final var state = VisitorState.of(keyValues, valueValues, baseQuantifierSupplier.get(), ImmutableList.of(), -1, 0);
        final var groupingColumnsExpansion = pop(groupingExpr.expand(push(state)));

        allExpansionsBuilder.add(groupingColumnsExpansion);


        final var keySize = keyValues.size();

//        if (primaryKey != null) {
//            // unfortunately we must copy as the returned list is not guaranteed to be mutable which is needed for the
//            // trimPrimaryKey() function as it is causing a side-effect
//            final var trimmedPrimaryKeys = Lists.newArrayList(primaryKey.normalizeKeyForPositions());
//            index.trimPrimaryKey(trimmedPrimaryKeys);
//
//            for (int i = 0; i < trimmedPrimaryKeys.size(); i++) {
//                final KeyExpression primaryKeyPart = trimmedPrimaryKeys.get(i);
//
//                final var initialStateForKeyPart =
//                        VisitorState.of(keyValues,
//                                Lists.newArrayList(),
//                                baseQuantifier,
//                                ImmutableList.of(),
//                                -1,
//                                keySize + i);
//                final var primaryKeyPartExpansion =
//                        pop(primaryKeyPart.expand(push(initialStateForKeyPart)));
//                allExpansionsBuilder
//                        .add(primaryKeyPartExpansion);
//            }
//        }

        final var completeExpansion = GraphExpansion.ofOthers(allExpansionsBuilder.build());
        final var groupingColumnsAliases = groupingColumnsExpansion.getPlaceholderAliases();
        final var sortedGroupingColumnsQgm = new MatchableSortExpression(groupingColumnsAliases, isReverse, completeExpansion.buildSelect());
        final var traversal = ExpressionRefTraversal.withRoot(GroupExpressionRef.of(sortedGroupingColumnsQgm));
        return new AggregateIndexMatchCandidate(index, traversal, groupingColumnsAliases, recordTypes);
    }

    public static boolean isAggregateIndex(@Nonnull final String indexType) {
        return allowedIndexTypes.contains(indexType);
    }

    private static final Set<String> allowedIndexTypes = new LinkedHashSet<>();

    static {
        allowedIndexTypes.add(IndexTypes.COUNT);
        allowedIndexTypes.add(IndexTypes.SUM);
        allowedIndexTypes.add(IndexTypes.MIN_EVER_LONG);
        allowedIndexTypes.add(IndexTypes.MAX_EVER_LONG);
    }
}
