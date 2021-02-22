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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Class to expand primary data access into a candidate. The visitation methods are left unchanged from the super class
 * {@link ValueIndexLikeExpansionVisitor}, this class merely provides a specific {@link #expand} method.
 */
public class PrimaryAccessExpansionVisitor extends ValueIndexLikeExpansionVisitor {
    @Nonnull
    private final Set<String> availableRecordTypes;
    @Nonnull
    private final Set<String> recordTypes;

    public PrimaryAccessExpansionVisitor(@Nonnull final Set<String> availableRecordTypes, @Nonnull final Set<String> recordTypes) {
        this.availableRecordTypes = ImmutableSet.copyOf(availableRecordTypes);
        this.recordTypes = ImmutableSet.copyOf(recordTypes);
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public PrimaryScanMatchCandidate expand(@Nonnull final Quantifier.ForEach baseQuantifier,
                                            @Nullable final KeyExpression primaryKey,
                                            final boolean isReverse) {
        Preconditions.checkArgument(primaryKey != null);
        Debugger.updateIndex(ValueComparisonRangePredicate.Placeholder.class, old -> 0);

        // expand
        final GraphExpansion graphExpansion =
                pop(primaryKey.expand(push(VisitorState.of(baseQuantifier.getAlias(), ImmutableList.of(), -1, 0))));

        final GraphExpansion allExpansions =
                GraphExpansion.ofOthers(ImmutableList.of(GraphExpansion.ofResultValueAndQuantifier(new QuantifiedObjectValue(baseQuantifier.getAlias()), baseQuantifier),
                        graphExpansion));

        final List<CorrelationIdentifier> parameters = allExpansions.getPlaceholderAliases();

        final RelationalExpression expression =
                new MatchableSortExpression(parameters, isReverse, allExpansions.buildSelect());

        return new PrimaryScanMatchCandidate(
                ExpressionRefTraversal.withRoot(GroupExpressionRef.of(expression)),
                parameters,
                availableRecordTypes,
                recordTypes,
                primaryKey);
    }
}
