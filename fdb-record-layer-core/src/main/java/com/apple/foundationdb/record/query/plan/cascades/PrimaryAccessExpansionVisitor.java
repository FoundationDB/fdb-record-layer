/*
 * PrimaryAccessExpansionVisitor.java
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
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueWithRanges;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Class to expand primary data access into a candidate. The visitation methods are left unchanged from the super class
 * {@link KeyExpressionExpansionVisitor}, this class merely provides a specific {@link #expand} method.
 */
public class PrimaryAccessExpansionVisitor extends KeyExpressionExpansionVisitor implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private final List<RecordType> availableRecordTypes;
    @Nonnull
    private final List<RecordType> recordTypes;

    public PrimaryAccessExpansionVisitor(@Nonnull final Collection<RecordType> availableRecordTypes, @Nonnull final Collection<RecordType> recordTypes) {
        this.availableRecordTypes = ImmutableList.copyOf(availableRecordTypes);
        this.recordTypes = ImmutableList.copyOf(recordTypes);
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
    public PrimaryScanMatchCandidate expand(@Nonnull final Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                            @Nullable final KeyExpression primaryKey,
                                            final boolean isReverse) {
        Objects.requireNonNull(primaryKey);
        Debugger.updateIndex(ValueWithRanges.Placeholder.class, old -> 0);

        final var baseQuantifier = baseQuantifierSupplier.get();

        // expand
        final var graphExpansion =
                pop(primaryKey.expand(push(VisitorState.of(Lists.newArrayList(),
                        Lists.newArrayList(),
                        baseQuantifier,
                        ImmutableList.of(),
                        -1,
                        0))));

        final var allExpansions =
                GraphExpansion.ofOthers(GraphExpansion.ofQuantifier(baseQuantifier), graphExpansion);

        final var parameters = allExpansions.getPlaceholderAliases();

        final var expression =
                new MatchableSortExpression(parameters, isReverse, allExpansions.buildSelect());

        return new PrimaryScanMatchCandidate(
                ExpressionRefTraversal.withRoot(GroupExpressionRef.of(expression)),
                parameters,
                availableRecordTypes,
                recordTypes,
                primaryKey,
                baseQuantifier.getFlowedObjectType());
    }
}
