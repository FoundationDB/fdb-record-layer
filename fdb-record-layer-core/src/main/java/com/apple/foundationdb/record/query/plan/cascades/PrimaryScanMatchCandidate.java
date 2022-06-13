/*
 * MatchCandidate.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * Case class to represent a match candidate that is backed by an index.
 */
public class PrimaryScanMatchCandidate implements MatchCandidate, ValueIndexLikeMatchCandidate {
    /**
     * Holds the parameter names for all necessary parameters that need to be bound during matching.
     */
    @Nonnull
    private final List<CorrelationIdentifier> parameters;

    /**
     * Traversal object of the primary scan graph (not the query graph).
     */
    @Nonnull
    private final ExpressionRefTraversal traversal;

    /**
     * Set of record types that are available in the context of the query.
     */
    @Nonnull
    private final Set<String> availableRecordTypes;

    /**
     * Set of record types that are actually queried.
     */
    @Nonnull
    private final Set<String> queriedRecordTypes;

    @Nonnull
    private final KeyExpression alternativeKeyExpression;

    public PrimaryScanMatchCandidate(@Nonnull final ExpressionRefTraversal traversal,
                                     @Nonnull final List<CorrelationIdentifier> parameters,
                                     @Nonnull Set<String> availableRecordTypes,
                                     @Nonnull Set<String> queriedRecordTypes,
                                     @Nonnull final KeyExpression alternativeKeyExpression) {
        this.traversal = traversal;
        this.parameters = ImmutableList.copyOf(parameters);
        this.availableRecordTypes = ImmutableSet.copyOf(availableRecordTypes);
        this.queriedRecordTypes = ImmutableSet.copyOf(queriedRecordTypes);
        this.alternativeKeyExpression = alternativeKeyExpression;
    }

    @Nonnull
    @Override
    public String getName() {
        return "primary(" + String.join(",", queriedRecordTypes) + ")";
    }

    @Nonnull
    @Override
    public ExpressionRefTraversal getTraversal() {
        return traversal;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getSargableAliases() {
        return parameters;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getOrderingAliases() {
        return getSargableAliases();
    }

    @Nonnull
    public Set<String> getAvailableRecordTypes() {
        return availableRecordTypes;
    }

    @Nonnull
    public Set<String> getQueriedRecordTypes() {
        return queriedRecordTypes;
    }

    @Nonnull
    @Override
    public KeyExpression getAlternativeKeyExpression() {
        return alternativeKeyExpression;
    }

    @Nonnull
    @Override
    public RelationalExpression toEquivalentExpression(@Nonnull RecordMetaData recordMetaData,
                                                       @Nonnull PartialMatch partialMatch,
                                                       @Nonnull final PlanContext planContext,
                                                       @Nonnull final List<ComparisonRange> comparisonRanges) {
        final var reverseScanOrder =
                partialMatch.getMatchInfo()
                        .deriveReverseScanOrder()
                        .orElseThrow(() -> new RecordCoreException("match info should unambiguously indicate reversed-ness of can"));
        return new LogicalTypeFilterExpression(getQueriedRecordTypes(),
                new PrimaryScanExpression(getAvailableRecordTypes(),
                        Type.Record.fromFieldDescriptorsMap(recordMetaData.getFieldDescriptorMapFromNames(getAvailableRecordTypes())),
                        comparisonRanges,
                        reverseScanOrder),
                Type.Record.fromFieldDescriptorsMap(recordMetaData.getFieldDescriptorMapFromNames(getQueriedRecordTypes())));
    }
}
