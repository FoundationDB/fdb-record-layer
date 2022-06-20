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
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Case class to represent a match candidate that is backed by an index.
 */
public class PrimaryScanMatchCandidate implements MatchCandidate, ValueIndexLikeMatchCandidate, WithPrimaryKeyMatchCandidate {
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
    private final List<RecordType> availableRecordTypes;

    /**
     * Set of record types that are actually queried.
     */
    @Nonnull
    private final List<RecordType> queriedRecordTypes;

    @Nonnull
    private final KeyExpression primaryKey;

    public PrimaryScanMatchCandidate(@Nonnull final ExpressionRefTraversal traversal,
                                     @Nonnull final List<CorrelationIdentifier> parameters,
                                     @Nonnull final Collection<RecordType> availableRecordTypes,
                                     @Nonnull final Collection<RecordType> queriedRecordTypes,
                                     @Nonnull final KeyExpression primaryKey) {
        this.traversal = traversal;
        this.parameters = ImmutableList.copyOf(parameters);
        this.availableRecordTypes = ImmutableList.copyOf(availableRecordTypes);
        this.queriedRecordTypes = ImmutableList.copyOf(queriedRecordTypes);
        this.primaryKey = primaryKey;
    }

    @Nonnull
    @Override
    public String getName() {
        return "primary(" + String.join(",", getAvailableRecordTypeNames()) + ")";
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
    public List<RecordType> getAvailableRecordTypes() {
        return availableRecordTypes;
    }

    @Nonnull
    public Set<String> getAvailableRecordTypeNames() {
        return getAvailableRecordTypes().stream()
                .map(RecordType::getName)
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public List<RecordType> getQueriedRecordTypes() {
        return queriedRecordTypes;
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryKey() {
        return primaryKey;
    }

    @Nonnull
    @Override
    public KeyExpression getAlternativeKeyExpression() {
        return getPrimaryKey();
    }

    @Nonnull
    @Override
    public RelationalExpression toEquivalentExpression(@Nonnull PartialMatch partialMatch,
                                                       @Nonnull final PlanContext planContext,
                                                       @Nonnull final List<ComparisonRange> comparisonRanges) {
        final var reverseScanOrder =
                partialMatch.getMatchInfo()
                        .deriveReverseScanOrder()
                        .orElseThrow(() -> new RecordCoreException("match info should unambiguously indicate reversed-ness of can"));
        return new LogicalTypeFilterExpression(getQueriedRecordTypeNames(),
                new PrimaryScanExpression(getAvailableRecordTypeNames(),
                        Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(getAvailableRecordTypes())),
                        comparisonRanges,
                        reverseScanOrder,
                        primaryKey),
                Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(getQueriedRecordTypes())));
    }
}
