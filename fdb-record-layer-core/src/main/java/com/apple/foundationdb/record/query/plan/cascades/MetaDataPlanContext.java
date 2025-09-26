/*
 * MetaDataPlanContext.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMatchCandidateRegistry;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.properties.RecordTypesProperty.recordTypes;

/**
 * A {@link PlanContext} where the underlying meta-data comes from {@link RecordMetaData} and {@link RecordStoreState}
 * objects, as is generally the case when planning actual queries.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public class MetaDataPlanContext implements PlanContext {
    @Nonnull
    private final RecordQueryPlannerConfiguration plannerConfiguration;

    @Nonnull
    private final Set<MatchCandidate> matchCandidates;

    private MetaDataPlanContext(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                                @Nonnull final Set<MatchCandidate> matchCandidates) {
        this.plannerConfiguration = plannerConfiguration;
        this.matchCandidates = ImmutableSet.copyOf(matchCandidates);
    }

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getPlannerConfiguration() {
        return plannerConfiguration;
    }

    @Nullable
    private static KeyExpression commonPrimaryKey(@Nonnull Iterable<RecordType> recordTypes) {
        KeyExpression common = null;
        boolean first = true;
        for (RecordType recordType : recordTypes) {
            if (first) {
                common = recordType.getPrimaryKey();
                first = false;
            } else if (!common.equals(recordType.getPrimaryKey())) {
                return null;
            }
        }
        return common;
    }
    
    @Nonnull
    @Override
    public Set<MatchCandidate> getMatchCandidates() {
        return matchCandidates;
    }

    @Nonnull
    private static List<Index> readableOf(@Nonnull RecordStoreState recordStoreState,
                                          @Nonnull List<Index> indexes) {
        if (recordStoreState.allIndexesReadable()) {
            return indexes;
        } else {
            return indexes.stream().filter(recordStoreState::isReadable).collect(Collectors.toList());
        }
    }

    @Nonnull
    public static PlanContext forRecordQuery(@Nonnull RecordQueryPlannerConfiguration plannerConfiguration,
                                             @Nonnull RecordMetaData metaData,
                                             @Nonnull RecordStoreState recordStoreState,
                                             @Nonnull IndexMatchCandidateRegistry matchCandidateRegistry,
                                             @Nonnull RecordQuery query) {
        final Optional<Collection<String>> queriedRecordTypeNamesOptional = query.getRecordTypes().isEmpty() ? Optional.empty() : Optional.of(query.getRecordTypes());
        final Optional<Collection<String>> allowedIndexesOptional = query.hasAllowedIndexes() ? Optional.of(Objects.requireNonNull(query.getAllowedIndexes())) : Optional.empty();
        final var indexQueryabilityFilter = query.getIndexQueryabilityFilter();
        final var isSortReverse = query.isSortReverse();

        List<Index> indexList = new ArrayList<>();
        final Set<String> queriedRecordTypeNames;
        final KeyExpression commonPrimaryKey;
        recordStoreState.beginRead();
        try {
            if (queriedRecordTypeNamesOptional.isEmpty()) { // ALL_TYPES
                commonPrimaryKey = commonPrimaryKey(metaData.getRecordTypes().values());
                queriedRecordTypeNames = metaData.getRecordTypes().keySet();
            } else {
                queriedRecordTypeNames = ImmutableSet.copyOf(queriedRecordTypeNamesOptional.get());
                final List<RecordType> queriedRecordTypes = queriedRecordTypeNames.stream().map(metaData::getRecordType).collect(Collectors.toList());
                if (queriedRecordTypes.size() == 1) {
                    final RecordType recordType = queriedRecordTypes.get(0);
                    indexList.addAll(readableOf(recordStoreState, recordType.getIndexes()));
                    indexList.addAll(readableOf(recordStoreState, recordType.getMultiTypeIndexes()));
                    commonPrimaryKey = recordType.getPrimaryKey();
                } else {
                    boolean first = true;
                    for (RecordType recordType : queriedRecordTypes) {
                        if (first) {
                            indexList.addAll(readableOf(recordStoreState, recordType.getMultiTypeIndexes()));
                            first = false;
                        } else {
                            indexList.retainAll(readableOf(recordStoreState, recordType.getMultiTypeIndexes()));
                        }
                    }
                    commonPrimaryKey = commonPrimaryKey(queriedRecordTypes);
                }
            }

            indexList.addAll(readableOf(recordStoreState, metaData.getUniversalIndexes()));
        } finally {
            recordStoreState.endRead();
        }

        if (allowedIndexesOptional.isPresent()) {
            final Collection<String> allowedIndexes = allowedIndexesOptional.get();
            indexList.removeIf(index -> !allowedIndexes.contains(index.getName()));
        } else {
            indexList.removeIf(index -> !indexQueryabilityFilter.isQueryable(index));
        }

        final ImmutableSet.Builder<MatchCandidate> matchCandidatesBuilder = ImmutableSet.builder();
        for (Index index : indexList) {
            matchCandidatesBuilder.addAll(matchCandidateRegistry.createMatchCandidates(metaData, index, isSortReverse));
        }

        MatchCandidateExpansion.fromPrimaryDefinition(metaData, queriedRecordTypeNames, commonPrimaryKey, isSortReverse)
                .ifPresent(matchCandidatesBuilder::add);

        return new MetaDataPlanContext(plannerConfiguration, matchCandidatesBuilder.build());
    }

    public static PlanContext forRootReference(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                                               @Nonnull final RecordMetaData metaData,
                                               @Nonnull final RecordStoreState recordStoreState,
                                               @Nonnull final IndexMatchCandidateRegistry matchCandidateRegistry,
                                               @Nonnull final Reference rootReference,
                                               @Nonnull final Optional<Collection<String>> allowedIndexesOptional,
                                               @Nonnull final IndexQueryabilityFilter indexQueryabilityFilter) {
        final var queriedRecordTypeNames = recordTypes().evaluate(rootReference);

        if (queriedRecordTypeNames.isEmpty()) {
            return new MetaDataPlanContext(plannerConfiguration, ImmutableSet.of());
        }

        final var queriedRecordTypes =
                queriedRecordTypeNames.stream().map(metaData::getRecordType).collect(Collectors.toList());
        final var indexList = Lists.<Index>newArrayList();
        recordStoreState.beginRead();
        try {
            for (final var recordType : queriedRecordTypes) {
                indexList.addAll(readableOf(recordStoreState, recordType.getAllIndexes()));
            }
        } finally {
            recordStoreState.endRead();
        }

        if (allowedIndexesOptional.isPresent()) {
            final Collection<String> allowedIndexes = allowedIndexesOptional.get();
            indexList.removeIf(index -> !allowedIndexes.contains(index.getName()));
        } else {
            indexList.removeIf(index -> !indexQueryabilityFilter.isQueryable(index));
        }

        final ImmutableSet.Builder<MatchCandidate> matchCandidatesBuilder = ImmutableSet.builder();
        for (final var index : indexList) {
            matchCandidatesBuilder.addAll(matchCandidateRegistry.createMatchCandidates(metaData, index, false));
        }

        for (final var recordType : queriedRecordTypes) {
            MatchCandidateExpansion.fromPrimaryDefinition(metaData,
                            ImmutableSet.of(recordType.getName()),
                            recordType.getPrimaryKey(),
                            false)
                    .ifPresent(matchCandidatesBuilder::add);
        }

        return new MetaDataPlanContext(plannerConfiguration, matchCandidatesBuilder.build());
    }
}
