/*
 * MatchCandidateExpansion.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;

/**
 * Utility methods for expanding certain indexes into {@link MatchCandidate}s. This should be used by
 * methods like {@link com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory#createMatchCandidates(RecordMetaData, Index, boolean)}
 * to create the match candidate that will be used by the {@link CascadesPlanner} during query
 * planning. Note that indexes may create multiple match candidates, but individual {@link ExpansionVisitor}s
 * will return at most one candidate. So certain methods here return an {@link Optional}. The utility
 * method {@link #optionalToIterable(Optional)} can be used to turn that {@link Optional} into s
 * list containing the one candidate (if set).
 */
@API(API.Status.INTERNAL)
public final class MatchCandidateExpansion {
    @Nonnull
    private static final Logger LOGGER = LoggerFactory.getLogger(MatchCandidateExpansion.class);

    private MatchCandidateExpansion() {
    }

    /**
     * Utility method to turn an {@link Optional} into an {@link Iterable}. If the
     * optional is not empty, this will return a collection with a single element.
     * If the optional is empty, it will return an empty collection.
     *
     * @param optional an optional that may contain a match candidate
     * @return a collection containing the contents of the optional if set
     */
    @Nonnull
    public static <T> Iterable<T> optionalToIterable(@Nonnull Optional<T> optional) {
        return optional
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    @Nonnull
    public static Iterable<MatchCandidate> expandValueIndexMatchCandidate(@Nonnull RecordMetaData metaData, @Nonnull Index index, boolean isReverse) {
        final IndexExpansionInfo info = IndexExpansionInfo.createInfo(metaData, index, isReverse);
        return optionalToIterable(expandValueIndexMatchCandidate(info));
    }

    @Nonnull
    public static Optional<MatchCandidate> expandValueIndexMatchCandidate(@Nonnull IndexExpansionInfo info) {
        return expandIndexMatchCandidate(info, info.getCommonPrimaryKeyForTypes(),
                new ValueIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes()));
    }

    @Nonnull
    public static Iterable<MatchCandidate> expandAggregateIndexMatchCandidate(@Nonnull RecordMetaData metaData, @Nonnull Index index, boolean isReverse) {
        final IndexExpansionInfo info = IndexExpansionInfo.createInfo(metaData, index, isReverse);
        return optionalToIterable(expandAggregateIndexMatchCandidate(info));
    }

    @Nonnull
    public static Optional<MatchCandidate> expandAggregateIndexMatchCandidate(@Nonnull IndexExpansionInfo info) {
        // Override the common primary key here. We always want it to be null because the primary key is not
        // included in the expanded aggregate index
        return expandIndexMatchCandidate(info, null,
                new AggregateIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes()));
    }

    @Nonnull
    public static Optional<MatchCandidate> expandIndexMatchCandidate(@Nonnull IndexExpansionInfo info,
                                                                     @Nullable KeyExpression commonPrimaryKey,
                                                                     @Nonnull final ExpansionVisitor<?> expansionVisitor) {
        final var baseRef = createBaseRef(info, new IndexAccessHint(info.getIndexName()));
        try {
            return Optional.of(expansionVisitor.expand(() -> Quantifier.forEach(baseRef), commonPrimaryKey, info.isReverse()));
        } catch (final UnsupportedOperationException uOE) {
            // just log and return empty
            if (LOGGER.isDebugEnabled()) {
                final String message =
                        KeyValueLogMessage.of("unsupported index",
                                "reason", uOE.getMessage(),
                                "indexName", info.getIndexName());
                LOGGER.debug(message, uOE);
            }
        }
        return Optional.empty();
    }

    @Nonnull
    public static Optional<MatchCandidate> fromPrimaryDefinition(@Nonnull final RecordMetaData metaData,
                                                                 @Nonnull final Set<String> queriedRecordTypeNames,
                                                                 @Nullable KeyExpression primaryKey,
                                                                 final boolean isReverse) {
        if (primaryKey != null) {
            final var availableRecordTypes = metaData.getRecordTypes().values();
            final var queriedRecordTypes =
                    availableRecordTypes.stream()
                            .filter(recordType -> queriedRecordTypeNames.contains(recordType.getName()))
                            .collect(ImmutableList.toImmutableList());

            final var baseRef = createBaseRef(metaData.getRecordTypes().keySet(), queriedRecordTypeNames, metaData.getPlannerType(queriedRecordTypeNames), new PrimaryAccessHint());
            final var expansionVisitor = new PrimaryAccessExpansionVisitor(availableRecordTypes, queriedRecordTypes);
            return Optional.of(expansionVisitor.expand(() -> Quantifier.forEach(baseRef), primaryKey, isReverse));
        }

        return Optional.empty();
    }

    @Nonnull
    private static Reference createBaseRef(@Nonnull IndexExpansionInfo info,
                                          @Nonnull AccessHint accessHint) {
        return createBaseRef(info.getAvailableRecordTypeNames(), info.getIndexedRecordTypeNames(), info.getBaseType(), accessHint);
    }

    @Nonnull
    private static Reference createBaseRef(@Nonnull final Set<String> availableRecordTypeNames,
                                          @Nonnull final Set<String> queriedRecordTypeNames,
                                          @Nonnull final Type.Record baseType,
                                          @Nonnull AccessHint accessHint) {
        final var quantifier =
                Quantifier.forEach(
                        Reference.initialOf(
                                new FullUnorderedScanExpression(availableRecordTypeNames,
                                        new Type.AnyRecord(false),
                                        new AccessHints(accessHint))));
        return Reference.initialOf(
                new LogicalTypeFilterExpression(queriedRecordTypeNames,
                        quantifier,
                        baseType));
    }
}
