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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class MatchCandidateExpansion {
    private static final Logger LOGGER = LoggerFactory.getLogger(MatchCandidateExpansion.class);

    private MatchCandidateExpansion() {
    }

    @Nonnull
    public static Iterable<MatchCandidate> expandValueIndexMatchCandidate(@Nonnull RecordMetaData metaData, @Nonnull Index index, boolean isReverse) {
        final IndexExpansionInfo info = createInfo(metaData, index, isReverse);
        return expandValueIndexMatchCandidate(info)
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    public static Optional<MatchCandidate> expandValueIndexMatchCandidate(@Nonnull IndexExpansionInfo info) {
        return expandIndexMatchCandidate(info, info.getCommonPrimaryKeyForTypes(), new ValueIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes()));
    }

    @Nonnull
    public static Iterable<MatchCandidate> expandAggregateIndexMatchCandidate(@Nonnull RecordMetaData metaData, @Nonnull Index index, boolean isReverse) {
        final IndexExpansionInfo info = createInfo(metaData, index, isReverse);
        return expandAggregateIndexMatchCandidate(info)
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    @Nonnull
    public static Optional<MatchCandidate> expandAggregateIndexMatchCandidate(@Nonnull IndexExpansionInfo info) {
        final var aggregateIndexExpansionVisitor = IndexTypes.BITMAP_VALUE.equals(info.getIndexType())
                ? new BitmapAggregateIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes())
                : new AggregateIndexExpansionVisitor(info.getIndex(), info.getIndexedRecordTypes());
        // Override the common primary key here. We always want it to be null because the primary key is not
        // included in the expanded aggregate index
        return expandIndexMatchCandidate(info, null, aggregateIndexExpansionVisitor);
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

            final var baseRef = createBaseRef(metaData.getRecordTypes().keySet(), queriedRecordTypeNames, queriedRecordTypes, new PrimaryAccessHint());
            final var expansionVisitor = new PrimaryAccessExpansionVisitor(availableRecordTypes, queriedRecordTypes);
            return Optional.of(expansionVisitor.expand(() -> Quantifier.forEach(baseRef), primaryKey, isReverse));
        }

        return Optional.empty();
    }

    @Nonnull
    private static Reference createBaseRef(@Nonnull IndexExpansionInfo info,
                                          @Nonnull AccessHint accessHint) {
        return createBaseRef(info.getAvailableRecordTypeNames(), info.getIndexedRecordTypeNames(), info.getIndexedRecordTypes(), accessHint);
    }

    @Nonnull
    private static Reference createBaseRef(@Nonnull final Set<String> availableRecordTypeNames,
                                          @Nonnull final Set<String> queriedRecordTypeNames,
                                          @Nonnull final Collection<RecordType> queriedRecordTypes,
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
                        Type.Record.fromFieldDescriptorsMap(RecordMetaData.getFieldDescriptorMapFromTypes(queriedRecordTypes))));
    }

    /**
     * Class encapsulating the information necessary to create a {@link MatchCandidate}
     * for an {@link Index}. This exists as a convenience class that allows certain
     * information to be computed once and referenced during match candidate creation.
     */
    @API(API.Status.INTERNAL)
    public static class IndexExpansionInfo {
        @Nonnull
        private final RecordMetaData metaData;
        @Nonnull
        private final Index index;
        private final boolean reverse;
        @Nullable
        private final KeyExpression commonPrimaryKeyForTypes;
        @Nonnull
        private final Collection<RecordType> indexedRecordTypes;
        @Nonnull
        private final Set<String> indexedRecordTypeNames;

        private IndexExpansionInfo(@Nonnull RecordMetaData metaData,
                                   @Nonnull Index index,
                                   boolean reverse,
                                   @Nonnull Collection<RecordType> indexedRecordTypes,
                                   @Nonnull Set<String> indexedRecordTypeNames,
                                   @Nullable KeyExpression commonPrimaryKeyForTypes) {
            this.metaData = metaData;
            this.index = index;
            this.reverse = reverse;
            this.indexedRecordTypes = indexedRecordTypes;
            this.indexedRecordTypeNames = indexedRecordTypeNames;
            this.commonPrimaryKeyForTypes = commonPrimaryKeyForTypes;
        }

        @Nonnull
        public RecordMetaData getMetaData() {
            return metaData;
        }

        @Nonnull
        public Index getIndex() {
            return index;
        }

        @Nonnull
        public String getIndexName() {
            return index.getName();
        }

        @Nonnull
        public String getIndexType() {
            return index.getType();
        }

        public boolean isReverse() {
            return reverse;
        }

        @Nonnull
        public Collection<RecordType> getIndexedRecordTypes() {
            return indexedRecordTypes;
        }

        @Nonnull
        public Set<String> getIndexedRecordTypeNames() {
            return indexedRecordTypeNames;
        }

        @Nullable
        public KeyExpression getCommonPrimaryKeyForTypes() {
            return commonPrimaryKeyForTypes;
        }

        @Nonnull
        public Set<String> getAvailableRecordTypeNames() {
            return metaData.getRecordTypes().keySet();
        }
    }

    /**
     * Create an {@link IndexExpansionInfo} for a given index.
     * This wraps the given parameters into a single object, as well
     * as enriching the given parameters with pre-calculated items that
     * can then be used during index expansion.
     *
     * @param metaData the meta-data that is the source of the index
     * @param index the index that we are expanding
     * @param reverse whether the query requires this scan be in reverse
     * @return an object encapsulating information about the index
     */
    @Nonnull
    public static IndexExpansionInfo createInfo(@Nonnull RecordMetaData metaData,
                                                @Nonnull Index index,
                                                boolean reverse) {
        @Nonnull
        final Collection<RecordType> indexedRecordTypes = Collections.unmodifiableCollection(metaData.recordTypesForIndex(index));
        @Nonnull
        final Set<String> indexedRecordTypeNames = indexedRecordTypes.stream()
                .map(RecordType::getName)
                .collect(ImmutableSet.toImmutableSet());
        @Nullable
        final KeyExpression commonPrimaryKeyForTypes = RecordMetaData.commonPrimaryKey(indexedRecordTypes);

        return new IndexExpansionInfo(metaData, index, reverse, indexedRecordTypes, indexedRecordTypeNames, commonPrimaryKeyForTypes);
    }
}
