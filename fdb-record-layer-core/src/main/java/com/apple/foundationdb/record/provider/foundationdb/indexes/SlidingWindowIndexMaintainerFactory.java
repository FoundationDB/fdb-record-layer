/*
 * SlidingWindowIndexMaintainerFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

/**
 * A decorator factory that wraps the {@link VectorIndexMaintainerFactory}'s output with a
 * {@link SlidingWindowIndexMaintainer} when the index carries a
 * {@link IndexPredicate.RowNumberWindowPredicate}.
 *
 * <p>Currently, sliding window semantics are only supported on
 * vector (HNSW) indexes. The sliding window limits the number of
 * vectors maintained in the HNSW graph by evicting the worst entries and re-electing from
 * overflow when entries are deleted. This is particularly useful for bounding the size of
 * expensive HNSW structures while maintaining search quality over a curated subset.</p>
 *
 * <p>This factory does not register its own index types via {@link #getIndexTypes()} (it returns
 * an empty list). Instead, the {@link com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl}
 * detects a sliding window index using {@link #isSlidingWindowIndex(Index)} and wraps the
 * vector index factory with this one. All operations — validation, maintainer creation, and match
 * candidate creation — are forwarded to the delegate after applying sliding-window-specific
 * logic where needed.</p>
 *
 * <p>The wrapping is transparent: consumers of the factory interface (e.g. {@code FDBRecordStore})
 * do not need to be aware of the sliding window decoration.</p>
 */
@API(API.Status.EXPERIMENTAL)
public class SlidingWindowIndexMaintainerFactory implements IndexMaintainerFactory {

    @Nonnull
    private final IndexMaintainerFactory delegateFactory;

    /**
     * Creates a new sliding window factory wrapping the given delegate vector index factory.
     *
     * @param delegateFactory the underlying vector index factory
     */
    public SlidingWindowIndexMaintainerFactory(@Nonnull IndexMaintainerFactory delegateFactory) {
        this.delegateFactory = delegateFactory;
    }

    /**
     * Checks whether the given index is a vector index with a
     * {@link IndexPredicate.RowNumberWindowPredicate} in its predicate tree, indicating it
     * should be wrapped with sliding window semantics. Only vector indexes are eligible for sliding window decoration.
     *
     * @param index the index to check
     * @return {@code true} if the index is a vector index with a sliding window predicate
     */
    public static boolean isSlidingWindowIndex(@Nonnull Index index) {
        return IndexTypes.VECTOR.equals(index.getType()) && findRowNumberWindowPredicate(index.getPredicate()) != null;
    }

    /**
     * Returns an empty list. This factory does not own any index type — it decorates
     * other factories and is wired in by the registry when a sliding window predicate is detected.
     */
    @Nonnull
    @Override
    public Iterable<String> getIndexTypes() {
        return ImmutableList.of();
    }

    /**
     * Returns a {@link SlidingWindowIndexValidator} that validates sliding-window-specific
     * constraints (single record type, no synthetic types, predicate placement) and then
     * delegates to the underlying factory's validator for type-specific checks (e.g. HNSW
     * configuration for vector indexes).
     *
     * @param index the index to validate
     * @return a composite validator
     */
    @Nonnull
    @Override
    public IndexValidator getIndexValidator(@Nonnull Index index) {
        return new SlidingWindowIndexValidator(index);
    }

    /**
     * Creates the delegate {@link IndexMaintainer} via the underlying factory, then wraps it
     * with a {@link SlidingWindowIndexMaintainer} that enforces the window semantics (eviction,
     * re-election, boundary tracking) on all mutations.
     *
     * @param state the state for the new index maintainer
     * @return a {@link SlidingWindowIndexMaintainer} wrapping the delegate
     */
    @Nonnull
    @Override
    public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
        final IndexMaintainer delegate = delegateFactory.getIndexMaintainer(state);
        return new SlidingWindowIndexMaintainer(state, delegate);
    }

    /**
     * Delegates match candidate creation to the underlying factory. The sliding window
     * does not alter how the planner matches against the index — it only affects mutations.
     *
     * @param metaData the record meta-data
     * @param index the index to create match candidates for
     * @param reverse whether the index is scanned in reverse
     * @return match candidates from the delegate factory
     */
    @Nonnull
    @Override
    public Iterable<MatchCandidate> createMatchCandidates(@Nonnull RecordMetaData metaData,
                                                           @Nonnull Index index, boolean reverse) {
        return delegateFactory.createMatchCandidates(metaData, index, reverse);
    }

    /**
     * Recursively searches a predicate tree for a {@link IndexPredicate.RowNumberWindowPredicate}.
     * The predicate may appear directly or as a child of an {@link IndexPredicate.AndPredicate}.
     *
     * @param predicate the root predicate to search (may be {@code null})
     * @return the found predicate, or {@code null} if none exists
     */
    @Nullable
    private static IndexPredicate.RowNumberWindowPredicate findRowNumberWindowPredicate(
            @Nullable IndexPredicate predicate) {
        if (predicate == null) {
            return null;
        }
        if (predicate instanceof IndexPredicate.RowNumberWindowPredicate) {
            return (IndexPredicate.RowNumberWindowPredicate) predicate;
        }
        if (predicate instanceof IndexPredicate.AndPredicate) {
            for (IndexPredicate child : ((IndexPredicate.AndPredicate) predicate).getChildren()) {
                IndexPredicate.RowNumberWindowPredicate found = findRowNumberWindowPredicate(child);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    /**
     * Validates sliding-window-specific constraints on the index, then delegates to the
     * underlying factory's validator for type-specific validation.
     *
     * <p>Sliding window constraints:</p>
     * <ul>
     *     <li>The index must be on exactly one (non-synthetic) record type.</li>
     *     <li>The index must have a predicate containing a
     *         {@link IndexPredicate.RowNumberWindowPredicate}.</li>
     *     <li>The {@link IndexPredicate.RowNumberWindowPredicate} must appear on a pure conjunctive
     *         (AND-only) path — it must not be nested under an OR or NOT.</li>
     * </ul>
     */
    private final class SlidingWindowIndexValidator extends IndexValidator {

        @Nonnull
        private final IndexValidator delegateIndexValidator;

        @Nonnull
        private final Index index;

        SlidingWindowIndexValidator(@Nonnull final Index index) {
            super(index);
            this.delegateIndexValidator = delegateFactory.getIndexValidator(index);
            this.index = index;
        }

        @Override
        public void validate(@Nonnull final MetaDataValidator metaDataValidator) {
            final RecordMetaData metaData = metaDataValidator.getRecordMetaData();
            final Collection<RecordType> delegateRecordTypes = metaData.recordTypesForIndex(index);

            if (delegateRecordTypes.isEmpty()) {
                throw new MetaDataException("sliding window index delegate is defined on an empty set of types");
            }
            if (delegateRecordTypes.size() != 1) {
                throw new MetaDataException("sliding window index delegate has multiple types",
                        LogMessageKeys.INDEX_NAME, index.getName());
            }
            if (delegateRecordTypes.stream().anyMatch(RecordType::isSynthetic)) {
                throw new MetaDataException("sliding window index is on synthetic record types",
                        LogMessageKeys.INDEX_NAME, index.getName());
            }
            if (!IndexTypes.VECTOR.equals(index.getType())) {
                throw new MetaDataException("sliding window index can only be defined on vector indexes");
            }
            @Nullable final IndexPredicate predicate = index.getPredicate();
            if (predicate == null) {
                throw new MetaDataException("attempt to create sliding window index without index predicate");
            }
            if (index.getBooleanOption(IndexOptions.UNIQUE_OPTION, false)) {
                throw new MetaDataException("sliding window index does not support unique indexes");
            }
            IndexPredicate.validateRowNumberWindowPlacement(index.getPredicate());
            delegateIndexValidator.validate(metaDataValidator);
        }
    }

}
