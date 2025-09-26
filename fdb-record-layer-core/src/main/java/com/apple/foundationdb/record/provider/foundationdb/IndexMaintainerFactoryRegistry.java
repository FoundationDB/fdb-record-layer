/*
 * IndexMaintainerFactoryRegistry.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;

import javax.annotation.Nonnull;

/**
 * Registry of {@link IndexMaintainerFactory} objects. Each index type has an associated
 * implementation of {@link IndexMaintainerFactory} that can generate {@link IndexMaintainer}s
 * and {@link IndexValidator}s for indexes of that type. This registry stores the mapping of
 * the index type to one of those factory classes.
 */
@API(API.Status.UNSTABLE)
public interface IndexMaintainerFactoryRegistry extends IndexMaintainerRegistry, IndexMatchCandidateRegistry {
    /**
     * Get the {@link IndexMaintainerFactory} for the given index.
     * This should look at the index's {@linkplain Index#getType() type}
     * in order to identify the correct one.
     *
     * @param index the index to retrieve the maintainer factory for
     * @return an {@link IndexMaintainerFactory}
     * @throws com.apple.foundationdb.record.metadata.MetaDataException if the
     *     {@link IndexMaintainerFactory} cannot be retrieved (e.g., if the index type
     *     is unknown)
     */
    @Nonnull
    IndexMaintainerFactory getIndexMaintainerFactory(@Nonnull Index index);

    @Nonnull
    @Override
    default IndexValidator getIndexValidator(@Nonnull Index index) {
        final IndexMaintainerFactory factory = getIndexMaintainerFactory(index);
        return factory.getIndexValidator(index);
    }

    @Nonnull
    @Override
    default IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
        final IndexMaintainerFactory factory = getIndexMaintainerFactory(state.index);
        return factory.getIndexMaintainer(state);
    }

    @Override
    default Iterable<MatchCandidate> createMatchCandidates(@Nonnull RecordMetaData metaData, @Nonnull Index index, boolean reverse) {
        final IndexMaintainerFactory factory = getIndexMaintainerFactory(index);
        return factory.createMatchCandidates(metaData, index, reverse);
    }
}
