/*
 * IndexMatchCandidateRegistry.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;

import javax.annotation.Nonnull;

/**
 * A registry for mapping indexes to {@link MatchCandidate}s that can be used during
 * planning by the {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}.
 * This is structured as a registry so that new index types can define the match candidate
 * without needing to be defined in the core Record Layer repository.
 *
 * @see IndexMaintainerFactory#createMatchCandidates(RecordMetaData, Index, boolean)
 * @see IndexMaintainerFactoryRegistry for a sub-interface that delegates to the index maintainer factory
 * @see IndexMaintainerFactoryRegistryImpl for the default implementation
 */
public interface IndexMatchCandidateRegistry {

    /**
     * Create match candidates for the given {@link Index}.
     *
     * @param metaData the meta-data in which the index sits
     * @param index the index to expand into {@link MatchCandidate}s
     * @param reverse whether the query calls for the candidate to be reversed
     * @return a collection of {@link MatchCandidate}s representing this index
     * @see IndexMaintainerFactory#createMatchCandidates(RecordMetaData, Index, boolean)
     */
    Iterable<MatchCandidate> createMatchCandidates(@Nonnull RecordMetaData metaData,
                                                   @Nonnull Index index,
                                                   boolean reverse);
}
