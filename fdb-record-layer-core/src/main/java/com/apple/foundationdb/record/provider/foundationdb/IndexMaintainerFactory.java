/*
 * IndexMaintainerFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;

import javax.annotation.Nonnull;
import java.util.Collections;

/**
 * A factory for {@link IndexMaintainer}.
 *
 * <p>
 * An index maintainer factory is associated with one or more index types.
 * It is also responsible for validation of {@link Index} meta-data.
 * </p>
 *
 * <p>
 * An index maintainer factory would typically be annotated to allow the classpath-based registry to find it.
 * </p>
 * <pre><code>
 * &#64;AutoService(IndexMaintainerFactory.class)
 * </code></pre>
 *
 * @see IndexMaintainerRegistry
 *
 */
@API(API.Status.UNSTABLE)
public interface IndexMaintainerFactory {
    /**
     * Get the index types supported by this factory.
     * @return a collection of strings of index types supported by this factory
     */
    @Nonnull
    Iterable<String> getIndexTypes();

    /**
     * Get a validator for the given index meta-data.
     * @param index an index that was produced by this factory
     * @return a validator for this kind of index
     */
    @Nonnull
    IndexValidator getIndexValidator(Index index);

    /**
     * Get an index maintainer for the given record store and index meta-data.
     * @param state the state of the new index maintainer
     * @return a new index maintainer for the type of index given
     */
    @Nonnull
    IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state);

    /**
     * Create {@link MatchCandidate}s to use for the given {@link Index}.
     * Implementors can assume that the index is one of this factory's
     * {@linkplain #getIndexTypes() supported types}. Note that this is
     * structured as a method on the maintainer factory so that indexes
     * defined outside the core repository can define the structure
     * used by the planner to match them. However, use cases should be
     * aware that the planner does not provide a stable API (yet) for
     * what these match candidates should contain, and so outside
     * implementors should be mindful that it is on them to keep up
     * with the planner as it changes. For that reason, this should only
     * be overridden by users who know what they are doing.
     *
     * <p>
     * By default, this returns an empty collection. This means that the
     * index will not be matchable by the {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}.
     * Index types that want to be used in plans should implement this
     * method. Utility methods in {@link MatchCandidate} can be used to
     * create, for example, canonical expansions of indexes that behave like
     * indexes defined in the core repository (e.g., {@link com.apple.foundationdb.record.metadata.IndexTypes#VALUE}
     * indexes).
     * </p>
     *
     * <p>
     * Indexes may return more than one {@link MatchCandidate}. This can be
     * useful for index types that have more than one way of being scanned.
     * For example, the {@link com.apple.foundationdb.record.metadata.IndexTypes#RANK}
     * index can be scanned {@link com.apple.foundationdb.record.IndexScanType#BY_VALUE},
     * at which point it behaves just like a {@link com.apple.foundationdb.record.metadata.IndexTypes#VALUE}
     * index, or it can be scanned {@link com.apple.foundationdb.record.IndexScanType#BY_RANK},
     * at which point it behaves very differently. The different scan types are reflected
     * in different {@link MatchCandidate} which encode different information about
     * the index and how it should be scanned.
     * </p>
     *
     * @param metaData the meta-data in which the index is defined
     * @param index the index to expend
     * @param reverse whether the index is to be scanned in reverse
     * @return a collection of {@link MatchCandidate}s to match the index against
     */
    @Nonnull
    default Iterable<MatchCandidate> createMatchCandidates(@Nonnull RecordMetaData metaData, @Nonnull Index index, boolean reverse) {
        return Collections.emptyList();
    }
}
