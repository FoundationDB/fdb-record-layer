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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;

import javax.annotation.Nonnull;

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
}
