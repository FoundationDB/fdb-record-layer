/*
 * SubspaceSplitter.java
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

package com.apple.foundationdb.map;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An interface to split a raw FoundationDB key into a subspace and (possibly) a
 * "tag". Because subspaces are really just prefixes that are applied to keys,
 * the subspace that a key is actually in will be specific to a given application.
 * This interface also provides a method that, if implemented, allows the user
 * to associate a "tag" with the subspace. This allows the user to associate
 * some application-meaningful value to the subspace. For example, the user
 * might have several subspaces of data that all share a common prefix followed
 * by a {@link com.apple.foundationdb.tuple.Tuple Tuple}-encoded integer.
 * The user might choose to implement {@link #subspaceOf(byte[]) subspaceOf()}
 * so that, given a key, it returns the full subspace (through the integer)
 * and implements {@link #subspaceTag(Subspace) subspaceTag()} so that
 * it returns the integer that is the last part of the prefix.
 *
 * @param <T> type of tag returned by the subspace splitter
 */
@API(API.Status.EXPERIMENTAL)
public interface SubspaceSplitter<T> {
    /**
     * Determine a {@link Subspace} that the given key is contained
     * within. In theory, any prefix of <code>keyBytes</code> could
     * be used as the raw-prefix of the subspace, but the implementation
     * should choose a prefix that is useful for grouping keys in
     * a way that is useful for that application.
     *
     * @param keyBytes the raw bytes of some FoundationDB key
     * @return a {@link Subspace} that contains <code>keyBytes</code>
     */
    @Nonnull
    Subspace subspaceOf(@Nonnull byte[] keyBytes);

    /**
     * Compute and return some application-specific "tag" for
     * a given subspace. This information can then be used
     * by the application to associate data with the subspace
     * it came from. The {@link Subspace} passed in can be assumed
     * to be the result of some call to {@link #subspaceOf(byte[]) subspaceOf()},
     * so implementations can make assumptions about the format
     * of the prefix of the subspace based on what are
     * legal return-values of the implementation of that function.
     * In general, if this function is called twice with the same
     * <code>Subspace</code>, it should return the same value
     * both times.
     *
     * <p>
     * By default, this function will just return <code>null</code>.
     * </p>
     *
     * @param subspace the {@link Subspace} to generate a tag from
     * @return some tag to associate with the provided subspace.
     */
    @Nullable
    default T subspaceTag(@Nonnull Subspace subspace) {
        return null;
    }
}
