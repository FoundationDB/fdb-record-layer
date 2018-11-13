/*
 * KeySpacePath.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A <code>KeySpacePath</code> represents a discrete path through a directory tree defined by a <code>KeySpace</code>.
 * A <code>KeySpacePath</code> is started via {@link KeySpace#path(FDBRecordContext, String)},
 * and a specific path may be traced down through the directory structure via successive calls to
 * {@link #add(String, Object)}. Once a desired path has been fully constructed, {@link #toTuple()} is used
 * to turn the resulting path into a <code>Tuple</code> to represent the FDB row key.
 */
@API(API.Status.MAINTAINED)
public interface KeySpacePath {

    /**
     * Get the length of the path.
     * @return the length of the path
     */
    default int size() {
        KeySpacePath root = this;
        int size = 0;
        while (root != null) {
            ++size;
            root = root.getParent();
        }
        return size;
    }

    /**
     * Creates a copy of the entire path utilizing a new transaction context. Certain operations, such as
     * {@link #listAsync(String, byte[], ScanProperties)}, may take more time or resources to perform than can
     * can be encompassed in a single transaction, so this method provides a mechanism by which the path can
     * be reconstructed with a new transaction between continuations.
     *
     * @param newContext the new record context to use
     * @return a copy of this <code>KeySpacePath</code> containing a new transaction context. Note that the
     *   information contained in this path will still be based upon the transaction state at the time at which
     *   it was originally created; only new operations on the returned path will utilize the new transaction
     *   context
     */
    @Nonnull
    KeySpacePath copyWithNewContext(@Nonnull FDBRecordContext newContext);

    /**
     * Returns the {@link FDBRecordContext} this path was created with.
     * @return {@link FDBRecordContext} this path was created with
     */
    @Nonnull
    FDBRecordContext getContext();

    /**
     * Adds the constant value for subdirectory <code>dirName</code> to the directory path.
     * @param dirName the name of the subdirectory to add to the path
     * @return this path
     * @throws NoSuchDirectoryException if the specified subdirectory does not exist
     * @throws com.apple.foundationdb.record.RecordCoreArgumentException if the subdirectory does not have a constant value
     */
    @Nonnull
    KeySpacePath add(@Nonnull String dirName);

    /**
     * Adds a <code>value</code> for a specific subdirectory <code>dirName</code> to the directory path.
     * @param dirName the name of the subdirectory to add to the path
     * @param value the value to use for the subdirectory
     * @return this path
     * @throws NoSuchDirectoryException if the specified subdirectory does not exist
     * @throws com.apple.foundationdb.record.RecordCoreArgumentException if the type of the value is not appropriate for the
     *   provided directory or differs from the constant value specified for the directory
     */
    @Nonnull
    KeySpacePath add(@Nonnull String dirName, @Nullable Object value);

    /**
     * If this path was created via {@link KeySpace#pathFromKey(FDBRecordContext, Tuple)}, this returns
     * any remaining portion of the input tuple that was not used to construct the path.
     * @return the remaining portion of the original input tuple or <code>null</code>
     */
    @Nullable
    Tuple getRemainder();

    /**
     * Returns the parent of this entry or null if this is the root of the path.
     * @return the parent keyspace path
     */
    @Nullable
    KeySpacePath getParent();

    /**
     * Returns the directory name for this path element.
     * @return the directory name
     */
    @Nonnull
    String getDirectoryName();

    /**
     * Returns the directory that corresponds to this path entry.
     * @return returns the directory that corresponds to this path entry
     */
    @Nonnull
    KeySpaceDirectory getDirectory();

    /**
     * Returns the value that was provided when to {@link #add(String, Object)} when this path was constructed.
     * Note that for some directory types, such as {@link DirectoryLayerDirectory}, this may not be the value
     * that is actually stored at this path element. For that, use {@link #getResolvedValue()}.
     * @return the path value
     */
    @Nullable
    Object getValue();

    /**
     * Get the value that will be stored for the directory at the specific path element.
     * @return a future that completes with the resolved form of the path value
     * @see #getValue
     */
    @Nonnull
    CompletableFuture<Object> getResolvedValue();

    /**
     * Get the metadata stored for the directory entry at the specific path element.
     * @return a future that completes with the metadata associated with the path element
     */
    @Nonnull
    CompletableFuture<byte[]> getResolvedPathMetadata();

    /**
     * Converts this path into a tuple, during this process the value that was provided for the directory, or
     * was resolved by the directory implementation, is validated to ensure that it is a valid type for the
     * directory.
     * @return the tuple form of this path
     * @throws com.apple.foundationdb.record.RecordCoreArgumentException If the value generated for a position in the path is not valid for
     *   that particular position.
     */
    @Nonnull
    Tuple toTuple();

    @Nonnull
    CompletableFuture<Tuple> toTupleAsync();

    /**
     * Converts the tuple produced for this path to a subspace.
     * @return a future that completes with the subspace for this path
     */
    @Nonnull
    default CompletableFuture<Subspace> toSubspaceAsync() {
        return toTupleAsync().thenApply(Subspace::new);
    }

    /**
     * Synchronous form of {@link #toSubspaceAsync()}.
     * @return the subspace for this path
     */
    @Nonnull
    default Subspace toSubspace() {
        return new Subspace(toTuple());
    }

    /**
     * Flattens the path into a list of <code>KeySpacePath</code> entries, with the root of the path
     * located at position 0.
     * @return this path as a list
     */
    @Nonnull
    List<KeySpacePath> flatten();

    /**
     * Get whether data exists for this path.
     * @return a future that evaluates to {@code true} if data exists for this path
     */
    @Nonnull
    CompletableFuture<Boolean> hasDataAsync();

    /**
     * Synchronous version of {@link #hasDataAsync()}.
     * @return {@code true} if data exists for this path
     */
    boolean hasData();

    /**
     * Delete all data from this path. Use with care.
     * @return a future that will delete all data underneath of this path
     */
    @Nonnull
    CompletableFuture<Void> deleteAllDataAsync();

    /**
     * Synchronous version of {@link #deleteAllDataAsync()}.
     */
    void deleteAllData();

    /**
     * For a given subdirectory from this path element, return a list of paths for all available keys in the FDB
     * keyspace for that directory. For example, given the tree:
     * <pre>
     * root
     *   +- node
     *       +- leaf
     * </pre>
     * Performing a <code>listAsync</code> from a given <code>node</code>, will result in a list of paths, one for
     * each <code>leaf</code> that is available within the <code>node</code>'s scope.
     *
     * <p>The listing is performed by reading the first key of the data type (and possibly constant value) for the
     * subdirectory and, if a key is found, skipping to the next available value after the first one that was found,
     * and so one, each time resulting in an additional <code>KeySpacePath</code> that is returned.  In each case,
     * the returned <code>KeySpacePath</code> may contain a remainder (see {@link #getRemainder()}) of the portion
     * of the key tuple that was read.
     *
     * @param subdirName the name of the subdirectory that is to be listed
     * @param continuation an optional continuation from a previous list attempt
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     * @throws NoSuchDirectoryException if the subdirectory name provided does not exist
     * @throws com.apple.foundationdb.record.RecordCoreException if a key found during the listing process did not correspond to
     *    the directory tree
     */
    @Nonnull
    RecordCursor<KeySpacePath> listAsync(@Nonnull String subdirName, @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties);

    /**
     * Synchronous version of <code>listAsync</code>.
     * @param subdirName the name of the subdirectory that is to be listed
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     */
    @Nonnull
    List<KeySpacePath> list(@Nonnull String subdirName, @Nonnull ScanProperties scanProperties);

    /**
     * Synchronous version of <code>listAsync</code> that performs a forward, serializable scan.
     * @param subdirName the name of the subdirectory that is to be listed
     * @return a list of fully qualified paths for each value contained within this directory
     */
    @Nonnull
    default List<KeySpacePath> list(@Nonnull String subdirName) {
        return list(subdirName, ScanProperties.FORWARD_SCAN);
    }
}
