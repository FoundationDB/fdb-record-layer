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
import com.apple.foundationdb.record.ValueRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A <code>KeySpacePath</code> represents a discrete path through a directory tree defined by a <code>KeySpace</code>.
 * A <code>KeySpacePath</code> is started via {@link KeySpace#path(String)},
 * and a specific path may be traced down through the directory structure via successive calls to
 * {@link #add(String, Object)}. Once a desired path has been fully constructed, {@link #toTuple(FDBRecordContext)} is used
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
     * <p>This method has been deprecated. Paths should no longer be started with {@link KeySpace#path(FDBRecordContext, String, Object)},
     * and therefore should no longer need the ability to create a copy with a different context. Instead you should
     * be starting your paths with {@link KeySpace#path(String, Object)} and then only employ a
     * <code>FDBRecordContext</code> when the path is resolved with {@link #toTuple(FDBRecordContext)}
     *
     * @param newContext the new record context to use
     * @return a copy of this <code>KeySpacePath</code> containing a new transaction context. Note that the
     *   information contained in this path will still be based upon the transaction state at the time at which
     *   it was originally created; only new operations on the returned path will utilize the new transaction
     *   context
     * @deprecated use {@link KeySpace#path(String, Object)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    KeySpacePath copyWithNewContext(@Nonnull FDBRecordContext newContext);

    /**
     * Returns the {@link FDBRecordContext} this path was created with.
     * @return {@link FDBRecordContext} this path was created with
     *
     * @throws IllegalStateException if the path was not original started with a context
     *
     * @deprecated paths should no longer be constructed with a <code>FDBRecordContext</code>, instead all operations
     *   that require a context in order to resolve a tuple or subspace should pass the context in at that point
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
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
     * Retrieve the value that is to be stored for this directory entry.  For example, if the directory associated
     * with this entry is a <code>DirectoryLayerDirectory</code> the value returned will be the number assigned
     * by the directory layer for this path entry's value.
     *
     * @param context the context in which to resolve the value
     * @return future that will resolve to value to be store for this path element.  Note that if the path
     *   was produced via {@link KeySpace#pathFromKeyAsync(FDBRecordContext, Tuple)} or {@link #listAsync(FDBRecordContext, String, byte[], ScanProperties)},
     *   then the future that is returned will have already been completed (i.e it is safe to retrieve the
     *   value without blocking)
     */
    @Nonnull
    CompletableFuture<PathValue> resolveAsync(@Nonnull FDBRecordContext context);

    /**
     * If this path was created via a call to <code>pathFromKey</code> or <code>listAsync</code> (or their blocking
     * variants), this method may be used to determine what the underlying value was physically stored in the key.
     *
     * @return the value that was stored for the path element
     * @throws IllegalStateException if this path element was not produced from one of the above method calls
     */
    @Nonnull
    PathValue getStoredValue();

    /**
     * @return true if it is legal to call {@link #getStoredValue()}.
     */
    boolean hasStoredValue();

    /**
     * Returns a future representing the value that is to be stored for this position in the path when
     * the path value has been resolved by the directory.
     *
     * @return the value that will be stored for this path entry
     * @deprecated use {@link #resolveAsync(FDBRecordContext)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default CompletableFuture<Object> getResolvedValue() {
        return resolveAsync(getContext()).thenApply(PathValue::getResolvedValue);
    }

    /**
     * Returns the meta-data (if any) that is stored for this directory value.
     * @return a future that will complete to the meta-data for the stored directory value or null if there is no
     *   meta-data associated with the value
     * @deprecated use {@link #resolveAsync(FDBRecordContext)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default CompletableFuture<byte[]> getResolvedPathMetadata() {
        return resolveAsync(getContext()).thenApply(PathValue::getMetadata);
    }

    /**
     * Converts this path into a tuple. During this process the value that was provided for the directory, or
     * was resolved by the directory implementation, is validated to ensure that it is a valid type for the
     * directory.
     *
     * @return the tuple form of this path
     * @deprecated use {@link #toTuple(FDBRecordContext)} instead
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default Tuple toTuple() {
        return toTuple(getContext());
    }

    /**
     * Converts this path into a tuple. During this process the value that was provided for the directory, or
     * was resolved by the directory implementation, is validated to ensure that it is a valid type for the
     * directory.
     *
     * @param context the context in which to resolve the path
     * @return the tuple form of this path
     * @throws com.apple.foundationdb.record.RecordCoreArgumentException if the value generated for a position in the path is not valid for
     *   that particular position
     */
    @Nonnull
    default Tuple toTuple(@Nonnull FDBRecordContext context) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, toTupleAsync(context));
    }

    /**
     * Converts this path into a tuple. During this process the value that was provided for the directory, or
     * was resolved by the directory implementation, is validated to ensure that it is a valid type for the
     * directory.
     *
     * @return a future that will complete to the tuple representation of this path
     * @deprecated use {@link #toTupleAsync(FDBRecordContext)} instead
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default CompletableFuture<Tuple> toTupleAsync() {
        return toTupleAsync(getContext());
    }

    /**
     * Converts this path into a tuple. During this process the value that was provided for the directory, or
     * was resolved by the directory implementation, is validated to ensure that it is a valid type for the
     * directory.
     *
     * @param context the context in which the path is to be resolved
     * @return a future that will complete to the tuple representation of this path
     */
    @Nonnull
    CompletableFuture<Tuple> toTupleAsync(@Nonnull FDBRecordContext context);

    /**
     * Converts the tuple produced for this path to a subspace.
     *
     * @return The subspace from the resolved path.
     * @deprecated use {@link #toSubspace(FDBRecordContext)} instead
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default Subspace toSubspace() {
        return toSubspace(getContext());
    }

    /**
     * Converts the tuple produced for this path to a subspace.
     *
     * @param context the context in which to resolve the path
     * @return The subspace from the resolved path.
     */
    default Subspace toSubspace(FDBRecordContext context) {
        return new Subspace(toTuple(context));
    }

    /**
     * Converts the tuple produced for this path to a subspace.
     *
     * @param context the context in which to resolve the path
     * @return a future that completes with the subspace for this path
     */
    @Nonnull
    default CompletableFuture<Subspace> toSubspaceAsync(@Nonnull FDBRecordContext context) {
        return toTupleAsync(context).thenApply(Subspace::new);
    }

    /**
     * Converts the tuple produced for this path to a subspace.
     *
     * @return a future that completes with the subspace for this path
     * @deprecated use {@link #toSubspaceAsync(FDBRecordContext)} instead
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default CompletableFuture<Subspace> toSubspaceAsync() {
        return toTupleAsync(getContext()).thenApply(Subspace::new);
    }

    /**
     * Flattens the path into a list of <code>KeySpacePath</code> entries, with the root of the path
     * located at position 0.
     * @return this path as a list
     */
    @Nonnull
    List<KeySpacePath> flatten();

    /**
     * Check whether data exists for this path.
     * @return a future that evaluates to {@code true} if data exists for this path
     * @deprecated use {@link #hasDataAsync(FDBRecordContext)} instead
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default CompletableFuture<Boolean> hasDataAsync() {
        return hasDataAsync(getContext());
    }

    /**
     * Check whether data exists for this path.
     *
     * @param context the context in which the path is resolved and a scan is performed looking for data
     * @return a future that evaluates to {@code true} if data exists for this path
     */
    @Nonnull
    CompletableFuture<Boolean> hasDataAsync(FDBRecordContext context);

    /**
     * Synchronous version of {@link #hasDataAsync()}.
     *
     * @return {@code true} if data exists for this path
     * @deprecated use {@link #hasData(FDBRecordContext)} instead
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    default boolean hasData() {
        return hasData(getContext());
    }

    /**
     * Synchronous version of {@link #hasDataAsync(FDBRecordContext)}.
     *
     * @param context the context in which the path is resolved and a scan is performed looking for data
     * @return {@code true} if data exists for this path
     */
    default boolean hasData(@Nonnull FDBRecordContext context) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_SCAN, hasDataAsync(context));
    }

    /**
     * Delete all data from this path. Use with care.
     *
     * @return a future that will delete all data underneath of this path
     * @deprecated use {@link #deleteAllDataAsync(FDBRecordContext)} instead
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default CompletableFuture<Void> deleteAllDataAsync() {
        return deleteAllDataAsync(getContext());
    }

    /**
     * Delete all data from this path. Use with care.
     *
     * @param context the context in which the path is resolved and the delete operation takes place
     * @return a future that will delete all data underneath of this path
     */
    @Nonnull
    CompletableFuture<Void> deleteAllDataAsync(@Nonnull FDBRecordContext context);

    /**
     * Synchronous version of {@link #deleteAllDataAsync()}.
     *
     * @deprecated use {@link #deleteAllData(FDBRecordContext)} instead
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    default void deleteAllData() {
        deleteAllData(getContext());
    }

    /**
     * Synchronous version of {@link #deleteAllDataAsync(FDBRecordContext)}.
     *
     * @param context the context in which the path is resolved and the delete operation takes place
     */
    default void deleteAllData(@Nonnull FDBRecordContext context) {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_CLEAR, deleteAllDataAsync(context));
    }

    /**
     * For a given subdirectory from this path element, return a list of paths for all available keys in the FDB
     * keyspace for that directory.
     *
     * @param subdirName the name of the subdirectory that is to be listed
     * @param continuation an optional continuation from a previous list attempt
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     * @throws NoSuchDirectoryException if the subdirectory name provided does not exist
     * @throws com.apple.foundationdb.record.RecordCoreException if a key found during the listing process did not correspond to
     *    the directory tree
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     * @deprecated use {@link #listAsync(FDBRecordContext, String, byte[], ScanProperties)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default RecordCursor<KeySpacePath> listAsync(@Nonnull String subdirName, @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        return listAsync(getContext(), subdirName, null, continuation, scanProperties);
    }

    /**
     * For a given subdirectory from this path element, return a list of paths for all available keys in the FDB
     * keyspace for that directory.
     *
     * @param subdirName the name of the subdirectory that is to be listed
     * @param range the range of the subdirectory values to be listed. All will be listed if it is <code>null</code>.
     *     If the directory is restricted to a specific constant value, it has to be <code>null</code>
     * @param continuation an optional continuation from a previous list attempt
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     * @throws NoSuchDirectoryException if the subdirectory name provided does not exist
     * @throws com.apple.foundationdb.record.RecordCoreException if a key found during the listing process did not correspond to
     *    the directory tree
     * @throws IllegalStateException if the path was started without a context (e.g. it was started with
     *   {@link KeySpace#path(String, Object)})
     * @deprecated use {@link #listAsync(FDBRecordContext, String, ValueRange, byte[], ScanProperties)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default RecordCursor<KeySpacePath> listAsync(@Nonnull String subdirName, 
                                                 @Nullable ValueRange<?> range,
                                                 @Nullable byte[] continuation,
                                                 @Nonnull ScanProperties scanProperties) {
        return listAsync(getContext(), subdirName, range, continuation, scanProperties);
    }

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
     * @param context the transaction in which to perform the listing
     * @param subdirName the name of the subdirectory that is to be listed
     * @param continuation an optional continuation from a previous list attempt
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     * @throws NoSuchDirectoryException if the subdirectory name provided does not exist
     * @throws com.apple.foundationdb.record.RecordCoreException if a key found during the listing process did not correspond to
     *    the directory tree
     */
    @Nonnull
    default RecordCursor<KeySpacePath> listAsync(@Nonnull FDBRecordContext context,
                                                 @Nonnull String subdirName, @Nullable byte[] continuation,
                                                 @Nonnull ScanProperties scanProperties) {
        return listAsync(context, subdirName, null, continuation, scanProperties);
    }

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
     * @param context the transaction in which to perform the listing
     * @param subdirName the name of the subdirectory that is to be listed
     * @param range the range of the subdirectory values to be listed. All will be listed if it is <code>null</code>.
     *     If the directory is restricted to a specific constant value, it has to be <code>null</code>
     * @param continuation an optional continuation from a previous list attempt
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     * @throws NoSuchDirectoryException if the subdirectory name provided does not exist
     * @throws com.apple.foundationdb.record.RecordCoreException if a key found during the listing process did not correspond to
     *    the directory tree
     */
    @Nonnull
    RecordCursor<KeySpacePath> listAsync(@Nonnull FDBRecordContext context, 
                                         @Nonnull String subdirName, 
                                         @Nullable ValueRange<?> range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties);

    /**
     * Synchronous version of <code>listAsync</code>.
     *
     * @param subdirName the name of the subdirectory that is to be listed
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     * @deprecated use {@link #list(FDBRecordContext, String, ScanProperties)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default List<KeySpacePath> list(@Nonnull String subdirName, @Nonnull ScanProperties scanProperties) {
        return list(getContext(), subdirName, scanProperties);
    }

    /**
     * Synchronous version of <code>listAsync</code>.
     *
     * @param context the transaction in which to perform the listing
     * @param subdirName the name of the subdirectory that is to be listed
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     */
    @Nonnull
    default List<KeySpacePath> list(@Nonnull FDBRecordContext context, @Nonnull String subdirName,
                                    @Nonnull ScanProperties scanProperties) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST, listAsync(context, subdirName, null, scanProperties).asList());
    }

    /**
     * Synchronous version of <code>listAsync</code>.
     *
     * @param context the transaction in which to perform the listing
     * @param subdirName the name of the subdirectory that is to be listed
     * @param range the range of the subdirectory values to be listed. All will be listed if it is <code>null</code>.
     *     If the directory is restricted to a specific constant value, it has to be <code>null</code>
     * @param continuation an optional continuation from a previous list attempt
     * @param scanProperties details for how the scan should be performed
     * @return a list of fully qualified paths for each value contained within this directory
     */
    @Nonnull
    default List<KeySpacePath> list(@Nonnull FDBRecordContext context, @Nonnull String subdirName,
                                    @Nullable ValueRange<?> range,
                                    @Nullable byte[] continuation,
                                    @Nonnull ScanProperties scanProperties) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST, listAsync(context, subdirName, range, continuation, scanProperties).asList());
    }

    /**
     * Synchronous version of <code>listAsync</code> that performs a forward, serializable scan.
     *
     * @param subdirName the name of the subdirectory that is to be listed
     * @return a list of fully qualified paths for each value contained within this directory
     *
     * @deprecated use {@link #list(FDBRecordContext, String, ScanProperties)} instead
     */
    @API(API.Status.DEPRECATED)
    @Deprecated
    @Nonnull
    default List<KeySpacePath> list(@Nonnull String subdirName) {
        return list(getContext(), subdirName, ScanProperties.FORWARD_SCAN);
    }

    /**
     * Synchronous version of <code>listAsync</code> that performs a forward, serializable scan.
     *
     * @param context the transaction in which to perform the listing
     * @param subdirName the name of the subdirectory that is to be listed
     * @return a list of fully qualified paths for each value contained within this directory
     */
    @Nonnull
    default List<KeySpacePath> list(@Nonnull FDBRecordContext context, @Nonnull String subdirName) {
        return list(context, subdirName, ScanProperties.FORWARD_SCAN);
    }
}
