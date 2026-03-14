/*
 * KeySpaceDirectory.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.KeyRange;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.ValueRange;
import com.apple.foundationdb.record.cursors.ChainedCursor;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Defines a directory within a {@link KeySpace}.
 */
@API(API.Status.UNSTABLE)
public class KeySpaceDirectory {
    /**
     * Return value from <code>pathFromKey</code> to indicate that a given tuple value is not appropriate
     * for a given directory.
     */
    protected static final CompletableFuture<Optional<ResolvedKeySpacePath>> DIRECTORY_NOT_FOR_KEY =
            CompletableFuture.completedFuture(Optional.empty());

    /**
     * The value of a directory that may contain any value of the type specified for the directory (as opposed
     * to a directory that is restricted to a specific constant value).
     */
    public static final Object ANY_VALUE = new AnyValue();

    @Nullable
    protected KeySpaceDirectory parent;
    @Nonnull
    protected final String name;
    @Nonnull
    protected final KeyType keyType;
    @Nullable
    protected final Object value;
    @Nonnull
    protected final Map<String, KeySpaceDirectory> subdirsByName = new HashMap<>();
    @Nonnull
    protected final List<KeySpaceDirectory> subdirs = new ArrayList<>();
    @Nullable
    protected Function<KeySpacePath, KeySpacePath> wrapper;

    /**
     * Creates a directory.
     * The wrapper is useful if you want to
     * decorate the path with additional functionality, such as the ability to retrieve a child path
     * element via an explicit function name rather than a string constant (e.g. <code>getEmployeePath(int id)</code>).
     *
     * @param name the name of the directory
     * @param keyType the data type of the values that may be contained within the directory
     * @param value a <code>value</code> of {@link #ANY_VALUE} indicates that any value of the specified
     * type may be stored in the directory, otherwise specifies a constant value that represents the
     * directory
     * @param wrapper if non-null, specifies a function that may be used to wrap any <code>KeySpacePath</code>
     * objects return from {@link KeySpace#resolveFromKeyAsync(FDBRecordContext, Tuple)}.
     *
     * @throws RecordCoreArgumentException if the provided value constant value is not valid for the
     * type of directory being created
     */
    @SuppressWarnings({"PMD.CompareObjectsWithEquals", "this-escape"})
    public KeySpaceDirectory(@Nonnull String name, @Nonnull KeyType keyType, @Nullable Object value,
                             @Nullable Function<KeySpacePath, KeySpacePath> wrapper) {

        this.name = name;
        this.keyType = keyType;
        this.value = value;
        this.wrapper = wrapper;

        if (value != keyType.getAnyValue()) {
            // NOTE: this method actually _is_ overridden by DirectoryLayerDirectory, but not in a way that depends
            // on its constructor having been called.
            validateConstant(value);
        }
    }

    /**
     * Creates a directory that can hold any value of a given type.
     * The wrapper is useful if you want to
     * decorate the path with additional functionality, such as the ability to retrieve a child path
     * element via an explicit function name rather than a string constant (e.g. <code>getEmployeePath(int id)</code>).
     * @param name the name of the directory
     * @param keyType the data type of the values that may be contained within the directory
     * @param wrapper if non-null, specifies a function that may be used to wrap any <code>KeySpacePath</code>
     * objects returned from {@link KeySpace#resolveFromKeyAsync(FDBRecordContext, Tuple)}
     */
    public KeySpaceDirectory(@Nonnull String name, @Nonnull KeyType keyType, @Nullable Function<KeySpacePath, KeySpacePath> wrapper) {
        this(name, keyType, keyType.getAnyValue(), wrapper);
    }

    /**
     * Creates a directory that can hold any value of a given type.
     * @param name the name of the directory
     * @param keyType the data type of the values that may be contained within the directory
     */
    public KeySpaceDirectory(@Nonnull String name, @Nonnull KeyType keyType) {
        this(name, keyType, keyType.getAnyValue(), null);
    }

    public KeySpaceDirectory(@Nonnull String name, @Nonnull KeyType keyType, @Nullable Object value) {
        this(name, keyType, value, null);
    }

    /**
     * Get the depth in the directory tree of this directory.
     * @return the depth in the directory tree of this directory
     */
    public int depth() {
        int depth = 0;
        KeySpaceDirectory current = this;
        while (current != null) {
            ++depth;
            current = current.getParent();
        }
        return depth;
    }

    /**
     * Called during creation to validate that the constant value provided is of a valid type for this
     * directory.
     * @param value constant value to validate
     */
    protected void validateConstant(@Nullable Object value) {
        if (!keyType.isMatch(value)) {
            throw new RecordCoreArgumentException("Illegal constant value provided for directory",
                    LogMessageKeys.DIR_NAME, getName(),
                    LogMessageKeys.DIR_VALUE, value);
        }
    }

    /**
     * Validate that the given value can be used with this directory.
     * <p>
     *     Ideally this would be called as part of {@link KeySpacePath#add(String, Object)} to ensure the provided value
     *     is valid, but that code has existed for a long time, so it's possible clients are adding without respecting
     *     the type. You should call this before calling add to make sure you don't have the same mistakes. At some point
     *     this will be embedded in {@code add} once there's some confidence that it won't break anyone's environments.
     * </p>
     * @param value a potential value
     * @throws RecordCoreArgumentException if the value is not valid
     */
    @API(API.Status.EXPERIMENTAL)
    public void validateValue(@Nullable Object value) {
        // Validate that the value is valid for this directory
        if (!isValueValid(value)) {
            throw new RecordCoreArgumentException("Value does not match directory requirements")
                .addLogInfo(LogMessageKeys.DIR_NAME, name,
                    LogMessageKeys.EXPECTED_TYPE, getKeyType(),
                    LogMessageKeys.ACTUAL, value,
                    "actual_type", value == null ? "null" : value.getClass().getName(),
                    "expected_value", getValue());
        }
    }

    /**
     * Checks if the provided value is valid for this directory. This method can be overridden by subclasses
     * to provide custom validation logic. For example, {@link DirectoryLayerDirectory} accepts String
     * values (logical names) even though its key type is LONG.
     *
     * @param value the value to validate
     * @return {@code true} if the value is valid for this directory
     */
    @API(API.Status.EXPERIMENTAL)
    public boolean isValueValid(@Nullable Object value) {
        // Check if value matches the key type
        if (!keyType.isMatch(value)) {
            return false;
        }
        // If this directory has a constant value, check that the provided value matches it
        if (this.value != ANY_VALUE) {
            if (this.value instanceof byte[] && value instanceof byte[]) {
                return Arrays.equals((byte[]) this.value, (byte[]) value);
            } else {
                return Objects.equals(this.value, value);
            }
        }
        return true;
    }

    /**
     * Given a position in a tuple, checks to see if this directory is compatible with the value at the
     * position, returning either a path indicating that it was compatible or nothing if it was not compatible.
     * This method allows overriding implementations to consume as much or as little of the tuple as necessary
     * (for example, you could have a directory that needed two tuple elements to represent itself) or to
     * have finer grained control over how the <code>KeySpacePath</code> for itself is constructed.
     * If key size if less than the actual key length, the path remainder will reflect
     * the left over key elements.
     *
     * @param context the database context
     * @param parent the parent path element
     * @param key the tuple being parsed
     * @param keySize the logical key size.
     * @param keyIndex the position in the index being parsed
     * @return the {@link KeySpacePath} representing the leaf-most path for the provided {@code key}
     * or <code>Optional.empty()</code> if this directory is not compatible with the element at
     * {@code keyIndex} position in the {@code key}
     */
    @Nonnull
    protected CompletableFuture<Optional<ResolvedKeySpacePath>> pathFromKey(@Nonnull FDBRecordContext context,
                                                                            @Nullable ResolvedKeySpacePath parent,
                                                                            @Nonnull Tuple key,
                                                                            final int keySize,
                                                                            final int keyIndex) {
        final Object tupleValue = key.get(keyIndex);
        if (KeyType.typeOf(tupleValue) != getKeyType()
                || (this.value != ANY_VALUE && !areEqual(this.value, tupleValue))) {
            return DIRECTORY_NOT_FOR_KEY;
        }

        final KeySpacePath parentPath = parent == null ? null : parent.toPath();
        final PathValue pathValue = new PathValue(tupleValue);

        return toTupleValueAsync(context, tupleValue).thenCompose(resolvedValue -> {
            // Have we hit the leaf of the tree or run out of tuple to process?
            if (subdirs.isEmpty() || keyIndex + 1 == keySize) {
                final Tuple remainder = (keyIndex + 1 == key.size()) ? null : TupleHelpers.subTuple(key, keyIndex + 1, key.size());
                final KeySpacePath path = KeySpacePathImpl.newPath(parentPath, this, tupleValue);

                return CompletableFuture.completedFuture(
                        Optional.of(new ResolvedKeySpacePath(parent, path, new PathValue(tupleValue), remainder)));
            } else {
                final KeySpacePath path = KeySpacePathImpl.newPath(parentPath, this, tupleValue);
                return findChildForKey(context,
                        new ResolvedKeySpacePath(parent, path, pathValue, null),
                        key, keySize, keyIndex + 1).thenApply(Optional::of);
            }
        });
    }

    /**
     * Iterates over the subdirectories of this directory looking for one that is compatible with the given value.
     * @param context the database context
     * @param parent the parent path element
     * @param value the child value to be resolved
     * @return a future that completes with the matching keyspace path
     * @throws RecordCoreArgumentException if no compatible child can be found
     */
    @Nonnull
    public CompletableFuture<ResolvedKeySpacePath> findChildForValue(@Nonnull FDBRecordContext context,
                                                                     @Nullable ResolvedKeySpacePath parent,
                                                                     @Nullable Object value) {
        final Tuple key = Tuple.from(value);
        return findChildForKey(context, parent, key, 1, 0);
    }

    /**
     * Iterates over the subdirectories of this directory looking for one that is compatible with the
     * <code>key</code> tuple, starting at position <code>keyIndex</code>.
     * If the key size is less than the actual key length, the path remainder will reflect
     * the left over key elements.
     * @param context the database context
     * @param parent the parent path element
     * @param key the tuple being parsed
     * @param keySize the logical key size
     * @param keyIndex the position in the index being parsed
     * @return a future that completes with the matching keyspace path
     * @throws RecordCoreArgumentException if no compatible child can be found
     */
    @Nonnull
    protected CompletableFuture<ResolvedKeySpacePath> findChildForKey(@Nonnull FDBRecordContext context,
                                                                      @Nullable ResolvedKeySpacePath parent,
                                                                      @Nonnull Tuple key,
                                                                      final int keySize,
                                                                      final int keyIndex) {
        return nextChildForKey(0, context, parent, key, keySize, keyIndex);
    }

    protected CompletableFuture<ResolvedKeySpacePath> nextChildForKey(final int childIndex,
                                                                      @Nonnull FDBRecordContext context,
                                                                      @Nullable ResolvedKeySpacePath parent,
                                                                      @Nonnull Tuple key,
                                                                      final int keySize,
                                                                      final int keyIndex) {
        if (childIndex >= subdirs.size()) {
            throw new RecordCoreArgumentException("No subdirectory available to hold provided type",
                    LogMessageKeys.PARENT_DIR, getName(),
                    "key_tuple", key,
                    "key_tuple_pos", keyIndex,
                    "key_tuple_type", key.get(keyIndex) == null ? "NULL" : key.get(keyIndex).getClass().getName());
        }

        KeySpaceDirectory subdir = subdirs.get(childIndex);
        return subdir.pathFromKey(context, parent, key, keySize, keyIndex).thenCompose(maybeChildPath -> {
            if (maybeChildPath.isPresent()) {
                return CompletableFuture.completedFuture(maybeChildPath.get());
            }
            return nextChildForKey(childIndex + 1, context, parent, key, keySize, keyIndex);
        });
    }

    /**
     * Adds a subdirectory within the directory.
     * @param subdirectory the subdirectory to add
     * @return this directory
     * @throws RecordCoreArgumentException if a subdirectory of the same name already exists, or a subdirectory of the
     *   same type already exists with the same constant value
     */
    @Nonnull
    public KeySpaceDirectory addSubdirectory(@Nonnull KeySpaceDirectory subdirectory) {
        for (KeySpaceDirectory existingSubdir : subdirsByName.values()) {
            if (existingSubdir.getName().equals(subdirectory.getName())) {
                throw new RecordCoreArgumentException("Subdirectory already exists",
                        LogMessageKeys.PARENT_DIR, getName(),
                        LogMessageKeys.DIR_NAME, subdirectory.getName());
            }

            if (existingSubdir.getKeyType() == subdirectory.getKeyType()) {
                if (!existingSubdir.isConstant() || !subdirectory.isConstant()) {
                    throw new RecordCoreArgumentException("Cannot add directory due to overlapping type",
                            LogMessageKeys.PARENT_DIR, getName(),
                            LogMessageKeys.DIR_NAME, existingSubdir.getName(),
                            LogMessageKeys.DIR_TYPE, subdirectory.getKeyType());
                }
                if (Objects.equals(subdirectory.getValue(), existingSubdir.getValue())) {
                    throw new RecordCoreArgumentException("Cannot add directory due to overlapping constant value",
                            LogMessageKeys.PARENT_DIR, getName(),
                            LogMessageKeys.DIR_NAME, existingSubdir.getName(),
                            LogMessageKeys.DIR_TYPE, existingSubdir.getKeyType(),
                            LogMessageKeys.DIR_VALUE, existingSubdir.getValue());
                }

                // Check compatibility in both directions
                if (!existingSubdir.isCompatible(this, subdirectory)
                        || !subdirectory.isCompatible(this, existingSubdir)) {
                    throw new RecordCoreArgumentException("Cannot add directory due to incompatibility with existing subdirectory",
                            LogMessageKeys.PARENT_DIR, getName(),
                            LogMessageKeys.DIR_TYPE, existingSubdir.getKeyType(),
                            LogMessageKeys.DIR_VALUE, existingSubdir.getValue());
                }
            }
        }
        subdirectory.setParent(this);
        subdirsByName.put(subdirectory.getName(), subdirectory);
        subdirs.add(subdirectory);
        return this;
    }

    /**
     * When a subdirectory is being added to a directory and an existing subdirectory is found that stores the
     * same data type but with a different constant value, this method will be called both on the directory being
     * added as well as the existing subdirectory to allow them to determine if they are compatible with each other.
     * A concrete example use case for this is the <code>DirectoryLayerDirectory</code>: let's say we had something
     * like:
     * <pre>
     *    dir.addSubdirectory(new DirectoryLayerDirectory("dir1", "constValue"))
     *       .addSubdirectory(new KeySpaceDirectory("dir2", KeyType.LONG, 1)
     * </pre>
     * In this case, we are adding two directories which are of the same type, LONG (<code>DirectorylayerDirectory</code>
     * is implicitly of type long) and, although, the two appear to have different constant values and, thus, should
     * be compatible with each other, the <code>DirectoryLayerDirectory</code> dynamically converts it's
     * <code>"constValue"</code> into a LONG and, thus, <b>could</b> end up colliding with the constant value of <code>1</code>
     * used for <code>"dir2"</code>. To prevent this, the <code>DirectoryLayerDirectory</code> must override this method
     * to block any peer that isn't also a <code>DirectorylayerDirectory</code>.
     *
     * If this directory is being added to <code>parent</code>, then <code>dir</code> is the existing
     * peer that has the same storage data type but a different constant value.  If this directory already exists
     * in <code>parent</code>, then <code>dir</code> is a new peer that is being added that has the same data type
     * but a different constant value.
     *
     * @param parent the parent directory in which a subdirectory is being added
     * @param dir the existing peer directory
     * @return true if the directories can co-exist, false otherwise
     */
    protected boolean isCompatible(@Nonnull KeySpaceDirectory parent, @Nonnull KeySpaceDirectory dir) {
        return true;
    }

    private void setParent(@Nonnull KeySpaceDirectory parent) {
        if (this.parent != null) {
            throw new RecordCoreArgumentException("Cannot re-parent a directory");
        }
        this.parent = parent;
    }

    /**
     * Returns whether or not the directory is a leaf directory (has no subdirectories).
     * @return true if the directory is a leaf directory, false otherwise
     */
    public boolean isLeaf() {
        return subdirsByName.isEmpty();
    }

    /**
     * Retrieves a subdirectory.
     * @param name the name of the subdirectory to return
     * @return the subdirectory with the specified <code>name</code>
     * @throws NoSuchDirectoryException if the directory does not exist
     */
    @Nonnull
    public KeySpaceDirectory getSubdirectory(@Nonnull String name) {
        KeySpaceDirectory dir = subdirsByName.get(name);
        if (dir == null) {
            throw new NoSuchDirectoryException(this, name);
        }
        return dir;
    }

    /**
     * If a <code>wrapper</code> was installed for this directory, the provided <code>path</code> will be
     * wrapped by the <code>wrapper</code> and returned, otherwise the original <code>path</code> is returned.
     *
     * @param path the path to be wrapped
     * @return the wrapped path or the path provided if no wrapper is installed
     */
    @Nonnull
    protected KeySpacePath wrap(@Nonnull KeySpacePath path) {
        if (wrapper != null) {
            return wrapper.apply(path);
        }
        return path;
    }

    @Nonnull
    protected RecordCursor<ResolvedKeySpacePath> listSubdirectoryAsync(@Nullable KeySpacePath listFrom,
                                                                       @Nonnull FDBRecordContext context,
                                                                       @Nonnull String subdirName,
                                                                       @Nullable byte[] continuation,
                                                                       @Nonnull ScanProperties scanProperties) {
        return listSubdirectoryAsync(listFrom, context, subdirName, null, continuation, scanProperties);
    }

    @Nonnull
    @SuppressWarnings({"squid:S2095", // SonarQube doesn't realize that the cursor is wrapped and returned
                       "PMD.CompareObjectsWithEquals"})
    protected RecordCursor<ResolvedKeySpacePath> listSubdirectoryAsync(@Nullable KeySpacePath listFrom,
                                                                       @Nonnull FDBRecordContext context,
                                                                       @Nonnull String subdirName,
                                                                       @Nullable ValueRange<?> valueRange,
                                                                       @Nullable byte[] continuation,
                                                                       @Nonnull ScanProperties scanProperties) {
        if (listFrom != null && listFrom.getDirectory() != this) {
            throw new RecordCoreException("Provided path does not belong to this directory")
                    .addLogInfo("path", listFrom, "directory", this.getName());
        }

        final KeySpaceDirectory subdir = getSubdirectory(subdirName);
        final CompletableFuture<ResolvedKeySpacePath> resolvedFromFuture = listFrom == null
                ? CompletableFuture.completedFuture(null)
                : listFrom.toResolvedPathAsync(context);

        // The chained cursor cannot implement reverse scan, so we implement it by having the
        // inner key value cursor do the reversing but telling the chained cursor we are moving
        // forward.
        final ScanProperties chainedCursorScanProperties;
        if (scanProperties.isReverse()) {
            chainedCursorScanProperties = scanProperties.setReverse(false);
        } else {
            chainedCursorScanProperties = scanProperties;
        }

        // For the read of the individual row keys, we only want to read a single key. In addition,
        // the ChainedCursor is going to do counting of our reads to apply any limits that were specified
        // on the ScanProperties.  We don't want the inner KeyValueCursor in nextTuple() to ALSO count those
        // same reads so we clear out its limits.
        final ScanProperties keyReadScanProperties = scanProperties.with(
                props -> props.clearState().setReturnedRowLimit(1));

        return new LazyCursor<>(
                resolvedFromFuture.thenCompose(resolvedFrom -> {
                    final Subspace subspace = resolvedFrom == null ? new Subspace() : resolvedFrom.toSubspace();
                    return subdir.getValueRange(context, valueRange, subspace).thenApply(range -> {
                        final RecordCursor<Tuple> cursor = new ChainedCursor<>(
                                context,
                                lastKey -> nextTuple(context, subspace, range, lastKey, keyReadScanProperties),
                                Tuple::pack,
                                Tuple::fromBytes,
                                continuation,
                                chainedCursorScanProperties);

                        return cursor.mapPipelined(tuple -> {
                            final Tuple key = Tuple.fromList(tuple.getItems());
                            return findChildForKey(context, resolvedFrom, key, 1, 0);
                        }, 1);
                    });
                }),
                context.getExecutor()
        );
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private CompletableFuture<KeyRange> getValueRange(@Nonnull FDBRecordContext context,
                                                      @Nullable ValueRange<?> valueRange,
                                                      @Nonnull Subspace subspace) {
        final byte[] startKey;
        final byte[] stopKey;
        final EndpointType startType;
        final EndpointType stopType;

        if (!isConstant()) {
            if (valueRange != null && valueRange.getLowEndpoint() != EndpointType.TREE_START) {
                if (KeyType.typeOf(valueRange.getLow()) != getKeyType()) {
                    throw invalidValueTypeException(KeyType.typeOf(valueRange.getLow()), getKeyType(), getName(), valueRange);
                }
                startKey = subspace.pack(valueRange.getLow());
                startType = valueRange.getLowEndpoint();
                if (startType != EndpointType.RANGE_INCLUSIVE && startType != EndpointType.RANGE_EXCLUSIVE) {
                    throw new RecordCoreArgumentException("Endpoint type not supported for directory list",
                            LogMessageKeys.ENDPOINT_TYPE, startType);
                }
            } else {
                final byte[] subspaceBytes = subspace.pack();
                startKey = new byte[subspaceBytes.length + 1];
                System.arraycopy(subspaceBytes, 0, startKey, 0, subspaceBytes.length);
                startKey[subspaceBytes.length] = getKeyType().getTypeLowBounds();
                startType = EndpointType.RANGE_INCLUSIVE;
            }

            if (valueRange != null && valueRange.getHighEndpoint() != EndpointType.TREE_END) {
                if (KeyType.typeOf(valueRange.getHigh()) != getKeyType()) {
                    throw invalidValueTypeException(KeyType.typeOf(valueRange.getHigh()), getKeyType(), getName(), valueRange);
                }
                stopKey = subspace.pack(valueRange.getHigh());
                stopType = valueRange.getHighEndpoint();
                if (stopType != EndpointType.RANGE_INCLUSIVE && stopType != EndpointType.RANGE_EXCLUSIVE) {
                    throw new RecordCoreArgumentException("Endpoint type not supported for directory list",
                            LogMessageKeys.ENDPOINT_TYPE, stopType);
                }
            } else {
                final byte[] subspaceBytes = subspace.pack();
                stopKey = new byte[subspaceBytes.length + 1];
                System.arraycopy(subspaceBytes, 0, stopKey, 0, subspaceBytes.length);
                stopKey[subspaceBytes.length] = getKeyType().getTypeHighBounds();
                stopType = EndpointType.RANGE_EXCLUSIVE;
            }

            return CompletableFuture.completedFuture(new KeyRange(startKey, startType, stopKey, stopType));
        } else {
            if (valueRange != null) {
                throw new RecordCoreArgumentException("range is not applicable when the subdirectory has a value.",
                        LogMessageKeys.DIR_NAME, getName(),
                        LogMessageKeys.RANGE, valueRange);
            }
            return toTupleValueAsync(context, this.value)
                    .thenApply(resolvedValue -> {
                        final byte[] key = subspace.pack(Tuple.from(resolvedValue.getResolvedValue()));
                        return new KeyRange(key, EndpointType.RANGE_INCLUSIVE,
                                ByteArrayUtil.strinc(key), EndpointType.RANGE_EXCLUSIVE);
                    });
        }
    }

    private RecordCoreArgumentException invalidValueTypeException(KeyType rangeValueType, KeyType expectedValueType,
                                                                  String dirName, ValueRange<?> range) {
        throw new RecordCoreArgumentException("value type provided for range is invalid for directory type",
                LogMessageKeys.RANGE_VALUE_TYPE, rangeValueType,
                LogMessageKeys.EXPECTED_VALUE_TYPE, expectedValueType,
                LogMessageKeys.DIR_NAME, dirName,
                LogMessageKeys.RANGE, range);
    }

    @SuppressWarnings("PMD.CloseResource")
    private CompletableFuture<Optional<Tuple>> nextTuple(@Nonnull FDBRecordContext context,
                                                         @Nonnull Subspace subspace,
                                                         @Nonnull KeyRange range,
                                                         @Nonnull Optional<Tuple> lastTuple,
                                                         @Nonnull ScanProperties scanProperties) {
        final KeyValueCursor cursor;
        if (!lastTuple.isPresent()) {
            cursor = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setRange(range)
                    .setContinuation(null)
                    .setScanProperties(scanProperties)
                    .build();
        } else {
            KeyValueCursor.Builder builder = KeyValueCursor.Builder.withSubspace(subspace)
                    .setContext(context)
                    .setContinuation(null)
                    .setScanProperties(scanProperties);

            if (scanProperties.isReverse()) {
                cursor = builder.setLow(range.getLowKey(), range.getLowEndpoint())
                        .setHigh(subspace.pack(lastTuple.get().get(0)), EndpointType.RANGE_EXCLUSIVE)
                        .build();
            } else {
                cursor = builder.setLow(subspace.pack(lastTuple.get().get(0)), EndpointType.RANGE_EXCLUSIVE)
                        .setHigh(range.getHighKey(), range.getHighEndpoint())
                        .build();
            }
        }

        return cursor.onNext().thenApply(next -> {
            if (next.hasNext()) {
                KeyValue kv = next.get();
                return Optional.of(subspace.unpack(kv.getKey()));
            }
            return Optional.empty();
        } );
    }

    /**
     * Returns the set of subdirectories contained within this directory.
     * @return the set of subdirectories contained within this directory
     */
    @Nonnull
    public List<KeySpaceDirectory> getSubdirectories() {
        return subdirs;
    }

    /**
     * Asks the directory implementation to "resolve" a tuple value. The meaning of resolve can vary amongst different
     * types of directorys but, for example, the <code>DirectoryLayerDirectory</code> would be expecting the value to be
     * a <code>String</code> and would use the directory layer to then convert it into a <code>Long</code>.
     * @param context the context in which to perform the resolution
     * @param value the value to be resolved
     * @return a future containing the resolved value
     */
    @Nonnull
    protected final CompletableFuture<PathValue> toTupleValueAsync(@Nonnull FDBRecordContext context, @Nullable Object value) {
        return toTupleValueAsyncImpl(context, value)
                .thenApply( pathValue -> {
                    validateResolvedValue(pathValue.getResolvedValue());
                    return pathValue;
                });
    }

    @Nullable
    protected PathValue toTupleValue(@Nonnull FDBRecordContext context, @Nullable Object value) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_DIRECTORY_RESOLVE, toTupleValueAsync(context, value));
    }

    /**
     * This method is called during the process of turning a {@link KeySpacePath} into a <code>Tuple</code>. This
     * method should never be directly invoked from anything other than <code>toTupleValueAsync</code>, it is purely
     * intended for new  <code>KeySpaceDirectory</code> implementations to override.
     *
     * @param context the context in which the tuple is being constructed
     * @param value the value that was provided to be stored in this directory
     * @return a future that will result in the value to be physically stored for the provided input <code>value</code>
     * @throws RecordCoreArgumentException if the value provided for this directory is incompatible with the
     *   definition of this directory
     */
    @Nonnull
    protected CompletableFuture<PathValue> toTupleValueAsyncImpl(@Nonnull FDBRecordContext context, @Nullable Object value) {
        if (this.isConstant() && !areEqual(this.value, value)) {
            throw new RecordCoreArgumentException("Illegal value provided",
                    "provided_value", value,
                    "expected_value", this.value);
        }
        return CompletableFuture.completedFuture(new PathValue(value));
    }

    /**
     * Ensures that a value is suitable to be stored within this directory.
     *
     * @param value the value to be stored in this directory
     * @return the <code>value</code>
     * @throws RecordCoreArgumentException if the value is not suitable for storing in this directory
     */
    @Nullable
    protected Object validateResolvedValue(@Nullable Object value) {
        if (!keyType.isMatch(value)) {
            throw new RecordCoreArgumentException("Illegal value type provided for directory",
                    LogMessageKeys.DIR_NAME, getName(),
                    "provided_value", value,
                    "expected_type", keyType,
                    "provided_type", (value == null ? "NULL" : value.getClass().getName()));
        }

        return value;
    }

    /**
     * Returns this directory's parent.
     * @return this directory's parent, or {@code null} if there is no parent
     */
    @Nullable
    public KeySpaceDirectory getParent() {
        return parent;
    }

    /**
     * Returns the name of this directory.
     * @return the name of this directory
     */
    @Nonnull
    public String getName() {
        return name;
    }

    /**
     * When displaying the name of this directory in the tree output from {@link #toString()} allows the
     * directory implementation to ornament the name in any fashion it sees fit.
     * @return the display name of the directory in the tree output
     */
    @Nonnull
    public String getNameInTree() {
        return name;
    }

    /**
     * Returns the type of values this directory stores.
     * @return the type of values this directory stores
     */
    @Nonnull
    public KeyType getKeyType() {
        return keyType;
    }

    /**
     * Returns the constant value that this directory stores.
     * A return value of {@link #ANY_VALUE} indicates
     * that this directory may contain any value of the type indicated by {@link #getKeyType()}
     * (see also {@link #isConstant()}.
     * @return the constant value that this directory stores
     */
    @Nullable
    public Object getValue() {
        return value;
    }

    /**
     * Whether this directory has a constant value, or can be any value.
     * @return {@code true} if this directory has a constant value, or {@code false} if it can accept any value.
     */
    public boolean isConstant() {
        return value != ANY_VALUE;
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals") // we use ref
    protected static boolean areEqual(@Nullable Object o1, @Nullable Object o2) {
        if (o1 == null) {
            return o2 == null;
        } else {
            if (o2 == null) {
                return false;
            }
        }

        // Handle ANY_VALUE specially - typeOf does not support ANY_VALUE
        boolean isAnyValue = (o1 == ANY_VALUE || o2 == ANY_VALUE);
        if (isAnyValue) {
            return Objects.equals(o1, o2);
        }

        KeyType o1Type = KeyType.typeOf(o1);
        if (o1Type != KeyType.typeOf(o2)) {
            return false;
        }

        switch (o1Type) {
            case BYTES:
                return Arrays.equals((byte[]) o1, (byte[]) o2);
            case LONG:
                return ((Number)o1).longValue() == ((Number)o2).longValue();
            case STRING:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case UUID:
                return o1.equals(o2);
            default:
                throw new RecordCoreException("Unexpected key type " + o1Type);
        }
    }

    protected static int valueHashCode(@Nullable Object value) {
        if (value == null) {
            return 0;
        }

        // Handle ANY_VALUE specially
        if (value == ANY_VALUE) {
            return System.identityHashCode(value);
        }

        switch (KeyType.typeOf(value)) {
            case BYTES:
                return Arrays.hashCode((byte[]) value);
            case LONG:
            case STRING:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case UUID:
                return Objects.hashCode(value);
            default:
                throw new RecordCoreException("Unexpected key type " + KeyType.typeOf(value));
        }
    }

    /**
     * Returns the path that leads up to this directory (including this directory), and returns it as a string
     * that looks something like a filesystem path.
     *
     * @return the path to this directory as a string
     */
    public String toPathString() {
        StringBuilder sb = new StringBuilder();
        appendPath(sb, this);
        return sb.toString();
    }

    private void appendPath(StringBuilder sb, KeySpaceDirectory dir) {
        @Nullable
        KeySpaceDirectory parent = dir.getParent();

        if (parent != null) {
            appendPath(sb, parent);
        }
        sb.append("/").append(dir.getName());
    }

    @Override
    public String toString() {
        try {
            try (StringWriter out = new StringWriter()) {
                toTree(out);
                return out.toString();
            }
        } catch (IOException e) {
            // Should never happen
            throw new RecordCoreException(e.getMessage(), e);
        }
    }

    /**
     * Sends the keyspace as an ascii-art tree to an output stream.
     * @param out the print destination
     * @throws IOException if there is a problem with the destination
     */
    public void toTree(Writer out) throws IOException {
        toTree(out, this, 0, false, new BitSet());
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private void toTree(Writer out, KeySpaceDirectory dir, int indent, boolean hasAnotherSibling,
                        BitSet downspouts) throws IOException {

        for (int i = 0; i < indent; i++) {
            if (downspouts.get(i)) {
                out.append(" | ");
            } else {
                out.append("   ");
            }
        }

        if (dir.getParent() != null) {
            out.append(" +- ");
        }

        out.append(dir.getNameInTree());
        out.append(" (");
        out.append(dir.getKeyType().toString());
        if (dir.getValue() != dir.getKeyType().getAnyValue()) {
            out.append("=");
            out.append(dir.getValue() == null ? "null" : dir.getValue().toString());
        }
        out.append(")").append("\n");

        if (!dir.isLeaf()) {
            List<KeySpaceDirectory> dirSubdirs = dir.getSubdirectories();
            downspouts.set(indent, hasAnotherSibling);
            for (int i = 0; i < dirSubdirs.size(); i++) {
                toTree(out, dirSubdirs.get(i), indent + 1, i < (dirSubdirs.size() - 1), downspouts);
            }
            downspouts.set(indent, false);
        }
    }

    /**
     * The available data types for directories.
     */
    public enum KeyType {
        // The values for typeLowBounds and typeHighBounds don't belong here, but rather belong
        // either being exposed as part of TupleUtil's currently private constants, or via a
        // method call that can take a subspace and produce a range that will scan all values of
        // a given type in the subspace.
        // TODO: API for string prefix queries within Tuples (https://github.com/apple/foundationdb/issues/282)
        NULL(v -> v == null, null, (byte) 0x00, (byte) 0x01),
        BYTES(v -> v instanceof byte[], (byte) 0x01, (byte) 0x02),
        STRING(String.class, (byte) 0x02, (byte) 0x03),
        LONG(v -> v instanceof Long || v instanceof Integer, (byte) 0x0b, (byte) 0x1e),
        FLOAT(Float.class, (byte) 0x20, (byte) 0x21),
        DOUBLE(Double.class, (byte) 0x21, (byte) 0x22),
        BOOLEAN(v -> v instanceof Boolean, (byte) 0x26, (byte) 0x28),
        UUID(UUID.class, (byte) 0x30, (byte) 0x31);

        // Function that tests a value to see if it is of this type
        @Nonnull
        @SpotBugsSuppressWarnings("SE_BAD_FIELD")
        final Function<Object, Boolean> matcher;
        // Value used by a directory to indicate that it can accept any value of this type
        @Nullable
        final Object anyValue;

        final byte typeLowBounds;
        final byte typeHighBounds;

        KeyType(@Nonnull Class<?> expectedType, byte typeLowBounds, byte typeHighBounds) {
            this(v -> v != null && expectedType.isAssignableFrom(v.getClass()), ANY_VALUE, typeLowBounds, typeHighBounds);
        }

        KeyType(@Nonnull Function<Object, Boolean> matcher, byte typeLowBounds, byte typeHighBounds) {
            this(matcher, ANY_VALUE, typeLowBounds, typeHighBounds);
        }

        KeyType(@Nonnull Function<Object, Boolean> matcher, @Nullable Object anyValue,
                byte typeLowBounds, byte typeHighBounds) {
            this.matcher = matcher;
            this.typeLowBounds = typeLowBounds;
            this.typeHighBounds = typeHighBounds;
            this.anyValue = anyValue;
        }

        /**
         * Get whether the provided value can be represented by this type.
         * @param value the value to check
         * @return {@code true} if {@code value} can be represented by this type
         */
        public boolean isMatch(@Nullable Object value) {
            return matcher.apply(value);
        }

        public Object getAnyValue() {
            return anyValue;
        }

        public byte getTypeLowBounds() {
            return typeLowBounds;
        }

        public byte getTypeHighBounds() {
            return typeHighBounds;
        }

        public static KeyType typeOf(@Nullable Object value) {
            for (KeyType type : values()) {
                if (type.matcher.apply(value)) {
                    return type;
                }
            }

            throw new RecordCoreArgumentException("No directory type matches value",
                    "value", value,
                    "value_type", value.getClass().getName());
        }
    }

    /**
     * A singleton class representing that this directory can contain any value of the associated type.
     */
    private static class AnyValue {
        /**
         * Do not call this constuctor, reference the constant {@link #ANY_VALUE}.
         */
        private AnyValue() {
        }

        // explicitly not implementing equals, so that it falls back to `Object.equals` which is reference equality.

        @Override
        public String toString() {
            return "*any*";
        }
    }
}


