/*
 * DirectoryLayerDirectory.java
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A <code>KeySpaceDirectory</code> that maps a <code>STRING</code> value to a compact <code>LONG</code> value
 * using the FDB directory layer. The <code>DirectoryLayerDirectory</code> may be used in one of two different
 * fashions, either mapping a constant string value to a long, or being used to map any string value placed in
 * the directory to a long. For example:
 * <pre>
 *     KeySpace keyspace = new KeySpace(
 *         new DirectoryLayerDirectory("library", "library")
 *             .addSubdirectory(new DirectoryLayerDirectory("book_title")));
 * </pre>
 * Defines a simple directory tree, in which the root of the path is a <code>LONG</code> value representing our
 * application called "library", under which lives data for a set of books in the library stored under the
 * title of the book ("book_title"), again represented as a <code>LONG</code> value by mapping the book title via
 * the FDB directory layer.
 * <p>When creating a path through a directory layer directory, you may either specify the string name that
 * you wish to place in the directory, like so:
 * <pre>
 *     keySpace.path("library").add("book_title", "Twenty Thousand Leagues Under the Sea").toTuple();
 * </pre>
 * or you may retrieve it by directory layer value:
 * <pre>
 *     keySpace.path("library").add(443L).toTuple();
 * </pre>
 * Retrieving by directory layer value will result in an exception if the value provided is either not a valid
 * directory layer value, or it is not the value that corresponds to the constant name for this directory.
 */
@API(API.Status.MAINTAINED)
public class DirectoryLayerDirectory extends KeySpaceDirectory {
    @Nonnull
    private final Function<FDBRecordContext, CompletableFuture<LocatableResolver>> scopeGenerator;
    @Nonnull
    private final ResolverCreateHooks createHooks;

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>.
     * @param name The logical name of the directory
     */
    public DirectoryLayerDirectory(@Nonnull String name) {
        this(name, ANY_VALUE, null);
    }

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>.
     * @param name The logical name of the directory
     * @param wrapper Wrapper function, see: {@link KeySpaceDirectory#KeySpaceDirectory(String, KeyType, Function)}
     */
    public DirectoryLayerDirectory(@Nonnull String name, Function<KeySpacePath, KeySpacePath> wrapper) {
        this(name, ANY_VALUE, wrapper);
    }

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>.
     * @param name The logical name of the directory
     * @param value The value of the directory entry (the string which will be translated to an int by the resolver)
     */
    public DirectoryLayerDirectory(@Nonnull String name, @Nullable Object value) {
        this(name, value, null);
    }

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>. Sets the <code>createHook</code> to
     * {@link ResolverCreateHooks} their default values.
     * @param name The logical name of the directory
     * @param value The value of the directory entry (the string which will be translated to an int by the resolver)
     * @param wrapper Wrapper function, see: {@link KeySpaceDirectory#KeySpaceDirectory(String, KeyType, Function)}
     */
    public DirectoryLayerDirectory(@Nonnull String name, @Nullable Object value,
                                   @Nullable Function<KeySpacePath, KeySpacePath> wrapper) {
        this(name, value, wrapper,
                context -> CompletableFuture.completedFuture(ExtendedDirectoryLayer.global(context.getDatabase())),
                ResolverCreateHooks.getDefault());
    }

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>.
     * @param name The logical name of the directory
     * @param wrapper Wrapper function, see: {@link KeySpaceDirectory#KeySpaceDirectory(String, KeyType, Function)}
     * @param scopeGenerator A function which will be called with the context of a {@link KeySpacePath} which contains this
     * directory. It returns a future (since it may need to read from the database) that completes with the {@link LocatableResolver}
     * to use.
     * @param createHooks The set of {@link ResolverCreateHooks} to run if, when getting a path through this directory,
     * we need to create an entry in the {@link LocatableResolver}. These checks can be used to, for example, add metadata
     * to resolver entries for this directory, or to transactionally verify that the {@link LocatableResolver} returned
     * by the <code>scopeGenerator</code> is correct.
     */
    public DirectoryLayerDirectory(@Nonnull String name,
                                    @Nullable Function<KeySpacePath, KeySpacePath> wrapper,
                                    @Nonnull Function<FDBRecordContext, CompletableFuture<LocatableResolver>> scopeGenerator,
                                    @Nonnull ResolverCreateHooks createHooks) {
        this(name, ANY_VALUE, wrapper, scopeGenerator, createHooks);
    }

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>.
     * @param name The logical name of the directory
     * @param value The value of the directory entry (the string which will be translated to an int by the resolver)
     * @param wrapper Wrapper function, see: {@link KeySpaceDirectory#KeySpaceDirectory(String, KeyType, Function)}
     * @param scopeGenerator A function which will be called with the context of a {@link KeySpacePath} which contains this
     * directory. It returns a future (since it may need to read from the database) that completes with the {@link LocatableResolver}
     * to use.
     * @param createHooks The set of {@link ResolverCreateHooks} to run if, when getting a path through this directory,
     * we need to create an entry in the {@link LocatableResolver}. These checks can be used to, for example, add metadata
     * to resolver entries for this directory, or to transactionally verify that the {@link LocatableResolver} returned
     * by the <code>scopeGenerator</code> is correct.
     */
    public DirectoryLayerDirectory(@Nonnull String name, @Nullable Object value,
                            @Nullable Function<KeySpacePath, KeySpacePath> wrapper,
                            @Nonnull Function<FDBRecordContext, CompletableFuture<LocatableResolver>> scopeGenerator,
                            @Nonnull ResolverCreateHooks createHooks) {
        super(name, KeyType.LONG, value, wrapper);
        this.scopeGenerator = scopeGenerator;
        this.createHooks = createHooks;
    }

    @Override
    protected void validateConstant(@Nullable Object value) {
        if (!(value instanceof String)) {
            throw new RecordCoreArgumentException("Illegal constant value type provided for directory",
                    LogMessageKeys.DIR_NAME, getName(),
                    "dir_value", value);
        }
    }

    // TODO: fix this for scoped directory layers
    // TODO: DirectoryLayerDirectory should support scopes and correctly detect incompatible peer (https://github.com/FoundationDB/fdb-record-layer/issues/10)
    @Override
    protected boolean isCompatible(@Nonnull KeySpaceDirectory parent, @Nonnull KeySpaceDirectory dir) {
        return (dir instanceof DirectoryLayerDirectory);
    }

    @Override
    @Nonnull
    @SuppressWarnings("squid:S1604") // need annotation so no lambda
    protected CompletableFuture<PathValue> toTupleValueAsync(@Nonnull FDBRecordContext context, @Nullable Object value) {
        // We allow someone to explicitly pass the value of a directory layer entry, however if
        // this directory is hard-wired to a specific value, then the value passed in is compared
        // with the directory layer to ensure it is valid.
        if (value instanceof Long) {
            if (this.value != ANY_VALUE) {
                if (!(this.value instanceof String)) {
                    throw new RecordCoreArgumentException("DirectoryLayerDirectory should be of string value",
                            LogMessageKeys.DIR_NAME, getName(),
                            "dir_layer_name", this.value,
                            LogMessageKeys.PROVIDED_VALUE, value);
                }

                return lookupInScope(context, (String) this.value)
                        .thenApply(new Function<ResolverResult, PathValue>() {
                            @Override
                            @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "https://github.com/spotbugs/spotbugs/issues/552")
                            public PathValue apply(ResolverResult resolved) {
                                if (resolved.getValue() != (Long)value) {
                                    throw new RecordCoreArgumentException("Provided directory layer value "
                                                                          + "does not correspond to actual directory layer value for directory",
                                            LogMessageKeys.DIR_NAME, DirectoryLayerDirectory.this.getName(),
                                            "dir_layer_name", DirectoryLayerDirectory.this.value,
                                            LogMessageKeys.PROVIDED_VALUE, value,
                                            "expected_value", resolved);
                                }
                                return toPathValue(resolved);
                            }
                        });
            }

            // To be on the safe side, look up the value from the reverse directory cache to ensure that
            // the value provided actually exists in the directory. Note that the reverseLookup method will
            // throw a NoSuchElementException if the value doesn't exist.
            return doReverseLookup(context, (Long) value)
                    // If we've been given the resolved value, when we reconstruct the path we need to go lookup the key
                    // to get the metadata as well
                    .thenCompose(key -> lookupInScope(context, key))
                    .thenApply(DirectoryLayerDirectory::toPathValue);
        }

        if (!(value instanceof String)) {
            throw new RecordCoreArgumentException("Invalid value type provided for directory",
                    LogMessageKeys.DIR_NAME, getName(),
                    LogMessageKeys.PROVIDED_VALUE, value,
                    "provided_type", (value == null ? "null" : value.getClass().getName()),
                    "expected_type", "[STRING, LONG]");
        }

        if (this.value != ANY_VALUE && !areEqual(this.value, value)) {
            throw new RecordCoreArgumentException("Illegal value type provided for directory",
                    LogMessageKeys.DIR_NAME, getName(),
                    LogMessageKeys.PROVIDED_VALUE, value,
                    "expected_value", this.value);
        }

        return lookupInScope(context, (String) value).thenApply(DirectoryLayerDirectory::toPathValue);
    }

    @Nonnull
    @Override
    protected CompletableFuture<Optional<KeySpacePath>> pathFromKey(@Nonnull FDBRecordContext context,
                                                                    @Nullable KeySpacePath parent,
                                                                    @Nonnull Tuple key,
                                                                    int keySize,
                                                                    int keyIndex) {
        final Object tupleValue = key.get(keyIndex);
        // Only a directory layer value can be reversed into this directory
        if (!(tupleValue instanceof Long)) {
            return DIRECTORY_NOT_FOR_KEY;
        }

        return doReverseLookup(context, (Long)tupleValue)
                .thenCompose(directoryString -> {
                    // This is a valid directory layer value, but it isn't destined for this directory.
                    if (this.value != ANY_VALUE && !(directoryString.equals(this.value))) {
                        return DIRECTORY_NOT_FOR_KEY;
                    }
                    return lookupInScope(context, directoryString).thenCompose(directoryResolverResult -> {
                        final int childKeyIndex = keyIndex + 1;
                        final boolean canHaveChild = !subdirs.isEmpty() && childKeyIndex < keySize;
                        final Tuple remainder = canHaveChild || childKeyIndex == key.size()
                                                ? null
                                                : TupleHelpers.subTuple(key, childKeyIndex, key.size());

                        // Make sure that the path is constructed with the text-name from the directory layer.
                        KeySpacePath myPath = KeySpacePathImpl.newPath(parent, this, context, directoryString,
                                CompletableFuture.completedFuture(toPathValue(directoryResolverResult)), remainder);

                        // We are finished if there are no more subdirectories or no more tuple to consume
                        if (!canHaveChild) {
                            return CompletableFuture.completedFuture(Optional.of(myPath));
                        }

                        return findChildForKey(context, myPath, key, keySize, childKeyIndex).thenApply(Optional::of);
                    });
                });
    }

    @Nonnull
    @Override
    public String getNameInTree() {
        return "[" + getName() + "]";
    }

    @Nonnull
    private CompletableFuture<String> doReverseLookup(@Nonnull FDBRecordContext context, Long dir) {
        return scopeGenerator.apply(context)
                .thenCompose(resolver -> resolver.reverseLookup(context.getTimer(), dir));
    }

    @Nonnull
    private CompletableFuture<ResolverResult> lookupInScope(@Nonnull final FDBRecordContext context, @Nonnull final String key) {
        return scopeGenerator.apply(context).thenCompose(resolver ->
            resolver.resolveWithMetadata(context.getTimer(), key, createHooks));
    }

    @Nonnull
    private static PathValue toPathValue(@Nonnull ResolverResult result) {
        return new PathValue(result.getValue(), result.getMetadata());
    }
}
