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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import org.jspecify.annotations.Nullable;
import java.util.Objects;
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
 *     keySpace.path("library").add("book_title", "Twenty Thousand Leagues Under the Sea").toTuple(context);
 * </pre>
 * or you may retrieve it by directory layer value:
 * <pre>
 *     keySpace.path("library").add(443L).toTuple(context);
 * </pre>
 * Retrieving by directory layer value will result in an exception if the value provided is either not a valid
 * directory layer value, or it is not the value that corresponds to the constant name for this directory.
 */
@API(API.Status.UNSTABLE)
public class DirectoryLayerDirectory extends KeySpaceDirectory {
    private final Function<FDBRecordContext, CompletableFuture<LocatableResolver>> scopeGenerator;
    private final ResolverCreateHooks createHooks;

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>.
     * @param name The logical name of the directory
     */
    public DirectoryLayerDirectory(String name) {
        this(name, ANY_VALUE, null);
    }

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>.
     * @param name The logical name of the directory
     * @param wrapper Wrapper function, see: {@link KeySpaceDirectory#KeySpaceDirectory(String, KeyType, Function)}
     */
    public DirectoryLayerDirectory(String name, Function<KeySpacePath, KeySpacePath> wrapper) {
        this(name, ANY_VALUE, wrapper);
    }

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>.
     * @param name The logical name of the directory
     * @param value The value of the directory entry (the string which will be translated to an int by the resolver)
     */
    public DirectoryLayerDirectory(String name, @Nullable Object value) {
        this(name, value, null);
    }

    /**
     * Constructor for <code>DirectoryLayerDirectory</code>. Sets the <code>createHook</code> to
     * {@link ResolverCreateHooks} their default values.
     * @param name The logical name of the directory
     * @param value The value of the directory entry (the string which will be translated to an int by the resolver)
     * @param wrapper Wrapper function, see: {@link KeySpaceDirectory#KeySpaceDirectory(String, KeyType, Function)}
     */
    public DirectoryLayerDirectory(String name, @Nullable Object value,
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
    public DirectoryLayerDirectory(String name,
                                    @Nullable Function<KeySpacePath, KeySpacePath> wrapper,
                                    Function<FDBRecordContext, CompletableFuture<LocatableResolver>> scopeGenerator,
                                    ResolverCreateHooks createHooks) {
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
    public DirectoryLayerDirectory(String name, @Nullable Object value,
                            @Nullable Function<KeySpacePath, KeySpacePath> wrapper,
                            Function<FDBRecordContext, CompletableFuture<LocatableResolver>> scopeGenerator,
                            ResolverCreateHooks createHooks) {
        super(name, KeyType.LONG, value, wrapper);
        this.scopeGenerator = scopeGenerator;
        this.createHooks = createHooks;
    }

    @Override
    public boolean isValueValid(@Nullable Object value) {
        // DirectoryLayerDirectory accepts both String (logical names) and Long (directory layer values),
        // but we're making this method stricter, and I hope that using Long is only for a handful of tests,
        // despite comments saying that the resolved value should be allowed.
        // Since the long value is cluster specific, providing a long is most likely a bug, and if not, has a high
        // likelihood of becoming a bug if you ever connect to multiple clusters. Note that there is no performance
        // benefit to providing a long since `toTupleAsync` needs to get the string back to be consistent.
        // note: null is not valid, and `null` is not `instanceof String`, `toTupleValueAsync` does the same validation.
        if (value instanceof String) {
            // If this directory has a constant value, check that the provided value matches it
            return Objects.equals(getValue(), KeySpaceDirectory.ANY_VALUE) || Objects.equals(getValue(), value);
        }
        return false;
    }

    @Override
    @SuppressWarnings("NullAway") // RecordCoreArgumentException's varargs constructor parameter is not annotated @Nullable
                                   // even though value can genuinely be null.
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
    protected boolean isCompatible(KeySpaceDirectory parent, KeySpaceDirectory dir) {
        return (dir instanceof DirectoryLayerDirectory);
    }

    @Override
    @SuppressWarnings({"squid:S1604", "NullAway"}) // need annotation so no lambda; RecordCoreArgumentException's varargs
                                                    // constructor parameter is not annotated @Nullable even though
                                                    // value/this.value can genuinely be null.
    protected CompletableFuture<PathValue> toTupleValueAsyncImpl(FDBRecordContext context, @Nullable Object value) {
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

    @Override
    protected CompletableFuture<Optional<ResolvedKeySpacePath>> pathFromKey(FDBRecordContext context,
                                                                            @Nullable ResolvedKeySpacePath parent,
                                                                            Tuple key,
                                                                            int keySize,
                                                                            int keyIndex) {
        final Object tupleValue = key.get(keyIndex);
        // Only a directory layer value can be reversed into this directory
        if (!(tupleValue instanceof Long)) {
            return DIRECTORY_NOT_FOR_KEY;
        }

        final KeySpacePath parentPath = parent == null ? null : parent.toPath();

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
                        final PathValue pathValue = toPathValue(directoryResolverResult);

                        // Make sure that the path is constructed with the text-name from the directory layer.
                        ResolvedKeySpacePath myPath = new ResolvedKeySpacePath(parent,
                                KeySpacePathImpl.newPath(parentPath, this, directoryString),
                                pathValue, remainder);

                        // We are finished if there are no more subdirectories or no more tuple to consume
                        if (!canHaveChild) {
                            return CompletableFuture.completedFuture(Optional.of(myPath));
                        }

                        return findChildForKey(context, myPath, key, keySize, childKeyIndex).thenApply(Optional::of);
                    });
                });
    }

    @Override
    public String getNameInTree() {
        return "[" + getName() + "]";
    }

    private CompletableFuture<String> doReverseLookup(FDBRecordContext context, Long dir) {
        return scopeGenerator.apply(context)
                .thenCompose(resolver -> resolver.reverseLookup(context, dir));
    }

    private CompletableFuture<ResolverResult> lookupInScope(final FDBRecordContext context, final String key) {
        return scopeGenerator.apply(context).thenCompose(resolver ->
            resolver.resolveWithMetadata(context, key, createHooks));
    }

    @SuppressWarnings("NullAway") // ResolverResult#getMetadata uses the @Nullable byte[] annotation position (which
                                   // NullAway does not reliably recognize) while PathValue's constructor uses the
                                   // reliable byte @Nullable [] position; both are genuinely nullable.
    private static PathValue toPathValue(ResolverResult result) {
        return new PathValue(result.getValue(), result.getMetadata());
    }
}
