/*
 * KeySpacePathImpl.java
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.ValueRange;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class KeySpacePathImpl implements KeySpacePath {

    @Nonnull
    protected final KeySpaceDirectory directory;
    @Nullable
    protected final KeySpacePath parent;
    @Nullable
    private final Object value;

    /**
     * Create a path element.
     *
     * @param parent the parent path element
     * @param directory the directory in which this path element resides
     * @param value the value to be used for this directory. This may not be the actual value that will be used
     *    in the tuple produced from the path
     */
    private KeySpacePathImpl(@Nullable KeySpacePath parent,
                             @Nonnull KeySpaceDirectory directory,
                             @Nullable Object value) {
        this.directory = directory;
        this.value = value;
        this.parent = parent;
    }

    @Nonnull
    static KeySpacePath newPath(@Nullable KeySpacePath parent,
                                @Nonnull KeySpaceDirectory directory,
                                @Nullable Object value) {
        return directory.wrap(new KeySpacePathImpl(parent, directory, value));
    }

    @Nonnull
    static KeySpacePath newPath(@Nullable KeySpacePath parent,
                                @Nonnull KeySpaceDirectory directory) {
        return directory.wrap(new KeySpacePathImpl(parent, directory, directory.getValue()));
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public KeySpacePath add(@Nonnull String dirName) {
        KeySpaceDirectory nextDir = directory.getSubdirectory(dirName);
        if (nextDir.getValue() == KeySpaceDirectory.ANY_VALUE) {
            throw new RecordCoreArgumentException("Directory requires an explicit value",
                    "dir_name", nextDir.getName());
        }

        return add(dirName, nextDir.getValue());
    }

    @Nonnull
    @Override
    public KeySpacePath add(@Nonnull String dirName, @Nullable Object value) {
        KeySpaceDirectory subdir = directory.getSubdirectory(dirName);
        return subdir.wrap(new KeySpacePathImpl(self(), subdir, value));
    }

    @Nonnull
    @Override
    public RecordCursor<ResolvedKeySpacePath> listSubdirectoryAsync(@Nonnull FDBRecordContext context, @Nonnull String subdirName, @Nullable ValueRange<?> range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return directory.listSubdirectoryAsync(self(), context, subdirName, range, continuation, scanProperties);
    }

    @Nullable
    @Override
    public KeySpacePath getParent() {
        return parent;
    }

    @Nonnull
    @Override
    public String getDirectoryName() {
        return directory.getName();
    }

    @Nonnull
    @Override
    public KeySpaceDirectory getDirectory() {
        return directory;
    }

    @Nullable
    @Override
    public Object getValue() {
        return value;
    }

    @Nonnull
    @Override
    public CompletableFuture<PathValue> resolveAsync(@Nonnull FDBRecordContext context) {
        return getDirectory().toTupleValueAsync(context, getValue());
    }

    @Nonnull
    @Override
    public List<KeySpacePath> flatten() {
        List<KeySpacePath> reversePath = new ArrayList<>();
        KeySpacePath current = self();
        while (current != null) {
            reversePath.add(current);
            current = current.getParent();
        }

        return Lists.reverse(reversePath);
    }

    @Nonnull
    @Override
    public CompletableFuture<Tuple> toTupleAsync(@Nonnull FDBRecordContext context) {
        final List<CompletableFuture<Object>> work = flatten().stream()
                .map(entry -> entry.resolveAsync(context).thenApply(PathValue::getResolvedValue))
                .collect(Collectors.toList());

        return AsyncUtil.getAll(work).thenApply(Tuple::fromList);
    }

    @Nonnull
    @Override
    public CompletableFuture<ResolvedKeySpacePath> toResolvedPathAsync(@Nonnull FDBRecordContext context) {
        final List<KeySpacePath> flatPath = flatten();
        final List<CompletableFuture<PathValue>> work = flatPath.stream()
                .map(entry -> entry.resolveAsync(context))
                .collect(Collectors.toList());
        return AsyncUtil.getAll(work).thenApply( pathValues -> {
            ResolvedKeySpacePath current = null;
            for (int i = 0; i < pathValues.size(); i++) {
                final KeySpacePath path = flatPath.get(i);
                current = new ResolvedKeySpacePath(current,
                        path.getDirectory().wrap(path), pathValues.get(i), null);
            }
            return current;
        });
    }

    @Nonnull
    @VisibleForTesting
    CompletableFuture<ResolvedKeySpacePath> toResolvedPathAsync(@Nonnull final FDBRecordContext context, final byte[] key) {
        final Tuple keyTuple = Tuple.fromBytes(key);
        return toResolvedPathAsync(context).thenCompose(resolvedPath -> {
            // Now use the resolved path to find the child for the key
            // We need to figure out how much of the key corresponds to the resolved path
            Tuple pathTuple = resolvedPath.toTuple();
            int pathLength = pathTuple.size();

            if (!TupleHelpers.isPrefix(pathTuple, keyTuple)) {
                throw new RecordCoreArgumentException("Key is not under this path")
                        .addLogInfo(LogMessageKeys.EXPECTED, pathTuple,
                                LogMessageKeys.ACTUAL, keyTuple);
            }

            // The remaining part of the key should be resolved from the resolved path's directory
            if (keyTuple.size() > pathLength) {
                // There's more in the key than just the path, so resolve the rest
                if (resolvedPath.getDirectory().getSubdirectories().isEmpty()) {
                    return CompletableFuture.completedFuture(
                            new ResolvedKeySpacePath(resolvedPath.getParent(), resolvedPath.toPath(),
                                    resolvedPath.getResolvedPathValue(),
                                    TupleHelpers.subTuple(keyTuple, pathTuple.size(), keyTuple.size())));
                } else {
                    return resolvedPath.getDirectory().findChildForKey(context, resolvedPath, keyTuple, keyTuple.size(), pathLength);
                }
            } else {
                // The key exactly matches the path
                return CompletableFuture.completedFuture(resolvedPath);
            }
        });
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> hasDataAsync(@Nonnull FDBRecordContext context) {
        return toTupleAsync(context).thenCompose( tuple -> {
            final byte[] rangeStart = tuple.pack();
            final byte[] rangeEnd = ByteArrayUtil.strinc(rangeStart);
            return context.ensureActive().getRange(rangeStart, rangeEnd, 1).iterator().onHasNext();
        });
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> deleteAllDataAsync(@Nonnull FDBRecordContext context) {
        context.setDirtyStoreState(true);
        context.setMetaDataVersionStamp();
        return toTupleAsync(context).thenApply( tuple -> {
            final byte[] rangeStart = tuple.pack();
            final byte[] rangeEnd = ByteArrayUtil.strinc(rangeStart);
            context.clear(new Range(rangeStart, rangeEnd));
            return null;
        });
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof KeySpacePath)) {
            return false;
        }
        KeySpacePath that = (KeySpacePath) obj;

        // Directories use reference equality, because the expected usage is that they go into a
        // singleton KeySpace.
        boolean directoriesEqual = this.getDirectory().equals(that.getDirectory());

        // the values might be byte[]
        return directoriesEqual &&
                KeySpaceDirectory.areEqual(this.getValue(), that.getValue()) &&
                Objects.equals(this.getParent(), that.getParent());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getDirectory(),
                KeySpaceDirectory.valueHashCode(getValue()),
                parent);
    }

    @Override
    public String toString(@Nullable Tuple t) {
        Iterator<Object> it = null;
        if (t != null) {
            it = t.getItems().iterator();
        }
        StringBuilder sb = new StringBuilder();

        for (KeySpacePath entry : flatten()) {
            sb.append('/').append(entry.getDirectoryName()).append(':');
            Object dirValue = entry.getValue();
            Object storedValue = null;
            if (it != null && it.hasNext()) {
                storedValue = it.next();
            }
            if (storedValue != null && !Objects.equals(dirValue, storedValue)) {
                ResolvedKeySpacePath.appendValue(sb, storedValue);
                sb.append("->");
            }
            ResolvedKeySpacePath.appendValue(sb, dirValue);
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toString(null);
    }

    @Nonnull
    @Override
    public RecordCursor<DataInKeySpacePath> exportAllData(@Nonnull FDBRecordContext context,
                                                          @Nullable byte[] continuation,
                                                          @Nonnull ScanProperties scanProperties) {
        return new LazyCursor<>(toTupleAsync(context)
                .thenApply(tuple -> KeyValueCursor.Builder.withSubspace(new Subspace(tuple))
                        .setContext(context)
                        .setContinuation(continuation)
                        .setScanProperties(scanProperties)
                        .build()),
                context.getExecutor())
                .mapPipelined(keyValue ->
                    toResolvedPathAsync(context, keyValue.getKey())
                            .thenApply(resolvedKey ->
                                    new DataInKeySpacePath(resolvedKey.toPath(), resolvedKey.getRemainder(), keyValue.getValue())),
                    1);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> importData(@Nonnull FDBRecordContext context,
                                              @Nonnull Iterable<DataInKeySpacePath> dataToImport) {
        return toTupleAsync(context).thenCompose(targetTuple -> {
            // We use a mapPipelined here to help control the rate of insertions into the directory layer if those
            // are happening.
            // DirectoryLayer operations are done in a separate transaction for two reasons:
            // 1. To reduce conflicts
            // 2. So that it can immediately be cached without having to do it in a post-commit hook, which can make
            //    things complicated
            // So if we just spun off a future for every data item, they would conflict like crazy, and retrying all
            // of those conflicts would cause this future to take way longer than if we pipeline.
            // This shouldn't make much of a difference in the general case because almost all the directory layer
            // lookups should be from cache.
            final RecordCursor<Void> insertionWork = RecordCursor.fromIterator(context.getExecutor(), dataToImport.iterator())
                    .mapPipelined(dataItem ->
                            dataItem.getPath().toTupleAsync(context).thenAccept(itemPathTuple -> {
                                // Validate that this data belongs under this path
                                if (!TupleHelpers.isPrefix(targetTuple, itemPathTuple)) {
                                    throw new RecordCoreIllegalImportDataException(
                                            "Data item path does not belong under target path",
                                            "target", targetTuple,
                                            "item", itemPathTuple);
                                }

                                // Reconstruct the key using the path and remainder
                                Tuple keyTuple = itemPathTuple;
                                if (dataItem.getRemainder() != null) {
                                    keyTuple = keyTuple.addAll(dataItem.getRemainder());
                                }

                                // Store the data
                                byte[] keyBytes = keyTuple.pack();
                                byte[] valueBytes = dataItem.getValue();
                                context.ensureActive().set(keyBytes, valueBytes);
                            }),
                            1);
            // Use forEach to force consuming the entire cursor, which will cause the inserts to happen
            final CompletableFuture<Void> allInsertions = insertionWork.forEach(vignore -> { })
                    .whenComplete((vignore, e) -> insertionWork.close());
            return context.instrument(FDBStoreTimer.Events.IMPORT_DATA, allInsertions);
        });
    }

    /**
     * Returns this path properly wrapped in whatever implementation the directory the path is contained in dictates.
     */
    @Nonnull
    private KeySpacePath self() {
        return directory.wrap(this);
    }
}
