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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
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

    // The following variables are only available with a path has been reconstructed from a tuple
    private final boolean wasFromTuple;
    @Nullable
    private final PathValue resolvedPathValue;
    @Nullable
    protected final Tuple remainder;

    /**
     * Create a path element.
     *
     * @param parent the parent path element
     * @param directory the directory in which this path element resides
     * @param value the value to be used for this directory. This may not be the actual value that will be used
     *    in the tuple produced from the path
     * @param wasFromTuple this will be true if this path was produced using {@link KeySpace#pathFromKey(FDBRecordContext, Tuple)}
     * @param resolvedPathValue if <code>wasFromTuple</code> is true, this is the future that will be resolved when the
     *    path is resolved
     * @param remainder if <code>wasFromTuple</code> is true, this is any remainder of the euple that fell off the
     *    end of the path
     */
    private KeySpacePathImpl(@Nullable KeySpacePath parent,
                             @Nonnull KeySpaceDirectory directory,
                             @Nullable Object value,
                             boolean wasFromTuple,
                             @Nullable PathValue resolvedPathValue,
                             @Nullable Tuple remainder) {
        // Run-time assertion.
        if (!wasFromTuple && (resolvedPathValue != null || remainder != null)) {
            throw new IllegalArgumentException("Paths that weren't resolved from a tuple cannot provide storage information!");
        }

        this.directory = directory;
        this.value = value;
        this.parent = parent;
        this.wasFromTuple = wasFromTuple;
        this.resolvedPathValue = resolvedPathValue;
        this.remainder = remainder;
    }

    @Nonnull
    static KeySpacePath newPath(@Nullable KeySpacePath parent,
                                @Nonnull KeySpaceDirectory directory,
                                @Nullable Object value,
                                boolean wasFromTuple,
                                @Nullable PathValue resolvedPathValue,
                                @Nullable Tuple remainder) {
        return directory.wrap(new KeySpacePathImpl(parent, directory, value, wasFromTuple,
                resolvedPathValue, remainder));
    }

    @Nonnull
    static KeySpacePath newPath(@Nullable KeySpacePath parent,
                                @Nonnull KeySpaceDirectory directory) {
        return directory.wrap(new KeySpacePathImpl(parent, directory, directory.getValue(),
                false, null, null));
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
        return subdir.wrap(new KeySpacePathImpl(self(), subdir, value,
                false, null, null));
    }

    @Deprecated
    @Nonnull
    @Override
    public RecordCursor<KeySpacePath> listAsync(@Nonnull FDBRecordContext context, 
                                                @Nonnull String subdirName, 
                                                @Nullable ValueRange<?> range,
                                                @Nullable byte[] continuation,
                                                @Nonnull ScanProperties scanProperties) {
        return directory.listSubdirectoryAsync(self(), context, subdirName, range, continuation, scanProperties)
                .map(ResolvedKeySpacePath::toPath);
    }

    @Nonnull
    @Override
    public RecordCursor<ResolvedKeySpacePath> listSubdirectoryAsync(@Nonnull FDBRecordContext context, @Nonnull String subdirName, @Nullable ValueRange<?> range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        return directory.listSubdirectoryAsync(self(), context, subdirName, range, continuation, scanProperties);
    }

    @Deprecated
    @Nullable
    @Override
    public Tuple getRemainder() {
        return remainder;
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

    @Deprecated
    @Nonnull
    @Override
    public PathValue getStoredValue() {
        if (!wasFromTuple) {
            throw new IllegalStateException("Path must be produced using pathFromKey() or list()");
        }
        return resolvedPathValue;
    }

    @Deprecated
    @Override
    public boolean hasStoredValue() {
        return wasFromTuple;
    }

    @Nonnull
    @Override
    public CompletableFuture<PathValue> resolveAsync(@Nonnull FDBRecordContext context) {
        if (wasFromTuple) {
            return CompletableFuture.completedFuture(resolvedPathValue);
        } else {
            return getDirectory().toTupleValueAsync(context, getValue());
        }
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

        // Check that the KeySpaceDirectories of the two paths are "equal enough".
        // Even this is probably overkill since the isCompatible check in KeySpaceDirectory
        // will keep us from doing anything too bad. We could move this check into KeySpaceDirectory
        // but comparing two directories by value would necessitate traversing the entire directory
        // tree, so instead we will use a narrower definition of equality here.
        boolean directoriesEqual = Objects.equals(this.getDirectory().getKeyType(), that.getDirectory().getKeyType()) &&
                                   Objects.equals(this.getDirectory().getName(), that.getDirectory().getName()) &&
                                   Objects.equals(this.getDirectory().getValue(), that.getDirectory().getValue());

        return directoriesEqual &&
               Objects.equals(this.getValue(), that.getValue()) &&
               Objects.equals(this.getParent(), that.getParent());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getDirectory().getKeyType(),
                getDirectory().getName(),
                getDirectory().getValue(),
                getValue(),
                parent);
    }

    @SuppressWarnings("deprecation")
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
            } else if (entry.hasStoredValue()) {
                storedValue = entry.getStoredValue().getResolvedValue();
            }
            if (storedValue != null && !Objects.equals(dirValue, storedValue)) {
                ResolvedKeySpacePath.appendValue(sb, storedValue);
                sb.append("->");
            }
            ResolvedKeySpacePath.appendValue(sb, dirValue);
        }

        if (getRemainder() != null) {
            sb.append('+').append(getRemainder());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toString(null);
    }

    /**
     * Returns this path properly wrapped in whatever implementation the directory the path is contained in dictates.
     */
    @Nonnull
    private KeySpacePath self() {
        return directory.wrap(this);
    }
}
