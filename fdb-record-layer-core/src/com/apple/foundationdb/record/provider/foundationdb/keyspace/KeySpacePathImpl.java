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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

class KeySpacePathImpl implements KeySpacePath {

    @Nonnull
    protected final FDBRecordContext context;
    @Nonnull
    protected final KeySpaceDirectory directory;
    @Nullable
    protected final KeySpacePath parent;
    @Nullable
    private final Object value;
    @Nonnull
    private final CompletableFuture<Object> resolvedValueFuture;
    @Nonnull
    private final CompletableFuture<byte[]> resolvedPathMetadata;
    @Nullable
    protected final Tuple remainder;

    private KeySpacePathImpl(@Nullable KeySpacePath parent, @Nonnull KeySpaceDirectory directory,
                            @Nonnull FDBRecordContext context, @Nullable Object value,
                            @Nonnull CompletableFuture<PathValue> resolvedPathData,
                            @Nullable Tuple remainder) {
        this.context = context;
        this.directory = directory;
        this.value = value;
        this.parent = parent;
        this.resolvedValueFuture = resolvedPathData
                .thenApply(PathValue::getResolvedValue)
                .thenApply(directory::validateResolvedValue);
        this.resolvedPathMetadata = resolvedPathData.thenApply(PathValue::getMetadata);
        this.remainder = remainder;
    }

    @Nonnull
    static KeySpacePath newPath(@Nullable KeySpacePath parent, @Nonnull KeySpaceDirectory directory,
                                @Nonnull FDBRecordContext context, @Nullable Object value,
                                @Nonnull CompletableFuture<PathValue> resolvedValueFuture,
                                @Nullable Tuple remainder) {
        return directory.wrap(new KeySpacePathImpl(parent, directory, context, value, resolvedValueFuture, remainder));
    }

    @Nonnull
    static KeySpacePath newPath(@Nullable KeySpacePath parent, @Nonnull FDBRecordContext context,
                                @Nonnull KeySpaceDirectory directory) {
        return directory.wrap(new KeySpacePathImpl(parent, directory, context, directory.getValue(),
                directory.toTupleValueAsync(context, directory.getValue()), null));
    }

    @Override
    @Nonnull
    public FDBRecordContext getContext() {
        return context;
    }

    @Nonnull
    @Override
    public KeySpacePath copyWithNewContext(@Nonnull FDBRecordContext newContext) {
        return newPath(parent == null ? null : parent.copyWithNewContext(newContext), directory, newContext, value,
                resolvedValueFuture.thenCombine(resolvedPathMetadata, PathValue::new), remainder);
    }

    @Nonnull
    @Override
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
        return subdir.wrap(new KeySpacePathImpl(self(), subdir, context, value, subdir.toTupleValueAsync(context, value), null));
    }

    @Nonnull
    @Override
    public List<KeySpacePath> list(@Nonnull String subdirName, ScanProperties scanProperties) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_LIST, listAsync(subdirName, null, scanProperties).asList());
    }

    @Nonnull
    @Override
    public RecordCursor<KeySpacePath> listAsync(@Nonnull String subdirName, @Nullable byte[] continuation,
                                                @Nonnull ScanProperties scanProperties) {
        return directory.listAsync(self(), context, subdirName, continuation, scanProperties);
    }


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

    @Nonnull
    @Override
    public CompletableFuture<Object> getResolvedValue() {
        return resolvedValueFuture;
    }

    @Nonnull
    @Override
    public CompletableFuture<byte[]> getResolvedPathMetadata() {
        return resolvedPathMetadata;
    }

    @Nonnull
    @Override
    public Tuple toTuple() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, toTupleAsync());
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
    public CompletableFuture<Tuple> toTupleAsync() {
        final List<CompletableFuture<Object>> work = flatten().stream()
                .map(KeySpacePath::getResolvedValue)
                .collect(Collectors.toList());

        return AsyncUtil.getAll(work).thenApply(Tuple::fromList);
    }

    @Nonnull
    @Override
    public CompletableFuture<Boolean> hasDataAsync() {
        return toTupleAsync().thenCompose( tuple -> {
            final byte[] rangeStart = tuple.pack();
            final byte[] rangeEnd = ByteArrayUtil.strinc(rangeStart);
            return context.ensureActive().getRange(rangeStart, rangeEnd, 1).iterator().onHasNext();
        });
    }

    @Override
    public boolean hasData() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_SCAN, hasDataAsync());
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> deleteAllDataAsync() {
        return toTupleAsync().thenApply( tuple -> {
            final byte[] rangeStart = tuple.pack();
            final byte[] rangeEnd = ByteArrayUtil.strinc(rangeStart);
            context.ensureActive().clear(rangeStart, rangeEnd);
            return null;
        });
    }

    @Override
    public void deleteAllData() {
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_CLEAR, deleteAllDataAsync());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (KeySpacePath entry : flatten()) {
            sb.append('/').append(entry.getDirectoryName()).append(':');
            if (entry.getResolvedValue().isDone() && !entry.getResolvedValue().isCompletedExceptionally()) {
                Object val;
                try {
                    val = entry.getResolvedValue().get();
                } catch (Exception e) {
                    val = entry.getValue();
                }

                appendValue(sb, val);

                if (!Objects.equals(val, entry.getValue())) {
                    sb.append("->");
                    appendValue(sb, entry.getValue());
                }
            } else {
                sb.append(value);
            }
        }

        if (getRemainder() != null) {
            sb.append('+').append(getRemainder());
        }

        return sb.toString();
    }

    private void appendValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof byte[]) {
            sb.append("0x");
            sb.append(ByteArrayUtil2.toHexString((byte[]) value));
        } else {
            sb.append(value);
        }
    }

    /**
     * Returns this path properly wrapped in whatever implementation the directory the path is contained in dictates.
     */
    @Nonnull
    private KeySpacePath self() {
        return directory.wrap(this);
    }
}
