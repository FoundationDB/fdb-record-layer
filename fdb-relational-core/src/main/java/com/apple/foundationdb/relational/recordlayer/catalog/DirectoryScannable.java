/*
 * DirectoryScannable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.ConcatCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolvedKeySpacePath;
import com.apple.foundationdb.record.util.TriFunction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.ImmutableKeyValue;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmptyTuple;
import com.apple.foundationdb.relational.recordlayer.KeyBuilder;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.ResumableIterator;
import com.apple.foundationdb.relational.recordlayer.Scannable;
import com.apple.foundationdb.relational.recordlayer.TupleUtils;

import java.net.URI;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DirectoryScannable implements Scannable {
    private final KeySpace keySpace;
    private final URI prefix;
    private final String[] fieldNames;
    private final Function<NestableTuple, NestableTuple> directoryTransform;

    public DirectoryScannable(KeySpace keySpace) {
        this(keySpace, URI.create("/"));
    }

    public DirectoryScannable(KeySpace keySpace, Function<NestableTuple, NestableTuple> dirTransform) {
        this(keySpace, URI.create("/"), new String[]{"path"}, dirTransform);
    }

    public DirectoryScannable(KeySpace keySpace, URI prefix) {
        this(keySpace, prefix, new String[]{"path"}, objects -> objects);
    }

    public DirectoryScannable(KeySpace keySpace, URI prefix, Function<NestableTuple, NestableTuple> directoryTransform) {
        this(keySpace, prefix, new String[]{"path"}, directoryTransform);
    }

    public DirectoryScannable(KeySpace keySpace, URI prefix, String[] fieldNames, Function<NestableTuple, NestableTuple> directoryTransform) {
        this.keySpace = keySpace;
        this.prefix = prefix;
        this.fieldNames = fieldNames;
        this.directoryTransform = directoryTransform;
    }

    @Override
    public ResumableIterator<KeyValue> openScan(@Nonnull Transaction transaction, @Nullable NestableTuple startKey, @Nullable NestableTuple endKey, @Nullable Continuation continuation, @Nonnull QueryProperties scanOptions) throws RelationalException {
        FDBRecordContext ctx = transaction.unwrap(FDBRecordContext.class);
        //TODO(bfines) add continuation here
        final KeySpacePath prefixPath = KeySpaceUtils.uriToPath(prefix, keySpace);
        KeySpaceDirectory ksd = prefixPath.getDirectory();
        String prefixDirName = prefixPath.getDirectoryName();

        int mergeLevel = 1;
        RecordCursor<ResolvedKeySpacePath> cursor;

        if (ksd.getKeyType().getAnyValue().equals(prefixPath.getValue())) {
            cursor = listDirectory(prefixPath.getParent(), prefixDirName, ctx);
        } else {
            cursor = listDirectory(prefixPath, null, ctx);
        }

        int ml = mergeLevel;
        RecordCursor<NestableTuple> mappedCursor = cursor.map(resolvedKeySpacePath -> {
            KeySpacePath path = resolvedKeySpacePath.toPath();
            String basePath = KeySpaceUtils.toPathString(path);
            Tuple remainder = resolvedKeySpacePath.getRemainder();
            Tuple tuple = new Tuple();
            tuple = tuple.addObject(basePath);
            for (int i = 0; i < ml; i++) {
                tuple = tuple.addObject(remainder.get(i));
            }
            return TupleUtils.toRelationalTuple(tuple);
        });

        return RecordLayerIterator.create(mappedCursor, value -> new ImmutableKeyValue(new EmptyTuple(), directoryTransform.apply(value)), false);
    }

    private RecordCursor<ResolvedKeySpacePath> listDirectory(KeySpacePath path, @Nullable String subdirName, FDBRecordContext ctx) {
        if (subdirName == null) {
            final KeySpaceDirectory directory = path.getDirectory();
            final List<KeySpaceDirectory> subdirectories = directory.getSubdirectories();
            if (subdirectories.size() > 1) {
                return new ListingGenerator(0, path, subdirectories).apply(ctx, ScanProperties.FORWARD_SCAN, null);
            } else {
                subdirName = subdirectories.get(0).getName();
            }
        }
        return path.listSubdirectoryAsync(ctx, subdirName, null, ScanProperties.FORWARD_SCAN);
    }

    private static final class ListingGenerator implements TriFunction<FDBRecordContext, ScanProperties, byte[], RecordCursor<ResolvedKeySpacePath>> {
        private final int position;
        private final KeySpacePath path;
        private final List<KeySpaceDirectory> subDirectories;

        private ListingGenerator(int position, KeySpacePath path, List<KeySpaceDirectory> subDirectories) {
            this.position = position;
            this.path = path;
            this.subDirectories = subDirectories;
        }

        @Override
        public RecordCursor<ResolvedKeySpacePath> apply(FDBRecordContext fdbRecordContext, ScanProperties scanProperties, byte[] bytes) {
            if (position == subDirectories.size() - 1) {
                return path.listSubdirectoryAsync(fdbRecordContext, subDirectories.get(position).getName(), bytes, scanProperties);
            } else {
                return new ConcatCursor<>(fdbRecordContext, scanProperties,
                        new ListingGenerator(position + 1, path, subDirectories),
                        (fdbRecordContext1, scanProperties1, bytes1) ->
                                path.listSubdirectoryAsync(fdbRecordContext1, subDirectories.get(position).getName(), bytes1, scanProperties1), bytes);
            }
        }
    }

    @Override
    public KeyValue get(@Nonnull Transaction t, @Nonnull NestableTuple key, @Nonnull QueryProperties scanOptions) throws RelationalException {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Override
    public String[] getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public String[] getKeyFieldNames() {
        return new String[]{};
    }

    @Override
    public KeyBuilder getKeyBuilder() {
        throw new UnsupportedOperationException("Key Builder is not supported for directory scannables");
    }

    @Nonnull
    @Override
    public String getName() {
        return "directory(" + prefix + ")";
    }
}
