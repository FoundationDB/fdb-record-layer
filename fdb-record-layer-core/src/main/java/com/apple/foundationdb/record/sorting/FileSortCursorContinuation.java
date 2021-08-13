/*
 * FileSortCursorContinuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordSortingProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
class FileSortCursorContinuation<K, V> implements RecordCursorContinuation {
    @Nonnull
    private final FileSortAdapter<K, V> adapter;

    private final boolean exhausted;
    private final boolean loading;
    @Nonnull
    private final Collection<V> inMemoryRecords;
    @Nonnull
    private final List<File> files;
    @Nonnull
    private final RecordCursorContinuation childContinuation;
    private final int recordPosition;
    private final long filePosition;
    
    @Nullable
    private RecordSortingProto.FileSortContinuation cachedProto;
    @Nullable
    private byte[] cachedBytes;

    FileSortCursorContinuation(@Nonnull FileSortAdapter<K, V> adapter,
                               boolean exhausted, boolean loading,
                               @Nonnull Collection<V> inMemoryRecords,
                               @Nonnull List<File> files, @Nonnull RecordCursorContinuation childContinuation,
                               int recordPosition, long filePosition) {
        this.exhausted = exhausted;
        this.loading = loading;
        this.adapter = adapter;
        this.inMemoryRecords = inMemoryRecords;
        this.files = files;
        this.childContinuation = childContinuation;
        this.recordPosition = recordPosition;
        this.filePosition = filePosition;
    }

    @Nonnull
    RecordSortingProto.FileSortContinuation toProto() {
        if (cachedProto == null) {
            RecordSortingProto.FileSortContinuation.Builder builder = RecordSortingProto.FileSortContinuation.newBuilder();
            if (loading) {
                builder.setLoading(true);
            }
            for (V record : inMemoryRecords) {
                builder.addInMemoryRecords(ByteString.copyFrom(adapter.serializeValue(record)));
            }
            for (File file : files) {
                builder.addFiles(file.getPath());
            }
            byte[] childBytes = childContinuation.toBytes();
            if (childBytes != null) {
                builder.setContinuation(ByteString.copyFrom(childBytes));
            }
            if (recordPosition > 0) {
                builder.setRecordPosition(recordPosition);
            }
            if (filePosition > 0) {
                builder.setFilePosition(filePosition);
            }
            cachedProto = builder.build();
        }
        return cachedProto;
    }

    @Override
    @Nullable
    public byte[] toBytes() {
        if (isEnd()) {
            return null;
        }
        if (cachedBytes == null) {
            cachedBytes = toProto().toByteArray();
        }
        return cachedBytes;
    }

    @Nonnull
    static <K, V> FileSortCursorContinuation<K, V> from(@Nonnull RecordSortingProto.FileSortContinuation parsed,
                                                        @Nonnull FileSortAdapter<K, V> adapter) {
        FileSortCursorContinuation<K, V> result = new FileSortCursorContinuation<>(
                adapter, false, parsed.getLoading(),
                parsed.getInMemoryRecordsList().stream().map(bs -> adapter.deserializeValue(bs.toByteArray())).collect(Collectors.toList()),
                parsed.getFilesList().stream().map(File::new).collect(Collectors.toList()),
                parsed.hasContinuation() ? ByteArrayContinuation.fromNullable(parsed.getContinuation().toByteArray()) : RecordCursorStartContinuation.START,
                parsed.getRecordPosition(), parsed.getFilePosition()
        );
        result.cachedProto = parsed;
        return result;
    }

    @Nonnull
    static <K, V> FileSortCursorContinuation<K, V> from(@Nullable byte[] unparsed,
                                                        @Nonnull FileSortAdapter<K, V> adapter) {
        FileSortCursorContinuation<K, V> result;
        if (unparsed == null) {
            result = new FileSortCursorContinuation<K, V>(adapter, false, true, Collections.emptyList(), Collections.emptyList(), RecordCursorStartContinuation.START, 0, 0);
        } else {
            try {
                result = from(RecordSortingProto.FileSortContinuation.parseFrom(unparsed), adapter);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsed));
            }
            result.cachedBytes = unparsed;
        }
        return result;
    }

    public boolean isLoading() {
        return loading;
    }

    @Nonnull
    public Collection<V> getInMemoryRecords() {
        return inMemoryRecords;
    }

    @Nonnull
    public List<File> getFiles() {
        return files;
    }

    @Nonnull
    RecordCursorContinuation getChild() {
        return childContinuation;
    }

    public int getRecordPosition() {
        return recordPosition;
    }

    public long getFilePosition() {
        return filePosition;
    }

    @Override
    public boolean isEnd() {
        return exhausted;
    }
}
