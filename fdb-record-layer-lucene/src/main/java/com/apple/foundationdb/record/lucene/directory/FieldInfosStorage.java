/*
 * FieldInfosStorage.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneFieldInfosProto;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.codec.LazyOpener;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Class for encapsulating logic around storing {@link org.apache.lucene.index.FieldInfos FieldInfos}, via
 * {@link com.apple.foundationdb.record.lucene.codec.LuceneOptimizedFieldInfosFormat LuceneOptimizedFieldInfosFormat}
 * in {@link FDBDirectory}, and caching the data for the life of the {@code directory}.
 */
public class FieldInfosStorage {

    public static final long GLOBAL_FIELD_INFOS_ID = -2;
    private final LazyOpener<Map<Long, LuceneFieldInfosProto.FieldInfos>> allFieldInfosSupplier;
    private final FDBDirectory directory;
    private final AtomicReference<ConcurrentMap<Long, AtomicInteger>> referenceCount;

    FieldInfosStorage(FDBDirectory directory) {
        allFieldInfosSupplier =
                LazyOpener.supply(
                        () -> directory.getAllFieldInfosStream().collect(Collectors.toConcurrentMap(
                                Pair::getLeft,
                                pair -> {
                                    try {
                                        return LuceneFieldInfosProto.FieldInfos.parseFrom(pair.getRight());
                                    } catch (InvalidProtocolBufferException e) {
                                        throw new UncheckedIOException(e);
                                    }
                                }
                        )));
        this.directory = directory;
        referenceCount = new AtomicReference<>();
    }

    @VisibleForTesting
    public Map<Long, LuceneFieldInfosProto.FieldInfos> getAllFieldInfos() throws IOException {
        return allFieldInfosSupplier.get();
    }

    public LuceneFieldInfosProto.FieldInfos readGlobalFieldInfos() throws IOException {
        return readFieldInfos(GLOBAL_FIELD_INFOS_ID);
    }

    public LuceneFieldInfosProto.FieldInfos readFieldInfos(long id) throws IOException {
        return getAllFieldInfos().get(id);
    }

    public long writeFieldInfos(LuceneFieldInfosProto.FieldInfos value) throws IOException {
        try {
            long id;
            if (Boolean.TRUE.equals(getAllFieldInfos().isEmpty())) {
                id = GLOBAL_FIELD_INFOS_ID;
            } else {
                id = directory.getIncrement();
            }
            writeFieldInfos(id, value);
            return id;
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    private void writeFieldInfos(final long id, final LuceneFieldInfosProto.FieldInfos value) throws IOException {
        try {
            directory.writeFieldInfos(id, value.toByteArray());
            getAllFieldInfos().put(id, value);
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    public void updateGlobalFieldInfos(LuceneFieldInfosProto.FieldInfos value) throws IOException {
        writeFieldInfos(GLOBAL_FIELD_INFOS_ID, value);
    }

    public FDBLuceneFileReference getFDBLuceneFileReference(String fileName) {
        return directory.getFDBLuceneFileReference(fileName);
    }

    public void setFieldInfoId(final Directory wrappingDirectory, String fileName, long id, BitSet fieldBitSet)
            throws IOException {
        setFieldInfoId(wrappingDirectory, fileName, id, ByteString.copyFrom(fieldBitSet.toByteArray()));
    }

    private void setFieldInfoId(final Directory wrappingDirectory, String fileName, long id, final ByteString bitSet)
            throws IOException {
        try {
            // We need to sync here in case we are interacting with an NRTCachingDirectory, which is used during NRT search
            // if we don't sync the reference won't be created on the source, and we'll fail....
            // sync is a no-op for FDBDirectory
            wrappingDirectory.sync(List.of(fileName));
            this.directory.setFieldInfoId(fileName, id, bitSet);
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    void initializeReferenceCount(final ConcurrentMap<Long, AtomicInteger> fieldInfosCount) {
        referenceCount.compareAndSet(null, fieldInfosCount);
    }

    public ConcurrentMap<Long, AtomicInteger> getReferenceCount() {
        return referenceCount.get();
    }

    public void addReference(final FDBLuceneFileReference reference) {
        if (reference.getFieldInfosId() != 0) {
            referenceCount.get()
                    .computeIfAbsent(reference.getFieldInfosId(), key -> new AtomicInteger(0))
                    .incrementAndGet();
        }
    }

    public boolean delete(final long id) throws IOException {
        if (id != 0 &&
                Objects.requireNonNull(referenceCount.get(), "fieldInfosReferenceCache")
                        .get(id).decrementAndGet() == 0) {
            getAllFieldInfos().remove(id);
            return true;
        }
        return false;
    }

    public static void copyFieldInfosId(Directory wrappingDirectory, final String fieldInfosFileName,
                                        final String entriesFile) throws IOException {
        try {
            @SuppressWarnings("PMD.CloseResource")
            // we don't need to close this because it is just extracting from the dir
            final FDBDirectoryBase directory = FDBDirectoryUtils.getFDBDirectoryNotCompound(wrappingDirectory);
            final FDBLuceneFileReference fieldInfosReference = directory.getFDBLuceneFileReference(fieldInfosFileName);
            if (fieldInfosReference == null) {
                throw new RecordCoreException("Reference not found")
                        .addLogInfo(LuceneLogMessageKeys.FILE_NAME, fieldInfosFileName);
            }
            directory.getFieldInfosStorage().setFieldInfoId(wrappingDirectory, entriesFile,
                    fieldInfosReference.getFieldInfosId(),
                    fieldInfosReference.getFieldInfosBitSet());
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }
}
