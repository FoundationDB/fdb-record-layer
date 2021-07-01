/*
 * SortedFileReader.java
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
import com.apple.foundationdb.record.RecordSortingProto;
import com.apple.foundationdb.record.provider.common.CipherPool;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.security.GeneralSecurityException;
import java.util.zip.InflaterInputStream;

/**
 * Read values from files written by {@link FileSorter}. Keys are skipped.
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public class SortedFileReader<V> implements AutoCloseable {
    @Nonnull
    private final FileInputStream fileStream;
    @Nonnull
    private final FileSortAdapter<?, V> adapter;

    private final boolean compressed;
    @Nullable
    private final java.security.Key encryptionKey;
    @Nullable
    private final Cipher cipher;
    @Nullable
    private final StoreTimer timer;

    private int sectionRecordStart;
    private int sectionRecordEnd;
    private int fileRecordEnd;
    private int recordPosition;
    private int recordSectionPosition;
    private long sectionFileStart;
    private long sectionFileEnd;

    @Nonnull
    private CodedInputStream headerStream;
    @Nonnull
    private CodedInputStream entryStream;

    public SortedFileReader(@Nonnull File file, @Nonnull FileSortAdapter<?, V> adapter, @Nullable StoreTimer timer,
                            int skip, int limit) throws IOException, GeneralSecurityException {
        fileStream = new FileInputStream(file);
        this.adapter = adapter;
        headerStream = CodedInputStream.newInstance(fileStream);
        entryStream = headerStream;
        compressed = adapter.isCompressed();
        encryptionKey = adapter.getEncryptionKey();
        if (encryptionKey != null) {
            cipher = CipherPool.borrowCipher();
        } else {
            cipher = null;
        }
        this.timer = timer;
        skipLimit(skip, limit);
    }

    private void skipLimit(int skip, int limit) throws IOException, GeneralSecurityException {
        final RecordSortingProto.SortFileHeader.Builder fileHeader = RecordSortingProto.SortFileHeader.newBuilder();
        headerStream.readMessage(fileHeader, ExtensionRegistryLite.getEmptyRegistry());
        sectionFileStart = headerStream.getTotalBytesRead();
        headerStream.resetSizeCounter();
        sectionFileEnd = sectionFileStart;  // As though end of previous one.
        if (skip > 0) {
            final FileChannel fileChannel = fileStream.getChannel();
            if (skip >= fileHeader.getNumberOfRecords()) {
                // We don't need to try to skip when there aren't enough records.
                fileChannel.position(fileChannel.size());
                recordPosition = fileRecordEnd = fileHeader.getNumberOfRecords();
                return;
            }
            while (true) {
                // Skip whole sections until skip position within one.
                final long startTime = System.nanoTime();
                final RecordSortingProto.SortSectionHeader.Builder sectionHeader = RecordSortingProto.SortSectionHeader.newBuilder();
                headerStream.readMessage(sectionHeader, ExtensionRegistryLite.getEmptyRegistry());
                sectionRecordStart = sectionHeader.getStartRecordNumber();
                sectionRecordEnd = sectionRecordStart + sectionHeader.getNumberOfRecords();
                long sectionRecordsPosition = sectionFileStart + headerStream.getTotalBytesRead();
                sectionFileEnd = sectionRecordsPosition + sectionHeader.getNumberOfBytes();
                if (sectionRecordEnd > skip) {
                    recordPosition = sectionHeader.getStartRecordNumber();
                    recordSectionPosition = 0;
                    if (compressed || encryptionKey != null) {
                        if (cipher != null) {
                            IvParameterSpec iv = new IvParameterSpec(sectionHeader.getEncryptionIv().toByteArray());
                            cipher.init(Cipher.DECRYPT_MODE, encryptionKey, iv);
                        }
                        fileChannel.position(sectionFileStart + headerStream.getTotalBytesRead());
                        InputStream inputStream = fileStream;
                        if (cipher != null) {
                            inputStream = new CipherInputStream(inputStream, cipher);
                        }
                        if (compressed) {
                            inputStream = new InflaterInputStream(inputStream);
                        }
                        entryStream = CodedInputStream.newInstance(inputStream);
                    } else {
                        entryStream = headerStream;
                    }
                    break;
                }
                sectionFileStart = sectionFileEnd;
                fileChannel.position(sectionFileStart);
                // We can't reuse coded streams as we skip around as they have a buffer.
                // Moreover there isn't a public way to make their 4K buffer smaller.
                headerStream = CodedInputStream.newInstance(fileStream);
                if (timer != null) {
                    timer.recordSinceNanoTime(SortEvents.Events.FILE_SORT_SKIP_SECTION, startTime);
                }
            }
            while (recordPosition < skip) {
                // Skip initial keys and values in this section.
                final long startTime = System.nanoTime();
                entryStream.skipRawBytes(entryStream.readRawVarint32());
                entryStream.skipRawBytes(entryStream.readRawVarint32());
                recordPosition++;
                recordSectionPosition++;
                if (timer != null) {
                    timer.recordSinceNanoTime(SortEvents.Events.FILE_SORT_SKIP_RECORD, startTime);
                }
            }
            limit += skip;
        }
        fileRecordEnd = Math.min(limit, fileHeader.getNumberOfRecords());
    }

    @Nullable
    public V read() throws IOException, GeneralSecurityException {
        if (recordPosition >= fileRecordEnd) {
            return null;
        }
        final long startTime = System.nanoTime();
        while (recordPosition >= sectionRecordEnd) {
            sectionFileStart = sectionFileEnd;
            final FileChannel fileChannel;
            if (compressed || encryptionKey != null) {
                fileChannel = fileStream.getChannel();
                fileChannel.position(sectionFileStart);
                headerStream = CodedInputStream.newInstance(fileStream);
            } else {
                fileChannel = null;
            }
            final RecordSortingProto.SortSectionHeader.Builder sectionHeader =  RecordSortingProto.SortSectionHeader.newBuilder();
            headerStream.readMessage(sectionHeader, ExtensionRegistryLite.getEmptyRegistry());
            sectionRecordStart = sectionHeader.getStartRecordNumber();
            sectionRecordEnd = sectionRecordStart + sectionHeader.getNumberOfRecords();
            long sectionRecordsPosition = sectionFileStart + headerStream.getTotalBytesRead();
            sectionFileEnd = sectionRecordsPosition + sectionHeader.getNumberOfBytes();
            recordPosition = sectionHeader.getStartRecordNumber();
            recordSectionPosition = 0;
            if (fileChannel != null) {
                if (cipher != null) {
                    IvParameterSpec iv = new IvParameterSpec(sectionHeader.getEncryptionIv().toByteArray());
                    cipher.init(Cipher.DECRYPT_MODE, encryptionKey, iv);
                }
                fileChannel.position(sectionRecordsPosition);
                InputStream inputStream = fileStream;
                if (cipher != null) {
                    inputStream = new CipherInputStream(inputStream, cipher);
                }
                if (compressed) {
                    inputStream = new InflaterInputStream(inputStream);
                }
                entryStream = CodedInputStream.newInstance(inputStream);
            }
        }
        entryStream.skipRawBytes(entryStream.readRawVarint32());
        final V record = adapter.readValue(entryStream);
        recordPosition++;
        recordSectionPosition++;
        if (timer != null) {
            timer.recordSinceNanoTime(SortEvents.Events.FILE_SORT_LOAD_RECORD, startTime);
        }
        return record;
    }

    public int getRecordPosition() {
        return recordPosition;
    }

    public long getFilePosition() {
        return sectionFileStart;
    }

    public int getRecordSectionPosition() {
        return recordSectionPosition;
    }

    @Override
    public void close() throws IOException {
        fileStream.close();
    }
}
