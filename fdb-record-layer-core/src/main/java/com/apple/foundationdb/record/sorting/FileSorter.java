/*
 * FileSorter.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordSortingProto;
import com.apple.foundationdb.record.provider.common.CipherPool;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistryLite;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Write sorted keyed values into {@link File}s.
 *
 * Subsets of the input data are sorted in memory using a {@link MemorySorter}.
 * Then these are saved to files.
 * The files are then merged and values returned in order from there.
 * In addition to a final merge at the end, files can be merged when there are too many pending files.
 *
 * The files are divided into sections with headers that allow skipping ahead sequentially
 * somewhat faster than would be possible if every record had to be read,
 * but without as much overhead as fully indexing record positions into file positions.
 *
 * Files can be optionally compressed and encrypted. This applies at the section level: the section
 * headers are cleartext and the keys and records are not individually compressed / encrypted.
 *
 *
 * If the input set is small enough, it can remain in memory in the tree map
 * and can be returned directly from there.
 * @param <K> type of key
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public class FileSorter<K, V>  {
    @Nonnull
    private final MemorySorter<K, V> mapSorter;
    @Nonnull
    private final FileSortAdapter<K, V> adapter;
    @Nullable
    private final StoreTimer timer;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final List<File> files;

    private LoadResult loadResult;

    public FileSorter(@Nonnull FileSortAdapter<K, V> adapter, @Nullable StoreTimer timer,
                      @Nonnull Executor executor) {
        this.adapter = adapter;
        this.timer = timer;
        this.executor = executor;
        mapSorter = new MemorySorter<>(adapter, timer);
        files = new ArrayList<>();
    }

    @Nonnull
    public MemorySorter<K, V> getMapSorter() {
        return mapSorter;
    }

    @Nonnull
    public List<File> getFiles() {
        return files;
    }

    /** The result of {@link #load}. */
    public static class LoadResult {
        private final boolean loadComplete;
        private final boolean inMemory;
        @Nonnull
        private final RecordCursorContinuation sourceContinuation;
        @Nullable
        private final RecordCursor.NoNextReason sourceNoNextReason;

        public LoadResult(boolean loadComplete, boolean inMemory,
                          @Nonnull RecordCursorContinuation sourceContinuation, @Nullable RecordCursor.NoNextReason sourceNoNextReason) {
            this.loadComplete = loadComplete;
            this.inMemory = inMemory;
            this.sourceContinuation = sourceContinuation;
            this.sourceNoNextReason = sourceNoNextReason;
        }

        public boolean isLoadComplete() {
            return loadComplete;
        }

        public boolean isInMemory() {
            return inMemory;
        }

        @Nonnull
        public RecordCursorContinuation getSourceContinuation() {
            return sourceContinuation;
        }

        @Nullable
        public RecordCursor.NoNextReason getSourceNoNextReason() {
            return sourceNoNextReason;
        }
    }

    public CompletableFuture<LoadResult> load(@Nonnull RecordCursor<V> source) {
        loadResult = null;
        return AsyncUtil.whileTrue(() -> mapSorter.load(source, null).thenCompose(mapResult -> {
            if (mapResult.isFull()) {
                return CompletableFuture.runAsync(() -> saveToNextFile(adapter.getMaxNumFiles()), executor).thenApply(vignore -> true);
            }
            if (mapResult.getSourceNoNextReason().isOutOfBand()) {
                loadResult = new LoadResult(false, false, mapResult.getSourceContinuation(), mapResult.getSourceNoNextReason());
                return AsyncUtil.READY_FALSE;
            } else if (files.isEmpty() && mapSorter.getMap().size() < adapter.getMinFileSize()) {
                loadResult = new LoadResult(true, true, mapResult.getSourceContinuation(), mapResult.getSourceNoNextReason());
                return AsyncUtil.READY_FALSE;
            } else {
                loadResult = new LoadResult(true, false, mapResult.getSourceContinuation(), mapResult.getSourceNoNextReason());
                return CompletableFuture.runAsync(() -> saveToNextFile(1), executor).thenApply(vignore -> false);
            }
        })).thenApply(vignore -> loadResult);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private void saveToNextFile(int maxNumFiles) {
        final long startTime = System.nanoTime();
        final boolean compress = adapter.isCompressed();
        final java.security.Key encryptionKey = adapter.getEncryptionKey();
        Cipher cipher = null;
        if (!mapSorter.getMap().isEmpty()) {
            File file;
            try {
                file = adapter.generateFilename();
                try (FileOutputStream fileStream = new FileOutputStream(file)) {
                    final FileChannel fileChannel = fileStream.getChannel();
                    final CodedOutputStream headerStream = CodedOutputStream.newInstance(fileStream);
                    // To stay the same size, field existence must not change.
                    final RecordSortingProto.SortFileHeader.Builder fileHeader = RecordSortingProto.SortFileHeader.newBuilder()
                            .setNumberOfRecords(0)
                            .setNumberOfSections(0);
                    headerStream.writeMessageNoTag(fileHeader.build());
                    final RecordSortingProto.SortSectionHeader.Builder sectionHeader = RecordSortingProto.SortSectionHeader.newBuilder()
                            .setNumberOfRecords(0)
                            .setNumberOfBytes(0);
                    if (encryptionKey != null) {
                        cipher = CipherPool.borrowCipher();
                        final byte[] iv = new byte[CipherPool.IV_SIZE];
                        adapter.getSecureRandom().nextBytes(iv);
                        cipher.init(Cipher.ENCRYPT_MODE, encryptionKey, new IvParameterSpec(iv));
                        sectionHeader.setEncryptionIv(ByteString.copyFrom(iv));
                    }
                    headerStream.writeMessageNoTag(sectionHeader.build());
                    final long headerEnd = headerStream.getTotalBytesWritten();
                    final OutputStream outputStream;
                    final CodedOutputStream entryStream;
                    if (compress || cipher != null) {
                        headerStream.flush();
                        OutputStream stream = new NoCloseFilterStream(fileStream);
                        if (cipher != null) {
                            stream = new CipherOutputStream(stream, cipher);
                        }
                        if (compress) {
                            stream = new DeflaterOutputStream(stream);
                        }
                        outputStream = stream;
                        entryStream = CodedOutputStream.newInstance(outputStream);
                    } else {
                        outputStream = fileStream;
                        entryStream = headerStream;
                    }
                    if (timer != null) {
                        timer.recordSinceNanoTime(SortEvents.Events.FILE_SORT_OPEN_FILE, startTime);
                    }
                    int numberOfRecords = 0;
                    for (Map.Entry<K, V> keyAndValue : mapSorter.getMap().entrySet()) {
                        final long recordStartTime = System.nanoTime();
                        entryStream.writeByteArrayNoTag(adapter.serializeKey(keyAndValue.getKey()));
                        adapter.writeValue(keyAndValue.getValue(), entryStream);
                        numberOfRecords++;
                        if (timer != null) {
                            timer.recordSinceNanoTime(SortEvents.Events.FILE_SORT_SAVE_RECORD, recordStartTime);
                        }
                    }
                    entryStream.flush();
                    if (outputStream != fileStream) {
                        outputStream.close();
                    }
                    final long fileLength = fileChannel.position();
                    fileChannel.position(0);
                    fileHeader.setNumberOfSections(1).setNumberOfRecords(numberOfRecords);
                    headerStream.writeMessageNoTag(fileHeader.build());
                    sectionHeader.setNumberOfRecords(numberOfRecords).setNumberOfBytes(fileLength - headerEnd);
                    headerStream.writeMessageNoTag(sectionHeader.build());
                    headerStream.flush();
                    if (fileChannel.position() != headerEnd) {
                        throw new RecordCoreException("header size changed");
                    }
                    fileChannel.position(fileLength);
                    if (timer != null) {
                        timer.increment(SortEvents.Counts.FILE_SORT_FILE_BYTES, (int)fileLength);
                    }
                }
            } catch (IOException | GeneralSecurityException ex) {
                throw new RecordCoreException(ex);
            } finally {
                if (cipher != null) {
                    CipherPool.returnCipher(cipher);
                }
            }
            files.add(file);
            mapSorter.getMap().clear();
        }
        if (files.size() > maxNumFiles) {
            File file;
            try {
                file = adapter.generateFilename();
                merge(files, file);
            } catch (IOException | GeneralSecurityException ex) {
                throw new RecordCoreException(ex);
            }
            files.clear();
            files.add(file);
        }
    }

    // Both DeflaterOutputStream and CipherOutputStream have the problem that close() does finish work that isn't otherwise
    // available as a flush()-type operation. So close down those streams while keeping the actual FileOutputStream open.
    // Since those streams are pretty thin wrappers around Deflater and Cipher, an alternative would be to implement better
    // contracts from scratch, which might also benefit record serialization.
    private static class NoCloseFilterStream extends FilterOutputStream {
        public NoCloseFilterStream(@Nonnull OutputStream stream) {
            super(stream);
        }

        @Override
        public void close() {
        }
    }

    private static class InputState implements Closeable {
        @Nonnull
        final File file;
        @Nonnull
        final FileInputStream fileStream;
        @Nonnull
        CodedInputStream headerStream;
        @Nonnull
        CodedInputStream entryStream;

        final boolean compressed;
        @Nullable
        final java.security.Key encryptionKey;
        @Nullable
        final Cipher cipher;

        @Nullable
        byte[] key;
        @Nullable
        byte[] value;

        long sectionFilePosition;
        int sectionRecordEnd;
        int fileRecordEnd;
        int recordPosition;
        
        public InputState(@Nonnull File file, @Nonnull FileSortAdapter<?, ?> adapter) throws IOException, GeneralSecurityException {
            this.file = file;
            fileStream = new FileInputStream(file);
            headerStream = CodedInputStream.newInstance(fileStream);
            entryStream = headerStream;
            compressed = adapter.isCompressed();
            encryptionKey = adapter.getEncryptionKey();
            if (encryptionKey != null) {
                cipher = CipherPool.borrowCipher();
            } else {
                cipher = null;
            }
            final RecordSortingProto.SortFileHeader.Builder builder =  RecordSortingProto.SortFileHeader.newBuilder();
            headerStream.readMessage(builder, ExtensionRegistryLite.getEmptyRegistry());
            fileRecordEnd = builder.getNumberOfRecords();
        }

        public void next() throws IOException, GeneralSecurityException {
            while (recordPosition >= sectionRecordEnd) {
                if (recordPosition >= fileRecordEnd) {
                    key = null;
                    value = null;
                    return;
                }
                final FileChannel fileChannel;
                if (compressed || encryptionKey != null) {
                    fileChannel = fileStream.getChannel();
                    if (recordPosition > 0) {
                        fileChannel.position(sectionFilePosition);
                        headerStream = CodedInputStream.newInstance(fileStream);
                    }
                } else {
                    fileChannel = null;
                }
                final RecordSortingProto.SortSectionHeader.Builder builder =  RecordSortingProto.SortSectionHeader.newBuilder();
                headerStream.readMessage(builder, ExtensionRegistryLite.getEmptyRegistry());
                sectionRecordEnd += builder.getNumberOfRecords();
                if (fileChannel != null) {
                    sectionFilePosition += headerStream.getTotalBytesRead();
                    fileChannel.position(sectionFilePosition);
                    sectionFilePosition += builder.getNumberOfBytes();
                    if (cipher != null) {
                        IvParameterSpec iv = new IvParameterSpec(builder.getEncryptionIv().toByteArray());
                        cipher.init(Cipher.DECRYPT_MODE, encryptionKey, iv);
                    }
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
            key = entryStream.readByteArray();
            value = entryStream.readByteArray();
            recordPosition++;
        }

        @Override
        public void close() throws IOException {
            if (cipher != null) {
                CipherPool.returnCipher(cipher);
            }
            fileStream.close();
        }
    }

    private static class OutputState implements Closeable {
        @Nonnull
        final File file;
        final int recordsPerSection;
        @Nonnull
        final FileOutputStream fileStream;
        @Nonnull
        final FileChannel fileChannel;
        @Nonnull
        CodedOutputStream headerStream;
        @Nonnull
        OutputStream outputStream;
        @Nonnull
        CodedOutputStream entryStream;

        final boolean compress;
        @Nullable
        final java.security.Key encryptionKey;
        @Nullable
        final SecureRandom secureRandom;
        @Nullable
        final Cipher cipher;

        @Nonnull
        final RecordSortingProto.SortFileHeader.Builder fileHeader;
        @Nonnull
        final RecordSortingProto.SortSectionHeader.Builder sectionHeader;

        long sectionHeaderPosition;
        long sectionRecordsPosition;

        public OutputState(@Nonnull File file, @Nonnull FileSortAdapter<?, ?> adapter) throws IOException, GeneralSecurityException {
            this.file = file;
            this.recordsPerSection = adapter.getRecordsPerSection();
            fileStream = new FileOutputStream(file);
            outputStream = fileStream;
            fileChannel = fileStream.getChannel();
            headerStream = CodedOutputStream.newInstance(fileStream);
            entryStream = headerStream;
            compress = adapter.isCompressed();
            encryptionKey = adapter.getEncryptionKey();
            if (encryptionKey != null) {
                secureRandom = adapter.getSecureRandom();
                cipher = CipherPool.borrowCipher();
            } else {
                secureRandom = null;
                cipher = null;
            }
            fileHeader = RecordSortingProto.SortFileHeader.newBuilder()
                    .setNumberOfSections(0)
                    .setNumberOfRecords(0);
            headerStream.writeMessageNoTag(fileHeader.build());
            sectionHeader = RecordSortingProto.SortSectionHeader.newBuilder()
                    .setSectionNumber(0)
                    .setStartRecordNumber(0)
                    .setNumberOfRecords(0)
                    .setNumberOfBytes(0);
            writeSectionHeader();
        }

        public void next(@Nonnull byte[] key, @Nonnull byte[] value) throws IOException, GeneralSecurityException {
            entryStream.writeByteArrayNoTag(key);
            entryStream.writeByteArrayNoTag(value);
            fileHeader.setNumberOfRecords(fileHeader.getNumberOfRecords() + 1);
            sectionHeader.setNumberOfRecords(sectionHeader.getNumberOfRecords() + 1);
            if (sectionHeader.getNumberOfRecords() >= recordsPerSection) {
                rewriteSectionHeader();
                fileHeader.setNumberOfSections(fileHeader.getNumberOfSections() + 1);
                sectionHeader.setSectionNumber(fileHeader.getNumberOfSections());
                sectionHeader.setStartRecordNumber(fileHeader.getNumberOfRecords());
                sectionHeader.setNumberOfRecords(0).setNumberOfBytes(0);
                writeSectionHeader();
            }
        }

        public void finish() throws IOException {
            rewriteSectionHeader();
            final long fileLength = fileChannel.position();
            fileChannel.position(0);
            headerStream.writeMessageNoTag(fileHeader.build());
            headerStream.flush();
            fileChannel.position(fileLength);
        }

        @Override
        public void close() throws IOException {
            if (cipher != null) {
                CipherPool.returnCipher(cipher);
            }
            fileStream.close();
        }

        void writeSectionHeader() throws IOException, GeneralSecurityException {
            headerStream.flush();
            sectionHeaderPosition = fileChannel.position();
            if (cipher != null) {
                final byte[] iv = new byte[CipherPool.IV_SIZE];
                secureRandom.nextBytes(iv);
                cipher.init(Cipher.ENCRYPT_MODE, encryptionKey, new IvParameterSpec(iv));
                sectionHeader.setEncryptionIv(ByteString.copyFrom(iv));
            }
            headerStream.writeMessageNoTag(sectionHeader.build());
            headerStream.flush();
            sectionRecordsPosition = fileChannel.position();
            if (compress || cipher != null) {
                headerStream.flush();
                outputStream = new NoCloseFilterStream(fileStream);
                if (cipher != null) {
                    outputStream = new CipherOutputStream(outputStream, cipher);
                }
                if (compress) {
                    outputStream = new DeflaterOutputStream(outputStream);
                }
                entryStream = CodedOutputStream.newInstance(outputStream);
            }
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        void rewriteSectionHeader() throws IOException {
            entryStream.flush();
            if (outputStream != fileStream) {
                outputStream.close();
            }
            final long fileLength = fileChannel.position();
            fileChannel.position(sectionHeaderPosition);
            headerStream.writeMessageNoTag(sectionHeader.setNumberOfBytes(fileLength - sectionRecordsPosition).build());
            headerStream.flush();
            if (fileChannel.position() != sectionRecordsPosition) {
                throw new RecordCoreException("header size changed");
            }
            fileChannel.position(fileLength);
        }
    }

    // TODO: If there were a limit on the total number of records saved, then each file could be limited to
    // that number and merge could stop when it is reached.
    @SuppressWarnings("PMD.EmptyCatchBlock")
    private void merge(@Nonnull Collection<File> inputFiles, @Nonnull File outputFile) throws IOException, GeneralSecurityException {
        final long startTime = System.nanoTime();
        final List<InputState> inputs = new ArrayList<>(inputFiles.size());
        OutputState output = null;
        boolean success = false;
        try {
            for (File file : inputFiles) {
                InputState input = new InputState(file, adapter);
                inputs.add(input);
                input.next();
            }
            output = new OutputState(outputFile, adapter);
            while (true) {
                InputState minState = null;
                for (InputState input : inputs) {
                    if (input.key != null) {
                        if (minState == null) {
                            minState = input;
                        } else {
                            int comp = adapter.isSerializedOrderReversed() ?
                                       ByteArrayUtil.compareUnsigned(input.key, minState.key) :
                                       ByteArrayUtil.compareUnsigned(minState.key, input.key);
                            if (comp > 0) {
                                minState = input;
                            }
                        }
                    }
                }
                if (minState == null) {
                    break;
                }
                output.next(minState.key, minState.value);
                minState.next();
            }
            output.finish();
            output.close();
            success = true;
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException ex) {
                    // swallow cleanup error
                }
                if (!success) {
                    try {
                        deleteFile(outputFile);
                    } catch (IOException ex) {
                        // swallow cleanup error
                    }
                }
            }
            for (InputState input : inputs) {
                try {
                    input.close();
                    if (success) {
                        deleteFile(input.file);
                    }
                } catch (IOException ex) {
                    // swallow cleanup error
                }
            }
            if (timer != null) {
                timer.recordSinceNanoTime(SortEvents.Events.FILE_SORT_MERGE_FILES, startTime);
            }
        }
    }

    public void deleteFiles() throws IOException {
        for (File file : files) {
            deleteFile(file);
        }
    }

    private void deleteFile(@Nonnull File file) throws IOException {
        // file.delete() doesn't have real error handling.
        Files.delete(file.toPath());
    }

}
