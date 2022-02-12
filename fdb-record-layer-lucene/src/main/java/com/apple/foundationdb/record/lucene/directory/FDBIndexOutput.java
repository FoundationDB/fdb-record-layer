/*
 * FDBIndexOutput.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.CRC32;

/**
 *
 * Implementation of IndexOutput representing the writing of data
 * in Lucene to a file.
 *
 * @see <a href="https://lucene.apache.org/core/7_6_0/core/org/apache/lucene/store/IndexOutput.html">IndexOutput</a>
 */
@API(API.Status.EXPERIMENTAL)
public final class FDBIndexOutput extends IndexOutput {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBIndexOutput.class);
    /**
     * Current size keeps track of the number of bytes written overall.
     */
    private int currentSize = 0;
    private int actualSize = 0;
    private ByteBuffer buffer;
    private final String resourceDescription;
    private final FDBDirectory fdbDirectory;
    private final long blockSize;
    private final CRC32 crc;
    private final long id;
    private static final ArrayBlockingQueue<ByteBuffer> BUFFERS;
    private static final int POOL_SIZE = 100;

    static {
        BUFFERS = new ArrayBlockingQueue<>(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; i++) {
            BUFFERS.add(ByteBuffer.allocate(FDBDirectory.DEFAULT_BLOCK_SIZE));
        }
    }

    /**
     * Create an FDBIndexOutput given a name and FDBDirectory.
     *
     * @param name name of resource
     * @param fdbDirectory existing FDBDirectory
     */
    public FDBIndexOutput(@Nonnull String name, @Nonnull FDBDirectory fdbDirectory) {
        this(name, name, fdbDirectory);
    }

    /**
     * Create an FDBIndexOutput given a resource description, name, and FDBDirectory.
     *
     * @param resourceDescription opaque description of file; used for logging
     * @param name name of resource
     * @param fdbDirectory existing FDBDirectory
     */
    public FDBIndexOutput(@Nonnull String resourceDescription, @Nonnull String name, @Nonnull FDBDirectory fdbDirectory) {
        super(resourceDescription, name);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(KeyValueLogMessage.of("init",
                    LuceneLogMessageKeys.RESOURCE, resourceDescription,
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        this.resourceDescription = resourceDescription;
        this.fdbDirectory = fdbDirectory;
        blockSize = fdbDirectory.getBlockSize();
        buffer = BUFFERS.poll();
        if (buffer == null) {
            buffer = ByteBuffer.allocate((int)blockSize);
        }
        crc = new CRC32();
        id = fdbDirectory.getIncrement();
    }

    /**
     * Close the directory which writes the FileReference.
     */
    @Override
    @SpotBugsSuppressWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", justification = "it is fine if it is not accepted")
    public void close() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("close()",
                    LuceneLogMessageKeys.RESOURCE, resourceDescription));
        }
        flush();
        fdbDirectory.writeFDBLuceneFileReference(resourceDescription, new FDBLuceneFileReference(id, currentSize, actualSize, blockSize));
        BUFFERS.offer(buffer);
    }

    @Override
    public long getFilePointer() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("getFilePointer()",
                    LuceneLogMessageKeys.RESOURCE, resourceDescription,
                    LuceneLogMessageKeys.POINTER, currentSize));
        }
        return currentSize;
    }

    @Override
    public long getChecksum() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("getChecksum()",
                    LuceneLogMessageKeys.CHECKSUM, crc.getValue()));
        }
        return crc.getValue();
    }

    @Override
    public void writeByte(final byte b) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("writeByte()"));
        }
        buffer.put(b);
        crc.update(b);
        currentSize++;
        if (currentSize % blockSize == 0) {
            flush();
        }
    }

    @Override
    public void copyBytes(@Nonnull final DataInput input, final long numBytes) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("copy bytes",
                    LuceneLogMessageKeys.INPUT, input,
                    LuceneLogMessageKeys.BYTE_NUMBER, numBytes));
        }
        super.copyBytes(input, numBytes);
    }

    /**
     *
     * This method will be called many times.
     *
     * @param bytes bytes to write
     * @param offset offset
     * @param length length
     */
    @Override
    public void writeBytes(@Nonnull final byte[] bytes, final int offset, final int length) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("writeBytes()",
                    LuceneLogMessageKeys.OFFSET, offset,
                    LuceneLogMessageKeys.LENGTH, length));
        }
        crc.update(bytes, offset, length);
        int bytesWritten = 0;
        while (bytesWritten < length) {
            int toWrite = (int) (length - bytesWritten + (currentSize % blockSize) > blockSize ? blockSize - (currentSize % blockSize) : length - bytesWritten);
            buffer.put(bytes, bytesWritten + offset, toWrite);
            bytesWritten += toWrite;
            currentSize += toWrite;
            if (currentSize % blockSize == 0) {
                flush();
            }
        }
    }

    private void flush() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("flush()",
                    LuceneLogMessageKeys.FILE_ID, id));
        }
        if (buffer.position() > 0) {
            buffer.flip();
            byte[] arr = new byte[buffer.remaining()];
            buffer.get(arr);
            actualSize += fdbDirectory.writeData(id, (int) ( (currentSize - 1) / blockSize), arr);
            buffer.clear();
        }
    }

    @Nonnull
    private String getLogMessage(@Nonnull String staticMsg, @Nullable final Object... keysAndValues) {
        return KeyValueLogMessage.build(staticMsg, keysAndValues)
                .addKeyAndValue(LogMessageKeys.SUBSPACE, fdbDirectory.getSubspace())
                .addKeyAndValue(LuceneLogMessageKeys.RESOURCE, resourceDescription)
                .toString();
    }
}
