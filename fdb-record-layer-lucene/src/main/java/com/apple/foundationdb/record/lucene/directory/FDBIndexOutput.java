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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.codec.PrefetchableBufferedChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
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
    private ByteBuffer buffer;
    private final String resourceDescription;
    private final FDBDirectory fdbDirectory;
    private final int blockSize;
    private final CRC32 crc;
    private final long id;
    private long actualSize;

    /**
     * Create an FDBIndexOutput given a name and FDBDirectory.
     *
     * @param name name of resource
     * @param fdbDirectory existing FDBDirectory
     */
    public FDBIndexOutput(@Nonnull String name, @Nonnull FDBDirectory fdbDirectory) throws IOException {
        this(name, name, fdbDirectory);
    }

    /**
     * Create an FDBIndexOutput given a resource description, name, and FDBDirectory.
     *
     * @param resourceDescription opaque description of file; used for logging
     * @param name name of resource
     * @param fdbDirectory existing FDBDirectory
     */
    public FDBIndexOutput(@Nonnull String resourceDescription, @Nonnull String name, @Nonnull FDBDirectory fdbDirectory) throws IOException {
        super(resourceDescription, name);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(KeyValueLogMessage.of("init",
                    LuceneLogMessageKeys.RESOURCE, resourceDescription,
                    LuceneLogMessageKeys.FILE_NAME, name));
        }
        this.resourceDescription = resourceDescription;
        this.fdbDirectory = fdbDirectory;
        actualSize = 0;
        blockSize = fdbDirectory.getBlockSize();
        buffer = ByteBuffer.allocate(blockSize);
        crc = new CRC32();
        id = fdbDirectory.getIncrement();
    }

    /**
     * Close the directory which writes the FileReference.
     */
    @Override
    @SpotBugsSuppressWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", justification = "it is fine if it is not accepted")
    public void close() throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("close()",
                    LuceneLogMessageKeys.RESOURCE, resourceDescription));
        }
        try {
            if (buffer != null) {
                flush();
                buffer = null; // prevent writing after close
                fdbDirectory.writeFDBLuceneFileReference(resourceDescription, new FDBLuceneFileReference(id, currentSize, actualSize, blockSize));
            }
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
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
    public void writeByte(final byte b) throws IOException {
        buffer.put(b);
        crc.update(b);
        currentSize++;
        flushIfFullBuffer();
    }

    /**
     * Internal method to set the total number of expected bytes to drive the read ahead of blocks.
     *
     * @param input input to setExpectedBytes
     * @param numBytes expected number of bytes
     */
    void setExpectedBytes(@Nonnull final PrefetchableBufferedChecksumIndexInput input, final long numBytes) {
        input.setExpectedBytes(numBytes);
    }

    @Override
    public void copyBytes(@Nonnull final DataInput input, final long numBytes) throws IOException {
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(getLogMessage("copy bytes",
                        LuceneLogMessageKeys.INPUT, input,
                        LuceneLogMessageKeys.BYTE_NUMBER, numBytes));
            }
            // This is an attempt to pre-fetch blocks to speed up large copies (Segment Merge Process)
            if (input instanceof PrefetchableBufferedChecksumIndexInput) {
                setExpectedBytes((PrefetchableBufferedChecksumIndexInput)input, numBytes);
            }
            super.copyBytes(input, numBytes);
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
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
    public void writeBytes(@Nonnull final byte[] bytes, final int offset, final int length) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("writeBytes()",
                    LuceneLogMessageKeys.OFFSET, offset,
                    LuceneLogMessageKeys.LENGTH, length));
        }
        crc.update(bytes, offset, length);
        int bytesWritten = 0;
        while (bytesWritten < length) {
            int toWrite = Math.min(
                    length - bytesWritten, // the total leftover bytes to write
                    (blockSize - buffer.position()) // the free space in this buffer
            );
            buffer.put(bytes, bytesWritten + offset, toWrite);
            bytesWritten += toWrite;
            currentSize += toWrite;
            flushIfFullBuffer();
        }
    }

    private void flush() throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("flush()",
                    LuceneLogMessageKeys.FILE_ID, id));
        }
        try {
            if (buffer.position() > 0) {
                buffer.flip();
                byte[] arr = new byte[buffer.remaining()];
                buffer.get(arr);
                actualSize += fdbDirectory.writeData(id, ((currentSize - 1) / blockSize), arr);
            }
            buffer.clear();
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    private void flushIfFullBuffer() throws IOException {
        if (buffer.position() >= blockSize) {
            flush();
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
