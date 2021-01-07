/*
 * FDBIndexOutput.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.CRC32;

/**
 *
 * Implementation of IndexOutput representing the writing of data
 * in Lucene to a file.
 *
 */
public final class FDBIndexOutput extends IndexOutput {
    private static final Logger LOG = LoggerFactory.getLogger(FDBIndexOutput.class);
    private int currentSize = 0;
    private ByteBuffer buffer;
    private final String resourceDescription;
    private final FDBDirectory fdbDirectory;
    private final long blockSize;
    private final CRC32 crc;
    private final long id;
    public static final ArrayBlockingQueue<ByteBuffer> BUFFERS;
    public static final int POOL_SIZE = 100;

    static {
        BUFFERS = new ArrayBlockingQueue<>(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; i++) {
            BUFFERS.add(ByteBuffer.allocate(FDBDirectory.DEFAULT_BLOCK_SIZE));
        }
    }

    /**
     * Create an FDBIndexOutput given a resource description, name, and FDBDirectory.
     *
     * @param resourceDescription description of resource
     * @param name name of resource
     * @param fdbDirectory existing FDBDirectory
     */
    public FDBIndexOutput(String resourceDescription, String name, FDBDirectory fdbDirectory) {
        super(resourceDescription, name);
        LOG.trace("init() -> resource={}, name={}", resourceDescription, name);
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
        LOG.trace("close() -> resource={}", resourceDescription);
        flush();
        fdbDirectory.writeFDBLuceneFileReference(resourceDescription, new FDBLuceneFileReference(id, currentSize, blockSize));
        BUFFERS.offer(buffer);
    }

    @Override
    public long getFilePointer() {
        LOG.trace("getFilePointer() -> resource={}, pointer={}", resourceDescription, currentSize);
        return currentSize;
    }

    @Override
    public long getChecksum() {
        LOG.trace("getChecksum() -> resource={}, checksum={}", resourceDescription, crc.getValue());
        return crc.getValue();
    }

    @Override
    public void writeByte(final byte b) {
        LOG.trace("writeByte() -> resource={}", resourceDescription);
        buffer.put(b);
        crc.update(b);
        currentSize++;
        if (currentSize % blockSize == 0) {
            flush();
        }
    }

    @Override
    public void copyBytes(final DataInput input, final long numBytes) throws IOException {
        LOG.debug("copy bytes input={}, numbytes={}", input, numBytes);
        super.copyBytes(input, numBytes);
    }

    @Override
    public void writeBytes(final byte[] bytes, final int offset, final int length) {
        LOG.trace("writeBytes() -> resource={}, offset={}, length={}", resourceDescription, offset, length);
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
        LOG.trace("flush() -> resource={}, id={}", resourceDescription, id);
        if (buffer.position() > 0) {
            buffer.flip();
            byte[] arr = new byte[buffer.remaining()];
            buffer.get(arr);
            fdbDirectory.writeData(id, (int) ( (currentSize - 1) / blockSize), arr);
            buffer.clear();
        }
    }

}
