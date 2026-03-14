/*
 * FDBIndexInput.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Verify.verify;

/**
 * Class that handles reading data cut into blocks (KeyValue) backed by an FDB keyspace.
 *
 * @see <a href="https://lucene.apache.org/core/7_6_0/core/org/apache/lucene/store/IndexInput.html">IndexInput</a>
 */
@API(API.Status.EXPERIMENTAL)
public final class FDBIndexInput extends IndexInput {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBIndexInput.class);
    private final String fileName;
    private final FDBDirectory fdbDirectory;
    private final CompletableFuture<FDBLuceneFileReference> reference;
    /*
     * Position within the current block
     */
    private long position;
    private CompletableFuture<byte[]> currentData;
    private int currentBlock;
    private final long initialOffset;
    private int numberOfSeeks = 0;
    // These actual values are added to remove a hotspot during byte reads.
    private byte[] actualCurrentData;
    private FDBLuceneFileReference actualReference;

    /**
     * Constructor to create an FDBIndexInput from a file referenced in the metadata keyspace.
     * <p>
     *     This constructor will begin an asynchronous query (lookahead) to the first block.
     * </p>
     *
     * @param fileName the name of the file being read
     * @param fdbDirectory FDB directory mapping
     * @throws IOException exception
     */
    public FDBIndexInput(@Nonnull final String fileName, @Nonnull final FDBDirectory fdbDirectory) throws IOException {
        this(fileName, fileName, fdbDirectory, fdbDirectory.getFDBLuceneFileReferenceAsync(fileName), 0L,
                0L, 0, null);
    }

    /**
     * Constructor that is utilized by splice calls to take into account initial offsets and modifications to length.
     *
     * @param resourceDescription opaque description of file; used for logging
     * @param fileName opaque description of file; used for logging
     * @param fdbDirectory FDB directory mapping
     * @param reference future Reference
     * @param initialOffset initialOffset
     * @param position currentBlockPosition
     * @param currentBlock block
     * @param currentData future with CurrentData Fetch
     * @throws IOException exception
     */
    public FDBIndexInput(@Nonnull final String resourceDescription, @Nonnull final String fileName, @Nonnull final FDBDirectory fdbDirectory,
                         @Nonnull CompletableFuture<FDBLuceneFileReference> reference, long initialOffset, long position,
                         int currentBlock, @Nullable CompletableFuture<byte[]> currentData) throws IOException {
        super(resourceDescription);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(KeyValueLogMessage.of("init()",
                    LuceneLogMessageKeys.RESOURCE, fileName));
        }
        this.fileName = fileName;
        this.fdbDirectory = fdbDirectory;
        this.reference = reference;
        this.position = position;
        this.currentBlock = currentBlock;
        this.currentData = currentData;
        this.initialOffset = initialOffset;
        if (currentData == null) {
            numberOfSeeks++;
            readBlock();
        } else {
            seek(position);
        }
    }

    @Nonnull
    private FDBLuceneFileReference getFileReference() {
        if (actualReference == null) {
            actualReference = fdbDirectory.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_FILE_REFERENCE, reference);
            if (actualReference == null) {
                throw new RecordCoreException("File Reference missing for open IndexInput")
                        .addLogInfo(LuceneLogMessageKeys.RESOURCE, fileName);
            }
        }
        return actualReference;
    }

    private byte[] getCurrentData() {
        if (actualCurrentData == null) {
            actualCurrentData = fdbDirectory.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_DATA_BLOCK, currentData);
        }
        return actualCurrentData;
    }

    /**
     * Reads the current block and resets the actualCurrentData Cache.
     *
     */
    private void readBlock() {
        this.currentData = fdbDirectory.readBlock(this, fileName, reference, currentBlock);
        this.actualCurrentData = null;
    }


    /**
     *
     * Close IndexInput (NoOp).
     */
    @Override
    public void close() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("close()",
                    LuceneLogMessageKeys.SEEK_NUM, numberOfSeeks));
        }
    }

    /**
     * The current relative position not including any offsets provided by slices.
     *
     * @return current relative position
     */
    @Override
    public long getFilePointer() {
        return position;
    }

    /**
     * Seeks to the offset provided.  That actual seek offset will take into account the
     * absolute position taking into account if the IndexInput has been Sliced.
     *
     * If the currentBlock is the same block after the offset, no physical seek will occur.
     *
     * @param offset positionToSeekTo
     * @throws IOException exception
     */
    @Override
    public void seek(final long offset) throws IOException {
        try {
            if (currentBlock != getBlock(offset)) {
                this.position = offset;
                this.currentBlock = getBlock(position);
                numberOfSeeks++;
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(getLogMessage("actual seek",
                            LuceneLogMessageKeys.OFFSET, offset));
                }
                readBlock(); // Physical Seek
            } else {
                this.position = offset;     // Logical Seek
            }
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    /**
     * The total length of the input provided by MetaData stored in the metadata keyspace.
     *
     * @return length
     */
    @Override
    public long length() {
        return getFileReference().getSize();
    }

    /**
     *
     * This takes an existing FDBIndexInput and provides a new initial offset plus
     * a FDBLuceneFileReference that is not <b>not</b> backed in the metadata keyspace.
     *
     * @param sliceDescription new resourceDescription of Slice
     * @param offset offset
     * @param length length
     * @return indexInput
     * @throws IOException exception
     */
    @Override
    @Nonnull
    public IndexInput slice(@Nonnull String sliceDescription, long offset, long length) throws IOException {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(getLogMessage("slice",
                    LogMessageKeys.DESCRIPTION, sliceDescription,
                    LuceneLogMessageKeys.OFFSET, offset,
                    LuceneLogMessageKeys.LENGTH, length));
        }
        // Good Place to perform stack dumps if you want to know who is performing a read...
        //Thread.dumpStack();
        try {
            final FDBLuceneFileReference fileReference = getFileReference();
            return new FDBIndexInput(getFullSliceDescription(sliceDescription), fileName, fdbDirectory, CompletableFuture.completedFuture(
                    new FDBLuceneFileReference(fileReference.getId(), length, length, fileReference.getBlockSize())),
                    offset + initialOffset, 0L, currentBlock, currentData);
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    /**
     *
     * The relative position plus any initial offsets provided (slice).
     *
     * @return position+initialOffset
     */
    private long absolutePosition() {
        return position + initialOffset;
    }

    /**
     * Read a byte based on the current relative position taking into account the
     * absolute position (slice).
     *
     * If the next byte will be on another block, asynchronously call read ahead.
     *
     * @return byte
     * @throws IOException exception
     */
    @Override
    public byte readByte() throws IOException {
        FDBLuceneFileReference fileReference;
        try {
            fileReference = getFileReference();
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }

        if (position >= fileReference.getSize()) {
            throw new EOFException("read past EOF: " + this);
        }
        try {
            int probe = (int)(absolutePosition() % fileReference.getBlockSize());
            position++;
            byte[] data = getCurrentData();
            verify(data != null, "current Data is null: " + fileName + " " + fileReference.getId());
            return data[probe];
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        } finally {
            if (absolutePosition() % fileReference.getBlockSize() == 0) {
                currentBlock++;
                numberOfSeeks++;
                readBlock();
            }
        }
    }

    /**
     * Read bytes based on the offset and taking into account the absolute position.
     *
     * Perform asynchronous read aheads when required.
     *
     * @param bytes bytes
     * @param offset offset
     * @param length length
     */
    @Override
    public void readBytes(@Nonnull final byte[] bytes, final int offset, final int length) throws IOException {
        try {
            int bytesRead = 0;
            final FDBLuceneFileReference fileReference = getFileReference();
            if (position + length > fileReference.getSize()) {
                throw new EOFException("read past EOF: " + this);
            }
            long blockSize = fileReference.getBlockSize();
            while (bytesRead < length) {
                long inBlockPosition = (absolutePosition() % blockSize);
                int toRead = (int)(length - bytesRead + inBlockPosition > blockSize ? blockSize - inBlockPosition : length - bytesRead);
                System.arraycopy(getCurrentData(), (int)inBlockPosition, bytes, bytesRead + offset, toRead);
                bytesRead += toRead;
                position += toRead;
                if (absolutePosition() % blockSize == 0) {
                    currentBlock++;
                    numberOfSeeks++;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(getLogMessage("hard seek",
                                LuceneLogMessageKeys.CURRENT_BLOCK, currentBlock,
                                LuceneLogMessageKeys.OFFSET, offset,
                                LuceneLogMessageKeys.LENGTH, length,
                                LuceneLogMessageKeys.POSITION, position,
                                LuceneLogMessageKeys.INITIAL_OFFSET, initialOffset));
                    }
                    readBlock();
                }
            }
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    /**
     * Retrieve the appropriate indexed block taking into account the absolute position
     * (possible splice offsets) and dividing by the block size stored in the metadata keyspace.
     *
     * @param position current position (not counting offset)
     * @return block
     */
    public int getBlock(long position) {
        return (int) ( (position + initialOffset) / getFileReference().getBlockSize());
    }

    @Nonnull
    private String getLogMessage(@Nonnull String staticMsg, @Nullable final Object... keysAndValues) {
        return KeyValueLogMessage.build(staticMsg, keysAndValues)
                .addKeyAndValue(LogMessageKeys.SUBSPACE, fdbDirectory.getSubspace())
                .addKeyAndValue(LuceneLogMessageKeys.RESOURCE, fileName)
                .toString();
    }

    /**
     * Prefetches the blocks from the underlying directory without manipulating the FDBInput.
     *
     * @param beginBlock Block to start caching.
     * @param length number of blocks to read forward
     * @return length supplied
     */
    public int prefetch(int beginBlock, int length) {
        for (int i = 0; i < length; i++) {
            fdbDirectory.readBlock(this, fileName, reference, beginBlock + i);
        }
        return length;
    }

}
