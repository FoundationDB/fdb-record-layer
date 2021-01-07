/*
 * FDBIndexInput.java
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

import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Class that handles reading data cut into blocks (KeyValue) backed by an FDB keyspace.
 *
 */
public class FDBIndexInput extends IndexInput {
    private static final Logger LOG = LoggerFactory.getLogger(FDBIndexInput.class);
    private final String resourceDescription;
    private final FDBDirectory fdbDirectory;
    private final CompletableFuture<FDBLuceneFileReference> reference;
    private long position;
    private CompletableFuture<byte[]> currentData;
    private int currentBlock;
    private final long initialOffset;
    private int numberOfSeeks = 0;

    /**
     * Constructor to create an FDBIndexInput from a file referenced in the metadata keyspace.
     *
     * This constructor will begin an asynchronous query (lookahead) to the first block.
     *
     * @param resourceDescription resourceName
     * @param fdbDirectory FDB directory mapping
     * @throws IOException exception
     */
    public FDBIndexInput(final String resourceDescription, final FDBDirectory fdbDirectory) throws IOException {
        this(resourceDescription, fdbDirectory, fdbDirectory.getFDBLuceneFileReference(resourceDescription), 0L,
                0L, 0, null);
    }

    /**
     * Constructor that is utilized by splice calls to take into account initial offsets and modifications to length.
     *
     * @param resourceDescription resourceName
     * @param fdbDirectory FDB directory mapping
     * @param reference future Reference
     * @param initalOffset initialOffset
     * @param position currentBlockPosition
     * @param currentBlock block
     * @param currentData future with CurrentData Fetch
     * @throws IOException exception
     */
    public FDBIndexInput(final String resourceDescription, final FDBDirectory fdbDirectory,
                         CompletableFuture<FDBLuceneFileReference> reference, long initalOffset, long position,
                         int currentBlock, CompletableFuture<byte[]> currentData) throws IOException {
        super(resourceDescription);
        LOG.trace("init() -> {}", resourceDescription);
        this.resourceDescription = resourceDescription;
        this.fdbDirectory = fdbDirectory;
        this.reference = reference;
        this.position = position;
        this.currentBlock = currentBlock;
        this.currentData = currentData;
        this.initialOffset = initalOffset;
        if (currentData == null) {
            numberOfSeeks++;
            this.currentData = fdbDirectory.seekData(resourceDescription, reference, currentBlock);
        } else {
            seek(position);
        }
    }

    /**
     *
     * Close IndexInput (NoOp).
     */
    @Override
    public void close() {
        LOG.trace("close() -> resource={}, numberOfSeeks={}", resourceDescription, numberOfSeeks);
    }

    /**
     * The current relative position not included any offssets provided by slices.
     *
     * @return current relative position
     */
    @Override
    public long getFilePointer() {
        LOG.trace("getFilePointer() -> resource={}, position={}", resourceDescription, position);
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
        LOG.trace("seek -> resource={}, offset={}", resourceDescription, offset);
        if (currentBlock != getBlock(offset)) {
            this.position = offset;
            this.currentBlock = getBlock(position);
            numberOfSeeks++;
            LOG.trace("actual seek -> resource={}, offset={}", resourceDescription, offset);
            this.currentData = fdbDirectory.seekData(resourceDescription, reference, currentBlock); // Physical Seek
        } else {
            this.position = offset;     // Logical Seek
        }
    }

    /**
     * The total length of the input provided by MetaData stored in the metadata keyspace.
     *
     * @return length
     */
    @Override
    public long length() {
        LOG.trace("length -> resource={}", resourceDescription);
        return reference.join().getSize();
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
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        LOG.trace("slice -> resource={}, desc={}, offset={}, length={}", resourceDescription, sliceDescription, offset, length);
        return new FDBIndexInput(resourceDescription, fdbDirectory, CompletableFuture.supplyAsync( () ->
                new FDBLuceneFileReference(reference.join().getId(), length, reference.join().getBlockSize())),
                offset + initialOffset, 0L, currentBlock, currentData
                );
    }

    /**
     *
     * The relative position plus any initial offsets provided (slice).
     *
     * @return postion+initialOffset
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
        LOG.trace("readByte resource={}", resourceDescription);
        try {
            int probe = (int)(absolutePosition() % reference.join().getBlockSize());
            position++;
            assert currentData.join() != null : "current Data is null: " + resourceDescription + " " + reference.join().getId();
            return currentData.join()[probe];
        } finally {
            if (absolutePosition()  % reference.join().getBlockSize() == 0) {
                currentBlock++;
                numberOfSeeks++;
                this.currentData = fdbDirectory.seekData(resourceDescription, reference, currentBlock);
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
     * @throws IOException exception
     */
    @Override
    public void readBytes(final byte[] bytes, final int offset, final int length) throws IOException {
        LOG.trace("readBytes resource={}, offset={}, length={}", resourceDescription, offset, length);
        int bytesRead = 0;
        long blockSize = reference.join().getBlockSize();
        while (bytesRead < length) {
            long inBlockPosition = (absolutePosition() % blockSize);
            int toRead = (int) (length - bytesRead + inBlockPosition > blockSize ? blockSize - inBlockPosition : length - bytesRead);
            System.arraycopy(currentData.join(), (int)inBlockPosition, bytes, bytesRead + offset, toRead);
            bytesRead += toRead;
            position += toRead;
            if (absolutePosition() % blockSize == 0) {
                currentBlock++;
                numberOfSeeks++;
                LOG.trace("hard seek resource={}, currentBlock={}, offset={}, length={}, position={}, initialOffset={}",
                        resourceDescription, currentBlock, offset, length, position, initialOffset);
                this.currentData = fdbDirectory.seekData(resourceDescription, reference, currentBlock);
            }
        }

    }

    /**
     * Retrieve the appropriate indexed block taking into account the absolute position
     * (possible splice offsets) and dividing by the block size stored in the metadata keyspace.
     *
     * @param position current position (not counting offset)
     * @return block
     */
    private int getBlock(long position) {
        return (int) ( (position + initialOffset) / reference.join().getBlockSize());
    }
}
