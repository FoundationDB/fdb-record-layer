/*
 * PrefetchableBufferedChecksumIndexInput.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.lucene.directory.FDBIndexInput;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * An Index Input that attempts to keep a 10 block buffer in front or at the read at a minimum.  The goal for this
 * class is to provide some parallelism for fetches from FDB when we are attempting to merge segments.  The signaling
 * of when to prefetch occurs in the FDBIndexOutput#copyBytes method.
 *
 */
public class PrefetchableBufferedChecksumIndexInput extends BufferedChecksumIndexInput {
    private final IndexInput indexInput;
    private static final int PREFETCH_BLOCKS = 10; // Attempt to keep a large enough buffer
    /**
     * GreatestFetchBlock from the underlying FDBIndexInput, -1 if not fetched.
     *
     */
    private int greatestFetchedBlock;
    /**
     * Maximum fetched block based on the numberOfBytesToPrefetch.
     *
     */
    private int maximumFetchedBlock;
    /**
     * Checks whether it is an instance of FDBIndexInput.
     *
     */
    private final boolean supportsPrefetch;

    public PrefetchableBufferedChecksumIndexInput(final IndexInput indexInput) {
        super(indexInput);
        this.indexInput = indexInput;
        this.supportsPrefetch =  indexInput instanceof FDBIndexInput;
    }

    /**
     * This method sets the total number of bytes to expect and then attempts to read ahead at least 10 blocks or the
     * remaining number of blocks while reading.
     *
     * @param numberOfBytesToPrefetch providing a signal to the total number of bytes to prefetch
     */
    public void setExpectedBytes(long numberOfBytesToPrefetch) {
        if (supportsPrefetch) {
            this.maximumFetchedBlock = ((FDBIndexInput) indexInput).getBlock(numberOfBytesToPrefetch - 1);
            this.greatestFetchedBlock = ((FDBIndexInput) indexInput).getBlock(0L) - 1;
            checkBuffer(0L);
        }
    }

    /**
     * Based on the greatestFetchBlock and the current positon of the underlying FDBIndexInput, it will decide how
     * many blocks to fetch.
     *
     * @param length position
     */
    public void checkBuffer(long length) {
        if (supportsPrefetch) {
            int idealPointToReadTo = ((FDBIndexInput)indexInput).getBlock(indexInput.getFilePointer() + length)
                                     + PREFETCH_BLOCKS;
            int blocksToBuffer;
            if (idealPointToReadTo > maximumFetchedBlock) {
                idealPointToReadTo = maximumFetchedBlock;
                blocksToBuffer = idealPointToReadTo - greatestFetchedBlock;
            } else {
                blocksToBuffer = idealPointToReadTo - greatestFetchedBlock - 1;
            }
            if (blocksToBuffer > 0) {
                greatestFetchedBlock += ((FDBIndexInput)indexInput)
                        .prefetch(greatestFetchedBlock + 1, blocksToBuffer);
            }
        }
    }

    @Override
    public byte readByte() throws IOException {
        try {
            checkBuffer(1L);
            return super.readByte();
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
        try {
            checkBuffer(len);
            super.readBytes(b, offset, len);
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

}
