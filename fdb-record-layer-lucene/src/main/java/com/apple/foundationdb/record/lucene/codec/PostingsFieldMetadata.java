/*
 * PostingsFieldMetadata.java
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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.lucene.LucenePostingsProto;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.lucene.util.BytesRef;

import java.io.UncheckedIOException;

/**
 * Metadata about each field (accumulates metadata for all terms in the field).
 * Wrapper around the protobuf that gets stored and retrieved.
 */
public class PostingsFieldMetadata {
    private final BytesRef minTerm;
    private final BytesRef maxTerm;
    private final long numTerms;
    private final long sumTotalTermFreq;
    private final long sumDocFreq;
    private final int cardinality;

    public PostingsFieldMetadata(final byte[] bytes) {
        try {
            LucenePostingsProto.TermMeta metadata = LucenePostingsProto.TermMeta.parseFrom(bytes);
            minTerm = new BytesRef(metadata.getMinTerm().toByteArray());
            maxTerm = new BytesRef(metadata.getMaxTerm().toByteArray());
            numTerms = metadata.getNumTerms();
            sumTotalTermFreq = metadata.getSumTotalFreq();
            sumDocFreq = metadata.getSumDocFreq();
            cardinality = metadata.getCardinality();
        } catch (InvalidProtocolBufferException ex) {
            throw new UncheckedIOException("Failed to parse field metadata", ex);
        }
    }

    public BytesRef getMinTerm() {
        return minTerm;
    }

    public BytesRef getMaxTerm() {
        return maxTerm;
    }

    public long getNumTerms() {
        return numTerms;
    }

    public long getSumTotalTermFreq() {
        return sumTotalTermFreq;
    }

    public long getSumDocFreq() {
        return sumDocFreq;
    }

    public int getCardinality() {
        return cardinality;
    }
}
