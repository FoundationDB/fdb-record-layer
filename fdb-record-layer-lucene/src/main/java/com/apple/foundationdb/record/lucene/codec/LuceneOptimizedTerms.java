/*
 * LuceneOptimizedTerms.java
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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.blocktree.FieldReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;

/**
 * FDB-optimized {@link Terms} modeled after {@link FieldReader}.
 */
public class LuceneOptimizedTerms extends Terms implements Accountable {
    private final String segmentName;
    private final FieldInfo fieldInfo;
    private final PostingsFieldMetadata metadata;
    private final FDBDirectory directory;
    private final PostingsReaderBase postingsReader;

    public LuceneOptimizedTerms(String segmentName, final FieldInfo fieldInfo, final PostingsFieldMetadata metadata, final FDBDirectory directory, final PostingsReaderBase postingsReader) {
        this.segmentName = segmentName;
        this.fieldInfo = fieldInfo;
        this.metadata = metadata;
        this.directory = directory;
        this.postingsReader = postingsReader;
    }

    @Override
    public BytesRef getMin() throws IOException {
        return metadata.getMinTerm();
    }

    @Override
    public BytesRef getMax() throws IOException {
        return metadata.getMaxTerm();
    }

    @Override
    public boolean hasFreqs() {
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    }

    @Override
    public boolean hasPositions() {
        return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
        return fieldInfo.hasPayloads();
    }

    @Override
    public TermsEnum iterator() throws IOException {
        return new LuceneOptimizedTermsEnum(segmentName, fieldInfo, directory, postingsReader);
    }

    @Override
    public long size() {
        return metadata.getNumTerms();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
        return metadata.getSumTotalTermFreq();
    }

    @Override
    public long getSumDocFreq() throws IOException {
        return metadata.getSumDocFreq();
    }

    @Override
    public int getDocCount() {
        return metadata.getCardinality();
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
        // TODO
        return super.intersect(compiled, startTerm);
    }

    @Override
    public long ramBytesUsed() {
        // TODO
        return 0;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        try {
            return getClass().getSimpleName() + "(seg=" + segmentName + " terms=" + size() + ",postings=" + getSumDocFreq() + ",positions=" + getSumTotalTermFreq() + ",docs=" + getDocCount() + ")";
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
