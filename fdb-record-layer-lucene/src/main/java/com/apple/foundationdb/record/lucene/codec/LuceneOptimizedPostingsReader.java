/*
 * LuceneOptimizedPostingsReader.java
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
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryUtils;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SlowImpactsEnum;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.Collection;

/**
 * FDB-optimized {@link PostingsReaderBase} modeled after {@link Lucene84PostingsReader}.
 */
public class LuceneOptimizedPostingsReader extends PostingsReaderBase {

    private String segmentName;
    private FDBDirectory directory;

    public LuceneOptimizedPostingsReader(final SegmentReadState state) {
        this.directory = FDBDirectoryUtils.getFDBDirectory(state.directory);
        this.segmentName = state.segmentInfo.name;
    }

    @Override
    public void init(final IndexInput termsIn, final SegmentReadState segmentReadState) {
    }

    @Override
    public BlockTermState newTermState() throws IOException {
        return new LuceneOptimizedBlockTermState();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public PostingsEnum postings(final FieldInfo fieldInfo, final BlockTermState state, final PostingsEnum reuse, final int flags) throws IOException {
        final boolean indexHasPositions = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        final boolean indexHasOffsets = fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        final boolean indexHasPayloads = fieldInfo.hasPayloads();

        final boolean hasPositions = indexHasPositions && PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS);
        final boolean hasOffsets = indexHasOffsets && PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS);
        final boolean hasPayloads = indexHasPayloads && PostingsEnum.featureRequested(flags, PostingsEnum.PAYLOADS);

        if (state instanceof LuceneOptimizedBlockTermState) {
            return new LuceneOptimizedPostingsEnum(segmentName, fieldInfo, (LuceneOptimizedBlockTermState)state, directory, hasPositions, hasOffsets, hasPayloads);
        } else {
            throw new IllegalStateException("Unexpected state type: " + state.getClass().getSimpleName());
        }
    }

    @Override
    public ImpactsEnum impacts(final FieldInfo fieldInfo, final BlockTermState state, final int flags) throws IOException {
        PostingsEnum postingsEnum = postings(fieldInfo, state, null, flags);
        // TODO: This can be improved
        return new SlowImpactsEnum(postingsEnum);
    }

    @Override
    public void decodeTerm(final DataInput in, final FieldInfo fieldInfo, final BlockTermState state, final boolean absolute) {
    }

    @Override
    public void checkIntegrity() throws IOException {
    }

    @Override
    public long ramBytesUsed() {
        // TODO
        return 0;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        // TODO
        return super.getChildResources();
    }
}
