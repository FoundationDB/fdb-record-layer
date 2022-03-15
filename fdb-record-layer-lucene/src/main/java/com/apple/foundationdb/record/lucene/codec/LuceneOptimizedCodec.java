/*
 * LuceneOptimizedCodec.java
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

package com.apple.foundationdb.record.lucene.codec;

import com.google.auto.service.AutoService;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene87.Lucene87Codec;

/**
 *
 * Codec with a few optimizations for speeding up compound files
 * sitting on FoundationDB.
 *
 * Optimizations:
 * - .cfe file references are not stored
 * - .cfe file data is serialized as entries on the current compound file reference (.cfs)
 * - .si file references are not stored
 * - .si file data is serialized as segmentInfo on the current compound file reference (.cfs)
 * - .si diagnostic information is not stored
 * - Removed checksum validation on Compound File Reader
 *
 * Forwards/backwards compatibility is not supported during transition from the {@link Lucene87Codec} to this implementation unfortunately.
 * Indexes need to rebuilt before handling search requests.
 *
 */
@AutoService(Codec.class)
public class LuceneOptimizedCodec extends Codec {

    private final Lucene87Codec baseCodec;
    private final LuceneOptimizedCompoundFormat compoundFormat;
    private final LuceneOptimizedSegmentInfoFormat segmentInfoFormat;

    /**
     * Instantiates a new codec.
     */
    public LuceneOptimizedCodec() {
        this(Lucene87Codec.Mode.BEST_SPEED);
    }

    /**
     * Instantiates a new codec, specifying the stored fields compression
     * mode to use.
     * The constant "RL" is an arbitrary name for the codec that will be written into the index segment.
     * @param mode stored fields compression mode to use for newly
     *             flushed/merged segments.
     */
    public LuceneOptimizedCodec(Lucene87Codec.Mode mode) {
        super("RL");
        baseCodec = new Lucene87Codec(mode);
        compoundFormat = new LuceneOptimizedCompoundFormat();
        segmentInfoFormat = new LuceneOptimizedSegmentInfoFormat();
    }


    @Override
    public PostingsFormat postingsFormat() {
        return baseCodec.postingsFormat();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return baseCodec.docValuesFormat();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return baseCodec.storedFieldsFormat();
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return baseCodec.termVectorsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return baseCodec.fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return segmentInfoFormat;
    }

    @Override
    public NormsFormat normsFormat() {
        return baseCodec.normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        return baseCodec.liveDocsFormat();
    }

    @Override
    public CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        return baseCodec.pointsFormat();
    }
}
