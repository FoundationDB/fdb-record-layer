/*
 * LuceneOptimizedPointsFormat.java
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

import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Lazy Reads the PointsFormat to limit the amount of bytes returned
 * from FDB.
 *
 */
public class LuceneOptimizedPointsFormat extends PointsFormat {

    PointsFormat pointsFormat;

    public LuceneOptimizedPointsFormat(PointsFormat pointsFormat) {
        this.pointsFormat = pointsFormat;
    }

    @Override
    public PointsWriter fieldsWriter(final SegmentWriteState state) throws IOException {
        return pointsFormat.fieldsWriter(state);
    }

    @Override
    public PointsReader fieldsReader(final SegmentReadState state) throws IOException {
        return new LazyPointsReader(state);
    }

    private class LazyPointsReader extends PointsReader {

        private LazyCloseable<PointsReader> pointsReader;

        private LazyPointsReader(final SegmentReadState state) {
            pointsReader = LazyCloseable.supply(() -> pointsFormat.fieldsReader(state));
        }

        @Override
        public void checkIntegrity() throws IOException {
            if (LuceneOptimizedPostingsFormat.allowCheckDataIntegrity) {
                pointsReader.get().checkIntegrity();
            }
        }

        @Override
        public PointValues getValues(final String field) throws IOException {
            return pointsReader.get().getValues(field);
        }

        @Override
        public void close() throws IOException {
            pointsReader.close();
        }

        @Override
        public long ramBytesUsed() {
            return pointsReader.getUnchecked().ramBytesUsed();
        }
    }


}
