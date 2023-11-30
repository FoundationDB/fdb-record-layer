/*
 * LuceneLiveDocsFormat.java
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

import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Collection;

/**
 *  Lazy Reads the LiveDocsFormat to limit the amount of bytes returned
 *  from FDB.
 *
 */
public class LuceneOptimizedLiveDocsFormat extends LiveDocsFormat {

    private LiveDocsFormat liveDocsFormat;

    public LuceneOptimizedLiveDocsFormat(LiveDocsFormat liveDocsFormat) {
        this.liveDocsFormat = liveDocsFormat;
    }

    @Override
    public Bits readLiveDocs(final Directory dir, final SegmentCommitInfo info, final IOContext context) throws IOException {
        return new LazyBits(dir, info, context);
    }

    @Override
    public void writeLiveDocs(final Bits bits, final Directory dir, final SegmentCommitInfo info, final int newDelCount, final IOContext context) throws IOException {
        liveDocsFormat.writeLiveDocs(bits, dir, info, newDelCount, context);
    }

    @Override
    public void files(final SegmentCommitInfo info, final Collection<String> files) throws IOException {
        liveDocsFormat.files(info, files);
    }

    private class LazyBits implements Bits {

        private final LazyOpener<Bits> bits;

        public LazyBits(final Directory dir, final SegmentCommitInfo info, final IOContext context) {
            bits = LazyOpener.supply(() -> liveDocsFormat.readLiveDocs(dir, info, context));
        }

        @Override
        public boolean get(final int index) {
            return bits.getUnchecked().get(index);
        }

        @Override
        public int length() {
            return bits.getUnchecked().length();
        }
    }

}
