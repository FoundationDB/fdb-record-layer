/*
 * LuceneOptimizedFieldInfosFormat.java
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

import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * This class attempts to reduce the number of .fnm entries when numerous segments are present in FDB.
 */
public class LuceneOptimizedFieldInfosFormat extends FieldInfosFormat {
    private final FieldInfosFormat fieldInfosFormat;

    public LuceneOptimizedFieldInfosFormat(FieldInfosFormat fieldInfosFormat) {
        this.fieldInfosFormat = fieldInfosFormat;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public FieldInfos read(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix, final IOContext iocontext) throws IOException {
        // Note on caching: This (fieldInfos) were cached here as well, to reduce the costs of parsing the (cached) bytes,
        // but that resulted in a deadlock. The current implementation reads and parses the data every time.
        return fieldInfosFormat.read(
                ((LuceneOptimizedCompoundReader)directory).getDirectory(), segmentInfo, segmentSuffix, iocontext);
    }

    @Override
    public void write(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix, final FieldInfos infos, final IOContext context) throws IOException {
        fieldInfosFormat.write(new LuceneOptimizedWrappedDirectory(directory, infos), segmentInfo, segmentSuffix, infos, context);
    }

}
