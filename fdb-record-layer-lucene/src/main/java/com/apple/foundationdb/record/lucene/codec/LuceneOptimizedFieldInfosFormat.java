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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * This class attempts to reduce the number of .fnm entries when numerous segments are present in FDB.
 *
 */
public class LuceneOptimizedFieldInfosFormat extends FieldInfosFormat {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneOptimizedFieldInfosFormat.class);

    private final FieldInfosFormat fieldInfosFormat;

    public LuceneOptimizedFieldInfosFormat(FieldInfosFormat fieldInfosFormat) {
        this.fieldInfosFormat = fieldInfosFormat;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public FieldInfos read(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix, final IOContext iocontext) throws IOException {
        // UnWrap Directory in attempt to use a cache mechanism
        if (directory instanceof LuceneOptimizedCompoundReader) {
            Directory next = ((LuceneOptimizedCompoundReader)directory).getDirectory();
            if (next instanceof LuceneOptimizedWrappedDirectory) {
                FDBDirectory fdbDirectory = ((LuceneOptimizedWrappedDirectory)next).getFdbDirectory();
                String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, "fnm");
                List<Long> bitSetWords = fdbDirectory.getFDBLuceneFileReference(
                        LuceneOptimizedWrappedDirectory.convertToDataFile(fileName)).getBitSetWords();
                BitSet bitSet = BitSet.valueOf(ArrayUtils.toPrimitive(bitSetWords.toArray(new Long[0])));
                try {
                    return fdbDirectory.getFieldInfos(bitSet, () -> {
                        try {
                            return fieldInfosFormat.read(
                                    ((LuceneOptimizedCompoundReader)directory).getDirectory(),
                                    segmentInfo, segmentSuffix, iocontext);
                        } catch (IOException ioe) {
                            LOGGER.error("Failure during reading FieldInfoFormats, corrupted index");
                            throw new RuntimeException(ioe);
                        }
                    });
                } catch (ExecutionException ee) {
                    throw new IOException(ee);
                }
            }
        }
        return fieldInfosFormat.read(
                ((LuceneOptimizedCompoundReader) directory).getDirectory(),
                segmentInfo, segmentSuffix, iocontext);
    }

    @Override
    public void write(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix, final FieldInfos infos, final IOContext context) throws IOException {
        fieldInfosFormat.write(new LuceneOptimizedWrappedDirectory(directory, infos), segmentInfo, segmentSuffix, infos, context);
    }

}
