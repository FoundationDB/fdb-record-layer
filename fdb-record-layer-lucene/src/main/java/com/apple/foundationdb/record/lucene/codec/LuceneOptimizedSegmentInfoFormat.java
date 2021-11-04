/*
 * LuceneOptimizedSegmentInfoFormat.java
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

import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.lucene70.Lucene70SegmentInfoFormat;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;

public class LuceneOptimizedSegmentInfoFormat extends SegmentInfoFormat {
    private final SegmentInfoFormat segmentInfosFormat = new Lucene70SegmentInfoFormat();

    public LuceneOptimizedSegmentInfoFormat() {
    }

    @Override
    public SegmentInfo read(final Directory directory, final String segmentName, final byte[] segmentID, final IOContext context) throws IOException {
        SegmentInfo segmentInfo = segmentInfosFormat.read(new LuceneOptimizedWrappedDirectory(directory), segmentName, segmentID, context);
        SegmentInfo segmentInfoToReturn = new SegmentInfo(directory, segmentInfo.getVersion(), segmentInfo.getMinVersion(), segmentInfo.name,
                segmentInfo.maxDoc(), segmentInfo.getUseCompoundFile(), segmentInfo.getCodec(), segmentInfo.getDiagnostics(),
                segmentInfo.getId(),
                segmentInfo.getAttributes(), segmentInfo.getIndexSort());
        segmentInfoToReturn.setFiles(segmentInfo.files());
        return segmentInfoToReturn;
    }

    @Override
    public void write(final Directory dir, final SegmentInfo info, final IOContext ioContext) throws IOException {
        SegmentInfo segmentInfoToWrite = new SegmentInfo(new LuceneOptimizedWrappedDirectory(dir), info.getVersion(), info.getMinVersion(), info.name,
                info.maxDoc(), info.getUseCompoundFile(), info.getCodec(), Collections.emptyMap(),
                info.getId(),
                info.getAttributes(), info.getIndexSort());
        segmentInfoToWrite.setFiles(info.files());
        segmentInfosFormat.write(new LuceneOptimizedWrappedDirectory(dir), segmentInfoToWrite, ioContext);
        final String fileName = IndexFileNames.segmentFileName(info.name, "", Lucene70SegmentInfoFormat.SI_EXTENSION);
        info.setFiles(info.files().stream().filter(file -> !file.equals(fileName)).collect(Collectors.toSet()));
    }
}
