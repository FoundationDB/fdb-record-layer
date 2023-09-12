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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneFieldInfosProto;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LuceneOptimizedFieldInfosFormat extends FieldInfosFormat {
    public static final String EXTENSION = "fip";

    @Override
    public FieldInfos read(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix, final IOContext iocontext) throws IOException {
        final String segmentName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION);
        try (IndexInput indexInput = directory.openInput(segmentName, iocontext)) {
            final long id = indexInput.readLong();
            final byte[] rawBytes = getFdbDirectory(directory).readFieldInfo(id);
            if (rawBytes == null) {
                throw new FileNotFoundException("Could not find field info");
            }
            final LuceneFieldInfosProto.FieldInfos.Builder protobuf = LuceneFieldInfosProto.FieldInfos.newBuilder().mergeFrom(rawBytes);
            FieldInfo[] fieldInfos = new FieldInfo[protobuf.getFieldInfoCount()];
            int i = 0;
            for (final LuceneFieldInfosProto.FieldInfo fieldInfo : protobuf.getFieldInfoList()) {
                fieldInfos[i] = new FieldInfo(fieldInfo.getName(),
                        fieldInfo.getNumber(),
                        fieldInfo.getStoreTermVectors(),
                        fieldInfo.getOmitsNorms(),
                        fieldInfo.getStorePayloads(),
                        protoToLucene(fieldInfo.getIndexOptions()),
                        protoToLucene(fieldInfo.getDocValues()),
                        fieldInfo.getDocValuesGen(),
                        protoToLucene(fieldInfo.getAttributesList()),
                        fieldInfo.getPointDimensionCount(),
                        fieldInfo.getPointIndexDimensionCount(),
                        fieldInfo.getPointNumBytes(),
                        fieldInfo.getSoftDeletesField());
                i++;
            }
            return new FieldInfos(fieldInfos);
        }
    }

    @Override
    public void write(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix, final FieldInfos infos, final IOContext context) throws IOException {
        final LuceneFieldInfosProto.FieldInfos.Builder protobuf = LuceneFieldInfosProto.FieldInfos.newBuilder();
        for (final FieldInfo fieldInfo : infos) {
            final LuceneFieldInfosProto.FieldInfo.Builder builder = protobuf.addFieldInfoBuilder()
                    .setName(fieldInfo.name)
                    .setNumber(fieldInfo.number)
                    .setStoreTermVectors(fieldInfo.hasVectors())
                    .setOmitsNorms(fieldInfo.omitsNorms())
                    .setStorePayloads(fieldInfo.hasPayloads())
                    .setIndexOptions(luceneToProto(fieldInfo.getIndexOptions()))
                    .setDocValues(luceneToProto(fieldInfo.getDocValuesType()))
                    .setDocValuesGen(fieldInfo.getDocValuesGen())
                    .setPointDimensionCount(fieldInfo.getPointDimensionCount())
                    .setPointIndexDimensionCount(fieldInfo.getPointIndexDimensionCount())
                    .setPointNumBytes(fieldInfo.getPointNumBytes())
                    .setSoftDeletesField(fieldInfo.isSoftDeletesField());
            for (final Map.Entry<String, String> attribute : fieldInfo.attributes().entrySet()) {
                builder.addAttributesBuilder()
                        .setKey(attribute.getKey())
                        .setValue(attribute.getValue());
            }
        }
        try (IndexOutput indexOutput = directory.createOutput(IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION), context)) {
            long id = getFdbDirectory(directory).writeFieldInfo(protobuf.build().toByteArray());
            indexOutput.writeLong(id);
        }
    }

    @Nonnull
    private static FDBDirectory getFdbDirectory(final Directory directory) {
        final Directory unwrapped = FilterDirectory.unwrap(directory);
        if (unwrapped instanceof FDBDirectory) {
            return (FDBDirectory)unwrapped;
        }
        if (unwrapped instanceof LuceneOptimizedCompoundReader) {
            return (FDBDirectory)FilterDirectory.unwrap(((LuceneOptimizedCompoundReader)unwrapped).getDirectory());
        }
        throw new RecordCoreException("Unexpected type of directory")
                .addLogInfo(LuceneLogMessageKeys.NAME, unwrapped.getClass().getSimpleName());
    }

    private Map<String, String> protoToLucene(final List<LuceneFieldInfosProto.Attribute> attributesList) {
        return attributesList.stream().collect(Collectors.toMap(
                LuceneFieldInfosProto.Attribute::getKey,
                LuceneFieldInfosProto.Attribute::getValue));
    }

    private DocValuesType protoToLucene(final LuceneFieldInfosProto.DocValues docValues) {
        switch (docValues) {
        case NO_DOC_VALUES:
            return DocValuesType.NONE;
        case NUMERIC:
            return DocValuesType.NUMERIC;
        case BINARY:
            return DocValuesType.BINARY;
        case SORTED:
            return DocValuesType.SORTED;
        case SORTED_SET:
            return DocValuesType.SORTED_SET;
        case SORTED_NUMERIC:
            return DocValuesType.SORTED_NUMERIC;
        default:
            throw new RecordCoreException("Unexpected enum value")
                    .addLogInfo(LuceneLogMessageKeys.NAME, docValues);
        }
    }

    private LuceneFieldInfosProto.DocValues luceneToProto(final DocValuesType docValuesType) {
        switch (docValuesType) {
        case NONE:
            return LuceneFieldInfosProto.DocValues.NO_DOC_VALUES;
        case NUMERIC:
            return LuceneFieldInfosProto.DocValues.NUMERIC;
        case BINARY:
            return LuceneFieldInfosProto.DocValues.BINARY;
        case SORTED:
            return LuceneFieldInfosProto.DocValues.SORTED;
        case SORTED_NUMERIC:
            return LuceneFieldInfosProto.DocValues.SORTED_NUMERIC;
        case SORTED_SET:
            return LuceneFieldInfosProto.DocValues.SORTED_SET;
        default:
            throw new RecordCoreException("Unexpected enum value")
                    .addLogInfo(LuceneLogMessageKeys.NAME, docValuesType);
        }
    }

    private IndexOptions protoToLucene(final LuceneFieldInfosProto.IndexOptions indexOptions) {
        switch (indexOptions) {
        case NO_INDEX_OPTIONS:
            return IndexOptions.NONE;
        case DOCS:
            return IndexOptions.DOCS;
        case DOCS_AND_FREQS:
            return IndexOptions.DOCS_AND_FREQS;
        case DOCS_AND_FREQS_AND_POSITIONS:
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
            return IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        default:
            throw new RecordCoreException("Unexpected enum value")
                    .addLogInfo(LuceneLogMessageKeys.NAME, indexOptions);
        }
    }


    private LuceneFieldInfosProto.IndexOptions luceneToProto(final IndexOptions indexOptions) {
        switch (indexOptions) {
        case NONE:
            return LuceneFieldInfosProto.IndexOptions.NO_INDEX_OPTIONS;
        case DOCS:
            return LuceneFieldInfosProto.IndexOptions.DOCS;
        case DOCS_AND_FREQS:
            return LuceneFieldInfosProto.IndexOptions.DOCS_AND_FREQS;
        case DOCS_AND_FREQS_AND_POSITIONS:
            return LuceneFieldInfosProto.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
        case DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS:
            return LuceneFieldInfosProto.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
        default:
            throw new RecordCoreException("Unexpected enum value")
                    .addLogInfo(LuceneLogMessageKeys.NAME, indexOptions);
        }
    }
}
