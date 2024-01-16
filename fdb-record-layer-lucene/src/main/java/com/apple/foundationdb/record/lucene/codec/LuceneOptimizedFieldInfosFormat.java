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
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryUtils;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import com.apple.foundationdb.record.lucene.directory.FieldInfosStorage;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link FieldInfosFormat} optimized for storage in the {@link FDBDirectory}.
 * <p>
 *     The key feature here is that it will store on the file reference a reference to a (potentially) shared protobuf
 *     for the FieldInfos, along with a bitset for the subset of the fields in that shared proto that are used in the
 *     associated segment. This deduplication is important because segments generally have the same mapping (there is a
 *     global mapping used when creating new segments), but we also need to support different segments having
 *     incompatible mappings. See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2284">Issue #2284</a>
 *     for more information about why we need to support incompatible mappings.
 * </p>
 */
public class LuceneOptimizedFieldInfosFormat extends FieldInfosFormat {

    public static final String EXTENSION = "fip";

    @Override
    @SuppressWarnings("PMD.CloseResource") // we extract the FDBDirectory, and that is closeable, but we aren't in charge of closing
    public FieldInfos read(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix, final IOContext iocontext) throws IOException {
        final String fileName = getFileName(directory, segmentInfo, segmentSuffix);
        return read(directory, fileName);
    }

    @VisibleForTesting
    @Nonnull
    public FieldInfos read(final Directory directory, final String fileName) throws IOException {
        final FieldInfosStorage fieldInfosStorage = FDBDirectoryUtils.getFDBDirectory(directory).getFieldInfosStorage();
        final FDBLuceneFileReference fileReference = fieldInfosStorage.getFDBLuceneFileReference(fileName);
        long id = fileReference.getFieldInfosId();
        final ByteString bitSetBytes = fileReference.getFieldInfosBitSet();
        if (bitSetBytes.isEmpty()) {
            return new FieldInfos(new FieldInfo[0]);
        }
        // the bitSet here indicates which field numbers from the protobuf are actually used for this segment.
        // There may be other fields in the protobuf that are not used in this segment
        BitSet bitSet = BitSet.valueOf(bitSetBytes.toByteArray());
        final LuceneFieldInfosProto.FieldInfos protobuf = fieldInfosStorage.readFieldInfos(id);
        List<FieldInfo> fieldInfos = new ArrayList<>();
        for (final LuceneFieldInfosProto.FieldInfo fieldInfo : protobuf.getFieldInfoList()) {
            if (bitSet.get(fieldInfo.getNumber())) {
                // this field is relevant for this segment, so add it to the fieldInfos
                fieldInfos.add(new FieldInfo(fieldInfo.getName(),
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
                        fieldInfo.getSoftDeletesField()));
            }
            // else field number is not used in the segment
            // Note: the field could have been added *after* this segment was created, or it could be that the segment
            // just didn't have any documents with that field.
        }
        return new FieldInfos(fieldInfos.toArray(new FieldInfo[0]));
    }

    @Override
    public void write(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix, final FieldInfos infos, final IOContext context) throws IOException {
        final String fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION);
        // create the output so that we create the file reference, and so that it is correctly tracked in the segment
        // info
        directory.createOutput(fileName, context).close();
        write(directory, infos, fileName);
    }

    @VisibleForTesting
    public void write(final Directory directory, final FieldInfos infos, final String fileName) throws IOException {
        // Bitset to track what fields in the global info we are using
        // we still save this even if not reusing global, in case we later reuse field infos other than the global one
        final BitSet bitSet = new BitSet();
        final LuceneFieldInfosProto.FieldInfos.Builder protobuf = luceneToProto(infos, bitSet);
        final FieldInfosStorage fieldInfosStorage = FDBDirectoryUtils.getFDBDirectory(directory).getFieldInfosStorage();
        LuceneFieldInfosProto.FieldInfos globalFieldInfos = fieldInfosStorage.readGlobalFieldInfos();
        boolean globalNeedsUpdating = false;
        boolean canReuseGlobal = true;
        if (globalFieldInfos == null) {
            globalNeedsUpdating = true;
            globalFieldInfos = protobuf.build();
        } else {
            final LuceneFieldInfosProto.FieldInfos.Builder globalFieldInfosBuilder = globalFieldInfos.toBuilder();
            final Map<Integer, LuceneFieldInfosProto.FieldInfo> globalFieldInfo = globalFieldInfosBuilder.getFieldInfoList().stream()
                    .collect(Collectors.toMap(LuceneFieldInfosProto.FieldInfo::getNumber, Function.identity()));
            for (final LuceneFieldInfosProto.FieldInfo fieldInfo : protobuf.getFieldInfoList()) {
                final LuceneFieldInfosProto.FieldInfo globalVersion = globalFieldInfo.get(fieldInfo.getNumber());
                if (globalVersion == null) {
                    // new field ... we can add it
                    globalFieldInfosBuilder.addFieldInfo(fieldInfo);
                    globalNeedsUpdating = true;
                } else {
                    if (!globalVersion.equals(fieldInfo)) {
                        canReuseGlobal = false;
                        break;
                    } // The field is already in the global, and we can continue to reuse
                }
            }
            if (canReuseGlobal && globalNeedsUpdating) {
                globalFieldInfos = globalFieldInfosBuilder.build();
            }
        }
        final long id;
        if (!canReuseGlobal) {
            // there was some incompatible field, we can't reuse the global FieldInfos
            id = fieldInfosStorage.writeFieldInfos(protobuf.build());
        } else {
            // we can reuse the global FieldInfos for this segment
            id = FieldInfosStorage.GLOBAL_FIELD_INFOS_ID;
            if (globalNeedsUpdating) {
                // the current global FieldInfos on disk did not have all the fields, so we need to update with the
                // added fields
                // Note: it would probably be harmless if we did serialize it, because this is just adding new fields,
                // but it could cause other, future segments to not be able to reuse the global FieldInfos
                fieldInfosStorage.updateGlobalFieldInfos(globalFieldInfos);
            }
        }
        fieldInfosStorage.setFieldInfoId(fileName, id, bitSet);
    }

    private Map<String, String> protoToLucene(final List<LuceneFieldInfosProto.Attribute> attributesList) {
        return attributesList.stream().collect(Collectors.toUnmodifiableMap(
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
                throw unexpectedEnumValue(docValues);
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
                throw unexpectedEnumValue(indexOptions);
        }
    }

    private LuceneFieldInfosProto.FieldInfos.Builder luceneToProto(final FieldInfos infos, final BitSet bitSet) {
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
                // Lucene doesn't explicitly state that these can't be null, but Lucene50 and Lucene60 FieldInfosFormat
                // will throw a NPE if they are
                builder.addAttributesBuilder()
                        .setKey(Objects.requireNonNull(attribute.getKey(), "FieldInfo attribute key"))
                        .setValue(Objects.requireNonNull(attribute.getValue(), "FieldInfo attribute value"));
            }
            bitSet.set(fieldInfo.number);
        }
        return protobuf;
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
                throw unexpectedEnumValue(docValuesType);
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
                throw unexpectedEnumValue(indexOptions);
        }
    }

    @Nonnull
    private static <T extends Enum<T>> RecordCoreException unexpectedEnumValue(final T enumValue) {
        return new RecordCoreException("Unexpected enum value")
                .addLogInfo(LuceneLogMessageKeys.NAME, enumValue);
    }

    @SuppressWarnings("PMD.CloseResource") // we are just unwrapping objects, not taking ownership
    @Nullable
    private static String getFileName(final Directory directory, final SegmentInfo segmentInfo, final String segmentSuffix) {
        String fileName;
        final Directory unwrapped = FilterDirectory.unwrap(directory);
        if (unwrapped instanceof FDBDirectory) {
            // We are reading directly from the FDBDirectory, so the fieldInfosId will be on the .fip file
            fileName = IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, EXTENSION);
        } else if (unwrapped instanceof LuceneOptimizedCompoundReader) {
            // We are reading from a compound directory, so the fieldInfosId will be on the .cfe file
            final LuceneOptimizedCompoundReader compoundReader = (LuceneOptimizedCompoundReader)unwrapped;
            fileName = compoundReader.getEntriesFileName();
        } else {
            throw new RecordCoreException("Unexpected type of directory")
                    .addLogInfo(LuceneLogMessageKeys.NAME, unwrapped.getClass().getSimpleName());
        }
        return fileName;
    }
}
