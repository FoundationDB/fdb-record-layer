/*
 * TestingCodec.java
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
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import com.google.auto.service.AutoService;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Wrapper around {@link LuceneOptimizedCodec} to better support tests provided by lucene.
 * <p>
 *     This adds some switches to change the behavior of the codec to work around some characteristics of the Lucene
 *     test framework that we consider to be irrelevant to the actual usages.
 * </p>
 */
@AutoService(Codec.class)
public class TestingCodec extends Codec {
    private final LuceneOptimizedCodec underlying;
    private static boolean disableLaziness;
    private static boolean disableLazinessForLiveDocs;
    private static boolean allowRandomCompoundFiles;

    public TestingCodec() {
        super("RLT");
        underlying = LuceneOptimizedCodec.CODEC;
    }

    /**
     * Some of the tests expect to be able to continue reading from the associated readers after the underlying file
     * has been deleted; this wraps the readers and forces them to read data when constructed.
     */
    public static void disableLaziness() {
        TestingCodec.disableLaziness = true;
    }

    /**
     * Some of the tests expect to be able to continue reading from the associated readers after the underlying file
     * has been deleted; this wraps the readers and forces the live docs to be read data when constructed.
     */
    public static void disableLazinessForLiveDocs() {
        TestingCodec.disableLazinessForLiveDocs = true;
    }

    /**
     * The tests for the compound format don't necessarily create segments in the way we expect them, so there might
     * not be a FieldInfos, and the code needs to be updated to handle that; if this is set, we will create our own
     * {@link LuceneOptimizedCompoundFormat} that doesn't treat the FieldInfos as special.
     */
    public static void allowRandomCompoundFiles() {
        TestingCodec.allowRandomCompoundFiles = true;
    }

    /**
     * Reset all of the static configs back to the default state.
     */
    public static void reset() {
        disableLaziness = false;
        disableLazinessForLiveDocs = false;
        allowRandomCompoundFiles = false;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return underlying.postingsFormat();
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        final DocValuesFormat underlyingDocValuesFormat = underlying.docValuesFormat();
        if (disableLaziness) {
            return new DocValuesFormat(underlyingDocValuesFormat.getName()) {
                @Override
                public DocValuesConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
                    return underlyingDocValuesFormat.fieldsConsumer(state);
                }

                @Override
                public DocValuesProducer fieldsProducer(final SegmentReadState state) throws IOException {
                    final DocValuesProducer underlyingProducer = underlyingDocValuesFormat.fieldsProducer(state);
                    underlyingProducer.ramBytesUsed();
                    return underlyingProducer;
                }
            };
        } else {
            return underlyingDocValuesFormat;
        }
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        final StoredFieldsFormat storedFieldsFormat = underlying.storedFieldsFormat();
        if (disableLaziness) {
            return new StoredFieldsFormat() {
                @Override
                public StoredFieldsReader fieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fn, final IOContext context) throws IOException {
                    final StoredFieldsReader storedFieldsReader = storedFieldsFormat.fieldsReader(directory, si, fn, context);
                    storedFieldsReader.ramBytesUsed();
                    return storedFieldsReader;
                }

                @Override
                public StoredFieldsWriter fieldsWriter(final Directory directory, final SegmentInfo si, final IOContext context) throws IOException {
                    return storedFieldsFormat.fieldsWriter(directory, si, context);
                }
            };
        } else {
            return storedFieldsFormat;
        }
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return underlying.termVectorsFormat();
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return underlying.fieldInfosFormat();
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return underlying.segmentInfoFormat();
    }

    @Override
    public NormsFormat normsFormat() {
        return underlying.normsFormat();
    }

    @Override
    public LiveDocsFormat liveDocsFormat() {
        final LiveDocsFormat liveDocsFormat = underlying.liveDocsFormat();
        if (disableLazinessForLiveDocs) {
            return new LiveDocsFormat() {
                @Override
                public Bits readLiveDocs(final Directory dir, final SegmentCommitInfo info, final IOContext context) throws IOException {
                    final Bits bits = liveDocsFormat.readLiveDocs(dir, info, context);
                    try {
                        bits.length();
                    } catch (UncheckedIOException e) {
                        throw e.getCause();
                    }
                    return bits;
                }

                @Override
                public void writeLiveDocs(final Bits bits, final Directory dir, final SegmentCommitInfo info, final int newDelCount, final IOContext context) throws IOException {
                    liveDocsFormat.writeLiveDocs(bits, dir, info, newDelCount, context);
                }

                @Override
                public void files(final SegmentCommitInfo info, final Collection<String> files) throws IOException {
                    liveDocsFormat.files(info, files);
                }
            };
        }
        return liveDocsFormat;
    }

    @Override
    public CompoundFormat compoundFormat() {
        if (allowRandomCompoundFiles) {
            return new LuceneOptimizedCompoundFormat(((LuceneOptimizedCompoundFormat)underlying.compoundFormat()).underlying) {
                @Override
                protected void copyFieldInfos(final SegmentInfo si, final Set<String> filesForAfter, final FDBDirectory directory) {
                    // copy the id, only if it's present
                    final Optional<String> fieldInfosName = filesForAfter.stream().filter(FDBDirectory::isFieldInfoFile).findFirst();
                    if (fieldInfosName.isPresent()) {
                        final String fieldInfosFileName = fieldInfosName.orElseThrow();
                        final FDBLuceneFileReference fieldInfosReference = directory.getFDBLuceneFileReference(fieldInfosFileName);
                        String entriesFile = IndexFileNames.segmentFileName(si.name, "", ENTRIES_EXTENSION);
                        directory.setFieldInfoId(entriesFile, fieldInfosReference.getFieldInfosId(), fieldInfosReference.getFieldInfosBitSet());
                    }
                }

                @Override
                protected void validateFileCounts(final Set<String> files, final int fieldInfos, final int storedFields) {
                    // assume a-ok
                }
            };
        } else {
            return underlying.compoundFormat();
        }
    }

    @Override
    public PointsFormat pointsFormat() {
        return underlying.pointsFormat();
    }
}
