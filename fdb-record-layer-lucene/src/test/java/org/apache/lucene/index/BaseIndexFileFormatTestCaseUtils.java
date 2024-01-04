/*
 * BaseIndexFileFormatTestCaseUtils.java
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

package org.apache.lucene.index;

import com.apple.foundationdb.record.lucene.codec.TestFDBDirectory;
import com.apple.foundationdb.record.lucene.codec.TestingCodec;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBTestBase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;

/**
 * Utilities for classes extending from {@link BaseIndexFileFormatTestCase}.
 */
public final class BaseIndexFileFormatTestCaseUtils {

    private BaseIndexFileFormatTestCaseUtils() {
    }

    public static boolean isUsingFDBDirectory() {
        return System.getProperty("tests.directory", "random").equals(TestFDBDirectory.class.getName());
    }

    @Nonnull
    public static Codec getCodec() {
        if (isUsingFDBDirectory()) {
            return new TestingCodec();
        } else {
            return CompressingCodec.randomInstance(new Random());
        }
    }

    public static void beforeClass() {
        // We have to manually copy these from FDBTestBase because we are a junit4 test class, thanks to Lucene,
        // but that class is JUnit4
        FDBTestBase.initFDB();
        FDBTestBase.setupBlockingInAsyncDetection();
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        factory.getDatabase();
    }

    public static void resetStaticConfigs() {
        TestingCodec.reset();
        TestFDBDirectory.reset();
    }

    /**
     * This is a direct copy of the {@link BaseIndexFileFormatTestCase#testMultiClose()}, except in that version it
     * always creates a {@link org.apache.lucene.store.FSDirectory}, whereas here, if testing against FDB we use the
     * directory under test.
     * <p>
     *     You can search for "--BEGIN CUSTOM--" and "--END CUSTOM--" in the code to see exactly which lines.
     * </p>
     * <p>
     *     Every extension of {@link BaseIndexFileFormatTestCase} needs to override this in the same way, so this has
     *     been extracted into a utils method. In order to do this, methods have been changed to refer to the
     *     {@code testCase} parameter, or to the relevant class if it's a static method
     * </p>
     *
     * @throws IOException if there's issues
     */
    public static void testMultiClose(BaseIndexFileFormatTestCase testCase) throws IOException {
        // first make a one doc index
        final Directory oneDocIndex = testCase.applyCreatedVersionMajor(LuceneTestCase.newDirectory());
        final IndexWriter iw = new IndexWriter(oneDocIndex, new IndexWriterConfig(new MockAnalyzer(LuceneTestCase.random())));
        final Document oneDoc = new Document();
        final FieldType customType = new FieldType(TextField.TYPE_STORED);
        customType.setStoreTermVectors(true);
        Field customField = new Field("field", "contents", customType);
        oneDoc.add(customField);
        oneDoc.add(new NumericDocValuesField("field", 5));
        iw.addDocument(oneDoc);
        LeafReader oneDocReader = LuceneTestCase.getOnlyLeafReader(DirectoryReader.open(iw));
        iw.close();

        // now feed to codec apis manually
        // --BEGIN CUSTOM--
        Directory dir;
        if (isUsingFDBDirectory()) {
            dir = LuceneTestCase.newDirectory();
        } else {
            // this is what the base code does
            // we use FSDir, things like ramdir are not guaranteed to cause fails if you write to them after close(), etc
            dir = LuceneTestCase.newFSDirectory(LuceneTestCase.createTempDir("justSoYouGetSomeChannelErrors"));
        }
        // --END CUSTOM--
        Codec codec = testCase.getCodec();

        SegmentInfo segmentInfo = new SegmentInfo(dir, Version.LATEST, Version.LATEST, "_0", 1, false, codec, Collections.emptyMap(), StringHelper.randomId(), Collections.emptyMap(), null);
        FieldInfo proto = oneDocReader.getFieldInfos().fieldInfo("field");
        FieldInfo field = new FieldInfo(proto.name, proto.number, proto.hasVectors(), proto.omitsNorms(), proto.hasPayloads(),
                proto.getIndexOptions(), proto.getDocValuesType(), proto.getDocValuesGen(), new HashMap<>(),
                proto.getPointDimensionCount(), proto.getPointIndexDimensionCount(), proto.getPointNumBytes(), proto.isSoftDeletesField());

        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] {field});

        SegmentWriteState writeState = new SegmentWriteState(null, dir,
                segmentInfo, fieldInfos,
                null, new IOContext(new FlushInfo(1, 20)));

        SegmentReadState readState = new SegmentReadState(dir, segmentInfo, fieldInfos, IOContext.READ);

        // PostingsFormat
        NormsProducer fakeNorms = new NormsProducer() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public NumericDocValues getNorms(FieldInfo field) throws IOException {
                if (field.hasNorms() == false) {
                    return null;
                }
                return oneDocReader.getNormValues(field.name);
            }

            @Override
            public void checkIntegrity() throws IOException {
            }

        };
        try (FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(writeState)) {
            final Fields fields = new Fields() {
                TreeSet<String> indexedFields = new TreeSet<>(FieldInfos.getIndexedFields(oneDocReader));

                @Override
                public Iterator<String> iterator() {
                    return indexedFields.iterator();
                }

                @Override
                public Terms terms(String field) throws IOException {
                    return oneDocReader.terms(field);
                }

                @Override
                public int size() {
                    return indexedFields.size();
                }
            };
            consumer.write(fields, fakeNorms);
            IOUtils.close(consumer);
            IOUtils.close(consumer);
        }
        try (FieldsProducer producer = codec.postingsFormat().fieldsProducer(readState)) {
            IOUtils.close(producer);
            IOUtils.close(producer);
        }

        // DocValuesFormat
        try (DocValuesConsumer consumer = codec.docValuesFormat().fieldsConsumer(writeState)) {
            consumer.addNumericField(field,
                    new EmptyDocValuesProducer() {
                        @Override
                        public NumericDocValues getNumeric(FieldInfo field) {
                            return new NumericDocValues() {
                                int docID = -1;

                                @Override
                                public int docID() {
                                    return docID;
                                }

                                @Override
                                public int nextDoc() {
                                    docID++;
                                    if (docID == 1) {
                                        docID = NO_MORE_DOCS;
                                    }
                                    return docID;
                                }

                                @Override
                                public int advance(int target) {
                                    if (docID <= 0 && target == 0) {
                                        docID = 0;
                                    } else {
                                        docID = NO_MORE_DOCS;
                                    }
                                    return docID;
                                }

                                @Override
                                public boolean advanceExact(int target) throws IOException {
                                    docID = target;
                                    return target == 0;
                                }

                                @Override
                                public long cost() {
                                    return 1;
                                }

                                @Override
                                public long longValue() {
                                    return 5;
                                }
                            };
                        }
                    });
            IOUtils.close(consumer);
            IOUtils.close(consumer);
        }
        try (DocValuesProducer producer = codec.docValuesFormat().fieldsProducer(readState)) {
            IOUtils.close(producer);
            IOUtils.close(producer);
        }

        // NormsFormat
        try (NormsConsumer consumer = codec.normsFormat().normsConsumer(writeState)) {
            consumer.addNormsField(field,
                    new NormsProducer() {
                        @Override
                        public NumericDocValues getNorms(FieldInfo field) {
                            return new NumericDocValues() {
                                int docID = -1;

                                @Override
                                public int docID() {
                                    return docID;
                                }

                                @Override
                                public int nextDoc() {
                                    docID++;
                                    if (docID == 1) {
                                        docID = NO_MORE_DOCS;
                                    }
                                    return docID;
                                }

                                @Override
                                public int advance(int target) {
                                    if (docID <= 0 && target == 0) {
                                        docID = 0;
                                    } else {
                                        docID = NO_MORE_DOCS;
                                    }
                                    return docID;
                                }

                                @Override
                                public boolean advanceExact(int target) throws IOException {
                                    docID = target;
                                    return target == 0;
                                }

                                @Override
                                public long cost() {
                                    return 1;
                                }

                                @Override
                                public long longValue() {
                                    return 5;
                                }
                            };
                        }

                        @Override
                        public void checkIntegrity() {
                        }

                        @Override
                        public void close() {
                        }

                        @Override
                        public long ramBytesUsed() {
                            return 0;
                        }
                    });
            IOUtils.close(consumer);
            IOUtils.close(consumer);
        }
        try (NormsProducer producer = codec.normsFormat().normsProducer(readState)) {
            IOUtils.close(producer);
            IOUtils.close(producer);
        }

        // TermVectorsFormat
        try (TermVectorsWriter consumer = codec.termVectorsFormat().vectorsWriter(dir, segmentInfo, writeState.context)) {
            consumer.startDocument(1);
            consumer.startField(field, 1, false, false, false);
            consumer.startTerm(new BytesRef("testing"), 2);
            consumer.finishTerm();
            consumer.finishField();
            consumer.finishDocument();
            consumer.finish(fieldInfos, 1);
            IOUtils.close(consumer);
            IOUtils.close(consumer);
        }
        try (TermVectorsReader producer = codec.termVectorsFormat().vectorsReader(dir, segmentInfo, fieldInfos, readState.context)) {
            IOUtils.close(producer);
            IOUtils.close(producer);
        }

        // StoredFieldsFormat
        try (StoredFieldsWriter consumer = codec.storedFieldsFormat().fieldsWriter(dir, segmentInfo, writeState.context)) {
            consumer.startDocument();
            consumer.writeField(field, customField);
            consumer.finishDocument();
            consumer.finish(fieldInfos, 1);
            IOUtils.close(consumer);
            IOUtils.close(consumer);
        }
        try (StoredFieldsReader producer = codec.storedFieldsFormat().fieldsReader(dir, segmentInfo, fieldInfos, readState.context)) {
            IOUtils.close(producer);
            IOUtils.close(producer);
        }

        IOUtils.close(oneDocReader, oneDocIndex, dir);
    }

}
