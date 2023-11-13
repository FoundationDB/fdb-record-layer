/*
 * LuceneOptimizedFieldInfosFormatTest.java
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


import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Low-level test for {@link LuceneOptimizedFieldInfosFormatTest}.
 */
@Tag(Tags.RequiresFDB)
class LuceneOptimizedFieldInfosFormatTest extends FDBRecordStoreTestBase {

    final LuceneOptimizedFieldInfosFormat format = new LuceneOptimizedFieldInfosFormat();

    @ParameterizedTest
    @BooleanSource
    void simpleTest(boolean oneTransaction) throws Exception {
        var fieldInfos = singleFieldInfos("foo", 0);
        final var segment = new LightSegmentInfo();
        sameOrMultiTransaction(oneTransaction,
                directory -> write(directory, segment, fieldInfos),
                directory -> assertFieldInfosEqual(fieldInfos, read(directory, segment)));
    }

    @ParameterizedTest
    @BooleanSource
    void emptyDoc(boolean oneTransaction) throws Exception {
        var fieldInfos = new FieldInfos(new FieldInfo[0]);
        final var segment = new LightSegmentInfo();
        sameOrMultiTransaction(oneTransaction,
                directory -> write(directory, segment, fieldInfos),
                directory -> assertFieldInfosEqual(fieldInfos, read(directory, segment)));
    }

    @ParameterizedTest
    @BooleanSource
    void emptyDocInNextSegment(boolean oneTransaction) throws Exception {
        final FieldInfo fieldInfo1 = simpleFieldInfo("foo", 0);
        FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] { fieldInfo1 });
        FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] { });

        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2);
    }

    @ParameterizedTest
    @BooleanSource
    void addFieldInNextSegment(boolean oneTransaction) throws Exception {
        FieldInfo fieldInfo1 = simpleFieldInfo("foo", 0);
        FieldInfo fieldInfo2 = simpleFieldInfo("bar", 1);
        FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] { fieldInfo1 });
        FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] { fieldInfo1, fieldInfo2 });

        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2);
    }

    @ParameterizedTest
    @BooleanSource
    void fewerFieldsInNextSegment(boolean oneTransaction) throws Exception {
        FieldInfo fieldInfo1 = simpleFieldInfo("foo", 0);
        FieldInfo fieldInfo2 = simpleFieldInfo("bar", 1);
        FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] { fieldInfo1, fieldInfo2 });
        FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] { fieldInfo1 });

        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2);
    }

    @ParameterizedTest
    @BooleanSource
    void fewerFieldsInNextSegment2(boolean oneTransaction) throws Exception {
        FieldInfo fieldInfo1 = simpleFieldInfo("foo", 0);
        FieldInfo fieldInfo2 = simpleFieldInfo("bar", 1);
        FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] { fieldInfo1, fieldInfo2 });
        FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] { fieldInfo2 });

        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2);
    }

    /**
     * See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2284">Issue #2284</a>.
     */
    @ParameterizedTest
    @BooleanSource
    void mixedFieldNumbers(boolean oneTransaction) throws Exception {
        testWithTwoSegments(oneTransaction,
                singleFieldInfos("foo", 0),
                singleFieldInfos("foo", 1));
    }

    /**
     * See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2284">Issue #2284</a>.
     */
    @ParameterizedTest
    @BooleanSource
    void mixedFieldNames(boolean oneTransaction) throws Exception {
        testWithTwoSegments(oneTransaction,
                singleFieldInfos("foo", 0),
                singleFieldInfos("bar", 0));
    }

    /**
     * See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2284">Issue #2284</a>.
     */
    @ParameterizedTest
    @BooleanSource
    void mixedFieldNamesAndNumbers(boolean oneTransaction) throws Exception {
        final FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] {
                simpleFieldInfo("foo", 0),
                simpleFieldInfo("bar", 1)
        });
        final FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] {
                simpleFieldInfo("bar", 0),
                simpleFieldInfo("foo", 1)
        });
        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2);
    }

    @ParameterizedTest
    @BooleanSource
    void serializeAll(final boolean oneTransaction) throws Exception {
        final var fieldInfos = Stream.of(
                        trueOrFalse().map(storeTermVector -> fieldInfoA(storeTermVector, false, false)),
                        trueOrFalse().map(omitNorms -> fieldInfoA(false, omitNorms, false)),
                        trueOrFalse().map(storePayloads -> fieldInfoA(false, false, storePayloads)),
                        Arrays.stream(IndexOptions.values()).map(indexOptions ->
                                fieldInfoB(indexOptions, DocValuesType.NUMERIC, 1, Map.of())),
                        Arrays.stream(DocValuesType.values()).map(docValuesType ->
                                fieldInfoB(IndexOptions.DOCS, docValuesType, -1, Map.of())),
                        Stream.of(0, 1, 2).map(dvGen -> fieldInfoB(IndexOptions.DOCS, DocValuesType.NUMERIC, dvGen, Map.of())),
                        someAttributes().map(attributes -> fieldInfoB(IndexOptions.DOCS, DocValuesType.NUMERIC, 1, attributes)),
                        // There are a bunch of constraints around these being all 0 or not 0 (more-or-less). All 0 is heavily
                        // tested by everything else, so just test non-0 values here.
                        Stream.of(2, 3).map(pointDimensionCount -> fieldInfoC(pointDimensionCount, 1, 1, false)),
                        Stream.of(2, 3).map(pointIndexDimensionCount -> fieldInfoC(1, pointIndexDimensionCount, 1, false)),
                        Stream.of(2, 3).map(pointNumBytes -> fieldInfoC(1, 1, pointNumBytes, false)),
                        trueOrFalse().map(softDeletesField -> fieldInfoC(1, 1, 1, softDeletesField)))
                .flatMap(stream -> stream)
                .map(fieldInfo -> Pair.of(new LightSegmentInfo(), fieldInfo)).collect(Collectors.toList());
        sameOrMultiTransaction(oneTransaction,
                directory -> {
                    for (Pair<LightSegmentInfo, FieldInfo> pair : fieldInfos) {
                        write(directory, pair.getLeft(), new FieldInfos(new FieldInfo[] {pair.getRight()}));
                    }
                },
                directory -> {
                    for (Pair<LightSegmentInfo, FieldInfo> pair : fieldInfos) {
                        assertFieldInfosEqual(new FieldInfos(new FieldInfo[] { pair.getRight() }),
                                read(directory, pair.getLeft()));
                    }
                });
    }

    private Stream<Boolean> trueOrFalse() {
        return Stream.of(true, false);
    }

    private Stream<Map<String, String>> someAttributes() {
        return Stream.of(Map.of(), Map.of("box", "car"), Map.of("run", "far", "walk", "slow"));
    }

    @Nonnull
    private static FieldInfos singleFieldInfos(final String name, final int number) {
        return new FieldInfos(new FieldInfo[] {simpleFieldInfo(name, number)});
    }

    private void testWithTwoSegments(final boolean oneTransaction,
                                     final FieldInfos fieldInfos1,
                                     final FieldInfos fieldInfos2) throws Exception {
        final var segment1 = new LightSegmentInfo();
        final var segment2 = new LightSegmentInfo();

        sameOrMultiTransaction(oneTransaction,
                directory -> {
                    write(directory, segment1, fieldInfos1);
                    write(directory, segment2, fieldInfos2);
                },
                directory -> {
                    assertFieldInfosEqual(fieldInfos1, read(directory, segment1));
                    assertFieldInfosEqual(fieldInfos2, read(directory, segment2));
                });
    }

    private void sameOrMultiTransaction(boolean oneTransaction,
                                        TestHelpers.DangerousConsumer<FDBDirectory> setup,
                                        TestHelpers.DangerousConsumer<FDBDirectory> assertions) throws Exception {
        if (oneTransaction) {
            try (FDBRecordContext context = openContext()) {
                final FDBDirectory directory = createDirectory(context);
                setup.accept(directory);
                assertions.accept(directory);
            }
        } else {
            try (FDBRecordContext context = openContext()) {
                final FDBDirectory directory = createDirectory(context);
                setup.accept(directory);
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                final FDBDirectory directory = createDirectory(context);
                assertions.accept(directory);
            }
        }
    }

    private FieldInfos read(final FDBDirectory directory, final LightSegmentInfo segment) throws IOException {
        return format.read(directory, segment.create(directory), "", IOContext.DEFAULT);
    }

    private void write(final FDBDirectory directory, final LightSegmentInfo segment, final FieldInfos fieldInfos) throws IOException {
        format.write(directory, segment.create(directory), "", fieldInfos, IOContext.DEFAULT);
    }

    @Nonnull
    private static FieldInfo fieldInfoA(final boolean storeTermVector, final boolean omitNorms, final boolean storePayloads) {
        // Note: FieldInfo will ignore all these booleans if IndexOptions == NONE
        return new FieldInfo("aField", 1, storeTermVector, omitNorms, storePayloads,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, 1, Map.of(),
                0, 0, 0, false);
    }

    @Nonnull
    private static FieldInfo fieldInfoB(final IndexOptions indexOptions, final DocValuesType docValuesType, final int dvGen, final Map<String, String> attributes) {
        return new FieldInfo("aField", 1, false, false, false,
                indexOptions, docValuesType, dvGen, attributes,
                0, 0, 0, false);
    }

    @Nonnull
    private static FieldInfo fieldInfoC(final int pointDimensionCount, final int pointIndexDimensionCount, final int pointNumBytes, final boolean softDeletesField) {
        return new FieldInfo("aField", 1, false, false, false,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, 1, Map.of(),
                pointDimensionCount, pointIndexDimensionCount, pointNumBytes, softDeletesField);
    }

    @Nonnull
    private static FieldInfo simpleFieldInfo(final String name, final int number) {
        return new FieldInfo(name, number, false, false, false,
                IndexOptions.DOCS, DocValuesType.NUMERIC, 1, Map.of(), 0, 0, 0, false);
    }

    private void assertFieldInfosEqual(final FieldInfos expectedFieldInfos, final FieldInfos actualFieldInfos) {
        assertThat(actualFieldInfos.size(), Matchers.equalTo(expectedFieldInfos.size()));
        final Iterator<FieldInfo> expectedIterator = expectedFieldInfos.iterator();
        final Iterator<FieldInfo> actualIterator = actualFieldInfos.iterator();

        while (expectedIterator.hasNext() && actualIterator.hasNext()) {
            final FieldInfo expected = expectedIterator.next();
            final FieldInfo actual = actualIterator.next();
            assertEquals(expected.number, actual.number);
            assertEquals(expected.name, actual.name);
            assertEquals(expected.omitsNorms(), actual.omitsNorms());
            assertEquals(expected.hasVectors(), actual.hasVectors());
            assertEquals(expected.hasPayloads(), actual.hasPayloads());
            assertEquals(expected.getIndexOptions(), actual.getIndexOptions());
            assertEquals(expected.getDocValuesType(), actual.getDocValuesType());
            assertEquals(expected.getDocValuesGen(), actual.getDocValuesGen());
            assertEquals(expected.getPointDimensionCount(), actual.getPointDimensionCount());
            assertEquals(expected.getPointIndexDimensionCount(), actual.getPointIndexDimensionCount());
            assertEquals(expected.getPointNumBytes(), actual.getPointNumBytes());
            assertEquals(expected.isSoftDeletesField(), actual.isSoftDeletesField());
            assertEquals(expected.attributes(), actual.attributes());
        }
        Assertions.assertFalse(expectedIterator.hasNext());
        Assertions.assertFalse(actualIterator.hasNext());
    }

    private FDBDirectory createDirectory(final FDBRecordContext context) {
        return new FDBDirectory(path.toSubspace(context), context, true);
    }

    private static class LightSegmentInfo {

        private static int segmentCount = 0;
        final String name;
        final int maxDoc;
        final byte[] id;

        public LightSegmentInfo() {
            name = "_" + Long.toString(segmentCount++, Character.MAX_RADIX);
            maxDoc = 1;
            id = StringHelper.randomId();
        }

        public SegmentInfo create(final Directory directory) {
            return new SegmentInfo(directory, Version.LATEST, Version.LATEST,
                    name,
                    maxDoc,
                    false,
                    LuceneOptimizedCodec.CODEC,
                    Map.of(),
                    id,
                    Map.of(),
                    null);
        }
    }
}
