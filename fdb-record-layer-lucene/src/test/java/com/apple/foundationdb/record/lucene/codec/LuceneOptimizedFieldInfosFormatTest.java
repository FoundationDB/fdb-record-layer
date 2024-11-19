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
import com.apple.foundationdb.record.lucene.LuceneEvents;
import com.apple.foundationdb.record.lucene.LuceneFieldInfosProto;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FieldInfosStorage;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
    @ValueSource(ints = {0, 1, 2})
    void deleteOne(int transactionBoundary) throws Exception {
        var fieldInfos = singleFieldInfos("foo", 0);
        final var segment = new LightSegmentInfo();
        sameOrMultiTransaction(transactionBoundary,
                List.of(
                        directory -> write(directory, segment, fieldInfos),
                        directory -> directory.deleteFile(IndexFileNames.segmentFileName(segment.name, "", LuceneOptimizedFieldInfosFormat.EXTENSION)),
                        directory -> assertEquals(Map.of(), directory.getFieldInfosStorage().getAllFieldInfos())
                ));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void deleteMany(int transactionBoundary) throws Exception {
        // Create a bunch of segments all sharing the global FieldInfos, and then delete all the files, and the global
        // FieldInfos
        final int segmentCount = 4;
        final List<LightSegmentInfo> segments = IntStream.range(0, segmentCount).mapToObj(i -> new LightSegmentInfo())
                .collect(Collectors.toList());
        var fieldInfos = singleFieldInfos("foo", 0);
        sameOrMultiTransaction(transactionBoundary,
                List.of(
                        directory -> {
                            for (LightSegmentInfo segment : segments) {
                                write(directory, segment, fieldInfos);
                            }
                        },
                        directory -> {
                            assertThat(directory.getFieldInfosStorage().getAllFieldInfos().keySet(), Matchers.hasSize(1));
                            for (LightSegmentInfo segment : segments) {
                                directory.deleteFile(IndexFileNames.segmentFileName(segment.name, "", LuceneOptimizedFieldInfosFormat.EXTENSION));
                            }

                        },
                        directory -> assertEquals(Map.of(), directory.getFieldInfosStorage().getAllFieldInfos())
                ));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void deleteManyIncompatible(int transactionBoundary) throws Exception {
        // similar to deleteMany, but each segment gets its own FieldInfos
        final int segmentCount = 4;
        final List<LightSegmentInfo> segments = IntStream.range(0, segmentCount).mapToObj(i -> new LightSegmentInfo())
                .collect(Collectors.toList());
        sameOrMultiTransaction(transactionBoundary,
                List.of(
                        directory -> {
                            int fieldInfoNumber = 0;
                            for (LightSegmentInfo segment : segments) {
                                write(directory, segment, singleFieldInfos("foo" + fieldInfoNumber, 0));
                                fieldInfoNumber++;
                            }
                        },
                        directory -> {
                            assertThat(directory.getFieldInfosStorage().getAllFieldInfos().keySet(), Matchers.hasSize(segmentCount));
                            for (LightSegmentInfo segment : segments) {
                                directory.deleteFile(IndexFileNames.segmentFileName(segment.name, "", LuceneOptimizedFieldInfosFormat.EXTENSION));
                            }
                        },
                        directory -> assertEquals(Map.of(), directory.getFieldInfosStorage().getAllFieldInfos())
                ));
    }

    public static Stream<Arguments> deleteSome() {
        return IntStream.range(0, 4).boxed().flatMap(transactionBoundary ->
                IntStream.range(transactionBoundary - 1, 5).mapToObj(toDelete -> Arguments.of(transactionBoundary, toDelete)));
    }

    @ParameterizedTest
    @MethodSource
    void deleteSome(int transactionBoundary, int toDelete) throws Exception {
        final int segmentCount = 6;
        assertThat(segmentCount, Matchers.greaterThan(toDelete));
        final List<LightSegmentInfo> segments = IntStream.range(0, segmentCount).mapToObj(i -> new LightSegmentInfo())
                .collect(Collectors.toList());
        var fieldInfos = singleFieldInfos("foo", 0);

        sameOrMultiTransaction(transactionBoundary,
                Stream.concat(
                        Stream.of( // setup
                                directory -> {
                                    for (LightSegmentInfo segment : segments) {
                                        write(directory, segment, fieldInfos);
                                    }
                                }
                        ),
                        // delete and validate, some number will go in first transaction, some in the second
                        IntStream.range(0, toDelete).mapToObj(i -> (TestHelpers.DangerousConsumer<FDBDirectory>)directory -> {
                            directory.deleteFile(IndexFileNames.segmentFileName(segments.get(i).name, "", LuceneOptimizedFieldInfosFormat.EXTENSION));
                            Assertions.assertEquals(Set.of(FieldInfosStorage.GLOBAL_FIELD_INFOS_ID), directory.getFieldInfosStorage().getAllFieldInfos().keySet());
                        })
                ).collect(Collectors.toList()));
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

        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2, 1);
    }

    @ParameterizedTest
    @BooleanSource
    void addFieldInNextSegment(boolean oneTransaction) throws Exception {
        FieldInfo fieldInfo1 = simpleFieldInfo("foo", 0);
        FieldInfo fieldInfo2 = simpleFieldInfo("bar", 1);
        FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] { fieldInfo1 });
        FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] { fieldInfo1, fieldInfo2 });

        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2, 1);
    }

    @ParameterizedTest
    @BooleanSource
    void fewerFieldsInNextSegment(boolean oneTransaction) throws Exception {
        FieldInfo fieldInfo1 = simpleFieldInfo("foo", 0);
        FieldInfo fieldInfo2 = simpleFieldInfo("bar", 1);
        FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] { fieldInfo1, fieldInfo2 });
        FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] { fieldInfo1 });

        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2, 1);
    }

    @ParameterizedTest
    @BooleanSource
    void fewerFieldsInNextSegment2(boolean oneTransaction) throws Exception {
        FieldInfo fieldInfo1 = simpleFieldInfo("foo", 0);
        FieldInfo fieldInfo2 = simpleFieldInfo("bar", 1);
        FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] { fieldInfo1, fieldInfo2 });
        FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] { fieldInfo2 });

        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2, 1);
    }

    /**
     * See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2284">Issue #2284</a>.
     */
    @ParameterizedTest
    @BooleanSource
    void mixedFieldNumbers(boolean oneTransaction) throws Exception {
        // It is fine for these to all go in the Global FieldInfos, because each segment has its own bitset as to which
        // fields it sees, so the first segment will have just field number 0, and the second will have just
        // field number 1
        testWithTwoSegments(oneTransaction,
                singleFieldInfos("foo", 0),
                singleFieldInfos("foo", 1),
                1);
    }

    /**
     * See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2284">Issue #2284</a>.
     */
    @ParameterizedTest
    @BooleanSource
    void mixedFieldNames(boolean oneTransaction) throws Exception {
        testWithTwoSegments(oneTransaction,
                singleFieldInfos("foo", 0),
                singleFieldInfos("bar", 0),
                2);
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
        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2, 2);
    }

    @ParameterizedTest
    @BooleanSource
    void mixedOrderAttributes(boolean oneTransaction) throws Exception {
        final Map<String, String> attributes1 = new LinkedHashMap<>();
        final Map<String, String> attributes2 = new LinkedHashMap<>();
        attributes1.put("first", "a");
        attributes1.put("second", "b");
        attributes2.put("second", "b");
        attributes2.put("first", "a");
        // sanity checks that the iteration behaves as expected
        assertEquals(attributes1, attributes2);
        assertNotEquals(List.copyOf(attributes1.entrySet()), List.copyOf(attributes2.entrySet()));
        final FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] {
                fieldInfo("foo", 0, attributes1),
        });
        final FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] {
                fieldInfo("foo", 0, attributes2),
        });
        testWithTwoSegments(oneTransaction, fieldInfos1, fieldInfos2, 1);
    }


    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3})
    void fixGlobalOrdering(int transactionBreak) throws Exception {
        final Map<String, String> attributes = Map.of("first", "a", "second", "b");
        final FieldInfos fieldInfos1 = new FieldInfos(new FieldInfo[] {
                fieldInfo("foo", 0, attributes),
        });
        final FieldInfos fieldInfos2 = new FieldInfos(new FieldInfo[] {
                fieldInfo("foo", 0, attributes),
        });
        final var segment1 = new LightSegmentInfo();
        final var segment2 = new LightSegmentInfo();

        sameOrMultiTransaction(transactionBreak, List.of(
                directory -> {
                    write(directory, segment1, fieldInfos1);
                },
                directory -> {
                    final FieldInfosStorage fieldInfoStorage = directory.getFieldInfosStorage();
                    final LuceneFieldInfosProto.FieldInfos global = fieldInfoStorage
                            .readFieldInfos(FieldInfosStorage.GLOBAL_FIELD_INFOS_ID);
                    final LuceneFieldInfosProto.FieldInfos.Builder builder = global.toBuilder();
                    final LuceneFieldInfosProto.FieldInfo.Builder fieldInfoBuilder = builder.getFieldInfoBuilder(0);
                    final List<LuceneFieldInfosProto.Attribute> reversedAttributes = new ArrayList<>();
                    final List<LuceneFieldInfosProto.Attribute> serializedAttributes = fieldInfoBuilder.getAttributesList();
                    for (int i = serializedAttributes.size() - 1; i >= 0; i--) {
                        reversedAttributes.add(serializedAttributes.get(i));
                    }
                    fieldInfoBuilder.clearAttributes();
                    fieldInfoBuilder.addAllAttributes(reversedAttributes);
                    final LuceneFieldInfosProto.FieldInfos newGlobal = builder.build();
                    assertNotEquals(global.getFieldInfo(0), newGlobal.getFieldInfo(0));
                    fieldInfoStorage.updateGlobalFieldInfos(newGlobal);
                },
                directory -> {
                    final FieldInfosStorage fieldInfoStorage = directory.getFieldInfosStorage();
                    final LuceneFieldInfosProto.FieldInfos global = fieldInfoStorage
                            .readFieldInfos(FieldInfosStorage.GLOBAL_FIELD_INFOS_ID);
                    assertEquals(
                            global.getFieldInfo(0).getAttributesList().stream()
                                    .map(LuceneFieldInfosProto.Attribute::getKey)
                                    .collect(Collectors.toList()),
                            List.of("second", "first")
                    );
                    write(directory, segment2, fieldInfos2);
                },
                directory -> {
                    assertFieldInfosEqual(fieldInfos1, read(directory, segment1));
                    assertFieldInfosEqual(fieldInfos2, read(directory, segment2));
                    assertEquals(1, directory.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_READ_FIELD_INFOS,
                            directory.getFieldInfosCount()));

                    final FieldInfosStorage fieldInfoStorage = directory.getFieldInfosStorage();
                    final LuceneFieldInfosProto.FieldInfos global = fieldInfoStorage
                            .readFieldInfos(FieldInfosStorage.GLOBAL_FIELD_INFOS_ID);
                    assertEquals(
                            global.getFieldInfo(0).getAttributesList().stream()
                                    .map(LuceneFieldInfosProto.Attribute::getKey)
                                    .collect(Collectors.toList()),
                            List.of("first", "second")
                    );
                }));
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

    @ParameterizedTest
    @BooleanSource
    void nrtCachingDirectory(boolean oneTransaction) throws Exception {
        var fieldInfos = singleFieldInfos("foo", 0);
        final var segment = new LightSegmentInfo();
        final Function<FDBRecordContext, NRTCachingDirectory> createDirectory =
                context -> new NRTCachingDirectory(createDirectory(context), 20_000, 20_000);
        final IOContext ioContext = new IOContext(new MergeInfo(2, 200, true, 3));
        try (FDBRecordContext context = openContext()) {
            final Directory directory = createDirectory.apply(context);
            write(directory, segment, fieldInfos, ioContext);
            if (oneTransaction) {
                assertFieldInfosEqual(fieldInfos, read(directory, segment, ioContext));
            }
            context.commit();
        }
        if (!oneTransaction) {
            try (FDBRecordContext context = openContext()) {
                final Directory directory = createDirectory.apply(context);
                assertFieldInfosEqual(fieldInfos, read(directory, segment, ioContext));
                context.commit();
            }
        }
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
                                     final FieldInfos fieldInfos2,
                                     final int expectedCount) throws Exception {
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
                    assertEquals(expectedCount, directory.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_READ_FIELD_INFOS,
                            directory.getFieldInfosCount()));
                });
    }

    private void sameOrMultiTransaction(boolean oneTransaction,
                                        TestHelpers.DangerousConsumer<FDBDirectory> setup,
                                        TestHelpers.DangerousConsumer<FDBDirectory> assertions) throws Exception {
        sameOrMultiTransaction(oneTransaction ? 1 : 0, List.of(setup, assertions));
    }

    private void sameOrMultiTransaction(int oneTransaction, List<TestHelpers.DangerousConsumer<FDBDirectory>> operations) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final FDBDirectory directory = createDirectory(context);
            for (int i = 0; i < oneTransaction; i++) {
                operations.get(i).accept(directory);
            }
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            final FDBDirectory directory = createDirectory(context);
            for (int i = oneTransaction; i < operations.size(); i++) {
                operations.get(i).accept(directory);
            }
            context.commit();
        }
    }

    private FieldInfos read(final Directory directory, final LightSegmentInfo segment) throws IOException {
        return read(directory, segment, IOContext.DEFAULT);
    }

    private FieldInfos read(final Directory directory, final LightSegmentInfo segment, final IOContext ioContext) throws IOException {
        return format.read(directory, segment.create(directory), "", ioContext);
    }

    private void write(final Directory directory, final LightSegmentInfo segment, final FieldInfos fieldInfos) throws IOException {
        write(directory, segment, fieldInfos, IOContext.DEFAULT);
    }

    private void write(final Directory directory, final LightSegmentInfo segment, final FieldInfos fieldInfos, final IOContext ioContext) throws IOException {
        format.write(directory, segment.create(directory), "", fieldInfos, ioContext);
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
        return fieldInfo(name, number, Map.of());
    }

    @Nonnull
    private static FieldInfo fieldInfo(final String name, final int number, final Map<String, String> attributes) {
        return new FieldInfo(name, number, false, false, false,
                IndexOptions.DOCS, DocValuesType.NUMERIC, 1, attributes, 0, 0, 0, false);
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
        return new FDBDirectory(path.toSubspace(context), context,
                Map.of(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true"));
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
