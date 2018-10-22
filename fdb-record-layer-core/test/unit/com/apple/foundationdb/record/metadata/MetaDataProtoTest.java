/*
 * MetaDataProtoTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords5Proto;
import com.apple.foundationdb.record.TestRecords6Proto;
import com.apple.foundationdb.record.TestRecords7Proto;
import com.apple.foundationdb.record.TestRecordsIndexCompatProto;
import com.apple.foundationdb.record.TestRecordsMultiProto;
import com.apple.foundationdb.record.TestRecordsParentChildRelationshipProto;
import com.apple.foundationdb.record.TestRecordsRankProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for conversion of {@link RecordMetaDataProto.MetaData} to {@link RecordMetaData}.
 */
public class MetaDataProtoTest {
    private static final Descriptors.FileDescriptor[] BASE_DEPENDENCIES = {
            RecordMetaDataOptionsProto.getDescriptor()
    };
    private static final Descriptors.FileDescriptor[] FULL_DEPENDENCIES = {
            RecordMetaDataOptionsProto.getDescriptor(),
            TupleFieldsProto.getDescriptor()
    };

    public static void verifyEquals(@Nonnull Index index1, @Nonnull Index index2) {
        try {
            assertEquals(index1.getName(), index2.getName());
            assertEquals(index1.getRootExpression(), index2.getRootExpression());
            assertEquals(index1.getSubspaceKey(), index2.getSubspaceKey());
            assertEquals(index1.getColumnSize(), index2.getColumnSize());
            assertEquals(index1.getType(), index2.getType());
            assertEquals(index1.getOptions(), index2.getOptions());
            assertArrayEquals(index1.getPrimaryKeyComponentPositions(), index2.getPrimaryKeyComponentPositions());
            assertEquals(index1.getVersion(), index2.getVersion());
        } catch (AssertionError e) {
            fail("Failed when checking index " + index1.getName() + ": " + e.getMessage());
        }
    }

    public static void verifyEquals(@Nonnull RecordMetaData metaData1, @Nonnull RecordMetaData metaData2) {
        // Same record types.
        assertEquals(metaData1.getRecordTypes().keySet(), metaData2.getRecordTypes().keySet());
        assertEquals(getFields(metaData1.getUnionDescriptor()), getFields(metaData2.getUnionDescriptor()));
        assertEquals(getMessageNames(metaData1), getMessageNames(metaData2));

        // Same indices.
        List<Index> indexes1 = metaData1.getAllIndexes();
        List<Index> indexes2 = metaData2.getAllIndexes();
        assertEquals(indexes1.size(), indexes2.size());

        Map<String, Index> indexMap1 = indexes1.stream().collect(Collectors.toMap(Index::getName, Function.identity()));
        Map<String, Index> indexMap2 = indexes2.stream().collect(Collectors.toMap(Index::getName, Function.identity()));
        assertEquals(indexMap1.keySet(), indexMap2.keySet());

        for (String key : indexMap1.keySet()) {
            verifyEquals(indexMap1.get(key), indexMap2.get(key));
        }

        // Make sure indexes are applied to the same records.
        for (String recordTypeName : metaData1.getRecordTypes().keySet()) {
            RecordType rt1 = metaData1.getRecordType(recordTypeName);
            RecordType rt2 = metaData2.getRecordType(recordTypeName);
            assertEquals(getIndexNames(rt1), getIndexNames(rt2));
            assertEquals(getMultiTypeIndexNames(rt1), getMultiTypeIndexNames(rt2));
        }

        // Former indexes.
        assertEquals(metaData1.getFormerIndexes().size(), metaData2.getFormerIndexes().size());
        for (FormerIndex formerIndex : metaData1.getFormerIndexes()) {
            boolean found = false;
            for (FormerIndex otherFormerIndex : metaData2.getFormerIndexes()) {
                if (formerIndex.getVersion() == otherFormerIndex.getVersion()
                        && formerIndex.getSubspaceKey().equals(otherFormerIndex.getSubspaceKey())) {
                    found = true;
                    break;
                }
            }

            assertTrue(found, "Could not find matching former index");
        }

        // Paraphernalia
        assertEquals(metaData1.getVersion(), metaData2.getVersion());
        assertEquals(metaData1.isStoreRecordVersions(), metaData2.isStoreRecordVersions());
        assertEquals(metaData1.isSplitLongRecords(), metaData2.isSplitLongRecords());
        assertEquals(metaData1.getRecordCountKey(), metaData2.getRecordCountKey());
    }

    private static Set<String> getIndexNames(@Nonnull RecordType rt) {
        return rt.getIndexes().stream().map(Index::getName).collect(Collectors.toSet());
    }

    private static Set<String> getMultiTypeIndexNames(@Nonnull RecordType rt) {
        return rt.getMultiTypeIndexes().stream().map(Index::getName).collect(Collectors.toSet());
    }

    private static List<String> getFields(@Nonnull Descriptors.Descriptor descriptor) {
        return descriptor.getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList());
    }

    private static Set<String> getMessageNames(@Nonnull RecordMetaData metaData) {
        return metaData.getRecordsDescriptor().getMessageTypes().stream().map(Descriptors.Descriptor::getName).collect(Collectors.toSet());
    }

    @Test
    public void indexProto() throws KeyExpression.DeserializationException, KeyExpression.SerializationException {
        Index index = new Index("heat", Key.Expressions.field("temperature").nest("humidity"));
        Index reindex = new Index(index.toProto());
        verifyEquals(index, reindex);

        index = new Index("UV", Key.Expressions.concatenateFields("radiation", "cloud-cover").group(1), IndexTypes.RANK);
        reindex = new Index(index.toProto());
        verifyEquals(index, reindex);

        index = new Index("human-development", Key.Expressions.field("life-expectancy").groupBy(
                Key.Expressions.concat(Key.Expressions.field("education").nest("schooling"), Key.Expressions.field("income"))),
                Key.Expressions.field("united-nations"),
                IndexTypes.VALUE, Index.UNIQUE_OPTIONS);
        reindex = new Index(index.toProto());
        verifyEquals(index, reindex);
    }

    @Test
    public void metadataProtoSimple() throws KeyExpression.DeserializationException, KeyExpression.SerializationException {
        List<Descriptors.FileDescriptor> files = Arrays.asList(
                ExpressionTestsProto.getDescriptor(),
                TestRecords1Proto.getDescriptor(),
                TestRecords2Proto.getDescriptor(),
                TestRecords3Proto.getDescriptor(),
                TestRecords4Proto.getDescriptor(),
                TestRecords5Proto.getDescriptor(),
                TestRecords6Proto.getDescriptor(),
                TestRecords7Proto.getDescriptor(),
                TestRecordsMultiProto.getDescriptor(),
                TestRecordsParentChildRelationshipProto.getDescriptor(),
                TestRecordsRankProto.getDescriptor(),
                TestRecordsWithHeaderProto.getDescriptor(),
                TestRecordsWithUnionProto.getDescriptor(),
                TestRecordsIndexCompatProto.getDescriptor()
        );

        for (int i = 0; i < files.size(); i++) {
            Descriptors.FileDescriptor file = files.get(i);
            RecordMetaData metaData = RecordMetaData.build(file);
            RecordMetaData metaDataRedone = new RecordMetaDataBuilder(metaData.toProto(), i == 0 ? FULL_DEPENDENCIES : BASE_DEPENDENCIES, false).getRecordMetaData();
            verifyEquals(metaData, metaDataRedone);
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void metadataProtoComplex() throws KeyExpression.DeserializationException, KeyExpression.SerializationException {
        RecordMetaDataBuilder metaDataBuilder = new RecordMetaDataBuilder(TestRecords3Proto.getDescriptor());
        metaDataBuilder.getOnlyRecordType().setPrimaryKey(Key.Expressions.concatenateFields("parent_path", "child_name"));
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("MyHierarchicalRecord"),
                new Index("MHR$child$parentpath", Key.Expressions.concatenateFields("child_name", "parent_path"), IndexTypes.VALUE));
        RecordMetaData metaData = metaDataBuilder.getRecordMetaData();
        RecordMetaData metaDataRedone = new RecordMetaDataBuilder(metaData.toProto(), BASE_DEPENDENCIES, false).getRecordMetaData();
        verifyEquals(metaData, metaDataRedone);

        metaDataBuilder = new RecordMetaDataBuilder(TestRecords4Proto.getDescriptor());
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("RestaurantRecord"),
                new Index("RR$ratings", Key.Expressions.field("review", KeyExpression.FanType.FanOut).nest("rating").ungrouped(), IndexTypes.RANK));
        metaDataBuilder.removeIndex("RestaurantReviewer$name");
        metaData = metaDataBuilder.getRecordMetaData();
        metaDataRedone = new RecordMetaDataBuilder(metaData.toProto(), BASE_DEPENDENCIES, false).getRecordMetaData();
        assertEquals(1, metaData.getFormerIndexes().size());
        assertFalse(metaData.isSplitLongRecords());
        assertFalse(metaData.isStoreRecordVersions());
        verifyEquals(metaData, metaDataRedone);

        metaDataBuilder = new RecordMetaDataBuilder(TestRecordsMultiProto.getDescriptor());
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(
                    metaDataBuilder.getRecordType("MultiRecordOne"),
                    metaDataBuilder.getRecordType("MultiRecordTwo"),
                    metaDataBuilder.getRecordType("MultiRecordThree")),
                new Index("all$elements", Key.Expressions.field("element", KeyExpression.FanType.Concatenate),
                        Index.EMPTY_VALUE, IndexTypes.VALUE, Index.UNIQUE_OPTIONS));
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(
                    metaDataBuilder.getRecordType("MultiRecordTwo"),
                    metaDataBuilder.getRecordType("MultiRecordThree")),
                new Index("two&three$ego", Key.Expressions.field("ego"), Index.EMPTY_VALUE, IndexTypes.VALUE, Index.UNIQUE_OPTIONS));
        metaDataBuilder.addIndex(metaDataBuilder.getRecordType("MultiRecordOne"),
                new Index("one$name", Key.Expressions.field("name"), IndexTypes.VALUE));
        metaDataBuilder.setRecordCountKey(Key.Expressions.field("blah"));
        metaDataBuilder.removeIndex("one$name");
        metaDataBuilder.setStoreRecordVersions(true);
        metaData = metaDataBuilder.getRecordMetaData();
        assertEquals(2, metaData.getAllIndexes().size());
        assertEquals(1, metaData.getFormerIndexes().size());
        assertTrue(metaData.isSplitLongRecords());
        assertTrue(metaData.isStoreRecordVersions());
        metaDataRedone = new RecordMetaDataBuilder(metaData.toProto(), BASE_DEPENDENCIES, false).getRecordMetaData();
        verifyEquals(metaData, metaDataRedone);
    }

    @Test
    public void indexProtoOptions() throws Exception {
        RecordMetaData metaData = new RecordMetaDataBuilder(TestRecordsIndexCompatProto.getDescriptor()).getRecordMetaData();

        // Basic check that options are obeyed.
        assertTrue(metaData.hasIndex("MyModernRecord$index"));
        assertFalse(metaData.hasIndex("MyModernRecord$none"));

        RecordType compat = metaData.getRecordType("MyCompatRecord");
        RecordType modern = metaData.getRecordType("MyModernRecord");
        assertEquals(modern.getIndexes().size(), compat.getIndexes().size());
        for (Index modernIndex : modern.getIndexes()) {
            Index compatIndex = metaData.getIndex(modernIndex.getName().replace("Modern", "Compat"));
            assertEquals(modernIndex.getType(), compatIndex.getType());
            assertEquals(modernIndex.getOptions(), compatIndex.getOptions());
        }
    }

    public static RecordMetaDataProto.Field.Builder scalarField(String name) {
        return RecordMetaDataProto.Field.newBuilder()
                .setFieldName(name)
                .setFanType(RecordMetaDataProto.Field.FanType.SCALAR);
    }

    @Test
    public void versionstampIndexDeserialization() throws Exception {
        RecordMetaDataProto.MetaData.Builder protoBuilder = RecordMetaDataProto.MetaData.newBuilder();
        protoBuilder.setRecords(TestRecordsIndexCompatProto.getDescriptor().toProto());

        protoBuilder.addIndexesBuilder()
                .setName("VersionstampIndex")
                .setRootExpression(
                        RecordMetaDataProto.KeyExpression.newBuilder()
                                .setNesting(RecordMetaDataProto.Nesting.newBuilder()
                                        .setParent(scalarField("header"))
                                        .setChild(RecordMetaDataProto.KeyExpression.newBuilder()
                                                .setThen(RecordMetaDataProto.Then.newBuilder()
                                                        .addChild(RecordMetaDataProto.KeyExpression.newBuilder()
                                                                .setField(scalarField("childA")))
                                                        .addChild(RecordMetaDataProto.KeyExpression.newBuilder()
                                                                .setVersion(RecordMetaDataProto.Version.newBuilder()))))));
        RecordMetaData metaData = new RecordMetaDataBuilder(protoBuilder.build(), BASE_DEPENDENCIES, false).getRecordMetaData();
        Index versionstampIndex = metaData.getIndex("VersionstampIndex");
        assertEquals(1, versionstampIndex.getRootExpression().versionColumns());
    }

    @Test
    @SuppressWarnings("deprecation") // Older (deprecated) types had special grouping behavior
    public void indexGroupingCompatibility() throws Exception {
        RecordMetaDataProto.MetaData.Builder protoBuilder = RecordMetaDataProto.MetaData.newBuilder();
        protoBuilder.setRecords(TestRecordsIndexCompatProto.getDescriptor().toProto());

        protoBuilder.addIndexesBuilder()
                .setName("RecordCount")
                .setType(IndexTypes.COUNT)
                .addRecordType("MyModernRecord")
                .setRootExpression(RecordMetaDataProto.KeyExpression.newBuilder()
                        .setEmpty(RecordMetaDataProto.Empty.getDefaultInstance()));

        protoBuilder.addIndexesBuilder()
                .setName("MaxRecNo")
                .setType(IndexTypes.MAX_EVER)
                .addRecordType("MyModernRecord")
                .setRootExpression(RecordMetaDataProto.KeyExpression.newBuilder()
                        .setField(scalarField("rec_no")));

        protoBuilder.addIndexesBuilder()
                .setName("MaxRecNoGrouped")
                .setType(IndexTypes.MAX_EVER)
                .addRecordType("MyModernRecord")
                .setRootExpression(RecordMetaDataProto.KeyExpression.newBuilder()
                        .setThen(RecordMetaDataProto.Then.newBuilder()
                                .addChild(RecordMetaDataProto.KeyExpression.newBuilder()
                                        .setField(scalarField("index")))
                                .addChild(RecordMetaDataProto.KeyExpression.newBuilder()
                                        .setField(scalarField("rec_no")))));

        RecordMetaData metaData = new RecordMetaDataBuilder(protoBuilder.build(), BASE_DEPENDENCIES, true).getRecordMetaData();

        Index regularIndex = metaData.getIndex("MyCompatRecord$index");
        assertFalse(regularIndex.getRootExpression() instanceof GroupingKeyExpression, "should not have Grouping");

        Index compatRank = metaData.getIndex("MyCompatRecord$rank");
        assertTrue(compatRank.getRootExpression() instanceof GroupingKeyExpression, "should have Grouping");
        assertEquals(1, ((GroupingKeyExpression)compatRank.getRootExpression()).getGroupedCount());

        Index modernRank = metaData.getIndex("MyModernRecord$rank");
        assertTrue(modernRank.getRootExpression() instanceof GroupingKeyExpression, "should have Grouping");
        assertEquals(1, ((GroupingKeyExpression)modernRank.getRootExpression()).getGroupedCount());

        Index recordCount = metaData.getIndex("RecordCount");
        assertTrue(recordCount.getRootExpression() instanceof GroupingKeyExpression, "should have Grouping");
        assertEquals(0, ((GroupingKeyExpression)recordCount.getRootExpression()).getGroupedCount());

        Index maxRecNo = metaData.getIndex("MaxRecNo");
        assertTrue(maxRecNo.getRootExpression() instanceof GroupingKeyExpression, "should have Grouping");
        assertEquals(1, ((GroupingKeyExpression)maxRecNo.getRootExpression()).getGroupedCount());

        Index maxRecNoGrouped = metaData.getIndex("MaxRecNoGrouped");
        assertTrue(maxRecNoGrouped.getRootExpression() instanceof GroupingKeyExpression, "should have Grouping");
        assertEquals(1, ((GroupingKeyExpression)maxRecNoGrouped.getRootExpression()).getGroupedCount());
    }

}
