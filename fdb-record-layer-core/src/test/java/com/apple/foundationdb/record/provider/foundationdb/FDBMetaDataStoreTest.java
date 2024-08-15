/*
 * FDBMetaDataStoreTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.ProtoVersionSupplier;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestNoUnionEvolvedIllegalProto;
import com.apple.foundationdb.record.TestNoUnionEvolvedProto;
import com.apple.foundationdb.record.TestNoUnionEvolvedRenamedRecordTypeProto;
import com.apple.foundationdb.record.TestNoUnionProto;
import com.apple.foundationdb.record.TestRecords1EvolvedAgainProto;
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecordsDoubleNestedProto;
import com.apple.foundationdb.record.TestRecordsImplicitUsageNoUnionProto;
import com.apple.foundationdb.record.TestRecordsImplicitUsageProto;
import com.apple.foundationdb.record.TestRecordsImportProto;
import com.apple.foundationdb.record.TestRecordsImportedAndNewProto;
import com.apple.foundationdb.record.TestRecordsMultiProto;
import com.apple.foundationdb.record.TestRecordsNestedAsRecord;
import com.apple.foundationdb.record.TestRecordsOneOfProto;
import com.apple.foundationdb.record.TestRecordsParentChildRelationshipProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataProtoTest;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FDBMetaDataStore}.
 */
@Tag(Tags.RequiresFDB)
public class FDBMetaDataStoreTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    FDBDatabase fdb;
    KeySpacePath path;
    FDBMetaDataStore metaDataStore;

    @BeforeEach
    void setUp() {
        fdb = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.META_DATA_STORE);
    }

    public void openMetaDataStore(FDBRecordContext context) {
        metaDataStore = new FDBMetaDataStore(context, path);
        metaDataStore.setDependencies(new Descriptors.FileDescriptor[] {
                RecordMetaDataOptionsProto.getDescriptor()
        });
    }

    @Test
    public void simple() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);

            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();

            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            MetaDataProtoTest.verifyEquals(metaData, metaDataStore.getRecordMetaData());
        }
    }

    @Test
    public void manyTypes() {
        final int ntypes = 500;
        final int nfields = 10;

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);

            DescriptorProtos.FileDescriptorProto.Builder fileBuilder = DescriptorProtos.FileDescriptorProto.newBuilder();
            fileBuilder.addDependency(RecordMetaDataOptionsProto.getDescriptor().getName());

            DescriptorProtos.DescriptorProto.Builder unionBuilder = fileBuilder.addMessageTypeBuilder();
            unionBuilder.setName("RecordTypeUnion");
            DescriptorProtos.MessageOptions.Builder unionMessageOptions = DescriptorProtos.MessageOptions.newBuilder();
            RecordMetaDataOptionsProto.RecordTypeOptions.Builder unionOptions = RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder();
            unionOptions.setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION);
            unionMessageOptions.setExtension(RecordMetaDataOptionsProto.record, unionOptions.build());
            unionBuilder.setOptions(unionMessageOptions);

            for (int ri = 1; ri <= ntypes; ri++) {
                DescriptorProtos.DescriptorProto.Builder messageBuilder = fileBuilder.addMessageTypeBuilder();
                messageBuilder.setName("type_" + ri);
                for (int fi = 1; fi <= nfields; fi++) {
                    messageBuilder.addFieldBuilder()
                            .setName("field_" + fi)
                            .setNumber(fi)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
                }
                unionBuilder.addFieldBuilder()
                        .setNumber(ri)
                        .setName("_" + messageBuilder.getName())
                        .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(messageBuilder.getName());
            }

            RecordMetaDataProto.MetaData.Builder metaData = RecordMetaDataProto.MetaData.newBuilder();
            metaData.setRecords(fileBuilder);

            for (int ri = 1; ri <= ntypes; ri++) {
                metaData.addRecordTypesBuilder()
                        .setName("type_" + ri)
                        .getPrimaryKeyBuilder().getFieldBuilder()
                            .setFanType(RecordMetaDataProto.Field.FanType.SCALAR)
                            .setFieldName("field_1");
            }

            metaDataStore.saveRecordMetaData(metaData.build());

            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            for (int ri = 1; ri <= ntypes; ri++) {
                assertNotNull(metaDataStore.getRecordMetaData().getRecordType("type_" + ri));
            }
            context.commit();
        }
    }

    @Test
    public void historyCompat() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);

            RecordMetaDataProto.MetaData.Builder metaData = RecordMetaDataProto.MetaData.newBuilder();
            metaData.setRecords(TestRecords1Proto.getDescriptor().toProto());
            metaData.addRecordTypesBuilder()
                    .setName("MySimpleRecord")
                    .getPrimaryKeyBuilder().getFieldBuilder().setFieldName("rec_no").setFanType(RecordMetaDataProto.Field.FanType.SCALAR);
            metaData.addRecordTypesBuilder()
                    .setName("MyOtherRecord")
                    .getPrimaryKeyBuilder().getFieldBuilder().setFieldName("rec_no").setFanType(RecordMetaDataProto.Field.FanType.SCALAR);
            metaData.setVersion(101);
            metaDataStore.saveRecordMetaData(metaData.build());

            {
                // Adjust to look like old format store by moving everything under CURRENT_KEY up under root.
                Transaction tr = context.ensureActive();
                List<KeyValue> kvs = context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_META_DATA,
                        tr.getRange(metaDataStore.getSubspace().range(FDBMetaDataStore.CURRENT_KEY)).asList());
                context.clear(metaDataStore.getSubspace().range());
                for (KeyValue kv : kvs) {
                    Tuple tuple = Tuple.fromBytes(kv.getKey());
                    List<Object> items = tuple.getItems();
                    assertEquals(null, items.remove(items.size() - 2));
                    tuple = Tuple.fromList(items);
                    tr.set(tuple.pack(), kv.getValue());
                }
            }

            context.commit();
        }

        RecordMetaData before;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            before = metaDataStore.getRecordMetaData();
            context.commit();
        }
        assertNotNull(before.getRecordType("MySimpleRecord"));
        assertFalse(before.hasIndex("MyIndex"));

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);

            RecordMetaDataProto.MetaData.Builder metaData = RecordMetaDataProto.MetaData.newBuilder();
            metaData.setRecords(TestRecords1Proto.getDescriptor().toProto());
            metaData.addRecordTypesBuilder()
                    .setName("MySimpleRecord")
                    .getPrimaryKeyBuilder().getFieldBuilder().setFieldName("rec_no").setFanType(RecordMetaDataProto.Field.FanType.SCALAR);
            metaData.addIndexesBuilder()
                    .setName("MyIndex")
                    .addRecordType("MySimpleRecord")
                    .setAddedVersion(102)
                    .setLastModifiedVersion(102)
                    .getRootExpressionBuilder().getFieldBuilder()
                    .setFieldName("num_value_2")
                    .setFanType(RecordMetaDataProto.Field.FanType.SCALAR);
            metaData.addRecordTypesBuilder()
                    .setName("MyOtherRecord")
                    .getPrimaryKeyBuilder().getFieldBuilder().setFieldName("rec_no").setFanType(RecordMetaDataProto.Field.FanType.SCALAR);
            metaData.setVersion(102);
            metaDataStore.saveRecordMetaData(metaData.build());
            context.commit();
        }

        RecordMetaData after;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            after = metaDataStore.getRecordMetaData();
            context.commit();
        }
        assertNotNull(after.getRecordType("MySimpleRecord"));
        assertTrue(after.hasIndex("MyIndex"));

        RecordMetaData beforeAgain;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            beforeAgain = context.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_META_DATA, metaDataStore.loadVersion(before.getVersion()));
            context.commit();
        }
        assertEquals(before.getVersion(), beforeAgain.getVersion());
        assertNotNull(beforeAgain.getRecordType("MySimpleRecord"));
        assertFalse(beforeAgain.hasIndex("MyIndex"));

    }

    @Test
    public void withToProto() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsParentChildRelationshipProto.getDescriptor());
        metaDataBuilder.addIndex("MyChildRecord", "MyChildRecord$str_value", Key.Expressions.field("str_value"));
        metaDataBuilder.removeIndex("MyChildRecord$parent_rec_no");
        metaDataBuilder.addIndex("MyChildRecord", new Index("MyChildRecord$parent&str", Key.Expressions.concatenateFields("parent_rec_no", "str_value"), Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        metaDataBuilder.removeIndex("MyParentRecord$str_value_indexed");
        metaDataBuilder.addIndex("MyParentRecord", "MyParentRecord$str&child", Key.Expressions.concat(
                Key.Expressions.field("str_value_indexed"), Key.Expressions.field("child_rec_nos", KeyExpression.FanType.FanOut)));
        metaDataBuilder.addMultiTypeIndex(Arrays.asList(metaDataBuilder.getRecordType("MyChildRecord"), metaDataBuilder.getRecordType("MyParentRecord")),
                new Index("all$rec_nos", Key.Expressions.field("rec_no")));
        RecordMetaData metaData = metaDataBuilder.getRecordMetaData();

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData retrievedMetaData = metaDataStore.getRecordMetaData();
            MetaDataProtoTest.verifyEquals(metaData, retrievedMetaData);
        }
    }

    @Test
    public void withIncompatibleChange() {
        RecordMetaData metaData1 = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(metaData1);
            context.commit();
        }

        RecordMetaData metaData2 = RecordMetaData.build(metaData1.toProto().toBuilder().setVersion(metaData1.getVersion() + 1).clearIndexes().build());
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.saveRecordMetaData(metaData2));
            assertThat(e.getMessage(), containsString("index missing in new meta-data"));
            context.commit();
        }

        // Using the builder, this change should be fine.
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaData1.getAllIndexes().forEach(index -> metaDataBuilder.removeIndex(index.getName()));
        RecordMetaData metaData3 = metaDataBuilder.getRecordMetaData();
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(metaData3);
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData retrievedMetaData = metaDataStore.getRecordMetaData();
            MetaDataProtoTest.verifyEquals(metaData3, retrievedMetaData);
        }
    }

    @ParameterizedTest(name = "indexes [indexCounterBasedSubspaceKey = {0}]")
    @BooleanSource
    public void indexes(final boolean indexCounterBasedSubspaceKey) {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder();
            if (indexCounterBasedSubspaceKey) {
                builder.enableCounterBasedSubspaceKeys();
            }
            metaDataStore.saveRecordMetaData(builder.setRecords(TestRecords1Proto.getDescriptor()).getRecordMetaData());
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            metaDataStore.addIndex("MySimpleRecord", "testIndex", "rec_no");
            assertNotNull(metaDataStore.getRecordMetaData().getIndex("testIndex"));
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getIndex("testIndex"));
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getIndex("testIndex"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            MetaDataException e = assertThrows(MetaDataException.class, () ->
                    metaDataStore.addIndex("MySimpleRecord", "testIndex", "rec_no"));
            assertEquals("Index testIndex already defined", e.getMessage());
            metaDataStore.dropIndex("testIndex");
            context.commit();
            e = assertThrows(MetaDataException.class, () ->
                    metaDataStore.getRecordMetaData().getIndex("testIndex"));
            assertEquals("Index testIndex not defined", e.getMessage());
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            MetaDataException e = assertThrows(MetaDataException.class, () ->
                    metaDataStore.getRecordMetaData().getIndex("testIndex"));
            assertEquals("Index testIndex not defined", e.getMessage());
            e = assertThrows(MetaDataException.class, () -> metaDataStore.dropIndex("testIndex"));
            assertEquals("No index named testIndex defined", e.getMessage());
            context.commit();
        }
    }

    @Test
    public void withIndexesRequiringRebuild() {
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        RecordMetaDataProto.MetaData.Builder protoBuilder = metaData.toProto().toBuilder().setVersion(metaData.getVersion() + 1);
        protoBuilder.getIndexesBuilderList().forEach(index -> {
            if (index.getName().equals("MySimpleRecord$str_value_indexed")) {
                index.addOptions(RecordMetaDataProto.Index.Option.newBuilder().setKey(IndexOptions.UNIQUE_OPTION).setValue("true"));
                index.setLastModifiedVersion(metaData.getVersion() + 1);
            }
        });
        RecordMetaData metaData2 = RecordMetaData.build(protoBuilder.build());
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.saveRecordMetaData(metaData2));
            assertThat(e.getMessage(), containsString("last modified version of index changed"));
            MetaDataProtoTest.verifyEquals(metaData, metaDataStore.getRecordMetaData());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData retrievedMetaData = metaDataStore.getRecordMetaData();
            assertThat(retrievedMetaData.getIndex("MySimpleRecord$str_value_indexed").isUnique(), is(false));
            MetaDataProtoTest.verifyEquals(metaData, retrievedMetaData);
        }

        MetaDataEvolutionValidator laxerValidator = MetaDataEvolutionValidator.newBuilder()
                .setAllowIndexRebuilds(true)
                .build();
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.setEvolutionValidator(laxerValidator);
            metaDataStore.saveRecordMetaData(metaData2);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData retrievedMetaData = metaDataStore.getRecordMetaData();
            assertThat(retrievedMetaData.getIndex("MySimpleRecord$str_value_indexed").isUnique(), is(true));
            MetaDataProtoTest.verifyEquals(metaData2, metaDataStore.getRecordMetaData());
        }
    }

    @Test
    public void multiTypeIndex() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsMultiProto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.addUniversalIndex(FDBRecordStoreTestBase.globalCountIndex());
            metaDataStore.addMultiTypeIndex(Arrays.asList("MultiRecordOne", "MultiRecordTwo", "MultiRecordThree"),
                    new Index("all$elements", Key.Expressions.field("element", KeyExpression.FanType.Concatenate),
                            Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
            metaDataStore.addMultiTypeIndex(null,
                    new Index("all$elements2", Key.Expressions.field("element", KeyExpression.FanType.Concatenate),
                            Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
            metaDataStore.addMultiTypeIndex(Arrays.asList("MultiRecordTwo", "MultiRecordThree"),
                    new Index("two&three$ego", Key.Expressions.field("ego"), Index.EMPTY_VALUE, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
            metaDataStore.addMultiTypeIndex(Arrays.asList("MultiRecordOne"), new Index("one$name", Key.Expressions.field("name"), IndexTypes.VALUE));
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(5, metaData.getAllIndexes().size());
            assertEquals(0, metaData.getFormerIndexes().size());
            assertNotNull(metaData.getIndex("all$elements"));
            assertEquals(3, metaData.recordTypesForIndex(metaData.getIndex("all$elements")).size());
            assertNotNull(metaData.getIndex("all$elements2"));
            assertEquals(3, metaData.recordTypesForIndex(metaData.getIndex("all$elements2")).size());
            assertNotNull(metaData.getIndex("two&three$ego"));
            assertEquals(2, metaData.recordTypesForIndex(metaData.getIndex("two&three$ego")).size());
            assertNotNull(metaData.getIndex("one$name"));
            assertEquals(1, metaData.recordTypesForIndex(metaData.getIndex("one$name")).size());
            metaDataStore.dropIndex("one$name");
            MetaDataException e = assertThrows(MetaDataException.class, () ->
                    metaDataStore.getRecordMetaData().getIndex("one$name"));
            assertEquals("Index one$name not defined", e.getMessage());
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(4, metaData.getAllIndexes().size());
            assertEquals(1, metaData.getFormerIndexes().size());
            assertNotNull(metaDataStore.getRecordMetaData().getIndex("all$elements"));
            assertNotNull(metaDataStore.getRecordMetaData().getIndex("all$elements2"));
            assertNotNull(metaDataStore.getRecordMetaData().getIndex("two&three$ego"));
            MetaDataException e = assertThrows(MetaDataException.class, () ->
                    metaDataStore.getRecordMetaData().getIndex("one$name"));
            assertEquals("Index one$name not defined", e.getMessage());
        }
    }

    @Test
    public void withoutBumpingVersion() {
        RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(metaData);
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.saveRecordMetaData(metaData));
            assertThat(e.getMessage(), containsString("meta-data version must increase"));
            context.commit();
        }
    }

    @Test
    public void updateRecords() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyOtherRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("AnotherRecord"));
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyOtherRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("AnotherRecord"));
            context.commit();
        }
    }

    @ParameterizedTest(name = "updateRecordsWithNewUnionField [reorderFields = {0}]")
    @BooleanSource
    public void updateRecordsWithNewUnionField(boolean reorderFields) {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData oldMetaData = metaDataStore.getRecordMetaData();
            metaDataStore.mutateMetaData(metaDataProtoBuilder -> {
                final DescriptorProtos.FileDescriptorProto.Builder records = metaDataProtoBuilder.getRecordsBuilder();
                records.getMessageTypeBuilderList().stream()
                        .filter(message -> message.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME))
                        .forEach(unionMessage -> {
                            unionMessage.getFieldBuilderList().stream()
                                    .filter(field -> field.getName().equals("_MySimpleRecord"))
                                    .forEach(field -> field.setName("_MySimpleRecord_old"));

                            int newFieldNumber = unionMessage.getFieldBuilderList().stream()
                                    .mapToInt(DescriptorProtos.FieldDescriptorProto.Builder::getNumber)
                                    .max()
                                    .orElse(0) + 1;
                            DescriptorProtos.FieldDescriptorProto newField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName("." + TestRecords1Proto.MySimpleRecord.getDescriptor().getFullName())
                                    .setName("_MySimpleRecord_new")
                                    .setNumber(newFieldNumber)
                                    .build();
                            if (reorderFields) {
                                List<DescriptorProtos.FieldDescriptorProto> fieldList = new ArrayList<>(unionMessage.getFieldBuilderList().size() + 1);
                                fieldList.add(newField);
                                fieldList.addAll(unionMessage.getFieldList());
                                unionMessage.clearField();
                                unionMessage.addAllField(fieldList);
                            } else {
                                unionMessage.addField(newField);
                            }
                        });
            });
            RecordMetaData newMetaData = metaDataStore.getRecordMetaData();
            RecordType oldSimpleRecord = oldMetaData.getRecordType("MySimpleRecord");
            assertEquals(TestRecords1EvolvedProto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER,
                    oldMetaData.getUnionFieldForRecordType(oldSimpleRecord).getNumber());
            RecordType newSimpleRecord = newMetaData.getRecordType("MySimpleRecord");
            assertSame(newMetaData.getUnionDescriptor().findFieldByName("_MySimpleRecord_new"),
                    newMetaData.getUnionFieldForRecordType(newSimpleRecord));
            assertThat(oldMetaData.getUnionFieldForRecordType(oldSimpleRecord).getNumber(),
                    lessThan(newMetaData.getUnionFieldForRecordType(newSimpleRecord).getNumber()));
            assertEquals(oldSimpleRecord.getSinceVersion(), newSimpleRecord.getSinceVersion());

            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            RecordType simpleRecord = metaData.getRecordType("MySimpleRecord");
            assertEquals("_MySimpleRecord_new", metaData.getUnionFieldForRecordType(simpleRecord).getName());
            int newFieldNumber = TestRecords1Proto.RecordTypeUnion.getDescriptor().getFields().stream()
                    .mapToInt(Descriptors.FieldDescriptor::getNumber)
                    .max()
                    .orElse(0) + 1;
            assertEquals(newFieldNumber, metaData.getUnionFieldForRecordType(simpleRecord).getNumber());
        }
    }

    @Test
    public void updateRecordsWithExtensionOption() throws Descriptors.DescriptorValidationException {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }

        // Add an extension option specifying that a field should have a new index
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = TestRecords1Proto.getDescriptor().toProto().toBuilder();
        fileBuilder.getMessageTypeBuilderList().forEach(messageType -> {
            if (messageType.getName().equals("MySimpleRecord")) {
                messageType.getFieldBuilderList().forEach(field -> {
                    if (field.getName().equals("num_value_2")) {
                        RecordMetaDataOptionsProto.FieldOptions isIndexedOption = RecordMetaDataOptionsProto.FieldOptions.newBuilder()
                                .setIndex(RecordMetaDataOptionsProto.FieldOptions.IndexOption.newBuilder()
                                        .setType(IndexTypes.VALUE)
                                        .setUnique(true))
                                .build();
                        field.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.field, isIndexedOption);
                    }
                });
            }
        });
        Descriptors.FileDescriptor newFileDescriptor = Descriptors.FileDescriptor.buildFrom(fileBuilder.build(), new Descriptors.FileDescriptor[]{RecordMetaDataOptionsProto.getDescriptor()});

        // Validate that new extension option will result in new index when built from file.
        RecordMetaData metaDataFromFile = RecordMetaData.build(newFileDescriptor);
        Index newIndex = metaDataFromFile.getIndex("MySimpleRecord$num_value_2");
        assertEquals(Key.Expressions.field("num_value_2"), newIndex.getRootExpression());
        assertThat("newIndex not marked as unique", newIndex.isUnique());

        // Update records. Validate that created meta-data does not add index.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.updateRecords(newFileDescriptor);
            RecordMetaData metaData = metaDataStore.getRecordMetaData(); // read from local cache
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaData.getIndex("MySimpleRecord$num_value_2"));
            assertThat(e.getMessage(), containsString("Index MySimpleRecord$num_value_2 not defined"));
            context.commit();
        }

        // Validate that reading the index back from database does not add the index.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.updateRecords(newFileDescriptor);
            RecordMetaData metaData = metaDataStore.getRecordMetaData(); // read from the database
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaData.getIndex("MySimpleRecord$num_value_2"));
            assertThat(e.getMessage(), containsString("Index MySimpleRecord$num_value_2 not defined"));
        }
    }

    @Test
    public void updateRecordsWithLocalFileDescriptor() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));

            // Local file descriptor is not as evolved as the to-be updating records descriptor. It will fail.
            metaDataStore.setLocalFileDescriptor(TestRecords1Proto.getDescriptor());
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor()));
            assertEquals(e.getMessage(), "record type removed from union");

            metaDataStore.setLocalFileDescriptor(TestRecords1EvolvedAgainProto.getDescriptor());
            metaDataStore.updateRecords(TestRecords1EvolvedProto.getDescriptor());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyOtherRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("AnotherRecord"));
            e = assertThrows(MetaDataException.class, () -> metaDataStore.getRecordMetaData().getRecordType("OneMoreRecord"));
            assertEquals(e.getMessage(), "Unknown record type OneMoreRecord");
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyOtherRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("AnotherRecord"));
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.getRecordMetaData().getRecordType("OneMoreRecord"));
            assertEquals(e.getMessage(), "Unknown record type OneMoreRecord");
            context.commit();
        }
    }

    @Test
    public void extensionRegistry() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            // Default registry parses options
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            Descriptors.Descriptor mySimpleRecordDescriptor = metaData.getRecordType("MySimpleRecord").getDescriptor();
            assertNotSame(mySimpleRecordDescriptor.getFile(), TestRecords1Proto.getDescriptor());
            RecordMetaDataOptionsProto.FieldOptions recNoFieldOptions = mySimpleRecordDescriptor.findFieldByName("rec_no")
                    .getOptions().getExtension(RecordMetaDataOptionsProto.field);
            assertNotNull(recNoFieldOptions);
            assertThat(recNoFieldOptions.getPrimaryKey(), is(true));

            // Empty registry does not
            openMetaDataStore(context);
            metaDataStore.setExtensionRegistry(ExtensionRegistry.getEmptyRegistry());
            metaData = metaDataStore.getRecordMetaData();
            mySimpleRecordDescriptor = metaData.getRecordType("MySimpleRecord").getDescriptor();
            assertNotSame(mySimpleRecordDescriptor.getFile(), TestRecords1Proto.getDescriptor());
            assertThat(mySimpleRecordDescriptor.findFieldByName("rec_no").getOptions().hasExtension(RecordMetaDataOptionsProto.field), is(false));

            // Null registry behaves like the empty registry in proto2 and throws an exception in proto3
            openMetaDataStore(context);
            metaDataStore.setExtensionRegistry(null);
            if (ProtoVersionSupplier.getProtoVersion() == 2) {
                metaData = metaDataStore.getRecordMetaData();
                mySimpleRecordDescriptor = metaData.getRecordType("MySimpleRecord").getDescriptor();
                assertNotSame(mySimpleRecordDescriptor.getFile(), TestRecords1Proto.getDescriptor());
                assertThat(mySimpleRecordDescriptor.findFieldByName("rec_no").getOptions().hasExtension(RecordMetaDataOptionsProto.field), is(false));
            } else {
                final FDBMetaDataStore finalMetaDataStore = metaDataStore;
                assertThrows(NullPointerException.class, finalMetaDataStore::getRecordMetaData);
            }
        }
    }

    @Test
    public void extensionRegistryWithUnionDescriptor() {
        try (FDBRecordContext context = fdb.openContext()) {
            // test_records_3.proto relies on the union field annotation type
            openMetaDataStore(context);
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords3Proto.getDescriptor());
            builder.getRecordType("MyHierarchicalRecord").setPrimaryKey(Key.Expressions.concatenateFields("parent_path", "child_name"));
            RecordMetaData metaData = builder.build();
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            // Default registry parses options
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertNotSame(metaData.getUnionDescriptor(), TestRecords3Proto.UnionDescriptor.getDescriptor());
            assertEquals(metaData.getUnionDescriptor().toProto(), TestRecords3Proto.UnionDescriptor.getDescriptor().toProto());

            // Empty registry does not
            openMetaDataStore(context);
            metaDataStore.setExtensionRegistry(ExtensionRegistry.getEmptyRegistry());
            final FDBMetaDataStore finalMetaDataStore = metaDataStore;
            MetaDataException e = assertThrows(MetaDataException.class, finalMetaDataStore::getRecordMetaData);
            assertThat(e.getMessage(), containsString("Union descriptor is required"));

            // Null registry behaves like the empty registry in proto2 and throws an exception in proto3
            openMetaDataStore(context);
            metaDataStore.setExtensionRegistry(null);
            if (ProtoVersionSupplier.getProtoVersion() == 2) {
                final FDBMetaDataStore secondFinalMetaDataStore = metaDataStore;
                e = assertThrows(MetaDataException.class, secondFinalMetaDataStore::getRecordMetaData);
                assertThat(e.getMessage(), containsString("Union descriptor is required"));
            } else {
                final FDBMetaDataStore secondFinalMetaDataStore = metaDataStore;
                assertThrows(NullPointerException.class, secondFinalMetaDataStore::getRecordMetaData);
            }
        }
    }

    private void addRecordType(@Nonnull DescriptorProtos.DescriptorProto newRecordType, @Nonnull KeyExpression primaryKey) {
        metaDataStore.mutateMetaData(metaDataProto -> MetaDataProtoEditor.addRecordType(metaDataProto, newRecordType, primaryKey));
    }

    private void addRecordType(@Nonnull DescriptorProtos.DescriptorProto newRecordType, @Nonnull KeyExpression primaryKey, @Nonnull Index index) {
        metaDataStore.mutateMetaData(metaDataProto -> MetaDataProtoEditor.addRecordType(metaDataProto, newRecordType, primaryKey),
                recordMetaDataBuilder -> recordMetaDataBuilder.addIndex(newRecordType.getName(), index));
    }

    private void deprecateRecordType(@Nonnull String recordType) {
        metaDataStore.mutateMetaData((metaDataProto) -> MetaDataProtoEditor.deprecateRecordType(metaDataProto, recordType));
    }

    private void addField(@Nonnull String recordType, @Nonnull DescriptorProtos.FieldDescriptorProto field) {
        metaDataStore.mutateMetaData((metaDataProto) -> MetaDataProtoEditor.addField(metaDataProto, recordType, field));
    }

    private void deprecateField(@Nonnull String recordType, @Nonnull String fieldName) {
        metaDataStore.mutateMetaData((metaDataProto) -> MetaDataProtoEditor.deprecateField(metaDataProto, recordType, fieldName));
    }

    private void renameRecordType(@Nonnull String recordType, @Nonnull String newRecordTypeName) {
        metaDataStore.mutateMetaData((metaDataProto) -> MetaDataProtoEditor.renameRecordType(metaDataProto, recordType, newRecordTypeName));
    }

    private static void assertDeprecated(@Nonnull RecordMetaData metaData, @Nonnull String recordType) {
        RecordType recordTypeObj = metaData.getRecordType(recordType);
        assertTrue(metaData.getUnionFieldForRecordType(recordTypeObj).getOptions().getDeprecated());
    }

    @Test
    public void recordTypes() {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            version = metaData.getVersion();
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        // Add an existing record type.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version , metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MySimpleRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            MetaDataException e = assertThrows(MetaDataException.class, () -> addRecordType(newRecordType, Key.Expressions.field("rec_no")));
            assertEquals(e.getMessage(), "Record type MySimpleRecord already exists");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion()); // version should not change
            context.commit();
        }

        // Add a record type.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version , metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MyNewRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            addRecordType(newRecordType, Key.Expressions.field("rec_no"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getRecordType("MyNewRecord").getSinceVersion().intValue());
            context.commit();
        }

        // The old local file descriptor does not have the new record type. Using it should fail.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.setLocalFileDescriptor(TestRecords1Proto.getDescriptor());
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.getRecordMetaData());
            assertEquals("record type removed from union", e.getMessage());
            context.commit();
        }

        // Add a record type with index.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MyNewRecordWithIndex")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            addRecordType(newRecordType, Key.Expressions.field("rec_no"), new Index("MyNewRecordWithIndex$index", "rec_no"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecordWithIndex"));
            assertNotNull(metaDataStore.getRecordMetaData().getIndex("MyNewRecordWithIndex$index"));
            assertEquals(version + 2, metaDataStore.getRecordMetaData().getRecordType("MyNewRecordWithIndex").getSinceVersion().intValue());
            assertEquals(version + 3, metaDataStore.getRecordMetaData().getVersion()); // +1 because of the index.
            context.commit();
        }

        // Deprecate the just-added record types.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 3, metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecordWithIndex"));
            deprecateRecordType("MyNewRecord");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertDeprecated(metaDataStore.getRecordMetaData(), "MyNewRecord");
            assertEquals(version + 4, metaDataStore.getRecordMetaData().getVersion());
            deprecateRecordType("MyNewRecordWithIndex");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecordWithIndex"));
            assertDeprecated(metaDataStore.getRecordMetaData(), "MyNewRecordWithIndex");
            assertEquals(version + 5, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Deprecate a record type from the original proto.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 5, metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            deprecateRecordType("MySimpleRecord");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertDeprecated(metaDataStore.getRecordMetaData(), "MySimpleRecord");
            assertEquals(version + 6, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Deprecate a non-existent record type.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 6, metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertDeprecated(metaDataStore.getRecordMetaData(), "MySimpleRecord");
            MetaDataException e = assertThrows(MetaDataException.class, () -> deprecateRecordType("MyNonExistentRecord"));
            assertEquals(e.getMessage(), "Record type MyNonExistentRecord not found");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version + 6, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }
    }

    @Test
    public void deprecateImportedRecordType() {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsImportProto.getDescriptor());
            version = metaData.getVersion();
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        // Deprecate the record type
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(version, metaData.getVersion());
            assertNotNull(metaData.getRecordType("MySimpleRecord"));
            assertNotNull(metaData.getRecordType("MyLongRecord"));
            MetaDataException e = assertThrows(MetaDataException.class, () -> deprecateRecordType("MySimpleRecord"));
            assertEquals("Record type MySimpleRecord not found", e.getMessage());
        }
    }

    @Test
    public void deprecateWithNestedRecordType() {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsNestedAsRecord.getDescriptor());
            version = metaData.getVersion();
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }

        // Deprecate OuterRecord
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(version, metaData.getVersion());
            deprecateRecordType("OuterRecord");
            RecordMetaData newMetaData = metaDataStore.getRecordMetaData();
            assertThat(newMetaData.getVersion(), greaterThan(version));
            assertDeprecated(newMetaData, "OuterRecord");
            assertDeprecated(newMetaData, "InnerRecord");
            context.commit();
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertThat(metaData.getVersion(), greaterThan(version));
            assertDeprecated(metaData, "OuterRecord");
            assertDeprecated(metaData, "InnerRecord");
        }
    }

    @Test
    public void unionRecordTypes() {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            version = metaData.getVersion();
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        // Add a record type with default union name.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.DescriptorProto unionRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName(RecordMetaDataBuilder.DEFAULT_UNION_NAME)
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            MetaDataException e = assertThrows(MetaDataException.class, () -> addRecordType(unionRecordType, Key.Expressions.field("rec_no")));
            assertEquals(e.getMessage(), "Adding UNION record type not allowed");
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion()); // version must remain unchanged
            context.commit();
        }

        // Add a record type with non-default union name but union usage.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.MessageOptions.Builder unionMessageOptions = DescriptorProtos.MessageOptions.newBuilder()
                    .setExtension(RecordMetaDataOptionsProto.record,
                            RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder()
                                    .setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION).build());
            DescriptorProtos.DescriptorProto unionRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("NonDefaultUnionRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .setOptions(unionMessageOptions)
                    .build();
            MetaDataException e = assertThrows(MetaDataException.class, () -> addRecordType(unionRecordType, Key.Expressions.field("rec_no")));
            assertEquals(e.getMessage(), "Adding UNION record type not allowed");
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion()); // version must remain unchanged
            context.commit();
        }

        // Deprecate the record type union.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            MetaDataException e = assertThrows(MetaDataException.class, () -> deprecateRecordType(RecordMetaDataBuilder.DEFAULT_UNION_NAME));
            assertEquals(e.getMessage(), "Cannot deprecate the union");
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion()); // version must remain unchanged
            context.commit();
        }
    }

    @Test
    public void nonDefaultUnionRecordTypes() {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords3Proto.getDescriptor());
            metaDataBuilder.getOnlyRecordType().setPrimaryKey(Key.Expressions.concatenateFields("parent_path", "child_name"));
            RecordMetaData metaData = metaDataBuilder.getRecordMetaData();
            version = metaData.getVersion();
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
        }

        // Add a record type with the default union name to a record meta-data that has a non-default union name.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.DescriptorProto unionRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName(RecordMetaDataBuilder.DEFAULT_UNION_NAME)
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            MetaDataException e = assertThrows(MetaDataException.class, () -> addRecordType(unionRecordType, Key.Expressions.field("rec_no")));
            assertEquals(e.getMessage(), "Adding UNION record type not allowed");
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion()); // version must remain unchanged
            context.commit();
        }

        // Add a second record type with non-default union name but union usage.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.MessageOptions.Builder unionMessageOptions = DescriptorProtos.MessageOptions.newBuilder()
                    .setExtension(RecordMetaDataOptionsProto.record,
                            RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder()
                                    .setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION).build());
            DescriptorProtos.DescriptorProto unionRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("SecondNonDefaultUnionRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .setOptions(unionMessageOptions)
                    .build();
            MetaDataException e = assertThrows(MetaDataException.class, () -> addRecordType(unionRecordType, Key.Expressions.field("rec_no")));
            assertEquals(e.getMessage(), "Adding UNION record type not allowed");
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion()); // version must remain unchanged
            context.commit();
        }

        // Deprecate the record type union with non-default name.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            MetaDataException e = assertThrows(MetaDataException.class, () -> deprecateRecordType("UnionDescriptor"));
            assertEquals(e.getMessage(), "Cannot deprecate the union");
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion()); // version must remain unchanged
            context.commit();
        }

        // Add a record type to a meta-data that has non-default union name.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version , metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MyNewRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            addRecordType(newRecordType, Key.Expressions.field("rec_no"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getVersion());
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getRecordType("MyNewRecord").getSinceVersion().intValue());
            context.commit();
        }

        // Deprecate a record type from a meta-data that has non-default union name.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            deprecateRecordType("MyHierarchicalRecord");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            assertTrue(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("UnionDescriptor").findFieldByName("_MyHierarchicalRecord").getOptions().getDeprecated());
            assertEquals(version + 2, metaDataStore.getRecordMetaData().getVersion());
            // do not commit
        }

        // Validate that deprecation can happen when the type is fully qualified
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            deprecateRecordType(".com.apple.foundationdb.record.test3.MyHierarchicalRecord");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyHierarchicalRecord"));
            assertTrue(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("UnionDescriptor").findFieldByName("_MyHierarchicalRecord").getOptions().getDeprecated());
            assertEquals(version + 2, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }
    }

    private void addNestedRecordType(DescriptorProtos.DescriptorProto newRecordType) {
        metaDataStore.mutateMetaData((metaDataProto) -> MetaDataProtoEditor.addNestedRecordType(metaDataProto, newRecordType));
    }

    @Test
    public void nestedRecordTypes() {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            version = metaData.getVersion();
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        // Adding an existing record type should fail
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version , metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.MessageOptions.Builder nestedMessageOptions = DescriptorProtos.MessageOptions.newBuilder()
                    .setExtension(RecordMetaDataOptionsProto.record,
                            RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder()
                                    .setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED).build());
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MySimpleRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .setOptions(nestedMessageOptions)
                    .build();
            MetaDataException e = assertThrows(MetaDataException.class, () -> addNestedRecordType(newRecordType));
            assertEquals(e.getMessage(), "Record type MySimpleRecord already exists");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion()); // version should not change
            context.commit();
        }

        // Add a nested record type.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.MessageOptions.Builder nestedMessageOptions = DescriptorProtos.MessageOptions.newBuilder()
                    .setExtension(RecordMetaDataOptionsProto.record,
                            RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder()
                                    .setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.NESTED).build());
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MyNewNestedRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .setOptions(nestedMessageOptions)
                    .build();

            // Use addRecordType should fail
            MetaDataException e = assertThrows(MetaDataException.class, () -> addRecordType(newRecordType, Key.Expressions.field("rec_no")));
            assertEquals(e.getMessage(), "Use addNestedRecordType for adding NESTED record types");

            // Use addNestedRecordType should succeed
            addNestedRecordType(newRecordType);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            e = assertThrows(MetaDataException.class, () -> metaDataStore.getRecordMetaData().getRecordType("MyNewNestedRecord"));
            assertEquals(e.getMessage(), "Unknown record type MyNewNestedRecord");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MyNewNestedRecord"));
            assertNull(metaDataStore.getRecordMetaData().getRecordsDescriptor()
                    .findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME)
                    .findFieldByName("_MyNewNestedRecord"));
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());

            // addNestedRecordType is not idempotent!
            e = assertThrows(MetaDataException.class, () -> addNestedRecordType(newRecordType));
            assertEquals(e.getMessage(), "Record type MyNewNestedRecord already exists");
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Add nested type as a field
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MyNewNestedRecord"));
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getVersion());

            DescriptorProtos.FieldDescriptorProto field = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("newField")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName("MyNewNestedRecord")
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setNumber(10)
                    .build();
            addField("MySimpleRecord", field);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MySimpleRecord").findFieldByName("newField"));
            assertEquals(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MyNewNestedRecord"),
                    metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MySimpleRecord").findFieldByName("newField").getMessageType());
            assertEquals(version + 2 , metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Deprecate a nested record type.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 2 , metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MyNewNestedRecord"));
            deprecateField("MySimpleRecord", "newField");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertTrue(metaDataStore.getRecordMetaData().getRecordsDescriptor()
                    .findMessageTypeByName("MySimpleRecord")
                    .findFieldByName("newField")
                    .getOptions()
                    .getDeprecated());
            assertEquals(version + 3, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }
    }

    @Test
    public void fields() {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            version = metaData.getVersion();
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
        }

        // Add a new field
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version , metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.FieldDescriptorProto field = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("newField")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setNumber(10)
                    .build();
            addField("MySimpleRecord", field);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MySimpleRecord").findFieldByName("newField"));
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getVersion());

            // Add it again should fail
            MetaDataException e = assertThrows(MetaDataException.class, () -> addField("MySimpleRecord", field));
            assertEquals(e.getMessage(), "Field newField already exists in record type MySimpleRecord");
            context.commit();
        }

        // Add a field with non-existent record type
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.FieldDescriptorProto field = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("newFieldWithNonExistentRecordType")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName("NonExistentType")
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                    .setNumber(10)
                    .build();
            MetaDataException e = assertThrows(MetaDataException.class, () -> addField("MySimpleRecord", field));
            assertEquals(e.getMessage(), "Error converting from protobuf");
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Deprecate field should fail if record type or field does not exist
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());

            MetaDataException e = assertThrows(MetaDataException.class, () -> deprecateField("NonExistentRecordType", "field"));
            assertEquals(e.getMessage(), "Record type NonExistentRecordType does not exist");

            e = assertThrows(MetaDataException.class, () -> deprecateField("MySimpleRecord", "nonExistentField"));
            assertEquals(e.getMessage(), "Field nonExistentField not found in record type MySimpleRecord");
            context.commit();
        }

        // Deprecate a field
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MySimpleRecord").findFieldByName("num_value_2"));
            assertFalse(metaDataStore.getRecordMetaData()
                    .getRecordsDescriptor()
                    .findMessageTypeByName("MySimpleRecord")
                    .findFieldByName("num_value_2")
                    .getOptions()
                    .hasDeprecated());
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getVersion());
            deprecateField("MySimpleRecord", "num_value_2");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertTrue(metaDataStore.getRecordMetaData()
                    .getRecordsDescriptor()
                    .findMessageTypeByName("MySimpleRecord")
                    .findFieldByName("num_value_2")
                    .getOptions()
                    .getDeprecated());
            assertEquals(version + 2 , metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Deprecate the newly added field
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName("MySimpleRecord").findFieldByName("newField"));
            assertFalse(metaDataStore.getRecordMetaData()
                    .getRecordsDescriptor()
                    .findMessageTypeByName("MySimpleRecord")
                    .findFieldByName("newField")
                    .getOptions()
                    .hasDeprecated());
            assertEquals(version + 2 , metaDataStore.getRecordMetaData().getVersion());
            deprecateField("MySimpleRecord", "newField");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertTrue(metaDataStore.getRecordMetaData()
                    .getRecordsDescriptor()
                    .findMessageTypeByName("MySimpleRecord")
                    .findFieldByName("newField")
                    .getOptions()
                    .getDeprecated());
            assertEquals(version + 3 , metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }
    }

    @Test
    public void updateSchemaOptions() {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            version = metaDataStore.getRecordMetaData().getVersion();
            context.commit();
        }

        // Unset store record versions.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertTrue(metaDataStore.getRecordMetaData().isStoreRecordVersions());
            metaDataStore.updateStoreRecordVersions(false);
            context.commit();
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getVersion());
            assertFalse(metaDataStore.getRecordMetaData().isStoreRecordVersions());
        }

        // Set store record versions.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertFalse(metaDataStore.getRecordMetaData().isStoreRecordVersions());
            metaDataStore.updateStoreRecordVersions(true);
            context.commit();
            assertEquals(version + 2 , metaDataStore.getRecordMetaData().getVersion());
            assertTrue(metaDataStore.getRecordMetaData().isStoreRecordVersions());
        }

        // Enable split long records with default validator. It should fail.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertFalse(metaDataStore.getRecordMetaData().isSplitLongRecords());
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.enableSplitLongRecords());
            assertEquals(e.getMessage(), "new meta-data splits long records");
            context.commit();
            assertEquals(version + 2 , metaDataStore.getRecordMetaData().getVersion());
            assertTrue(metaDataStore.getRecordMetaData().isStoreRecordVersions());
        }

        // Enable split long records
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.setEvolutionValidator(MetaDataEvolutionValidator.newBuilder().setAllowUnsplitToSplit(true).build());
            assertFalse(metaDataStore.getRecordMetaData().isSplitLongRecords());
            metaDataStore.enableSplitLongRecords();
            context.commit();
            assertEquals(version + 3 , metaDataStore.getRecordMetaData().getVersion());
            assertTrue(metaDataStore.getRecordMetaData().isStoreRecordVersions());
        }

        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertTrue(metaDataStore.getRecordMetaData().isStoreRecordVersions());
            assertEquals(version + 3 , metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }
    }

    @Test
    public void recordTypesWithOneOfUnion() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaDataBuilder metaData = RecordMetaData.newBuilder().setRecords(TestRecordsOneOfProto.getDescriptor());
            final KeyExpression pkey = Key.Expressions.field("rec_no");
            metaData.getRecordType("MySimpleRecord").setPrimaryKey(pkey);
            metaData.getRecordType("MyOtherRecord").setPrimaryKey(pkey);
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }

        // Add a record type to oneOf. It should fail.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MyNewRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            MetaDataException e = assertThrows(MetaDataException.class, () -> addRecordType(newRecordType, Key.Expressions.field("rec_no")));
            assertEquals(e.getMessage(), "Adding record type to oneof is not allowed");
            context.commit();
        }
    }

    private enum TestProtoFiles {
        NO_UNION(TestNoUnionProto.getDescriptor()),
        DEFAULT_UNION(TestRecords1Proto.getDescriptor()),
        NON_DEFAULT_UNION(TestRecords4Proto.getDescriptor());

        private Descriptors.FileDescriptor fileDescriptor;

        TestProtoFiles(Descriptors.FileDescriptor fileDescriptor) {
            this.fileDescriptor = fileDescriptor;
        }

        @Nonnull
        public Descriptors.FileDescriptor getFileDescriptor() {
            return fileDescriptor;
        }
    }

    @EnumSource(TestProtoFiles.class)
    @ParameterizedTest(name = "noUnion [protoFile = {0}]")
    public void noUnion(@Nonnull TestProtoFiles protoFile) {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(protoFile.getFileDescriptor());
            context.commit();
            version = metaDataStore.getRecordMetaData().getVersion();
        }

        // Add a record type.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MyNewRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            addRecordType(newRecordType, Key.Expressions.field("rec_no"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getRecordType("MyNewRecord").getSinceVersion().intValue());
            context.commit();
        }
    }

    @ParameterizedTest(name = "noUnionUpdateRecords [repeatSaveOrDoUpdate = {0}]")
    @BooleanSource
    public void noUnionUpdateRecords(boolean repeatSaveOrDoUpdate) {
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(TestNoUnionProto.getDescriptor());
            context.commit();
            version = metaDataStore.getRecordMetaData().getVersion();
        }

        // Update records with an evolved proto. A new record type is added earlier in the file.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            if (repeatSaveOrDoUpdate) {
                metaDataStore.updateRecords(TestNoUnionEvolvedProto.getDescriptor());
            } else {
                metaDataStore.saveRecordMetaData(TestNoUnionEvolvedProto.getDescriptor());
            }
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyOtherRecord"));
            assertEquals("_MySimpleRecord", metaDataStore.getRecordMetaData().getUnionDescriptor().findFieldByNumber(1).getName());
            assertEquals("_MyOtherRecord", metaDataStore.getRecordMetaData().getUnionDescriptor().findFieldByNumber(2).getName());
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Renaming a record type is not allowed
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            MetaDataException e;
            if (repeatSaveOrDoUpdate) {
                e = assertThrows(MetaDataException.class, () -> metaDataStore.updateRecords(TestNoUnionEvolvedRenamedRecordTypeProto.getDescriptor()));
            } else {
                e = assertThrows(MetaDataException.class, () -> metaDataStore.saveRecordMetaData(TestNoUnionEvolvedRenamedRecordTypeProto.getDescriptor()));
            }
            assertEquals("Record type MySimpleRecord removed", e.getMessage());
            context.commit();
        }
    }

    @ParameterizedTest(name = "noUnionImplicitUsage [repeatSaveOrDoUpdate = {0}]")
    @BooleanSource
    public void noUnionImplicitUsage(boolean repeatSaveOrDoUpdate) {
        int version;

        // MyOtherRecord has no explicit usage. The proto file has a union and does not include MyOtherRecord.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(TestRecordsImplicitUsageProto.getDescriptor());
            context.commit();
            version = metaDataStore.getRecordMetaData().getVersion();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.getRecordMetaData().getRecordType("MyOtherRecord"));
            assertEquals(e.getMessage(), "Unknown record type MyOtherRecord");
        }

        // The evolved proto no longer has a union. The record types with no explicit usage should now automatically show up in the union.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version, metaDataStore.getRecordMetaData().getVersion());
            if (repeatSaveOrDoUpdate) {
                metaDataStore.updateRecords(TestRecordsImplicitUsageNoUnionProto.getDescriptor());
            } else {
                metaDataStore.saveRecordMetaData(TestRecordsImplicitUsageNoUnionProto.getDescriptor());
            }
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyOtherRecord"));
            assertEquals(version + 1, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }
    }


    @Test
    public void noUnionLocalFileDescriptor() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.saveRecordMetaData(TestNoUnionProto.getDescriptor());
            context.commit();
        }

        // The type cannot become NESTED in the local file descriptor
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.setLocalFileDescriptor(TestNoUnionEvolvedIllegalProto.getDescriptor());
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.getRecordMetaData());
            assertEquals("record type removed from union", e.getMessage());
            context.commit();
        }

        // Deprecate a record type.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            deprecateRecordType(".com.apple.foundationdb.record.testnounion.MySimpleRecord");
            context.commit();
        }

        // Pass a local file descriptor and make sure MySimpleRecord's deprecated.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.setLocalFileDescriptor(TestNoUnionProto.getDescriptor());
            Descriptors.FieldDescriptor deprecatedField = metaDataStore.getRecordMetaData().getUnionDescriptor().getFields().get(0);
            assertEquals("_MySimpleRecord", deprecatedField.getName());
            assertTrue(deprecatedField.getOptions().getDeprecated());
            context.commit();
        }
    }

    /**
     * A basic test to verify that basic renaming works.
     */
    @Test
    public void renameSimpleRecordType() {
        List<Index> simpleRecordIndexes;
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            simpleRecordIndexes = metaData.getRecordType("MySimpleRecord").getIndexes();
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType("MySimpleRecord", "MyNewSimpleRecord");
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            Descriptors.FieldDescriptor simpleField = metaData.getUnionDescriptor().findFieldByNumber(TestRecords1Proto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER);
            assertSame(metaData.getRecordType("MyNewSimpleRecord").getDescriptor(), simpleField.getMessageType());
            assertEquals("MyNewSimpleRecord", simpleField.getMessageType().getName());
            assertEquals("_MyNewSimpleRecord", simpleField.getName());
            assertEquals(ImmutableSet.of("MyNewSimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaData.getRecordType("MySimpleRecord"));
            assertEquals("Unknown record type MySimpleRecord", e.getMessage());
            assertEquals(simpleRecordIndexes.stream().map(Index::getName).collect(Collectors.toSet()),
                    metaData.getRecordType("MyNewSimpleRecord").getAllIndexes().stream().map(Index::getName).collect(Collectors.toSet()));
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            Descriptors.FieldDescriptor simpleField = metaData.getUnionDescriptor().findFieldByNumber(TestRecords1Proto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER);
            assertSame(metaData.getRecordType("MyNewSimpleRecord").getDescriptor(), simpleField.getMessageType());
            assertEquals("MyNewSimpleRecord", simpleField.getMessageType().getName());
            assertEquals("_MyNewSimpleRecord", simpleField.getName());
            assertEquals(ImmutableSet.of("MyNewSimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaData.getRecordType("MySimpleRecord"));
            assertEquals("Unknown record type MySimpleRecord", e.getMessage());
            assertEquals(simpleRecordIndexes.stream().map(Index::getName).collect(Collectors.toSet()),
                    metaData.getRecordType("MyNewSimpleRecord").getAllIndexes().stream().map(Index::getName).collect(Collectors.toSet()));
            context.commit();
        }
    }

    /**
     * Test whether fully qualifying a record type name works.
     */
    @Test
    public void renameFullyQualifiedSimpleRecordType() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            // In theory, fully qualifying the name could work, but it doesn't as implemented.
            MetaDataException e = assertThrows(MetaDataException.class, () -> renameRecordType(".com.apple.foundationdb.record.test1.MySimpleRecord", "MyNewSimpleRecord"));
            assertThat(e.getMessage(), containsString("No record type found"));
        }
    }

    /**
     * Verify that if a record appears multiple times in the union that (1) all of the appearances are changed to the new
     * type and that (2) only one type is renamed.
     */
    @Test
    public void renameSimpleWithMultipleUnionAppearances() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            metaDataStore.mutateMetaData(metaDataProtoBuilder ->
                    metaDataProtoBuilder.getRecordsBuilder().getMessageTypeBuilderList().forEach(messageType -> {
                        if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                            // Rename the current _MySimpleRecord field
                            messageType.getFieldBuilderList().forEach(field -> {
                                if (field.getName().equals("_MySimpleRecord")) {
                                    field.setName("_MySimpleRecord_v1");
                                }
                            });
                            messageType.addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                    .setTypeName("." + TestRecords1Proto.MySimpleRecord.getDescriptor().getFullName())
                                    .setName("_MySimpleRecord")
                                    .setNumber(messageType.getFieldBuilderList().stream().mapToInt(DescriptorProtos.FieldDescriptorProtoOrBuilder::getNumber).max().orElse(0) + 1)
                                    .build());
                        }
                    })
            );
            metaData = metaDataStore.getRecordMetaData();
            assertEquals(TestRecords1Proto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER,
                    metaData.getUnionDescriptor().findFieldByName("_MySimpleRecord_v1").getNumber());
            assertEquals(metaData.getUnionFieldForRecordType(metaData.getRecordType("MySimpleRecord")).getNumber(),
                    metaData.getUnionDescriptor().findFieldByName("_MySimpleRecord").getNumber());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType("MySimpleRecord", "MyNewSimpleRecord");
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(ImmutableSet.of("MyNewSimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            assertEquals(ImmutableSet.of("_MyNewSimpleRecord", "_MySimpleRecord_v1", "_MyOtherRecord"),
                    metaData.getUnionDescriptor().getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toSet()));
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(ImmutableSet.of("MyNewSimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            assertEquals(ImmutableSet.of("_MyNewSimpleRecord", "_MySimpleRecord_v1", "_MyOtherRecord"),
                    metaData.getUnionDescriptor().getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toSet()));
            context.commit();
        }
    }

    /**
     * This is somewhat of a weird case, but validate that if there is a union field for the new record type name that
     * looks like _NewRecordTypeName (for whatever reason), <em>don't</em> rename the union field to that because getting
     * the name looking right isn't worth throwing an error.
     */
    @Test
    public void renameSimpleWhereUnionFieldIsAlreadyTaken() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            metaDataStore.mutateMetaData(metaDataProtoBuilder ->
                    metaDataProtoBuilder.getRecordsBuilder().getMessageTypeBuilderList().forEach(messageType -> {
                        if (messageType.getName().equals(RecordMetaDataBuilder.DEFAULT_UNION_NAME)) {
                            // Rename the current _MySimpleRecord field
                            messageType.getFieldBuilderList().forEach(field -> {
                                if (field.getName().equals("_MyOtherRecord")) {
                                    field.setName("_MyNewSimpleRecord");
                                }
                            });
                        }
                    })
            );
            metaData = metaDataStore.getRecordMetaData();
            assertEquals(TestRecords1Proto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER,
                    metaData.getUnionDescriptor().findFieldByName("_MySimpleRecord").getNumber());
            assertEquals(TestRecords1Proto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER,
                    metaData.getUnionFieldForRecordType(metaData.getRecordType("MySimpleRecord")).getNumber());
            assertEquals(TestRecords1Proto.RecordTypeUnion._MYOTHERRECORD_FIELD_NUMBER,
                    metaData.getUnionDescriptor().findFieldByName("_MyNewSimpleRecord").getNumber());
            assertEquals(TestRecords1Proto.RecordTypeUnion._MYOTHERRECORD_FIELD_NUMBER,
                    metaData.getUnionFieldForRecordType(metaData.getRecordType("MyOtherRecord")).getNumber());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType("MySimpleRecord", "MyNewSimpleRecord");
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(ImmutableSet.of("MyNewSimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            assertEquals(ImmutableSet.of("_MySimpleRecord", "_MyNewSimpleRecord"), metaData.getUnionDescriptor().getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toSet()));
            assertEquals("_MySimpleRecord", metaData.getUnionFieldForRecordType(metaData.getRecordType("MyNewSimpleRecord")).getName());
            assertEquals("_MyNewSimpleRecord", metaData.getUnionFieldForRecordType(metaData.getRecordType("MyOtherRecord")).getName());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(ImmutableSet.of("MyNewSimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            assertEquals(ImmutableSet.of("_MySimpleRecord", "_MyNewSimpleRecord"), metaData.getUnionDescriptor().getFields().stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toSet()));
            assertEquals("_MySimpleRecord", metaData.getUnionFieldForRecordType(metaData.getRecordType("MyNewSimpleRecord")).getName());
            assertEquals("_MyNewSimpleRecord", metaData.getUnionFieldForRecordType(metaData.getRecordType("MyOtherRecord")).getName());
            context.commit();
        }
    }

    /**
     * Rename a {@code NESTED} record type.
     */
    @Test
    public void renameNestedRecordType() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords4Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        // Rename a type that is marked as NESTED
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType("RestaurantTag", "RestoTag");
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            Descriptors.FileDescriptor recordsDescriptor = metaData.getRecordsDescriptor();
            assertEquals(ImmutableSet.of("RestaurantReviewer", "ReviewerStats", "RestaurantReview", "RestoTag", "RestaurantRecord", "UnionDescriptor"),
                    recordsDescriptor.getMessageTypes().stream().map(Descriptors.Descriptor::getName).collect(Collectors.toSet()));
            Descriptors.FieldDescriptor tagsField = metaData.getRecordType("RestaurantRecord").getDescriptor().findFieldByName("tags");
            assertEquals("RestoTag", tagsField.getMessageType().getName());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            Descriptors.FileDescriptor recordsDescriptor = metaData.getRecordsDescriptor();
            assertEquals(ImmutableSet.of("RestaurantReviewer", "ReviewerStats", "RestaurantReview", "RestoTag", "RestaurantRecord", "UnionDescriptor"),
                    recordsDescriptor.getMessageTypes().stream().map(Descriptors.Descriptor::getName).collect(Collectors.toSet()));
            Descriptors.FieldDescriptor tagsField = metaData.getRecordType("RestaurantRecord").getDescriptor().findFieldByName("tags");
            assertEquals("RestoTag", tagsField.getMessageType().getName());
        }
        // Rename a nested type that doesn't have any explicit (record).usage
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType("ReviewerStats", "ReviewerStatistics");
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            Descriptors.FileDescriptor recordsDescriptor = metaData.getRecordsDescriptor();
            assertEquals(ImmutableSet.of("RestaurantReviewer", "ReviewerStatistics", "RestaurantReview", "RestoTag", "RestaurantRecord", "UnionDescriptor"),
                    recordsDescriptor.getMessageTypes().stream().map(Descriptors.Descriptor::getName).collect(Collectors.toSet()));
            Descriptors.FieldDescriptor stats = metaData.getRecordType("RestaurantReviewer").getDescriptor().findFieldByName("stats");
            assertEquals("ReviewerStatistics", stats.getMessageType().getName());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            Descriptors.FileDescriptor recordsDescriptor = metaData.getRecordsDescriptor();
            assertEquals(ImmutableSet.of("RestaurantReviewer", "ReviewerStatistics", "RestaurantReview", "RestoTag", "RestaurantRecord", "UnionDescriptor"),
                    recordsDescriptor.getMessageTypes().stream().map(Descriptors.Descriptor::getName).collect(Collectors.toSet()));
            Descriptors.FieldDescriptor stats = metaData.getRecordType("RestaurantReviewer").getDescriptor().findFieldByName("stats");
            assertEquals("ReviewerStatistics", stats.getMessageType().getName());
        }
    }

    /**
     * Validate that the union can be renamed.
     */
    @Test
    public void renameUnion() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        // Switch it from the default name to something else
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType(RecordMetaDataBuilder.DEFAULT_UNION_NAME, "RecordsOneUnion");
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals("RecordsOneUnion", metaData.getUnionDescriptor().getName());
            assertEquals(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals("RecordsOneUnion", metaData.getUnionDescriptor().getName());
            assertEquals(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
        }
        // Switch it back to the default name
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType("RecordsOneUnion", RecordMetaDataBuilder.DEFAULT_UNION_NAME);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(RecordMetaDataBuilder.DEFAULT_UNION_NAME, metaData.getUnionDescriptor().getName());
            assertEquals(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(RecordMetaDataBuilder.DEFAULT_UNION_NAME, metaData.getUnionDescriptor().getName());
            assertEquals(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
        }
    }

    private void unqualify(@Nonnull String packageName, @Nonnull String newPackage, @Nonnull DescriptorProtos.DescriptorProto.Builder messageTypeBuilder) {
        for (DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder : messageTypeBuilder.getFieldBuilderList()) {
            if (fieldBuilder.hasTypeName()) {
                String withoutPrefix;
                if (fieldBuilder.getTypeName().startsWith("." + packageName + ".")) {
                    withoutPrefix = fieldBuilder.getTypeName().substring(2 + packageName.length());
                } else if (fieldBuilder.getTypeName().startsWith(packageName + ".")) {
                    withoutPrefix = fieldBuilder.getTypeName().substring(1 + packageName.length());
                } else {
                    withoutPrefix = "";
                }
                if (!withoutPrefix.isEmpty()) {
                    String newTypeName;
                    if (newPackage.isEmpty()) {
                        newTypeName = withoutPrefix;
                    } else  {
                        newTypeName = newPackage + "." + withoutPrefix;
                    }
                    fieldBuilder.setTypeName(newTypeName);
                }
            }
        }
        for (DescriptorProtos.DescriptorProto.Builder nestedTypeBuilder : messageTypeBuilder.getNestedTypeBuilderList()) {
            unqualify(packageName, newPackage, nestedTypeBuilder);
        }
    }

    private void unqualify(@Nonnull String newPackage, @Nonnull DescriptorProtos.FileDescriptorProto.Builder fileBuilder) {
        for (DescriptorProtos.DescriptorProto.Builder messageTypeBuilder : fileBuilder.getMessageTypeBuilderList()) {
            unqualify(fileBuilder.getPackage(), newPackage, messageTypeBuilder);
        }
    }

    @Test
    public void ambiguousTypes() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsDoubleNestedProto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            MetaDataProtoEditor.AmbiguousTypeNameException e = assertThrows(MetaDataProtoEditor.AmbiguousTypeNameException.class, () -> metaDataStore.mutateMetaData(protoBuilder -> {
                final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = protoBuilder.getRecordsBuilder();
                unqualify("", fileBuilder);
                MetaDataProtoEditor.renameRecordType(protoBuilder, "OtherRecord", "OtterRecord");
            }));
            assertThat(e.getMessage(), containsString("might be of type .com.apple.foundationdb.record.test.doublenested.OtherRecord"));
            e = assertThrows(MetaDataProtoEditor.AmbiguousTypeNameException.class, () -> metaDataStore.mutateMetaData(protoBuilder -> {
                final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = protoBuilder.getRecordsBuilder();
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (message.getName().equals("OtherRecord")) {
                        unqualify(fileBuilder.getPackage(), "", message);
                    }
                });
                Optional<?> changedField = fileBuilder.getMessageTypeBuilderList().stream()
                        .filter(message -> message.getName().equals("OtherRecord"))
                        .flatMap(message -> message.getFieldBuilderList().stream())
                        .filter(field -> field.getTypeName().equals("OuterRecord"))
                        .findAny();
                assertTrue(changedField.isPresent());
                MetaDataProtoEditor.renameRecordType(protoBuilder, "OuterRecord", "OtterRecord");
            }));
            assertEquals("Field outer in message .com.apple.foundationdb.record.test.doublenested.OtherRecord of type OuterRecord might be of type .com.apple.foundationdb.record.test.doublenested.OuterRecord", e.getMessage());
        }
    }

    /**
     * Make sure the type rename can go all the way down.
     */
    @Test
    public void renameRecordTypeUsageInNested() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsDoubleNestedProto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType("OuterRecord", "OtterRecord");
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(ImmutableSet.of("OtterRecord", "MiddleRecord"), metaData.getRecordTypes().keySet());
            final Descriptors.Descriptor otterDescriptor = metaData.getRecordsDescriptor().findMessageTypeByName("OtterRecord");
            assertSame(otterDescriptor, metaData.getRecordType("OtterRecord").getDescriptor());
            assertSame(otterDescriptor, metaData.getRecordType("OtterRecord").getDescriptor()
                    .findNestedTypeByName("MiddleRecord")
                    .findNestedTypeByName("InnerRecord")
                    .findFieldByName("outer")
                    .getMessageType());
            assertSame(otterDescriptor, metaData.getRecordsDescriptor()
                    .findMessageTypeByName("OtherRecord")
                    .findFieldByName("outer")
                    .getMessageType());
            context.commit();
        }
    }

    /**
     * If there are nested types with a similar name, do not rename the other nested types.
     */
    @Test
    public void doNotRenameSimilarNestedType() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsDoubleNestedProto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.mutateMetaData(protoBuilder -> {
                // Unqualify the OtterRecord.MiddleRecord references in a way where they can be distinguished from globally-scoped MiddleRecords
                final DescriptorProtos.FileDescriptorProto.Builder fileBuilder = protoBuilder.getRecordsBuilder();
                fileBuilder.getMessageTypeBuilderList().forEach(message -> {
                    if (!message.getName().equals("MiddleRecord")) {
                        message.getFieldBuilderList().forEach(field -> {
                            if (field.getTypeName().equals(".com.apple.foundationdb.record.test.doublenested.OuterRecord.MiddleRecord")) {
                                field.setTypeName("doublenested.OuterRecord.MiddleRecord");
                            }
                        });
                    }
                });
                // Verify that at least two fields are changed
                assertThat(fileBuilder.getMessageTypeBuilderList().stream()
                        .flatMap(message -> message.getFieldBuilderList().stream())
                        .filter(field -> field.getTypeName().equals("doublenested.OuterRecord.MiddleRecord"))
                        .count(),
                        greaterThanOrEqualTo(1L));

                MetaDataProtoEditor.renameRecordType(protoBuilder, "MiddleRecord", "MuddledRecord");
            });
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertNotNull(metaData.getRecordsDescriptor().findMessageTypeByName("MuddledRecord"));
            assertEquals("MiddleRecord", metaData.getRecordsDescriptor().findMessageTypeByName("MuddledRecord").findFieldByName("other_middle").getMessageType().getName());
            assertEquals("MiddleRecord", metaData.getRecordType("OuterRecord").getDescriptor().findFieldByName("middle").getMessageType().getName());
        }
    }

    /**
     * Validate that a message type cannot be specified using "." syntax.
     */
    @Test
    public void nestedRecordDefinition() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsDoubleNestedProto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            MetaDataException e = assertThrows(MetaDataException.class, () -> renameRecordType("OuterRecord.MiddleRecord", "OuterRecord.MiddlingRecord"));
            assertEquals("No record type found with name OuterRecord.MiddleRecord", e.getMessage());
        }
    }

    /**
     * Validate that a message type cannot be renamed to something with a "." in it.
     */
    @Test
    public void newNameSuggestsNestedType() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsDoubleNestedProto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            MetaDataException e = assertThrows(MetaDataException.class, () -> renameRecordType("OuterRecord", "OuterRecord.SomeNestedRecord"));
            // We rely on underlying Protobuf validation for this case
            assertEquals("Error converting from protobuf", e.getMessage());
        }
    }

    /**
     * Validate that the identity rename works and changes nothing (except the meta-data version).
     */
    @Test
    public void identityRename() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaDataProto.MetaData metaDataProto = metaDataStore.getRecordMetaData().toProto();
            renameRecordType("MySimpleRecord", "MySimpleRecord");
            assertThat(metaDataProto.getVersion(), lessThan(metaDataStore.getRecordMetaData().getVersion()));
            RecordMetaDataProto.MetaData mutatedMetaDataProto = metaDataStore.getRecordMetaData().toProto().toBuilder().setVersion(metaDataProto.getVersion()).build();
            assertEquals(metaDataProto, mutatedMetaDataProto);
            MetaDataException e = assertThrows(MetaDataException.class, () -> renameRecordType("MyNonExistentRecord", "MyNonExistentRecord"));
            assertEquals("No record type found with name MyNonExistentRecord", e.getMessage());
            context.commit();
        }
    }

    /**
     * Validate that if a {@code NESTED} record with the same name as an imported record type
     * has its name changed, then the indexes do not change their record type.
     */
    @Test
    public void dontRenameRecordTypeInIndexesWhenClashingWithImported() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsImportedAndNewProto.getDescriptor());
            assertEquals(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            assertSame(metaData.getRecordType("MySimpleRecord").getDescriptor(), TestRecords1Proto.MySimpleRecord.getDescriptor());
            assertNotSame(metaData.getRecordType("MySimpleRecord").getDescriptor(), TestRecordsImportedAndNewProto.MySimpleRecord.getDescriptor());
            assertSame(metaData.getRecordType("MyOtherRecord").getDescriptor(), TestRecordsImportedAndNewProto.MyOtherRecord.getDescriptor());
            assertNotSame(metaData.getRecordType("MyOtherRecord").getDescriptor(), TestRecords1Proto.MyOtherRecord.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.mutateMetaData(metaDataProtoBuilder -> {
                MetaDataProtoEditor.renameRecordType(metaDataProtoBuilder, "MySimpleRecord", "MyNewSimpleRecord");
                assertThat(metaDataProtoBuilder.getRecordTypesList().stream().map(RecordMetaDataProto.RecordType::getName).collect(Collectors.toList()), hasItem("MySimpleRecord"));
                for (RecordMetaDataProto.Index index : metaDataProtoBuilder.getIndexesList()) {
                    assertThat(index.getRecordTypeList(), not(hasItem("MyNewSimpleRecord")));
                }
            });
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            assertEquals("MyNewSimpleRecord", metaData.getRecordsDescriptor()
                    .findMessageTypeByName("MyOtherRecord")
                    .findFieldByName("simple")
                    .getMessageType()
                    .getName());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = metaDataStore.getRecordMetaData();
            assertEquals(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            assertEquals("MyNewSimpleRecord", metaData.getRecordsDescriptor()
                    .findMessageTypeByName("MyOtherRecord")
                    .findFieldByName("simple")
                    .getMessageType()
                    .getName());
        }
    }

    /**
     * Validate that if a {@code NESTED} record with the same name as an imported record type has its
     * name changed, then the record type in the record type list does not change.
     */
    @Test
    public void dontRenameRecordTypeWhenClashingWithImported() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsImportedAndNewProto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            renameRecordType("MySimpleRecord", "MyLocalSimpleRecord"); // rename the nested record
            assertEquals(ImmutableSet.of("MySimpleRecord", "MyOtherRecord"), metaData.getRecordTypes().keySet());
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            MetaDataException e = assertThrows(MetaDataException.class, () -> renameRecordType("MyOtherRecord", "MySimpleRecord"));
            assertEquals("Cannot rename record type to MySimpleRecord as an imported record type of that name already exists", e.getMessage());
        }
    }

    private static void validateInnerRecordsInRightPlaces(@Nonnull RecordMetaData metaData) {
        Descriptors.FileDescriptor recordsDescriptor = metaData.getRecordsDescriptor();
        Descriptors.Descriptor innerRecord = recordsDescriptor.findMessageTypeByName("InnerRecord");
        assertNotNull(innerRecord);
        Descriptors.Descriptor outerRecord = recordsDescriptor.findMessageTypeByName("OuterRecord");
        assertNotNull(outerRecord);
        Descriptors.Descriptor middleRecord = outerRecord.findNestedTypeByName("MiddleRecord");
        assertNotNull(middleRecord);
        Descriptors.Descriptor nestedInnerRecord = middleRecord.findNestedTypeByName("InnerRecord");
        assertNotNull(nestedInnerRecord);
        Descriptors.FieldDescriptor innerField = outerRecord.findFieldByName("inner");
        assertSame(nestedInnerRecord, innerField.getMessageType());
        assertNotSame(innerRecord, innerField.getMessageType());
        Descriptors.FieldDescriptor nestedInnerField = outerRecord.findFieldByName("inner");
        assertSame(nestedInnerRecord, nestedInnerField.getMessageType());
        assertNotSame(innerRecord, nestedInnerField.getMessageType());
    }

    /**
     * Verify that if there is a nested type defined in a message, then changing a top-level record type
     * to that name won't cause any fields in that type to start pointing to the new type.
     */
    @Test
    public void renameRecordTypeWithClashingNested() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecordsDoubleNestedProto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType("OtherRecord", "InnerRecord");
            validateInnerRecordsInRightPlaces(metaDataStore.getRecordMetaData());

            // Unqualify the inner record's field type name
            metaDataStore.mutateMetaData(metaDataProtoBuilder -> {
                DescriptorProtos.FileDescriptorProto.Builder fileBuilder = metaDataProtoBuilder.getRecordsBuilder();
                DescriptorProtos.DescriptorProto.Builder outerBuilder = fileBuilder.getMessageTypeBuilderList().stream()
                        .filter(messageBuilder -> messageBuilder.getName().equals("OuterRecord"))
                        .findFirst()
                        .get();
                outerBuilder.getFieldBuilderList().stream()
                        .filter(fieldBuilder -> fieldBuilder.getName().equals("inner"))
                        .forEach(fieldBuilder -> fieldBuilder.setTypeName("MiddleRecord.InnerRecord"));
                outerBuilder.getNestedTypeBuilderList().stream()
                        .filter(messageBuilder -> messageBuilder.getName().equals("MiddleRecord"))
                        .flatMap(messageBuilder -> messageBuilder.getFieldBuilderList().stream())
                        .filter(fieldBuilder -> fieldBuilder.getName().equals("inner"))
                        .forEach(fieldBuilder -> fieldBuilder.setTypeName("InnerRecord"));
            });

            validateInnerRecordsInRightPlaces(metaDataStore.getRecordMetaData());

            // do not commit
        }

        // Validate that this won't update a field of type OuterRecord.InnerRecord even if the field type
        // isn't fully qualified.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            metaDataStore.mutateMetaData(metaDataProtoBuilder -> {
                DescriptorProtos.FileDescriptorProto.Builder fileBuilder = metaDataProtoBuilder.getRecordsBuilder();
                DescriptorProtos.DescriptorProto.Builder outerBuilder = fileBuilder.getMessageTypeBuilderList().stream()
                        .filter(messageBuilder -> messageBuilder.getName().equals("OuterRecord"))
                        .findFirst()
                        .get();
                outerBuilder.getFieldBuilderList().stream()
                        .filter(fieldBuilder -> fieldBuilder.getName().equals("inner"))
                        .forEach(fieldBuilder -> fieldBuilder.setTypeName("MiddleRecord.InnerRecord"));
                outerBuilder.getNestedTypeBuilderList().stream()
                        .filter(messageBuilder -> messageBuilder.getName().equals("MiddleRecord"))
                        .flatMap(messageBuilder -> messageBuilder.getFieldBuilderList().stream())
                        .filter(fieldBuilder -> fieldBuilder.getName().equals("inner"))
                        .forEach(fieldBuilder -> fieldBuilder.setTypeName("InnerRecord"));
            });
            renameRecordType("OtherRecord", "InnerRecord");
            validateInnerRecordsInRightPlaces(metaDataStore.getRecordMetaData());

            // do not commit
        }

        // Validate that if the field were unqualified in a way that introducing a new InnerRecord as a top-level record
        // would change the type that the Protobuf would not have built.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            MetaDataException e = assertThrows(MetaDataException.class, () -> metaDataStore.mutateMetaData(metaDataProtoBuilder -> {
                DescriptorProtos.FileDescriptorProto.Builder fileBuilder = metaDataProtoBuilder.getRecordsBuilder();
                fileBuilder.getMessageTypeBuilderList().stream()
                        .filter(messageBuilder -> messageBuilder.getName().equals("OuterRecord"))
                        .flatMap(messageBuilder -> messageBuilder.getFieldBuilderList().stream())
                        .filter(fieldBuilder -> fieldBuilder.getName().equals("inner"))
                        .forEach(fieldBuilder -> fieldBuilder.setTypeName("InnerRecord"));
            }));
            assertEquals("Error converting from protobuf", e.getMessage());
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(Descriptors.DescriptorValidationException.class));
            assertThat(e.getCause().getMessage(), containsString("\"InnerRecord\" is not defined."));

            // do not commit
        }
    }

    /**
     * Verify that renaming a regular record type to the default union name throws an error.
     */
    @Test
    public void dontRenameNonUnionToUnion() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            renameRecordType(RecordMetaDataBuilder.DEFAULT_UNION_NAME, "RecordOneUnion"); // to avoid conflicts
            MetaDataException e = assertThrows(MetaDataException.class, () -> renameRecordType("MySimpleRecord", RecordMetaDataBuilder.DEFAULT_UNION_NAME));
            assertEquals("Cannot rename record type to the default union name", e.getMessage());
        }
    }

    /**
     * Verify that renaming a non-existent record doesn't work.
     */
    @Test
    public void tryRenameNonExistentRecord() {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaData metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
            metaDataStore.saveRecordMetaData(metaData);
            context.commit();
        }
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            MetaDataException e = assertThrows(MetaDataException.class, () -> renameRecordType("MyNonExistentRecord", "SomethingElse"));
            assertEquals("No record type found with name MyNonExistentRecord", e.getMessage());
        }
    }
}
