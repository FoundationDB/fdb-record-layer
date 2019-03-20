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
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestNoUnionEvolvedIllegalProto;
import com.apple.foundationdb.record.TestNoUnionEvolvedProto;
import com.apple.foundationdb.record.TestNoUnionEvolvedRenamedRecordTypeProto;
import com.apple.foundationdb.record.TestNoUnionProto;
import com.apple.foundationdb.record.TestRecords1EvolvedAgainProto;
import com.apple.foundationdb.record.TestRecords1EvolvedProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecordsImplicitUsageNoUnionProto;
import com.apple.foundationdb.record.TestRecordsImplicitUsageProto;
import com.apple.foundationdb.record.TestRecordsMultiProto;
import com.apple.foundationdb.record.TestRecordsOneOfProto;
import com.apple.foundationdb.record.TestRecordsParentChildRelationshipProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataProtoTest;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FDBMetaDataStore}.
 */
@Tag(Tags.RequiresFDB)
public class FDBMetaDataStoreTest extends FDBTestBase {
    FDBDatabase fdb;
    FDBMetaDataStore metaDataStore;

    public void openMetaDataStore(FDBRecordContext context) {
        metaDataStore = new FDBMetaDataStore(context, TestKeySpace.getKeyspacePath("record-test", "unit", "metadataStore"));
        metaDataStore.setDependencies(new Descriptors.FileDescriptor[] {
                RecordMetaDataOptionsProto.getDescriptor()
        });
    }

    @BeforeEach
    public void setup() {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        fdb.run(context -> {
            openMetaDataStore(context);
            context.ensureActive().clear(metaDataStore.getSubspace().range());
            return null;
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
                context.ensureActive().clear(metaDataStore.getSubspace().range());
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

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "indexes [indexCounterBasedSubspaceKey = {0}]")
    public void indexes(final TestHelpers.BooleanEnum indexCounterBasedSubspaceKey) {
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder();
            if (indexCounterBasedSubspaceKey.toBoolean()) {
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
            metaDataStore.addUniversalIndex(FDBRecordStoreTestBase.COUNT_INDEX);
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
            assertTrue(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MyNewRecord").getOptions().getDeprecated());
            assertEquals(version + 4, metaDataStore.getRecordMetaData().getVersion());
            deprecateRecordType("MyNewRecordWithIndex");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecordWithIndex"));
            assertTrue(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MyNewRecordWithIndex").getOptions().getDeprecated());
            assertEquals(version + 5, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Deprecate a record type from the original proto.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 5, metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            deprecateRecordType(".com.apple.foundationdb.record.test1.MySimpleRecord"); // Record type needs to be fully qualified.
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertTrue(metaDataStore.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName(RecordMetaDataBuilder.DEFAULT_UNION_NAME).findFieldByName("_MySimpleRecord").getOptions().getDeprecated());
            assertEquals(version + 6, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
        }

        // Deprecate a non-existent record type.
        try (FDBRecordContext context = fdb.openContext()) {
            openMetaDataStore(context);
            assertEquals(version + 6, metaDataStore.getRecordMetaData().getVersion());
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            MetaDataException e = assertThrows(MetaDataException.class, () -> deprecateRecordType("MyNonExistentRecord"));
            assertEquals(e.getMessage(), "Record type MyNonExistentRecord not found");
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version + 6, metaDataStore.getRecordMetaData().getVersion());
            context.commit();
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

        // Add a record type with default union name to a record meta-data that has a non-default union name.
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
            deprecateRecordType(".com.apple.foundationdb.record.test3.MyHierarchicalRecord"); // Record type needs to be fully qualified.
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

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "noUnionUpdateRecords [repeatSaveOrDoUpdate = {0}]")
    public void noUnionUpdateRecords(TestHelpers.BooleanEnum repeatSaveOrDoUpdate) {
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
            if (repeatSaveOrDoUpdate.toBoolean()) {
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
            if (repeatSaveOrDoUpdate.toBoolean()) {
                e = assertThrows(MetaDataException.class, () -> metaDataStore.updateRecords(TestNoUnionEvolvedRenamedRecordTypeProto.getDescriptor()));
            } else {
                e = assertThrows(MetaDataException.class, () -> metaDataStore.saveRecordMetaData(TestNoUnionEvolvedRenamedRecordTypeProto.getDescriptor()));
            }
            assertEquals("Record type MySimpleRecord removed", e.getMessage());
            context.commit();
        }
    }

    @EnumSource(TestHelpers.BooleanEnum.class)
    @ParameterizedTest(name = "noUnionImplicitUsage [repeatSaveOrDoUpdate = {0}]")
    public void noUnionImplicitUsage(TestHelpers.BooleanEnum repeatSaveOrDoUpdate) {
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
            if (repeatSaveOrDoUpdate.toBoolean()) {
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
}
