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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
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

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
        ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
        RecordMetaDataOptionsProto.registerAllExtensions(extensionRegistry);
        metaDataStore.setExtensionRegistry(extensionRegistry);
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
}
