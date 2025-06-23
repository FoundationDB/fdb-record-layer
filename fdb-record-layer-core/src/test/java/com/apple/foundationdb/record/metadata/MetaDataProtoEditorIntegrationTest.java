/*
 * TypeRenamerIntegrationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditor;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditorUnitTest;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetaDataProtoEditorIntegrationTest extends FDBRecordStoreTestBase {

    @ParameterizedTest
    @BooleanSource("readWithRenamed")
    void mixedModeReadWrite(boolean readWithRenamed) throws IOException {
        MixedModeUtility mixedModeUtility = new MixedModeUtility("OneBoringType.json", readWithRenamed);

        mixedModeUtility.doWrite((metaData, rename) ->
                saveDynamicRecord(metaData, rename.apply("T1"), Map.of("ID", 1L)));

        mixedModeUtility.doRead((metaData, typeName) -> {
            final FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1L));
            assertEquals(mixedModeUtility.readTypeName.apply("T1"), record.getRecordType().getName());
        });
    }

    @ParameterizedTest
    @BooleanSource("readWithRenamed")
    void mixedModeMultiTypeIndex(boolean readWithRenamed) throws IOException {
        MixedModeUtility mixedModeUtility = new MixedModeUtility("MultiTypeIndex.json", readWithRenamed);

        mixedModeUtility.doWrite((metaData, rename) -> {
            saveDynamicRecord(metaData, rename.apply("T1"), Map.of("ID", 1L, "VALUE", 10L));
            saveDynamicRecord(metaData,  rename.apply("T2"), Map.of("ID", 2L, "VALUE", 15L));
            saveDynamicRecord(metaData, rename.apply("T1"), Map.of("ID", 3L, "VALUE", 15L));
            saveDynamicRecord(metaData,  rename.apply("T2"), Map.of("ID", 4L, "VALUE", 10L));
        });

        mixedModeUtility.doRead((metaData, rename) -> {
            final List<FDBIndexedRecord<Message>> records = recordStore.scanIndexRecords("theIndex").asList().join();
            assertEquals(
                    Map.of(1L, rename.apply("T1"),
                            2L, rename.apply("T2"),
                            3L, rename.apply("T1"),
                            4L, rename.apply("T2")),
                    records.stream().collect(Collectors.toMap(
                            message -> message.getRecord().getAllFields()
                                    .entrySet().stream().filter(entry -> entry.getKey().getName().equals("ID"))
                                    .findAny().orElseThrow().getValue(),
                            message -> message.getRecordType().getName())));
        });
    }

    private void saveDynamicRecord(final RecordMetaData originalMetaData,
                                   final String typeName,
                                   final Map<String, Object> fields) {
        final Descriptors.Descriptor typeDescriptor = originalMetaData.getRecordType(typeName).getDescriptor();
        final DynamicMessage.Builder builder = DynamicMessage.newBuilder(typeDescriptor);
        for (final Map.Entry<String, Object> field : fields.entrySet()) {
            builder.setField(typeDescriptor.findFieldByName(field.getKey()), field.getValue());
        }
        recordStore.saveRecord(builder.build());
    }

    private class MixedModeUtility {
        private final RecordMetaData writeMetaData;
        private final RecordMetaData readMetaData;
        private final Function<String, String> readTypeName;
        private final Function<String, String> writeTypeName;

        public MixedModeUtility(String fileName, boolean readWithRenamed) throws IOException {
            final RecordMetaDataProto.MetaData.Builder builder = MetaDataProtoEditorUnitTest.loadMetaData(fileName);
            final RecordMetaData originalMetaData = RecordMetaData.build(builder.build());

            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, () -> originalMetaData);
                commit(context);
            }

            RecordMetaDataBuilder.getDependencies(builder.build(), Map.of());
            MetaDataProtoEditor.renameRecordTypes(builder, this::renameType);
            final RecordMetaData newMetaData = RecordMetaData.build(builder.build());
            this.writeMetaData = readWithRenamed ? originalMetaData : newMetaData;
            this.readMetaData = readWithRenamed ? newMetaData : originalMetaData;
            this.readTypeName = readWithRenamed ? this::renameType : Function.identity();
            this.writeTypeName = readWithRenamed ? Function.identity() : this::renameType;
        }

        public void doWrite(BiConsumer<RecordMetaData, Function<String, String>> doWrite) {
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, () -> writeMetaData);
                doWrite.accept(this.writeMetaData, this.writeTypeName);
                commit(context);
            }
        }

        public void doRead(BiConsumer<RecordMetaData, Function<String, String>> doRead) {
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, () -> readMetaData);
                doRead.accept(readMetaData, readTypeName);
            }
        }

        private String renameType(String oldName) {
            return oldName + "__X";
        }
    }
}
