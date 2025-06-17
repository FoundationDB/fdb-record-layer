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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TypeRenamerIntegrationTest extends FDBRecordStoreTestBase {

    @Test
    void readAfterRename() throws IOException {
        final RecordMetaDataProto.MetaData.Builder builder = TypeRenamerUnitTest.loadMetaData("OneBoringType.json");
        final RecordMetaData originalMetaData = RecordMetaData.build(builder.build());

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, () -> originalMetaData);
            saveDynamicRecord(originalMetaData, "T1", Map.of("ID", 1L));
            commit(context);
        }
        new TypeRenamer(oldName -> oldName + "__X")
                .modify(builder, RecordMetaDataBuilder.getDependencies(builder.build(), Map.of()));
        final RecordMetaData newMetaData = RecordMetaData.build(builder.build());
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, () -> newMetaData);
            final FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1L));
            assertEquals("T1__X", record.getRecordType().getName());
        }
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
}
