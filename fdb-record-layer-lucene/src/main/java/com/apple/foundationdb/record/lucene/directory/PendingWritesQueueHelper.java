/*
 * PendingWritesQueueHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.lucene.LuceneDocumentFromRecord;
import com.apple.foundationdb.record.lucene.LuceneIndexExpressions;
import com.apple.foundationdb.record.lucene.LucenePendingWriteQueueProto;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@API(API.Status.INTERNAL)
public final class PendingWritesQueueHelper {
    /**
     * Convert a raw record back to a queue entry.
     */
    public static PendingWriteQueue.QueueEntry toQueueEntry(LuceneSerializer serializer, Tuple keyTuple, byte[] valueBytes) {
        try {
            final Versionstamp versionstamp = keyTuple.getVersionstamp(0);
            final byte[] value = serializer.decode(valueBytes);
            LucenePendingWriteQueueProto.PendingWriteItem item = LucenePendingWriteQueueProto.PendingWriteItem.parseFrom(value);
            return new PendingWriteQueue.QueueEntry(versionstamp, item);
        } catch (InvalidProtocolBufferException e) {
            throw new RecordCoreStorageException("Failed to parse queue item", e);
        }
    }

    /**
     * Convert DocumentField to protobuf DocumentField.
     */
    public static LucenePendingWriteQueueProto.DocumentField toProtoField(@Nonnull LuceneDocumentFromRecord.DocumentField field) {

        LucenePendingWriteQueueProto.DocumentField.Builder builder =
                LucenePendingWriteQueueProto.DocumentField.newBuilder()
                        .setFieldName(field.getFieldName())
                        .setStored(field.isStored())
                        .setSorted(field.isSorted());

        // Add field_configs map
        for (Map.Entry<String, Object> entry : field.getFieldConfigs().entrySet()) {
            LucenePendingWriteQueueProto.FieldConfigValue.Builder configBuilder =
                    LucenePendingWriteQueueProto.FieldConfigValue.newBuilder();

            Object configValue = entry.getValue();
            if (configValue instanceof String) {
                configBuilder.setStringValue((String)configValue);
            } else if (configValue instanceof Boolean) {
                configBuilder.setBooleanValue((Boolean)configValue);
            } else if (configValue instanceof Integer) {
                configBuilder.setIntValue((Integer)configValue);
            } else {
                throw new RecordCoreArgumentException("Unsupported field config type: " + configValue.getClass().getSimpleName() + " for field " + entry.getKey());
            }

            builder.putFieldConfigs(entry.getKey(), configBuilder.build());
        }

        // Set the appropriate value type based on field type
        Object value = field.getValue();
        switch (field.getType()) {
            case TEXT:
                builder.setTextValue((String)value);
                break;
            case STRING:
                builder.setStringValue((String)value);
                break;
            case INT:
                builder.setIntValue((Integer)value);
                break;
            case LONG:
                builder.setLongValue((Long)value);
                break;
            case DOUBLE:
                builder.setDoubleValue((Double)value);
                break;
            case BOOLEAN:
                builder.setBooleanValue((Boolean)value);
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type: " + field.getType() + " for field " + field.getFieldName());
        }

        return builder.build();
    }

    /**
     * Convert protobuf DocumentField list back to LuceneDocumentFromRecord.DocumentField list.
     */
    public static List<LuceneDocumentFromRecord.DocumentField> fromProtoFields(
            @Nonnull List<LucenePendingWriteQueueProto.DocumentField> protoFields) {

        List<LuceneDocumentFromRecord.DocumentField> fields = new ArrayList<>();
        for (LucenePendingWriteQueueProto.DocumentField protoField : protoFields) {
            String fieldName = protoField.getFieldName();
            Object value;
            LuceneIndexExpressions.DocumentFieldType fieldType;

            // Determine the value and type based on which field is set
            if (protoField.hasStringValue()) {
                value = protoField.getStringValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.STRING;
            } else if (protoField.hasTextValue()) {
                value = protoField.getTextValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.TEXT;
            } else if (protoField.hasIntValue()) {
                value = protoField.getIntValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.INT;
            } else if (protoField.hasLongValue()) {
                value = protoField.getLongValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.LONG;
            } else if (protoField.hasDoubleValue()) {
                value = protoField.getDoubleValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.DOUBLE;
            } else if (protoField.hasBooleanValue()) {
                value = protoField.getBooleanValue();
                fieldType = LuceneIndexExpressions.DocumentFieldType.BOOLEAN;
            } else {
                throw new IllegalStateException("DocumentField has no value set: " + fieldName);
            }

            // Convert field_configs from proto map to Map<String, Object>
            Map<String, Object> fieldConfigs = new HashMap<>();
            if (protoField.getFieldConfigsCount() > 0) {
                for (Map.Entry<String, LucenePendingWriteQueueProto.FieldConfigValue> entry : protoField.getFieldConfigsMap().entrySet()) {
                    Object configValue;
                    LucenePendingWriteQueueProto.FieldConfigValue protoConfigValue = entry.getValue();
                    switch (protoConfigValue.getValueCase()) {
                        case STRING_VALUE:
                            configValue = protoConfigValue.getStringValue();
                            break;
                        case BOOLEAN_VALUE:
                            configValue = protoConfigValue.getBooleanValue();
                            break;
                        case INT_VALUE:
                            configValue = protoConfigValue.getIntValue();
                            break;
                        case VALUE_NOT_SET:
                        default:
                            throw new IllegalStateException("FieldConfigValue has no value set for key: " + entry.getKey());
                    }
                    fieldConfigs.put(entry.getKey(), configValue);
                }
            }

            // Create DocumentField with stored, sorted, and field_configs from proto
            boolean stored = protoField.getStored();
            boolean sorted = protoField.getSorted();
            LuceneDocumentFromRecord.DocumentField field =
                    new LuceneDocumentFromRecord.DocumentField(fieldName, value, fieldType, stored, sorted, fieldConfigs);
            fields.add(field);
        }

        return fields;
    }

    // No constructor for utility class
    private PendingWritesQueueHelper() {
    }
}
