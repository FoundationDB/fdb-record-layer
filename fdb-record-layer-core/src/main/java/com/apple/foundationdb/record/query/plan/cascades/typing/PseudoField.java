/*
 * PseudoField.java
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

package com.apple.foundationdb.record.query.plan.cascades.typing;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroCopyByteString;

import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.function.Function;

public enum PseudoField {
    ROW_VERSION(Type.primitiveType(Type.TypeCode.VERSION, true), queriedRecord -> {
        final FDBRecordVersion version = queriedRecord.getVersion();
        return version == null ? null : ZeroCopyByteString.wrap(version.toBytes(false));
    }),
    ;

    private static final String PREFIX = "__";

    private final String fieldName;
    private final Type type;
    private final Function<FDBQueriedRecord<?>, @Nullable Object> valueExtractor;

    PseudoField(Type type, Function<FDBQueriedRecord<?>, @Nullable Object> valueExtractor) {
        this.fieldName = PREFIX + name();
        this.type = type;
        this.valueExtractor = valueExtractor;
    }

    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    private Object extractProtoValue(@Nullable FDBQueriedRecord<?> queriedRecord) {
        if (queriedRecord == null) {
            return null;
        }
        return valueExtractor.apply(queriedRecord);
    }

    public void fillInIfApplicable(Type.Record desiredType, @Nullable FDBQueriedRecord<?> queriedRecord, Message.Builder targetBuilder) {
        if (queriedRecord == null) {
            return;
        }
        final Map<String, Type.Record.Field> fieldNameMap = desiredType.getFieldNameFieldMap();
        @Nullable Field field = fieldNameMap.get(fieldName);
        if (field == null || !field.getFieldType().equals(type)) {
            // Field not in desired type
            return;
        }
        if (queriedRecord.getRecord().getDescriptorForType().findFieldByName(field.getFieldStorageName()) != null) {
            // Field is already defined in the record. Do not overwrite
            return;
        }
        @Nullable Object value = extractProtoValue(queriedRecord);
        if (value == null) {
            // Pseudo-field is not set. Nothing to do
            return;
        }

        // Set the appropriate field in the target field descriptor
        final Descriptors.FieldDescriptor targetFieldDescriptor = targetBuilder.getDescriptorForType().findFieldByName(field.getFieldStorageName());
        if (targetFieldDescriptor == null) {
            throw new RecordCoreException("pseudo-field definition missing from target builder")
                    .addLogInfo(LogMessageKeys.FIELD_NAME, fieldName)
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, queriedRecord.getRecordType().getName());
        }
        targetBuilder.setField(targetFieldDescriptor, value);
    }

    public Type getType() {
        return type;
    }
}
