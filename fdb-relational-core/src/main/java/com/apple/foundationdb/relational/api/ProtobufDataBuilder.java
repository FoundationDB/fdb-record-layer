/*
 * ProtobufDataBuilder.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.NullableArrayUtils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

public class ProtobufDataBuilder implements DynamicMessageBuilder {
    private final Descriptors.Descriptor typeDescriptor;

    private DynamicMessage.Builder data;

    public ProtobufDataBuilder(Descriptors.Descriptor typeDescriptor) {
        this.typeDescriptor = typeDescriptor;
        this.data = DynamicMessage.newBuilder(typeDescriptor);
    }

    @Override
    public Set<String> getFieldNames() {
        return typeDescriptor.getFields().stream()
                .map(Descriptors.FieldDescriptor::getName)
                .collect(Collectors.toSet());
    }

    @Override
    public String getFieldType(String fieldName) throws SQLException {
        final Descriptors.FieldDescriptor field = typeDescriptor.findFieldByName(fieldName);
        if (field == null) {
            throw new RelationalException(String.format("Field <%s> does not exist", fieldName), ErrorCode.INVALID_PARAMETER).toSqlException();
        }
        return ProtobufDdlUtil.getTypeName(field);
    }

    @ExcludeFromJacocoGeneratedReport // currently, used only for YAML testing
    @Override
    public boolean isPrimitive(int fieldNumber) throws SQLException {
        final var field = typeDescriptor.getFields().get(fieldNumber - 1);
        if (field == null) {
            throw new RelationalException(String.format("Field with number <%d> does not exist", fieldNumber), ErrorCode.INVALID_PARAMETER).toSqlException();
        }
        return !field.isRepeated() && !field.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.MESSAGE); // enum?
    }

    @Override
    public DynamicMessageBuilder setField(String fieldName, Object value) throws SQLException {
        try {
            final Descriptors.FieldDescriptor field = typeDescriptor.findFieldByName(fieldName);
            if (field == null) {
                throw new RelationalException(String.format("Field <%s> does not exist", fieldName), ErrorCode.INVALID_PARAMETER);
            }
            return setFieldInternal(field, value);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @ExcludeFromJacocoGeneratedReport // currently, used only for YAML testing
    @Override
    public DynamicMessageBuilder setField(int fieldNumber, Object value) throws SQLException {
        try {
            final Descriptors.FieldDescriptor field = typeDescriptor.getFields().get(fieldNumber - 1);
            if (field == null) {
                throw new RelationalException(String.format("Field with number (%d) does not exist", fieldNumber), ErrorCode.INVALID_PARAMETER);
            }
            return setFieldInternal(field, value);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    private DynamicMessageBuilder setFieldInternal(@Nonnull final Descriptors.FieldDescriptor field, Object value) throws RelationalException {
        data.setField(field, coerceObject(value, field));
        return this;
    }

    @Override
    public DynamicMessageBuilder addRepeatedField(String fieldName, Object value) throws SQLException {
        try {
            final Descriptors.FieldDescriptor field = typeDescriptor.findFieldByName(fieldName);
            if (field == null) {
                throw new RelationalException("Field <" + fieldName + "> does not exist", ErrorCode.INVALID_PARAMETER);
            }
            if (!field.isRepeated()) {
                throw new RelationalException("Field <" + fieldName + "> is not repeated", ErrorCode.INVALID_PARAMETER);
            }
            return addRepeatedFieldInternal(field, value);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @ExcludeFromJacocoGeneratedReport // currently, used only for YAML testing
    @Override
    public DynamicMessageBuilder addRepeatedField(int fieldNumber, Object value) throws SQLException {
        try {
            final Descriptors.FieldDescriptor field = typeDescriptor.getFields().get(fieldNumber - 1);
            if (field == null) {
                throw new RelationalException(String.format("Field with number (%d) does not exist", fieldNumber), ErrorCode.INVALID_PARAMETER);
            }
            if (!field.isRepeated()) {
                throw new RelationalException("Field with number <" + fieldNumber + "> is not repeated", ErrorCode.INVALID_PARAMETER);
            }
            return addRepeatedFieldInternal(field, value);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Nonnull
    private DynamicMessageBuilder addRepeatedFieldInternal(@Nonnull final Descriptors.FieldDescriptor field, Object value) throws RelationalException {
        data.addRepeatedField(field, coerceObject(value, field));
        return this;
    }

    @Override
    public DynamicMessageBuilder addRepeatedFields(String fieldName, Iterable<? extends Object> values, boolean isNullableArray) throws SQLException {
        try {
            if (isNullableArray) {
                DynamicMessageBuilder builder = getNestedMessageBuilder(fieldName);
                builder.addRepeatedFields(NullableArrayUtils.getRepeatedFieldName(), values, false);
                setField(fieldName, builder.build());
            } else {
                for (Object value : values) {
                    if (value == null) {
                        throw new RelationalException("Cannot add a null value to a non-nullable array", ErrorCode.NOT_NULL_VIOLATION);
                    }
                    addRepeatedField(fieldName, value);
                }
            }
            return this;
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public DynamicMessageBuilder addRepeatedFields(String fieldName, Iterable<? extends Object> values) throws SQLException {
        return addRepeatedFields(fieldName, values, true);
    }

    @ExcludeFromJacocoGeneratedReport // currently, used only for YAML testing
    @Override
    public DynamicMessageBuilder addRepeatedFields(int fieldNumber, Iterable<? extends Object> values) throws SQLException {
        for (Object value : values) {
            addRepeatedField(fieldNumber, value);
        }
        return this;
    }

    @Override
    public Message build() {
        return data.build();
    }

    @Override
    public <T extends Message> Message convertMessage(T m) throws SQLException {
        //dynamically attempt to convert the message type to this type, if it is necessary
        try {
            return convert(m, typeDescriptor);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public DynamicMessageBuilder getNestedMessageBuilder(String fieldName) throws SQLException {
        try {
            for (Descriptors.FieldDescriptor fd : typeDescriptor.getFields()) {
                if (fd.getName().equals(fieldName)) {
                    if (fd.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                        throw new RelationalException("Cannot get Nested data builder for field " + fieldName + " as it is not a nested structure", ErrorCode.INVALID_PARAMETER);
                    }
                    return new ProtobufDataBuilder(fd.getMessageType());
                }
            }
            throw new RelationalException("Field <" + fieldName + "> does not exist in this Type", ErrorCode.INVALID_PARAMETER);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @ExcludeFromJacocoGeneratedReport // currently, used only for YAML testing
    @Override
    public DynamicMessageBuilder getNestedMessageBuilder(int fieldNumber) throws SQLException {
        try {
            final Descriptors.FieldDescriptor fd = typeDescriptor.getFields().get(fieldNumber - 1);
            if (fd.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                throw new RelationalException("Cannot get Nested data builder for field with number <" + fieldNumber + "> as it is not a nested structure", ErrorCode.INVALID_PARAMETER);
            }
            return new ProtobufDataBuilder(fd.getMessageType());
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public Descriptors.Descriptor getDescriptor() {
        return typeDescriptor;
    }

    @Override
    @Nonnull
    public DynamicMessageBuilder newBuilder() {
        return new ProtobufDataBuilder(typeDescriptor);
    }

    private Message convert(Message m, Descriptors.Descriptor destinationDescriptor) throws RelationalException {
        /*
         * Annoying RecordLayer-ism, but they don't check for semantic equality, they check for
         * reference equality, which means that even if the message has the semantically identical
         * descriptor, we still have to convert it. However, if we somehow magically have identical references,
         * then we can shortcut out
         */

        if (m.getDescriptorForType() == destinationDescriptor) {
            return m;
        }
        DynamicMessage.Builder newMessage = DynamicMessage.newBuilder(destinationDescriptor);
        for (Descriptors.FieldDescriptor field : destinationDescriptor.getFields()) {
            Descriptors.FieldDescriptor messageField = m.getDescriptorForType().findFieldByName(field.getName());
            if (messageField == null) {
                if (!field.isOptional()) {
                    throw new RelationalException("Field <" + field.getName() + "> is missing from passed object", ErrorCode.INVALID_PARAMETER);
                }
                continue;
            }

            if (!canCoerce(messageField.getJavaType(), field.getJavaType())) {
                throw new RelationalException("Field <" + field.getName() + "> is of incorrect type", ErrorCode.INVALID_PARAMETER);
            }
            if (field.isRepeated() && !messageField.isRepeated()) {
                throw new RelationalException("Field <" + field.getName() + "> should be repeated", ErrorCode.INVALID_PARAMETER);
            } else if (!field.isRepeated() && messageField.isRepeated()) {
                throw new RelationalException("Field <" + field.getName() + "> should not be repeated", ErrorCode.INVALID_PARAMETER);
            }

            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                final Descriptors.Descriptor messageDesc = field.getMessageType();

                if (field.isRepeated()) {
                    for (int i = 0; i < m.getRepeatedFieldCount(messageField); i++) {
                        Message converted = convert((Message) m.getRepeatedField(messageField, i), messageDesc);
                        newMessage.addRepeatedField(field, converted);
                    }
                } else {
                    if (m.hasField(messageField)) {
                        Message converted = convert((Message) m.getField(messageField), messageDesc);
                        newMessage.setField(field, converted);
                    }
                }
            } else {
                if (field.isRepeated()) {
                    for (int i = 0; i < m.getRepeatedFieldCount(messageField); i++) {
                        final Object coerced = coerceObject(m.getRepeatedField(messageField, i), field);
                        newMessage.addRepeatedField(field, coerced);
                    }
                } else {
                    if (m.hasField(messageField)) {
                        Object coerced = coerceObject(m.getField(messageField), field);
                        newMessage.setField(field, coerced);
                    }
                }
            }
        }

        return newMessage.build();
    }

    private Object coerceObject(Object value, Descriptors.FieldDescriptor fieldDescriptor) throws RelationalException {
        var destType = fieldDescriptor.getJavaType();
        if (value instanceof Number) {
            Number n = (Number) value;
            switch (destType) {
                case INT:
                    value = n.intValue();
                    break;
                case LONG:
                    value = n.longValue();
                    break;
                case FLOAT:
                    value = n.floatValue();
                    break;
                case DOUBLE:
                    value = n.doubleValue();
                    break;
                case STRING:
                    value = n.toString();
                    break;
                case ENUM:
                    throw new InvalidTypeException("Invalid enum value " + n);
                default:
                //don't coerce, this shouldn't happen
            }
        } else if (value instanceof String) {
            String str = (String) value;
            switch (destType) {
                case INT:
                    value = Integer.parseInt(str);
                    break;
                case LONG:
                    value = Long.parseLong(str);
                    break;
                case FLOAT:
                    value = Float.parseFloat(str);
                    break;
                case DOUBLE:
                    value = Double.parseDouble(str);
                    break;
                case ENUM:
                    value = fieldDescriptor.getEnumType().findValueByName(str);
                    if (value == null) {
                        throw new InvalidTypeException("Invalid enum value '" + str + "'");
                    }
                    break;
                default:
                //don't coerce -- this is pointless, but it's here to make checkstyle happy
            }
        }
        return value;
    }

    private boolean canCoerce(Descriptors.FieldDescriptor.JavaType srcType, Descriptors.FieldDescriptor.JavaType destType) {
        // returns true if you can convert objects of srcType into objects of destType (i.e. int into long)
        if (srcType == Descriptors.FieldDescriptor.JavaType.INT) {
            switch (destType) {
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    return true;
                default:
                    return false;
            }
        } else if (srcType == Descriptors.FieldDescriptor.JavaType.FLOAT) {
            switch (destType) {
                case FLOAT:
                case DOUBLE:
                    return true;
                default:
                    return false;
            }
        } else {
            return srcType == destType;
        }

    }
}
