/*
 * RowStruct.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.net.URI;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.UUID;

/**
 * Implementation of {@link RelationalStruct} that is backed by a {@link Row}.
 */
public abstract class RowStruct implements RelationalStruct, EmbeddedRelationalStruct {

    private final StructMetaData metaData;

    protected boolean wasNull;

    protected RowStruct(StructMetaData metaData) {
        this.metaData = metaData;
    }

    @Override
    public StructMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public boolean getBoolean(int oneBasedPosition) throws SQLException {
        Object o = getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        if (o == null) {
            return false;
        }
        if (!(o instanceof Boolean)) {
            throw new SQLException("Boolean", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return (Boolean) o;
    }

    protected abstract Object getObjectInternal(int zeroBasedPosition) throws SQLException;

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(getOneBasedPosition(columnLabel));
    }

    @Override
    public byte[] getBytes(int oneBasedPosition) throws SQLException {
        Object o = getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        if (o == null) {
            return null;
        }
        if (o instanceof ByteString) {
            return ((ByteString) o).toByteArray();
        } else if (o instanceof byte[]) {
            return (byte[]) o;
        } else {
            throw new SQLException("byte[]", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(getOneBasedPosition(columnLabel));
    }

    @Override
    public int getInt(int oneBasedPosition) throws SQLException {
        Object o = getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        if (o == null) {
            return 0;
        }
        if (!(o instanceof Number)) {
            throw new SQLException(String.format(Locale.ROOT, "Cannot convert %s to Integer", o.getClass().toString()), ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).intValue();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(getOneBasedPosition(columnLabel));
    }

    @Override
    public long getLong(int oneBasedPosition) throws SQLException {
        Object o = getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException(String.format(Locale.ROOT, "Cannot convert %s to Long", o.getClass().toString()), ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).longValue();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(getOneBasedPosition(columnLabel));
    }

    @Override
    public float getFloat(int oneBasedPosition) throws SQLException {
        Object o = getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException("Float", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).floatValue();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(getOneBasedPosition(columnLabel));
    }

    @Override
    public double getDouble(int oneBasedPosition) throws SQLException {
        Object o = getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        if (o == null) {
            return 0L;
        }
        if (!(o instanceof Number)) {
            throw new SQLException("Double", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return ((Number) o).doubleValue();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(getOneBasedPosition(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(getOneBasedPosition(columnLabel));
    }

    @Override
    public Object getObject(int oneBasedPosition) throws SQLException {
        switch (metaData.getColumnType(oneBasedPosition)) {
            case Types.STRUCT:
                return getStruct(oneBasedPosition);
            case Types.ARRAY:
                return getArray(oneBasedPosition);
            case Types.BINARY:
                return getBytes(oneBasedPosition);
            case Types.OTHER:
            default:
                return getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        }
    }

    @Override
    public String getString(int oneBasedPosition) throws SQLException {
        Object o = getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        if (o == null) {
            return null; //TODO(bfines) default column value here
        }
        if (o instanceof String) {
            return (String) o;
        } else if (o instanceof Number) {
            return o.toString();
        } else if (o instanceof URI) {
            //special case for database URI fields
            return o.toString();
        } else if (o instanceof Enum<?>) {
            return ((Enum<?>) o).name();
        } else if (o instanceof Descriptors.EnumValueDescriptor) {
            return ProtoUtils.toUserIdentifier(((Descriptors.EnumValueDescriptor) o).getName());
        } else if (o instanceof ByteString) {
            return ((ByteString) o).toString();
        } else {
            throw new SQLException("String", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(getOneBasedPosition(columnLabel));
    }

    @Override
    public RelationalArray getArray(int oneBasedPosition) throws SQLException {
        if (metaData.getColumnType(oneBasedPosition) != Types.ARRAY) {
            throw new SQLException("Array", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
        Object obj = getObjectInternal(getZeroBasedPosition(oneBasedPosition));
        if (obj == null) {
            return null;
        }
        if (obj instanceof RelationalArray) {
            return (RelationalArray) obj;
        } else if (obj instanceof Collection) {
            final var coll = (Collection<?>) obj;
            final var arrayMetaData = metaData.getArrayMetaData(oneBasedPosition);
            final var elements = new ArrayList<>();
            for (final var t : coll) {
                if (t instanceof Message) {
                    elements.add(new ImmutableRowStruct(new MessageTuple((Message) t), arrayMetaData.getElementStructMetaData()));
                } else {
                    elements.add(t);
                }
            }
            return new RowArray(elements, arrayMetaData);
        } else if (obj instanceof Message) {
            Message message = (Message) obj;
            final var arrayMetaData = metaData.getArrayMetaData(oneBasedPosition);
            // verify message is a wrapped array
            if (!NullableArrayUtils.isWrappedArrayDescriptor(message.getDescriptorForType())) {
                throw new SQLException("Array", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
            }
            Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName(NullableArrayUtils.getRepeatedFieldName());
            final var fieldValues = (Collection<?>) message.getField(fieldDescriptor);
            final var elements = new ArrayList<>();
            for (var fieldValue : fieldValues) {
                final var sanitizedFieldValue = MessageTuple.sanitizeField(fieldValue, fieldDescriptor.getOptions());
                if (sanitizedFieldValue instanceof Message) {
                    elements.add(new ImmutableRowStruct(new MessageTuple((Message) sanitizedFieldValue), arrayMetaData.getElementStructMetaData()));
                }  else {
                    elements.add(sanitizedFieldValue);
                }
            }
            return new RowArray(elements, arrayMetaData);
        } else {
            throw new SQLException("Array", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
    }

    @Override
    public RelationalArray getArray(String columnLabel) throws SQLException {
        return getArray(getOneBasedPosition(columnLabel));
    }

    @Override
    public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
        if (metaData.getColumnType(oneBasedColumn) != Types.STRUCT) {
            throw new SQLException("Struct", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
        Object obj = getObjectInternal(getZeroBasedPosition(oneBasedColumn));
        if (obj == null) {
            return null;
        }
        if (obj instanceof RelationalStruct) {
            return (RelationalStruct) obj;
        } else if (obj instanceof Row) {
            return new ImmutableRowStruct((Row) obj, metaData.getStructMetaData(oneBasedColumn));
        } else if (obj instanceof Message) {
            return new ImmutableRowStruct(new MessageTuple((Message) obj), metaData.getStructMetaData(oneBasedColumn));
        } else if (obj instanceof UUID) {
            // We now have logic to understand UUID and convert it to primitive type, however, we still might have
            // plans that would treat UUID as a 'struct'. In this case, we re-convert UUID back to struct.
            final var uuid = (UUID) obj;
            return new ImmutableRowStruct(new ArrayRow(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()), metaData.getStructMetaData(oneBasedColumn));
        } else {
            throw new SQLException("Struct", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
    }

    @Override
    public UUID getUUID(String columnLabel) throws SQLException {
        return getUUID(getOneBasedPosition(columnLabel));
    }

    @Override
    public UUID getUUID(int oneBasedColumn) throws SQLException {
        if (metaData.getColumnType(oneBasedColumn) != Types.OTHER) {
            throw new SQLException("Expected UUID should have type OTHER", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
        Object obj = getObjectInternal(getZeroBasedPosition(oneBasedColumn));
        if (obj == null) {
            return null;
        }
        Assert.thatUnchecked(obj instanceof UUID, ErrorCode.CANNOT_CONVERT_TYPE, "Expected UUID, got {}", obj.getClass().getName());
        return (UUID) obj;
    }

    @Override
    public RelationalStruct getStruct(String columnLabel) throws SQLException {
        return getStruct(getOneBasedPosition(columnLabel));
    }

    private int getOneBasedPosition(String columnLabel) throws SQLException {
        for (int pos = 1; pos <= metaData.getColumnCount(); pos++) {
            if (metaData.getColumnName(pos).equalsIgnoreCase(columnLabel)) {
                return pos;
            }
        }
        throw new InvalidColumnReferenceException("Invalid column: " + columnLabel).toSqlException();
    }

    private int getZeroBasedPosition(int oneBasedPosition) {
        return metaData.getLeadingPhantomColumnCount() + oneBasedPosition - 1;
    }

    @Override
    public String toString() {
        // We want to preserve the last wasNull state and not let it get effected by toString() since this too uses
        // the getObject() call to fetch values.
        final var preservedWasNullValue = wasNull();
        final var builder = new StringBuilder();
        try {
            final var colCount = metaData.getColumnCount();
            builder.append("{ ");
            for (int i = 1; i <= colCount; i++) {
                builder.append(metaData.getColumnName(i))
                        .append(" -> ")
                        .append(getObject(i))
                        .append("\n"); // todo: indentation.
                if (i < colCount) {
                    builder.append(", ");
                }
            }
            builder.append("} ");
        } catch (SQLException e) {
            throw new UncheckedRelationalException(new RelationalException(e));
        } finally {
            // restore wasNull
            wasNull = preservedWasNullValue;
        }
        return builder.toString();
    }
}
