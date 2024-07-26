/*
 * EmbeddedRelationalStruct.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public interface EmbeddedRelationalStruct extends RelationalStruct {

    static RelationalStructBuilder newBuilder() {
        return new Builder();
    }

    class Builder implements RelationalStructBuilder {

        final List<FieldDescription> fields = new ArrayList<>();

        final List<Object> elements = new ArrayList<>();

        @Override
        public EmbeddedRelationalStruct build() {
            return new ImmutableRowStruct(new ArrayRow(elements.toArray()), new RelationalStructMetaData(fields.toArray(new FieldDescription[0])));
        }

        @Override
        public Builder addBoolean(String fieldName, boolean b) throws SQLException {
            return addPrimitive(fieldName, Types.BOOLEAN, b);
        }

        @Override
        public Builder addShort(String fieldName, short b) throws SQLException {
            return addPrimitive(fieldName, Types.SMALLINT, b);
        }

        @Override
        public Builder addLong(String fieldName, long l) throws SQLException {
            return addPrimitive(fieldName, Types.BIGINT, l);
        }

        @Override
        public Builder addFloat(String fieldName, float f) throws SQLException {
            return addPrimitive(fieldName, Types.FLOAT, f);
        }

        @Override
        public Builder addDouble(String fieldName, double d) throws SQLException {
            return addPrimitive(fieldName, Types.DOUBLE, d);
        }

        @Override
        public Builder addBytes(String fieldName, byte[] bytes) throws SQLException {
            return addPrimitive(fieldName, Types.BINARY, bytes);
        }

        @Override
        public Builder addString(String fieldName, @Nullable String s) throws SQLException {
            return addPrimitive(fieldName, Types.VARCHAR, s);
        }

        @Override
        public RelationalStructBuilder addObject(String fieldName, @Nullable Object obj, int targetSqlType) throws SQLException {
            if (targetSqlType == Types.STRUCT) {
                if (!(obj instanceof RelationalStruct)) {
                    throw new RelationalException(String.format("Expected object to be of type:STRUCT, but found type:%s",
                            obj == null ? "<NULL>" : SqlTypeNamesSupport.getSqlTypeName(SqlTypeSupport.getSqlTypeCodeFromObject(obj))),
                            ErrorCode.DATATYPE_MISMATCH).toSqlException();
                }
                return addStruct(fieldName, (RelationalStruct) obj);
            }
            if (targetSqlType == Types.ARRAY) {
                if (!(obj instanceof RelationalArray)) {
                    throw new RelationalException(String.format("Expected object to be of type:ARRAY, but found type:%s",
                            obj == null ? "<NULL>" : SqlTypeNamesSupport.getSqlTypeName(SqlTypeSupport.getSqlTypeCodeFromObject(obj))),
                            ErrorCode.DATATYPE_MISMATCH).toSqlException();
                }
                return addArray(fieldName, (RelationalArray) obj);
            }
            return addPrimitive(fieldName, targetSqlType, obj);
        }

        @Override
        public Builder addStruct(String fieldName, @Nonnull RelationalStruct struct) throws SQLException {
            fields.add(FieldDescription.struct(fieldName, DatabaseMetaData.columnNoNulls, struct.getMetaData()));
            elements.add(struct);
            return this;
        }

        @Override
        public Builder addArray(String fieldName, @Nonnull RelationalArray array) throws SQLException {
            fields.add(FieldDescription.array(fieldName, DatabaseMetaData.columnNoNulls, array.getMetaData()));
            elements.add(array);
            return this;
        }

        @Override
        public Builder addInt(String fieldName, int i) throws SQLException {
            return addPrimitive(fieldName, Types.INTEGER, i);
        }

        private Builder addPrimitive(@Nonnull String fieldName, int sqlType, @Nullable Object o) {
            fields.add(FieldDescription.primitive(fieldName, sqlType, DatabaseMetaData.columnNoNulls));
            elements.add(o);
            return this;
        }
    }

}
