/*
 * EmbeddedRelationalArray.java
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
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public interface EmbeddedRelationalArray extends RelationalArray {

    static RelationalArrayBuilder newBuilder() {
        return new Builder();
    }

    static RelationalArrayBuilder newBuilder(@Nonnull ArrayMetaData metaData) {
        return new Builder(metaData);
    }

    class Builder implements RelationalArrayBuilder {

        private ArrayMetaData metaData;
        private final List<Object> elements = new ArrayList<>();

        private Builder() {
        }

        private Builder(@Nonnull ArrayMetaData metaData) {
            this.metaData = metaData;
        }

        @Override
        public EmbeddedRelationalArray build() {
            return new RowArray(elements, metaData);
        }

        @Override
        public Builder addAll(@Nonnull Object... values) throws SQLException {
            for (var value : values) {
                final var sqlType = SqlTypeSupport.getSqlTypeCodeFromObject(value);
                if (sqlType == Types.STRUCT) {
                    addStruct(((RelationalStruct) value));
                } else {
                    addPrimitive(value, sqlType);
                }
            }
            return this;
        }

        @Override
        public Builder addLong(long value) throws SQLException {
            return addPrimitive(value, Types.BIGINT);
        }

        @Override
        public Builder addString(@Nonnull String value) throws SQLException {
            return addPrimitive(value, Types.VARCHAR);
        }

        @Override
        public Builder addBytes(@Nonnull byte[] value) throws SQLException {
            return addPrimitive(value, Types.BINARY);
        }

        private Builder addPrimitive(@Nonnull Object value, int sqlType) throws SQLException {
            try {
                checkMetadata(sqlType);
            } catch (RelationalException ve) {
                throw ve.toSqlException();
            }
            elements.add(value);
            return this;
        }

        @Override
        public Builder addStruct(RelationalStruct struct) throws SQLException {
            try {
                checkMetadata(struct.getMetaData());
            } catch (RelationalException ve) {
                throw ve.toSqlException();
            }
            elements.add(struct);
            return this;
        }

        private void checkMetadata(@Nonnull StructMetaData metaData) throws SQLException, RelationalException {
            if (this.metaData != null) {
                Assert.that(this.metaData.getElementType() == Types.STRUCT, ErrorCode.DATATYPE_MISMATCH, "Cannot determine the type of Array!");
                Assert.that(this.metaData.getElementStructMetaData().equals(metaData), ErrorCode.DATATYPE_MISMATCH, "Metadata of struct elements in array do not match!");
            } else {
                this.metaData = RelationalArrayMetaData.ofStruct(metaData, DatabaseMetaData.columnNullable);
            }
        }

        private void checkMetadata(int sqlType) throws SQLException, RelationalException {
            if (this.metaData != null) {
                Assert.that(this.metaData.getElementType() == sqlType, ErrorCode.DATATYPE_MISMATCH, "Cannot determine the type of Array!");
            } else {
                this.metaData = RelationalArrayMetaData.ofPrimitive(sqlType, DatabaseMetaData.columnNullable);
            }
        }
    }
}
