/*
 * EmbeddedRelationalArray.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public interface EmbeddedRelationalArray extends RelationalArray {

    static RelationalArrayBuilder newBuilder() {
        return new Builder();
    }

    static RelationalArrayBuilder newBuilder(@Nonnull DataType elementType) {
        return new Builder(elementType);
    }

    class Builder implements RelationalArrayBuilder {

        @Nullable private DataType elementType;
        @Nonnull private final List<Object> elements = new ArrayList<>();

        private Builder() {
        }

        private Builder(@Nonnull DataType elementType) {
            this.elementType = elementType;
        }

        @Override
        public EmbeddedRelationalArray build() throws SQLException {
            if (elementType == null) {
                return new RowArray(List.of(), RelationalArrayMetaData.of(DataType.ArrayType.from(DataType.Primitives.NULL.type())));
            }
            return new RowArray(elements, RelationalArrayMetaData.of(DataType.ArrayType.from(elementType, false)));
        }

        @Override
        public Builder addAll(@Nonnull Object... values) throws SQLException {
            for (var value : values) {
                if (value == null) {
                    throw new RelationalException("Cannot add NULL to an array.", ErrorCode.DATATYPE_MISMATCH).toSqlException();
                }
                if (value instanceof RelationalStruct) {
                    addStruct((RelationalStruct) value) ;
                } else if (value instanceof RelationalArray) {
                    throw new RelationalException("ARRAY element in ARRAY not supported.", ErrorCode.UNSUPPORTED_OPERATION).toSqlException();
                } else {
                    final var type = DataType.getDataTypeFromObject(value);
                    addField(value, type);
                }
            }
            return this;
        }

        @Override
        public Builder addLong(long value) throws SQLException {
            return addField(value, DataType.Primitives.LONG.type());
        }

        @Override
        public Builder addString(@Nonnull String value) throws SQLException {
            return addField(value, DataType.Primitives.STRING.type());
        }

        @Override
        public Builder addBytes(@Nonnull byte[] value) throws SQLException {
            return addField(value, DataType.Primitives.BYTES.type());
        }

        @Override
        public Builder addUuid(@Nonnull UUID uuid) throws SQLException {
            return addField(uuid, DataType.Primitives.UUID.type());
        }

        @Override
        public Builder addObject(@Nonnull Object obj) throws SQLException {
            if (obj instanceof RelationalStruct) {
                return addStruct((RelationalStruct) obj);
            }
            if (obj instanceof RelationalArray) {
                throw new RelationalException("Array objects in Array in supported", ErrorCode.INTERNAL_ERROR).toSqlException();
            }
            return addField(obj, DataType.getDataTypeFromObject(obj));
        }

        @Nonnull
        private Builder addField(@Nonnull Object value, @Nonnull DataType type) throws SQLException {
            try {
                checkType(type);
            } catch (RelationalException ve) {
                throw ve.toSqlException();
            }
            elements.add(value);
            return this;
        }

        @Override
        public Builder addStruct(RelationalStruct struct) throws SQLException {
            try {
                checkType(struct.getMetaData().getRelationalDataType());
            } catch (RelationalException ve) {
                throw ve.toSqlException();
            }
            elements.add(struct);
            return this;
        }

        private void checkType(DataType type) throws RelationalException {
            if (this.elementType == null) {
                this.elementType = type;
            } else if (this.elementType instanceof DataType.CompositeType) {
                Assert.that(((DataType.CompositeType) elementType).hasIdenticalStructure(type), ErrorCode.DATATYPE_MISMATCH, "Expected array element to be of type:%s, but found type:%s", this.elementType, type);
            } else  {
                Assert.that(this.elementType.equals(type), ErrorCode.DATATYPE_MISMATCH, "Expected array element to be of type:%s, but found type:%s", this.elementType, type);
            }
        }
    }
}
