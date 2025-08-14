/*
 * EmbeddedRelationalStruct.java
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

import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public interface EmbeddedRelationalStruct extends RelationalStruct {

    static RelationalStructBuilder newBuilder() {
        return new Builder();
    }

    class Builder implements RelationalStructBuilder {

        final List<DataType.StructType.Field> fields = new ArrayList<>();

        final List<Object> elements = new ArrayList<>();

        @Override
        public EmbeddedRelationalStruct build() {
            // Ideally, the name of the struct should be something that the user can specify, however, this actually
            // interferes with the DDL-defined types. Hence, making this random is the way to enforce coercion
            // between the two - rather than confusing the downstream planner.
            final var name = "ANONYMOUS_STRUCT_" + UUID.randomUUID().toString().replace("-", "_");
            final var type = DataType.StructType.from(name, fields, false);
            return new ImmutableRowStruct(new ArrayRow(elements.toArray()), RelationalStructMetaData.of(type));
        }

        @Override
        public Builder addBoolean(String fieldName, boolean b) {
            return addField(fieldName, DataType.Primitives.BOOLEAN.type(), b);
        }

        @Override
        public Builder addLong(String fieldName, long l) throws SQLException {
            return addField(fieldName, DataType.Primitives.LONG.type(), l);
        }

        @Override
        public Builder addFloat(String fieldName, float f) {
            return addField(fieldName, DataType.Primitives.FLOAT.type(), f);
        }

        @Override
        public Builder addDouble(String fieldName, double d) {
            return addField(fieldName, DataType.Primitives.DOUBLE.type(), d);
        }

        @Override
        public Builder addBytes(String fieldName, byte[] bytes) {
            return addField(fieldName, DataType.Primitives.BYTES.type(), bytes);
        }

        @Override
        public Builder addString(String fieldName, @Nullable String s) {
            return addField(fieldName, DataType.Primitives.STRING.type(), s);
        }

        @Override
        public Builder addUuid(String fieldName, @Nullable UUID uuid) {
            return addField(fieldName, DataType.Primitives.UUID.type(), uuid);
        }

        @Override
        public RelationalStructBuilder addObject(String fieldName, @Nullable Object obj) throws SQLException {
            if (obj instanceof RelationalStruct) {
                return addStruct(fieldName, (RelationalStruct) obj);
            }
            if (obj instanceof RelationalArray) {
                return addArray(fieldName, (RelationalArray) obj);
            }
            return addField(fieldName, DataType.getDataTypeFromObject(obj), obj);
        }

        @Override
        public Builder addStruct(String fieldName, @Nonnull RelationalStruct struct) throws SQLException {
            addField(fieldName, struct.getMetaData().getRelationalDataType(), struct);
            return this;
        }

        @Override
        public Builder addArray(String fieldName, @Nonnull RelationalArray array) throws SQLException {
            addField(fieldName, array.getMetaData().asRelationalType(), array);
            return this;
        }

        @Override
        public Builder addInt(String fieldName, int i) {
            return addField(fieldName, DataType.Primitives.INTEGER.type(), i);
        }

        private Builder addField(@Nonnull String fieldName, @Nonnull DataType type, @Nullable Object o) {
            fields.add(DataType.StructType.Field.from(fieldName, type, fields.size() + 1));
            elements.add(o);
            return this;
        }
    }
}
