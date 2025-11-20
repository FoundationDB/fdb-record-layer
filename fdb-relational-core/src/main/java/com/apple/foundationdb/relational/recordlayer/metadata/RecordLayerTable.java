/*
 * RecordLayerTable.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.api.metadata.Visitor;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.DescriptorProtos;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a {@link com.apple.foundationdb.relational.api.metadata.Table} that is backed by the Record Layer.
 */
@API(API.Status.EXPERIMENTAL)
public final class RecordLayerTable implements Table {

    @Nonnull
    private final String name;

    @Nonnull
    private final List<RecordLayerColumn> columns;

    @Nonnull
    private final Set<RecordLayerIndex> indexes;

    @Nonnull
    private final KeyExpression primaryKey;

    @Nonnull
    private final DataType.StructType dataType;

    @Nonnull
    private final Type.Record record;

    @Nonnull
    private final Map<Integer, DescriptorProtos.FieldOptions> generations;

    private RecordLayerTable(@Nonnull final String name,
                             @Nonnull final List<RecordLayerColumn> columns,
                             @Nonnull final Set<RecordLayerIndex> indexes,
                             @Nonnull final KeyExpression primaryKey,
                             @Nonnull final Map<Integer, DescriptorProtos.FieldOptions> generations,
                             final DataType.StructType dataType,
                             final Type.Record record) {
        this.name = name;
        this.columns = ImmutableList.copyOf(columns);
        this.indexes = ImmutableSet.copyOf(indexes);
        this.primaryKey = primaryKey;
        this.generations = generations;
        this.dataType = dataType == null ? calculateDataType() : dataType;
        this.record = record == null ? calculateRecordLayerType() : record;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public Set<RecordLayerIndex> getIndexes() {
        return indexes;
    }

    @Nonnull
    public Map<Integer, DescriptorProtos.FieldOptions> getGenerations() {
        return generations;
    }

    @Nonnull
    @Override
    public Collection<RecordLayerColumn> getColumns() {
        return columns;
    }

    @Nonnull
    public Type.Record getType() {
        return record;
    }

    @Nonnull
    public KeyExpression getPrimaryKey() {
        return primaryKey;
    }

    // TODO: remove
    @Override
    public void accept(@Nonnull final Visitor visitor) {
        visitor.visit(this);

        for (final var index : getIndexes()) {
            index.accept(visitor);
        }

        for (final var column : getColumns()) {
            column.accept(visitor);
        }
    }

    @Nonnull
    private Type.Record calculateRecordLayerType() {
        return (Type.Record) DataTypeUtils.toRecordLayerType(getDatatype());
    }

    @Nonnull
    private DataType.StructType calculateDataType() {
        final var columnTypes = ImmutableList.<DataType.StructType.Field>builder();
        for (final var column : columns) {
            columnTypes.add(DataType.StructType.Field.from(column.getName(), column.getDataType(), column.getIndex()));
        }
        /*
         * TODO (yhatem): note this is not entirely correct. Currently we're not setting nullable
         *               fields with reference types correctly. I think we need to be more smart about how
         *               we do this considering that we have two different ways of handling nulls in Relational
         *               Record Layer and ProtoBuf.
         *               consider the following example:
         *                      CREATE TYPE AS STRUCT B ( x bigint )
         *                      CREATE TABLE tbl1 (id bigint, v1 B null, v2 B not null, PRIMARY KEY(id))
         *               both Relational and RecordLayer will have two different types of the _same_ B since
         *               in one case, it is nullable (as per v1) and in another case it is not nullable (as per v2)
         *               If we, as we do today, serialize B into a single protobuf descriptor that is referenced
         *               by _both_ v1 and v2, we lose the nullability information.
         *               We could solve this problem by having _two_ descriptors of B in this case, this becomes possible
         *               with the Relational metadata API because we use internal names for referencing types instead
         *               of relying on the user-input type. i.e. we can create two protobuf descriptors for nullable
         *               and not-nullable referencing versions of B.
         *               I think this makes sense because nullablility is a property the participates in defining the
         *               identity of the type.
          */

        return DataType.StructType.from(getName(), columnTypes.build(), true);
    }

    @Nonnull
    @Override
    public DataType.StructType getDatatype() {
        return dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getName());
    }

    // TODO (yhatem) this does not look right and will probably need refinement.
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof RecordLayerTable)) {
            return false;
        }

        return this.getName().equals(((RecordLayerTable) obj).getName());
    }

    /**
     * A builder for {@link RecordLayerTable}.
     */
    public static final class Builder {
        private String name;

        @Nonnull
        private Set<RecordLayerIndex> indexes;

        @Nonnull
        private ImmutableList.Builder<RecordLayerColumn> columns;

        @Nonnull
        private List<KeyExpression> primaryKeyParts;

        @Nonnull
        private Map<Integer, DescriptorProtos.FieldOptions> generations;

        private DataType.StructType dataType;

        private Type.Record record;

        private Builder() {
            this.indexes = new LinkedHashSet<>();
            this.columns = ImmutableList.builder();
            this.primaryKeyParts = new ArrayList<>();
            this.generations = new LinkedHashMap<>();

            this.primaryKeyParts.add(Key.Expressions.recordType());
        }

        @Nonnull
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder setDatatype(@Nonnull DataType.StructType dataType) {
            this.dataType = dataType;
            return this;
        }

        @Nonnull
        public Builder setRecord(@Nonnull Type.Record record) {
            this.record = record;
            return this;
        }

        @Nonnull
        public Builder addIndex(@Nonnull final RecordLayerIndex index) {
            Assert.thatUnchecked(indexes.stream().noneMatch(i -> index.getName().equals(i.getName())),
                    ErrorCode.INDEX_ALREADY_EXISTS, () -> "attempt to add duplicate index '%s'" + index.getName());
            this.indexes.add(index);
            return this;
        }

        @Nonnull
        public Builder addIndexes(@Nonnull final Collection<RecordLayerIndex> indexes) {
            indexes.forEach(this::addIndex);
            return this;
        }

        @Nonnull
        public Builder addGenerations(@Nonnull final Map<Integer, DescriptorProtos.FieldOptions> generations) {
            generations.forEach(this::addGeneration);
            return this;
        }

        @Nonnull
        public Builder addGeneration(int number, @Nonnull DescriptorProtos.FieldOptions options) {
            Assert.thatUnchecked(!generations.containsKey(number), ErrorCode.TABLE_ALREADY_EXISTS, "Duplicate field number %d for generation of Table %s", number, name);
            Assert.thatUnchecked(!generations.containsValue(options), ErrorCode.TABLE_ALREADY_EXISTS, "Duplicated options for different generations of Table %s", name);
            generations.put(number, options);
            return this;
        }

        @Nonnull
        public Builder setPrimaryKey(@Nonnull final KeyExpression primaryKey) {
            if (primaryKey instanceof ThenKeyExpression) {
                this.primaryKeyParts = new ArrayList<>(((ThenKeyExpression) primaryKey).getChildren());
            } else if (primaryKey instanceof EmptyKeyExpression) {
                this.primaryKeyParts.clear();
            } else {
                this.primaryKeyParts.clear();
                this.primaryKeyParts.add(primaryKey);
            }
            return this;
        }

        @Nonnull
        public Builder addPrimaryKeyPart(@Nonnull final List<String> primaryKeyPart) {
            primaryKeyParts.add(toKeyExpression(record, primaryKeyPart));
            return this;
        }

        @Nonnull
        public Builder addColumn(@Nonnull final RecordLayerColumn column) {
            columns.add(column);
            return this;
        }

        @Nonnull
        public Builder addColumns(@Nonnull final Collection<RecordLayerColumn> columns) {
            this.columns.addAll(columns);
            return this;
        }

        @Nonnull
        private static KeyExpression toKeyExpression(@Nullable Type.Record type, @Nonnull final List<String> fields) {
            Assert.thatUnchecked(!fields.isEmpty());
            return toKeyExpression(type, fields.iterator());
        }

        @Nonnull
        private static KeyExpression toKeyExpression(@Nullable Type.Record type, @Nonnull final Iterator<String> fields) {
            Assert.thatUnchecked(fields.hasNext());
            String fieldName = fields.next();
            Type.Record.Field field = getFieldDefinition(type, fieldName);
            final FieldKeyExpression expression = Key.Expressions.field(getFieldStorageName(field, fieldName));
            if (fields.hasNext()) {
                Type.Record fieldType = getFieldRecordType(type, field);
                return expression.nest(toKeyExpression(fieldType, fields));
            } else {
                return expression;
            }
        }

        @Nullable
        private static Type.Record.Field getFieldDefinition(@Nullable Type.Record type, @Nonnull String fieldName) {
            return type == null ? null : type.getFieldNameFieldMap().get(fieldName);
        }

        @Nonnull
        private static String getFieldStorageName(@Nullable Type.Record.Field field, @Nonnull String fieldName) {
            return field == null ? ProtoUtils.toProtoBufCompliantName(fieldName) : field.getFieldStorageName();
        }

        @Nullable
        private static Type.Record getFieldRecordType(@Nullable Type.Record record, @Nullable Type.Record.Field field) {
            if (field == null) {
                return null;
            }
            Type fieldType = field.getFieldType();
            if (!(fieldType instanceof Type.Record)) {
                Assert.failUnchecked(ErrorCode.INVALID_COLUMN_REFERENCE, "Field '" + field.getFieldName() + "' on type '" + (record == null ? "UNKNONW" : record.getName()) + "' is not a struct");
            }
            return (Type.Record) fieldType;
        }

        @Nonnull
        private KeyExpression getPrimaryKey() {
            if (primaryKeyParts.isEmpty()) {
                return EmptyKeyExpression.EMPTY;
            } else if (primaryKeyParts.size() == 1) {
                return primaryKeyParts.get(0);
            } else {
                return Key.Expressions.concat(primaryKeyParts);
            }
        }

        @Nonnull
        public RecordLayerTable build() {
            Assert.notNullUnchecked(name, "table name is not set");

            final var columnsList = normalize(columns.build());

            Assert.thatUnchecked(!columnsList.isEmpty(), ErrorCode.INVALID_TABLE_DEFINITION, "Attempting to create table %s without columns", name);

            final var indexesSet = ImmutableSet.copyOf(indexes);

            return new RecordLayerTable(name, columnsList, indexesSet, getPrimaryKey(), generations, dataType, record);
        }

        @Nonnull
        public static Builder from(@Nonnull final RecordLayerTable table) {
            return newBuilder(false)
                    .setName(table.getName())
                    .addColumns(table.getColumns())
                    .setPrimaryKey(table.getPrimaryKey())
                    .addIndexes(table.getIndexes())
                    .setDatatype(table.getDatatype())
                    .setRecord(table.getType())
                    .addGenerations(table.getGenerations());
        }

        @Nonnull
        public static Builder from(@Nonnull final DataType.StructType structType) {
            return newBuilder(false)
                    .setName(structType.getName())
                    .addColumns(structType.getFields().stream().map(RecordLayerColumn::from).collect(Collectors.toList()))
                    .setDatatype(structType);
        }

        @Nonnull
        public static Builder from(@Nonnull final Type.Record record) {
            final var relationalType = DataTypeUtils.toRelationalType(record);
            Assert.thatUnchecked(relationalType instanceof DataType.StructType);
            final var asStruct = (DataType.StructType) relationalType;
            // todo (yhatem): we can avoid regenerating the corresponding record layer Record
            //       when we know that the passed record matches _exactly_ the corresponding Relational type.
            //       this could be achieved by checking whether the record has an explicit name and all of its
            //       fields (recursively) have explicit names, this seems to be mostly equally expensive though.
            return from(asStruct).setRecord(record);
        }

        @Nonnull
        private static List<RecordLayerColumn> normalize(@Nonnull final List<RecordLayerColumn> columns) {
            if (columns.stream().allMatch(c -> c.getIndex() >= 0)) {
                return columns;
            }
            final ImmutableList.Builder<RecordLayerColumn> result = ImmutableList.builder();
            for (int i = 1; i <= columns.size(); i++) {
                final var column = columns.get(i - 1);
                if (column.getIndex() < 0) {
                    result.add(RecordLayerColumn.newBuilder().setName(column.getName()).setIndex(i).setDataType(column.getDataType()).build());
                } else {
                    result.add(column);
                }
            }
            return result.build();
        }
    }

    @Nonnull
    public static Builder newBuilder(boolean intermingleTables) {
        Builder builder = new Builder();
        if (intermingleTables) {
            builder.setPrimaryKey(EmptyKeyExpression.EMPTY);
        }
        return builder;
    }
}
