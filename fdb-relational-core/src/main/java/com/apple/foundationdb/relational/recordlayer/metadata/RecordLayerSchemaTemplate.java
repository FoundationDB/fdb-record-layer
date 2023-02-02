/*
 * RecordLayerSchemaTemplate.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.FileDescriptorSerializer;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.RecordMetadataDeserializer;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.RecordMetadataSerializer;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class RecordLayerSchemaTemplate implements SchemaTemplate {

    @Nonnull
    private final String name;

    @Nonnull
    private final Set<RecordLayerTable> tables;

    private final long version;

    private final boolean enableLongRows;

    @Nonnull
    private final Supplier<RecordMetaData> metaDataSupplier;

    private RecordLayerSchemaTemplate(@Nonnull final String name,
                                      @Nonnull final Set<RecordLayerTable> tables,
                                      long version, boolean enableLongRows) {
        this.name = name;
        this.version = version;
        this.tables = tables;
        this.enableLongRows = enableLongRows;
        this.metaDataSupplier = Suppliers.memoize(this::buildRecordMetadata);
    }

    private RecordLayerSchemaTemplate(@Nonnull final String name,
                                      @Nonnull final Set<RecordLayerTable> tables,
                                      long version,
                                      boolean enableLongRows,
                                      @Nonnull final RecordMetaData cachedMetadata) {
        this.name = name;
        this.version = version;
        this.tables = tables;
        this.enableLongRows = enableLongRows;
        this.metaDataSupplier = Suppliers.memoize(() -> cachedMetadata);
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public boolean isEnableLongRows() {
        return enableLongRows;
    }

    @Nonnull
    @Override
    public Set<RecordLayerTable> getTables() {
        return tables;
    }

    @Nonnull
    @Override
    public RecordLayerSchema generateSchema(@Nonnull String databaseId, @Nonnull String schemaName) {
        return new RecordLayerSchema(schemaName, databaseId, this);
    }

    @Nonnull
    public Descriptors.Descriptor getDescriptor(@Nonnull final String tableName) {
        return toRecordMetadata().getRecordType(tableName).getDescriptor();
    }

    @Nonnull
    private RecordMetaData buildRecordMetadata() {
        final var fileDescriptorProtoSerializer = new FileDescriptorSerializer();
        accept(fileDescriptorProtoSerializer);
        final Descriptors.FileDescriptor fileDescriptor;
        try {
            fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                    fileDescriptorProtoSerializer.getFileBuilder().build(),
                    new Descriptors.FileDescriptor[]{RecordMetaDataProto.getDescriptor()});
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RelationalException(ErrorCode.SERIALIZATION_FAILURE, e).toUncheckedWrappedException();
        }
        final var recordMetadataSerializer = new RecordMetadataSerializer(fileDescriptor);
        accept(recordMetadataSerializer);
        return recordMetadataSerializer.getBuilder().build();
    }

    @Nonnull
    public RecordMetaData toRecordMetadata() {
        return metaDataSupplier.get();
    }

    @Nonnull
    public static RecordLayerSchemaTemplate fromRecordMetadata(@Nonnull final RecordMetaData metaData,
                                                               @Nonnull final String templateName,
                                                               long version) {
        final var deserializer = new RecordMetadataDeserializer(metaData);
        final var builder = deserializer.getSchemaTemplate(templateName, version, metaData.isSplitLongRecords());
        return builder.setCachedMetadata(metaData).build();
    }

    @Nonnull
    @Override
    public <T extends SchemaTemplate> T unwrap(@Nonnull final Class<T> iface) throws RelationalException {
        return iface.cast(this);
    }

    public static final class Builder {
        private String name;

        private long version;

        private boolean enableLongRows;

        private final Map<String, RecordLayerTable> tables;

        private final Map<String, DataType.Named> auxiliaryTypes; // for quick lookup

        private RecordMetaData cachedMetadata;

        private Builder() {
            tables = new LinkedHashMap<>();
            auxiliaryTypes = new LinkedHashMap<>();
            // enable long rows is TRUE by default
            enableLongRows = true;
        }

        @Nonnull
        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        @Nonnull
        public Builder setEnableLongRows(boolean value) {
            this.enableLongRows = value;
            return this;
        }

        @Nonnull
        public Builder addTable(@Nonnull RecordLayerTable table) {
            Assert.thatUnchecked(!tables.containsKey(table.getName()), String.format("table '%s' already exists", table.getName()),
                    ErrorCode.INVALID_SCHEMA_TEMPLATE);
            Assert.thatUnchecked(!auxiliaryTypes.containsKey(table.getName()), String.format("type with name '%s' already exists", table.getName()),
                    ErrorCode.INVALID_SCHEMA_TEMPLATE);
            tables.put(table.getName(), table);
            return this;
        }

        @Nonnull
        public Builder addTables(@Nonnull final Collection<RecordLayerTable> tables) {
            tables.forEach(this::addTable);
            return this;
        }

        /**
         * Adds an auxiliary type, an auxiliary type is a type that is merely created, so it can be referenced later on
         * in a table definition. Any {@link DataType.Named} data type can be added as an auxiliary type such as {@code enum}s
         * and {@code struct}s.
         *
         * @param auxiliaryType The auxiliary {@link DataType} to add.
         * @return {@code this} {@link Builder}.
         */
        @Nonnull
        public Builder addAuxiliaryType(@Nonnull DataType.Named auxiliaryType) {
            Assert.thatUnchecked(!tables.containsKey(auxiliaryType.getName()), String.format("a table with name '%s' already exists", auxiliaryType.getName()),
                    ErrorCode.INVALID_SCHEMA_TEMPLATE);
            Assert.thatUnchecked(!auxiliaryTypes.containsKey(auxiliaryType.getName()), String.format("type with name '%s' already exists", auxiliaryType.getName()),
                    ErrorCode.INVALID_SCHEMA_TEMPLATE);
            auxiliaryTypes.put(auxiliaryType.getName(), auxiliaryType);
            return this;
        }

        /**
         * Adds a collection auxiliary types, an auxiliary type is a type that is merely created, so it can be referenced later on
         * in a table definition. Any {@link DataType.Named} data type can be added as an auxiliary type such as {@code enum}s
         * and {@code struct}s.
         *
         * @param auxiliaryTypes The auxiliary {@link DataType}s to add.
         * @return {@code this} {@link Builder}.
         */
        @Nonnull
        public Builder addAuxiliaryTypes(@Nonnull Collection<DataType.Named> auxiliaryTypes) {
            auxiliaryTypes.forEach(this::addAuxiliaryType);
            return this;
        }

        @Nonnull
        Builder setCachedMetadata(@Nonnull final RecordMetaData metadata) {
            this.cachedMetadata = metadata;
            return this;
        }

        @Nonnull
        public RecordLayerTable findTable(@Nonnull final String name) {
            Assert.thatUnchecked(tables.containsKey(name), String.format("could not find '%s'", name));
            return tables.get(name);
        }

        @Nonnull
        public RecordLayerTable extractTable(@Nonnull final String name) {
            Assert.thatUnchecked(tables.containsKey(name), String.format("could not find '%s'", name));
            return tables.remove(name);
        }

        @Nonnull
        public Optional<DataType> findType(@Nonnull final String name) {
            // we should also check whether the name exists in _both_ databases.
            if (tables.containsKey(name)) {
                return Optional.of(tables.get(name).getDatatype());
            }

            if (auxiliaryTypes.containsKey(name)) {
                return Optional.of((DataType) auxiliaryTypes.get(name));
            }

            return Optional.empty();
        }

        @Nonnull
        public RecordLayerSchemaTemplate build() {
            Assert.thatUnchecked(!tables.isEmpty(), "schema template contains no tables", ErrorCode.INVALID_SCHEMA_TEMPLATE);

            // make sure all tables and auxiliary types are resolved
            boolean needsResolution = false;
            for (final var table : tables.values()) {
                if (!table.getDatatype().isResolved()) {
                    needsResolution = true;
                    break;
                }
            }

            if (!needsResolution) {
                for (final var auxiliaryType : auxiliaryTypes.values()) {
                    if (!((DataType) auxiliaryType).isResolved()) {
                        needsResolution = true;
                        break;
                    }
                }
            }

            if (needsResolution) {
                resolveTypes();
            }

            if (cachedMetadata != null) {
                return new RecordLayerSchemaTemplate(name, new LinkedHashSet<>(tables.values()), version, enableLongRows, cachedMetadata);
            } else {
                return new RecordLayerSchemaTemplate(name, new LinkedHashSet<>(tables.values()), version, enableLongRows);
            }
        }

        private void resolveTypes() {
            // collect all named types from tables + auxiliary types.
            final var mapBuilder = ImmutableMap.<String, DataType>builder();
            for (final var table : tables.values()) {
                mapBuilder.put(table.getName(), table.getDatatype());
            }
            for (final var auxiliaryType : auxiliaryTypes.entrySet()) {
                mapBuilder.put(auxiliaryType.getKey(), (DataType) auxiliaryType.getValue());
            }
            final var namedTypes = mapBuilder.build();

            // create dependency graph
            final var depsBuilder = ImmutableMap.<DataType, Set<DataType>>builder();
            for (final var table : tables.values()) {
                depsBuilder.put(table.getDatatype(), getDependencies(table.getDatatype(), namedTypes));
            }
            for (final var auxiliaryType : auxiliaryTypes.entrySet()) {
                depsBuilder.put((DataType) auxiliaryType.getValue(), getDependencies((DataType) auxiliaryType.getValue(), namedTypes));
            }
            final var deps = depsBuilder.build();

            // sort it
            final var sorted = TopologicalSort.anyTopologicalOrderPermutation(new HashSet<>(namedTypes.values()), id -> deps.getOrDefault(id, ImmutableSet.of()));
            Assert.thatUnchecked(sorted.isPresent(), "Invalid cyclic dependency in the schema definition", ErrorCode.INVALID_SCHEMA_TEMPLATE);

            // resolve types
            final var resolvedTypes = new LinkedHashMap<String, DataType.Named>();
            for (final var type : sorted.get()) {
                var typeToAdd = type;
                if (!type.isResolved()) {
                    typeToAdd = type.resolve(resolvedTypes);
                }
                if (typeToAdd instanceof DataType.Named) {
                    final var asNamed = (DataType.Named) typeToAdd;
                    resolvedTypes.put(asNamed.getName(), asNamed);
                }
            }

            // use the resolve types now to resolve tables and auxiliary types
            final var resolvedTables = ImmutableMap.<String, RecordLayerTable>builder();
            for (final var table : tables.values()) {
                if (!table.getDatatype().isResolved()) {
                    final var builder = RecordLayerTable.Builder
                            .from((DataType.StructType) table.getDatatype().resolve(resolvedTypes).withNullable(table.getDatatype().isNullable()))
                            .setPrimaryKey(table.getPrimaryKey())
                            .addIndexes(table.getIndexes())
                            .addGenerations(table.getGenerations());
                    resolvedTables.put(table.getName(), builder.build());
                } else {
                    resolvedTables.put(table.getName(), table);
                }
            }

            tables.clear();
            tables.putAll(resolvedTables.build());

            final var resolvedAuxiliaryTypes = ImmutableMap.<String, DataType.Named>builder();
            for (final var auxiliarytype : auxiliaryTypes.entrySet()) {
                final var dataType = (DataType) auxiliarytype.getValue();
                if (!dataType.isResolved()) {
                    resolvedAuxiliaryTypes.put(auxiliarytype.getKey(), (DataType.Named) ((DataType) resolvedTypes.get(auxiliarytype.getKey())).withNullable(dataType.isNullable()));
                } else {
                    resolvedAuxiliaryTypes.put(auxiliarytype.getKey(), auxiliarytype.getValue());
                }
            }

            auxiliaryTypes.clear();
            auxiliaryTypes.putAll(resolvedAuxiliaryTypes.build());
        }

        @Nonnull
        private static Set<DataType> getDependencies(@Nonnull final DataType dataType, @Nonnull final Map<String, DataType> types) {
            // TODO (yhatem) I think this doesn't work in case of recursive types.
            //               moreover, this does not work with inlined types, but this is ok since we don't support them anyway.
            switch (dataType.getCode()) {
                case ARRAY:
                    return getDependencies(((DataType.ArrayType) dataType).getElementType(), types);
                case STRUCT:
                    final var mapBuilder = ImmutableSet.<DataType>builder();
                    for (final var field : ((DataType.StructType) dataType).getFields()) {
                        final var fieldType = field.getType();
                        if (fieldType instanceof DataType.Named) {
                            final var depName = ((DataType.Named) fieldType).getName();
                            Assert.thatUnchecked(types.containsKey(depName), String.format("could not find type '%s'", depName));
                            mapBuilder.add(types.get(depName));
                        } else if (fieldType.getCode() == DataType.Code.ARRAY && ((DataType.ArrayType) fieldType).getElementType() instanceof DataType.Named) {
                            final var asArray = (DataType.ArrayType) fieldType;
                            final var depName = ((DataType.Named) asArray.getElementType()).getName();
                            Assert.thatUnchecked(types.containsKey(depName), String.format("could not find type '%s'", depName));
                            mapBuilder.add(types.get(depName));
                        }
                    }
                    return mapBuilder.build();
                case UNKNOWN:
                    final var typeName = ((DataType.UnknownType) dataType).getName();
                    Assert.thatUnchecked(types.containsKey(typeName), String.format("could not find type '%s'", typeName));
                    return Set.of(types.get(typeName));
                default:
                    return Set.of();
            }
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }
}
