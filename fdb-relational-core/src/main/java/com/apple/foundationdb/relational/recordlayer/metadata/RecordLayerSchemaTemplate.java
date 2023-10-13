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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Index;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.api.metadata.Visitor;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.FileDescriptorSerializer;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.RecordMetadataDeserializer;
import com.apple.foundationdb.relational.recordlayer.metadata.serde.RecordMetadataSerializer;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public final class RecordLayerSchemaTemplate implements SchemaTemplate {

    @Nonnull
    private final String name;

    @Nonnull
    private final Set<RecordLayerTable> tables;

    private final int version;

    private final boolean enableLongRows;

    private final boolean storeRowVersions;

    @Nonnull
    private final Supplier<RecordMetaData> metaDataSupplier;

    @Nonnull
    private final Supplier<Multimap<String, String>> tableIndexMappingSupplier;

    @Nonnull
    private final Supplier<Set<String>> indexesSupplier;

    private RecordLayerSchemaTemplate(@Nonnull final String name,
                                      @Nonnull final Set<RecordLayerTable> tables,
                                      int version,
                                      boolean enableLongRows,
                                      boolean storeRowVersions) {
        this.name = name;
        this.tables = tables;
        this.version = version;
        this.enableLongRows = enableLongRows;
        this.storeRowVersions = storeRowVersions;
        this.metaDataSupplier = Suppliers.memoize(this::buildRecordMetadata);
        this.tableIndexMappingSupplier = Suppliers.memoize(this::computeTableIndexMapping);
        this.indexesSupplier = Suppliers.memoize(this::computeIndexes);
    }

    private RecordLayerSchemaTemplate(@Nonnull final String name,
                                      @Nonnull final Set<RecordLayerTable> tables,
                                      int version,
                                      boolean enableLongRows,
                                      boolean storeRowVersions,
                                      @Nonnull final RecordMetaData cachedMetadata) {
        this.name = name;
        this.version = version;
        this.tables = tables;
        this.enableLongRows = enableLongRows;
        this.storeRowVersions = storeRowVersions;
        this.metaDataSupplier = Suppliers.memoize(() -> cachedMetadata);
        this.tableIndexMappingSupplier = Suppliers.memoize(this::computeTableIndexMapping);
        this.indexesSupplier = Suppliers.memoize(this::computeIndexes);
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public boolean isEnableLongRows() {
        return enableLongRows;
    }

    @Override
    public boolean isStoreRowVersions() {
        return storeRowVersions;
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
                                                               int version) {
        final var deserializer = new RecordMetadataDeserializer(metaData);
        final var builder = deserializer.getSchemaTemplate(templateName, version);
        return builder.setCachedMetadata(metaData).build();
    }

    /**
     * Retrieves a {@link Table} by looking up its name.
     *
     * @param tableName The name of the {@link Table}.
     * @return An {@link Optional} containing the {@link Table} if it is found, otherwise {@code Empty}.
     */
    @Nonnull
    @Override
    public Optional<Table> findTableByName(@Nonnull final String tableName) {
        for (final var table : getTables()) {
            if (table.getName().equals(tableName)) {
                return Optional.of(table);
            }
        }
        return Optional.empty();
    }

    @Nonnull
    private Multimap<String, String> computeTableIndexMapping() {
        final var result = ImmutableSetMultimap.<String, String>builder();
        for (final var table : getTables()) {
            for (final var index : table.getIndexes()) {
                result.put(table.getName(), index.getName());
            }
        }
        return result.build();
    }

    /**
     * Returns a list of all table-scoped {@link Index}es in the schema template.
     *
     * @return a multi-map whose key is the {@link Table} name, and value(s) is the {@link Index}.
     */
    @Override
    @Nonnull
    public Multimap<String, String> getTableIndexMapping() {
        return tableIndexMappingSupplier.get();
    }

    @Nonnull
    private Set<String> computeIndexes() {
        final Set<String> result = new TreeSet<>();

        for (final var table : getTables()) {
            for (final var index : table.getIndexes()) {
                result.add(index.getName());
            }
        }
        return result;
    }

    @Nonnull
    @Override
    public Set<String> getIndexes() throws RelationalException {
        return indexesSupplier.get();
    }

    @Nonnull
    @Override
    public BitSet getIndexEntriesAsBitset(@Nonnull final Optional<Set<String>> indexNames) throws RelationalException {
        final var indexSet = getIndexes(); // sorted (50)
        final var result = new BitSet(indexSet.size());
        if (indexNames.isEmpty()) { // all indexes are readable.
            result.set(0, indexSet.size()); // set all to '1'.
            return result;
        }
        final var allExists = indexSet.containsAll(indexNames.get());
        if (!allExists) {
            throw new RelationalException("could not find some of the provided index names ", ErrorCode.INVALID_SCHEMA_TEMPLATE);
        }
        int i = 0;
        for (final var index : indexSet) { // sorted
            if (indexNames.get().contains(index)) {
                result.set(i);
            }
            i++;
        }
        return result;
    }

    @Nonnull
    @Override
    public <T extends SchemaTemplate> T unwrap(@Nonnull final Class<T> iface) throws RelationalException {
        return iface.cast(this);
    }

    @Override
    public void accept(@Nonnull final Visitor visitor) {
        visitor.startVisit(this);
        visitor.visit(this);
        for (final var table : getTables()) {
            table.accept(visitor);
        }
        visitor.finishVisit(this);
    }

    public static final class Builder {
        private String name;

        private int version;

        private boolean enableLongRows;

        private boolean intermingleTables;

        private boolean storeRowVersions;

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
        public Builder setVersion(int version) {
            this.version = version;
            return this;
        }

        @Nonnull
        public Builder setEnableLongRows(boolean value) {
            this.enableLongRows = value;
            return this;
        }

        @Nonnull
        public Builder setIntermingleTables(boolean intermingleTables) {
            this.intermingleTables = intermingleTables;
            return this;
        }

        public boolean isIntermingleTables() {
            return intermingleTables;
        }

        @Nonnull
        public Builder setStoreRowVersions(boolean value) {
            this.storeRowVersions = value;
            return this;
        }

        @Nonnull
        public Builder addTable(@Nonnull RecordLayerTable table) {
            Assert.thatUnchecked(!tables.containsKey(table.getName()), String.format("table '%s' already exists", table.getName()),
                    ErrorCode.INVALID_SCHEMA_TEMPLATE);
            Assert.thatUnchecked(!auxiliaryTypes.containsKey(table.getName()), String.format("type with name '%s' already exists", table.getName()),
                    ErrorCode.INVALID_SCHEMA_TEMPLATE);
            if (!intermingleTables) {
                Assert.thatUnchecked(Key.Expressions.recordType().isPrefixKey(table.getPrimaryKey()),
                        String.format("table '%s' primary key '%s' is missing record type prefix", table.getName(), table.getPrimaryKey()),
                        ErrorCode.INTERNAL_ERROR);
            }
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
                return new RecordLayerSchemaTemplate(name, new LinkedHashSet<>(tables.values()), version, enableLongRows, storeRowVersions, cachedMetadata);
            } else {
                return new RecordLayerSchemaTemplate(name, new LinkedHashSet<>(tables.values()), version, enableLongRows, storeRowVersions);
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
            final Map<String, DataType.Named> resolvedTypes = new LinkedHashMap<>();
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
                    final var typeName = ((DataType.UnresolvedType) dataType).getName();
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

    @Nonnull
    @VisibleForTesting
    public Builder toBuilder() {
        return newBuilder()
                .setName(name)
                .setVersion(version)
                .setEnableLongRows(enableLongRows)
                .addTables(getTables());
    }
}
