/*
 * DdlTestUtil.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.PlannerConfiguration;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.util.Assert;
import com.google.protobuf.DescriptorProtos;
import org.assertj.core.api.Assertions;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DdlTestUtil {

    @Nonnull
    static PlanContext createVanillaPlanContext(@Nonnull final EmbeddedRelationalConnection connection,
                                                @Nonnull final String schemaTemplateName,
                                                @Nonnull final String databaseUri) throws RelationalException {
        return createVanillaPlanContext(connection, schemaTemplateName, databaseUri, PreparedParams.empty());
    }

    @Nonnull
    static PlanContext createVanillaPlanContext(@Nonnull final EmbeddedRelationalConnection connection,
                                                @Nonnull final String schemaTemplateName,
                                                @Nonnull final String databaseUri,
                                                @Nonnull final PreparedParams preparedParams) throws RelationalException {
        final var schemaTemplate = connection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toBuilder()
                .setVersion(1).setName(schemaTemplateName).build();
        RecordMetaDataProto.MetaData md = schemaTemplate.toRecordMetadata().toProto();
        return PlanContext.Builder.create()
                .withMetadata(RecordMetaData.build(md))
                .withMetricsCollector(Assert.notNullUnchecked(connection.getMetricCollector()))
                .withPlannerConfiguration(PlannerConfiguration.ofAllAvailableIndexes())
                .withUserVersion(0)
                .withDbUri(URI.create(databaseUri))
                .withDdlQueryFactory(NoOpQueryFactory.INSTANCE)
                .withConstantActionFactory(NoOpMetadataOperationsFactory.INSTANCE)
                .withSchemaTemplate(schemaTemplate)
                .withPreparedParameters(preparedParams)
                .build();
    }

    @Nonnull
    static PlanGenerator getPlanGenerator(@Nonnull final EmbeddedRelationalConnection embeddedConnection,
                                          @Nonnull final String schemaTemplateName,
                                          @Nonnull final String databaseUri) throws SQLException, RelationalException {
        final var planContext = createVanillaPlanContext(embeddedConnection, schemaTemplateName, databaseUri);
        final var storeState = new RecordStoreState(null, Map.of());
        try (var schema = embeddedConnection.getRecordLayerDatabase().loadSchema(embeddedConnection.getSchema())) {
            final var metadata = schema.loadStore().getRecordMetaData();
            return PlanGenerator.create(Optional.empty(), planContext, metadata, storeState, IndexMaintainerFactoryRegistryImpl.instance(), Options.NONE);
        }
    }

    @Nonnull
    static PlanGenerator getPlanGenerator(@Nonnull final EmbeddedRelationalConnection embeddedConnection,
                                          @Nonnull final String schemaTemplateName,
                                          @Nonnull final String databaseUri,
                                          @Nonnull final MetadataOperationsFactory metadataOperationsFactory) throws SQLException, RelationalException {
        return getPlanGenerator(embeddedConnection, schemaTemplateName, databaseUri, metadataOperationsFactory, PreparedParams.empty());
    }

    @Nonnull
    static PlanGenerator getPlanGenerator(@Nonnull final EmbeddedRelationalConnection embeddedConnection,
                                          @Nonnull final String schemaTemplateName,
                                          @Nonnull final String databaseUri,
                                          @Nonnull final MetadataOperationsFactory metadataOperationsFactory,
                                          @Nonnull final PreparedParams preparedParams) throws SQLException, RelationalException {
        return getPlanGenerator(embeddedConnection, schemaTemplateName, databaseUri, metadataOperationsFactory, preparedParams, Options.NONE);
    }

    @Nonnull
    static PlanGenerator getPlanGenerator(@Nonnull final EmbeddedRelationalConnection embeddedConnection,
                                          @Nonnull final String schemaTemplateName,
                                          @Nonnull final String databaseUri,
                                          @Nonnull final MetadataOperationsFactory metadataOperationsFactory,
                                          @Nonnull final PreparedParams preparedParams,
                                          @Nonnull final Options options) throws SQLException, RelationalException {
        final var planContext = PlanContext.Builder.unapply(createVanillaPlanContext(embeddedConnection, schemaTemplateName, databaseUri, preparedParams))
                .withConstantActionFactory(metadataOperationsFactory).build();
        final var storeState = new RecordStoreState(null, Map.of());
        try (var schema = embeddedConnection.getRecordLayerDatabase().loadSchema(embeddedConnection.getSchema())) {
            final var metadata = schema.loadStore().getRecordMetaData();
            return PlanGenerator.create(Optional.empty(), planContext, metadata, storeState, IndexMaintainerFactoryRegistryImpl.instance(), options);
        }
    }

    @Nonnull
    static PlanGenerator getPlanGenerator(@Nonnull final EmbeddedRelationalConnection embeddedConnection,
                                          @Nonnull final String schemaTemplateName,
                                          @Nonnull final String databaseUri,
                                          @Nonnull final DdlQueryFactory ddlQueryFactory) throws SQLException, RelationalException {
        final var planContext = PlanContext.Builder.unapply(createVanillaPlanContext(embeddedConnection, schemaTemplateName, databaseUri))
                .withDdlQueryFactory(ddlQueryFactory).build();
        final var storeState = new RecordStoreState(null, Map.of());
        try (var schema = embeddedConnection.getRecordLayerDatabase().loadSchema(embeddedConnection.getSchema())) {
            final var metadata = schema.loadStore().getRecordMetaData();
            return PlanGenerator.create(Optional.empty(), planContext, metadata, storeState, IndexMaintainerFactoryRegistryImpl.instance(), Options.NONE);
        }
    }

    /**
     * A wrapper around {@link DescriptorProtos.FieldDescriptorProto} that provides utility methods
     * for extracting column information in a test-friendly format.
     *
     * <p>This class is primarily used in DDL parsing tests to verify that SQL column definitions
     * are correctly translated into protobuf field descriptors. It translates protobuf type information
     * back into SQL-like type names for comparison against expected DDL statements.
     */
    public static final class ParsedColumn {

        @Nonnull
        private final DescriptorProtos.FieldDescriptorProto fieldDescriptor;

        /**
         * Creates a new ParsedColumn wrapper around the given field descriptor.
         *
         * @param fieldDescriptor the protobuf field descriptor to wrap
         */
        public ParsedColumn(@Nonnull final DescriptorProtos.FieldDescriptorProto fieldDescriptor) {
            this.fieldDescriptor = fieldDescriptor;
        }

        /**
         * Returns the name of the column as defined in the field descriptor.
         *
         * @return the column name
         */
        @Nonnull
        String getName() {
            return fieldDescriptor.getName();
        }

        /**
         * Translates the protobuf field type back into a SQL-compatible type string.
         *
         * <p>This method maps protobuf types (TYPE_INT32, TYPE_STRING, etc.) to their SQL equivalents
         * (integer, string, etc.). It also handles special cases like:
         * <ul>
         *   <li>Vector types with precision and dimensions (e.g., "vector(3, float)")</li>
         *   <li>Array types (LABEL_REPEATED fields)</li>
         *   <li>Custom message and enum types</li>
         * </ul>
         *
         * @return the SQL-compatible type string for this column
         * @throws IllegalStateException if an unexpected protobuf type or vector precision is encountered
         */
        @Nonnull
        String getType() {
            String type = "";
            if (fieldDescriptor.hasTypeName() && !fieldDescriptor.getTypeName().isEmpty()) {
                return fieldDescriptor.getTypeName();
            } else {
                switch (fieldDescriptor.getType()) {
                    case TYPE_INT32:
                        type += "integer";
                        break;
                    case TYPE_INT64:
                        type += "bigint";
                        break;
                    case TYPE_FLOAT:
                        type += "float";
                        break;
                    case TYPE_DOUBLE:
                        type += "double";
                        break;
                    case TYPE_BOOL:
                        type += "boolean";
                        break;
                    case TYPE_STRING:
                        type += "string";
                        break;
                    case TYPE_BYTES: {
                        final var fieldOptions = fieldDescriptor.getOptions().getExtension(RecordMetaDataOptionsProto.field);
                        if (fieldOptions.hasVectorOptions()) {
                            final var precision = fieldOptions.getVectorOptions().getPrecision();
                            final var dimensions = fieldOptions.getVectorOptions().getDimensions();
                            String elementType;
                            if (precision == 16) {
                                elementType = "half";
                            } else if (precision == 32) {
                                elementType = "float";
                            } else if (precision == 64) {
                                elementType = "double";
                            } else {
                                throw new IllegalStateException("Unexpected vector precision " + precision);
                            }
                            type += "vector(" + dimensions + ", " + elementType + ")";
                        } else {
                            type += "bytes";
                        }
                    }
                    break;
                    case TYPE_ENUM: // fallthrough
                    case TYPE_MESSAGE:
                        type += fieldDescriptor.getTypeName();
                        break;
                    default:
                        throw new IllegalStateException("Unexpected descriptor type " + fieldDescriptor.getType());
                }
            }

            if (fieldDescriptor.getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED) {
                type = " array";
            }

            return type;
        }
    }

    /**
     * A wrapper around {@link DescriptorProtos.DescriptorProto} that represents a structured type
     * (such as a STRUCT) defined in a DDL schema.
     *
     * <p>This class parses a protobuf message descriptor and extracts its fields as {@link ParsedColumn}
     * instances, providing a test-friendly interface for verifying DDL type definitions. It translates
     * protobuf descriptors back into DDL-compatible format for comparison in tests.
     */
    public static class ParsedType {

        @Nonnull
        private final DescriptorProtos.DescriptorProto descriptor;

        @Nonnull
        private final List<ParsedColumn> columns;

        /**
         * Creates a new ParsedType wrapper around the given descriptor.
         *
         * @param descriptor the protobuf descriptor representing this type
         */
        ParsedType(@Nonnull final DescriptorProtos.DescriptorProto descriptor) {
            this.descriptor = descriptor;
            this.columns = parseColumns();
        }

        /**
         * Returns the name of this type as defined in the descriptor.
         *
         * @return the type name
         */
        @Nonnull
        String getName() {
            return descriptor.getName();
        }

        /**
         * Returns the list of columns (fields) that make up this type.
         *
         * @return the list of parsed columns
         */
        @Nonnull
        List<ParsedColumn> getColumns() {
            return columns;
        }

        @Nonnull
        private List<ParsedColumn> parseColumns() {
            List<ParsedColumn> cols = new ArrayList<>(descriptor.getFieldCount());
            for (DescriptorProtos.FieldDescriptorProto field :descriptor.getFieldList()) {
                cols.add(new ParsedColumn(field));
            }
            return cols;
        }

        /**
         * Returns the columns formatted as DDL column definitions.
         *
         * <p>Each entry in the returned list is a string in the format "NAME TYPE",
         * matching the way the DDL language expects column definitions.
         *
         * @return a list of column definition strings
         */
        List<String> getColumnStrings() {
            return columns.stream().map(col -> col.getName() + " " + col.getType()).collect(Collectors.toList());
        }
    }

    /**
     * A wrapper around {@link DescriptorProtos.DescriptorProto} that represents a table
     * defined in a DDL schema.
     *
     * <p>This is a specialized version of {@link ParsedType} that specifically represents
     * tables (as opposed to user-defined types). Tables are distinguished from types by
     * their presence in the schema's union descriptor.
     */
    public static final class ParsedTable extends ParsedType {
        /**
         * Creates a new ParsedTable wrapper around the given descriptor.
         *
         * @param descriptor the protobuf descriptor representing this table
         */
        public ParsedTable(DescriptorProtos.DescriptorProto descriptor) {
            super(descriptor);
        }
    }

    /**
     * A wrapper around {@link DescriptorProtos.FileDescriptorProto} that represents a complete
     * DDL schema with tables and types.
     *
     * <p>This class parses a protobuf file descriptor and separates message types into tables
     * and user-defined types. Tables are identified by their presence in the schema's union
     * descriptor, while other message types are treated as custom types (e.g., STRUCTs).
     *
     * <p>This class is primarily used in DDL parsing tests to verify that entire schema
     * definitions are correctly translated from SQL to protobuf and back.
     */
    public static final class ParsedSchema {

        @Nonnull
        private final DescriptorProtos.FileDescriptorProto schemaDescriptor;

        @Nonnull
        private List<ParsedTable> tables;

        @Nonnull
        private List<ParsedType> types;

        /**
         * Creates a new ParsedSchema wrapper around the given file descriptor.
         *
         * <p>Upon construction, this immediately parses the descriptor to separate
         * tables from types.
         *
         * @param schemaDescriptor the protobuf file descriptor representing the schema
         */
        ParsedSchema(@Nonnull final DescriptorProtos.FileDescriptorProto schemaDescriptor) {
            this.schemaDescriptor = schemaDescriptor;

            buildTypesAndTables();
        }

        /**
         * Returns the list of tables defined in this schema.
         *
         * @return the list of parsed tables
         */
        @Nonnull
        List<ParsedTable> getTables() {
            return tables;
        }

        /**
         * Returns the list of user-defined types (non-table types) in this schema.
         *
         * @return the list of parsed types
         */
        @Nonnull
        List<ParsedType> getTypes() {
            return types;
        }

        private void buildTypesAndTables() {
            // Parses the FileDescriptor into types and tables
            // First, find the UnionDescriptor so that we know what are tables
            Set<String> tableNames = new HashSet<>();
            for (DescriptorProtos.DescriptorProto typeDescriptor : schemaDescriptor.getMessageTypeList()) {
                final RecordMetaDataOptionsProto.RecordTypeOptions extension = typeDescriptor.getOptions().getExtension(RecordMetaDataOptionsProto.record);
                if (extension.hasUsage() && extension.getUsage() == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION) {
                    // we found the Union Descriptor
                    for (DescriptorProtos.FieldDescriptorProto tableDescriptor : typeDescriptor.getFieldList()) {
                        tableNames.add(tableDescriptor.getTypeName());
                    }
                }
            }

            // now parse types and tables
            types = new ArrayList<>();
            tables = new ArrayList<>();

            for (DescriptorProtos.DescriptorProto typeDesc : schemaDescriptor.getMessageTypeList()) {
                if (tableNames.contains(typeDesc.getName())) {
                    tables.add(new ParsedTable(typeDesc));
                } else {
                    types.add(new ParsedType(typeDesc));
                }
            }
        }

        /**
         * Finds and returns a user-defined type by name.
         *
         * @param typeName the name of the type to find
         * @return the ParsedType if found, null otherwise
         */
        @Nonnull
        @SuppressWarnings("DataFlowIssue")
        ParsedType getType(@Nonnull final String typeName) {
            for (final ParsedType parsedType : types) {
                if (parsedType.getName().equals(typeName)) {
                    return parsedType;
                }
            }
            Assertions.fail("could not find type " + typeName);
            return null; // not reachable.
        }

        /**
         * Finds and returns a table by name.
         *
         * @param tableName the name of the table to find
         * @return the ParsedType representing the table if found, null otherwise
         */
        @Nonnull
        @SuppressWarnings("DataFlowIssue")
        ParsedType getTable(@Nonnull final String tableName) {
            for (final ParsedType table : tables) {
                if (table.getName().equals(tableName)) {
                    return table;
                }
            }
            Assertions.fail("could not find table" + tableName);
            return null; // not reachable.
        }
    }
}
