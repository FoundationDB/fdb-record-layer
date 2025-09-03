/*
 * CatalogMetaData.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.expressions.RecordKeyExpressionProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.catalog.CatalogMetaDataProvider;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.util.Assert;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * An implementation of {@link DatabaseMetaData}, that relies on the {@link StoreCatalog} to supply metadata info.
 */
@API(API.Status.EXPERIMENTAL)
public class CatalogMetaData implements RelationalDatabaseMetaData {
    private final StoreCatalog catalog;
    private final EmbeddedRelationalConnection conn;

    public CatalogMetaData(EmbeddedRelationalConnection conn, StoreCatalog catalog) {
        this.catalog = catalog;
        this.conn = conn;
    }

    @Nonnull
    @Override
    public RelationalResultSet getSchemas() throws SQLException {
        return getSchemas(conn.getPath().getPath(), null);
    }

    @Override
    public RelationalResultSet getSchemas(String catalogStr, String schemaPattern) throws SQLException {
        if (catalogStr == null) {
            throw new OperationUnsupportedException("Must use a non-null catalog name currently").toSqlException();
        }
        return conn.runIsolatedInTransactionIfPossible(() -> {
            try (RelationalResultSet rrs = catalog.listSchemas(conn.getTransaction(), URI.create(catalogStr), ContinuationImpl.BEGIN)) {
                //TODO(bfines) we need to transform this live, not materialize like this
                List<Row> simplifiedRows = new ArrayList<>();
                while (rrs.next()) {
                    Object[] data = {
                            conn.getPath(),
                            rrs.getString("SCHEMA_NAME"),
                    };
                    simplifiedRows.add(new ArrayRow(data));
                }
                final var schemasStructType = DataType.StructType.from("SCHEMAS", List.of(
                        DataType.StructType.Field.from("TABLE_CATALOG", DataType.Primitives.NULLABLE_STRING.type(), 0),
                        DataType.StructType.Field.from("TABLE_SCHEM", DataType.Primitives.NULLABLE_STRING.type(), 1)
                ), true);
                return new IteratorResultSet(RelationalStructMetaData.of(schemasStructType), simplifiedRows.iterator(), 0);
            } catch (SQLException sqle) {
                throw new RelationalException(sqle);
            }
        });
    }

    @Nonnull
    @Override
    public RelationalResultSet getTables(String database, String schema, String tableName, String[] types) throws SQLException {
        /*
         * The following are feature blocks, where the SQL spec calls for specific behavior that we either don't support
         * yet (or do not plan to ever support), so we through UNSUPPORTED_OPERATION errors for these. As we add in the
         * feature support, we should remove these checks.
         */
        if (database == null) {
            throw new SQLFeatureNotSupportedException("Cannot scan across Databases yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (schema == null) {
            throw new SQLFeatureNotSupportedException("Cannot scan across Schemas yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (tableName != null) {
            throw new SQLFeatureNotSupportedException("Table filters on getTables() is not supported yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (types != null) {
            throw new SQLFeatureNotSupportedException("Type filters on getTables() is not supported yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return conn.runIsolatedInTransactionIfPossible(() -> {
            final RecordMetaDataProto.MetaData schemaInfo = loadSchemaMetadata(database, schema);
            List<Row> tableList = schemaInfo.getRecordTypesList().stream()
                    .map(type -> new Object[]{
                            database,  //TABLE_CAT
                            schema,  //TABLE_SCHEM
                            type.getName(), //TABLE_NAME
                            type.hasSinceVersion() ? type.getSinceVersion() : null //TABLE_VERSION
                    })
                    .map(ArrayRow::new)
                    .collect(Collectors.toList());

            final var tablesStructType = DataType.StructType.from("TABLES", List.of(
                    DataType.StructType.Field.from("TABLE_CAT", DataType.Primitives.NULLABLE_STRING.type(), 0),
                    DataType.StructType.Field.from("TABLE_SCHEM", DataType.Primitives.NULLABLE_STRING.type(), 1),
                    DataType.StructType.Field.from("TABLE_NAME", DataType.Primitives.NULLABLE_STRING.type(), 2),
                    DataType.StructType.Field.from("TABLE_VERSION", DataType.Primitives.NULLABLE_LONG.type(), 3)

            ), true);
            return new IteratorResultSet(RelationalStructMetaData.of(tablesStructType), tableList.iterator(), 0);
        });
    }

    @Override
    public RelationalResultSet getPrimaryKeys(String database, String schema, String table) throws SQLException {
        return conn.runIsolatedInTransactionIfPossible(() -> {
            final RecordMetaDataProto.MetaData schemaInfo = loadSchemaMetadata(database, schema);
            Stream<Row> rows = schemaInfo.getRecordTypesList().stream()
                    .filter(type -> type.getName().equals(table))
                    .map(type -> {
                        RecordKeyExpressionProto.KeyExpression ke = type.getPrimaryKey();
                        return new AbstractMap.SimpleEntry<>(type.getName(), keyExpressionToPrimaryKey(ke));
                    }).flatMap(pks -> IntStream.range(0, pks.getValue().length)
                    .mapToObj(pos -> new ArrayRow(database,
                            schema,
                            pks.getKey(),
                            pks.getValue()[pos],
                            pos + 1,
                            null)));

            final var primaryKeysStructType = DataType.StructType.from("PRIMARY_KEYS", List.of(
                    DataType.StructType.Field.from("TABLE_CAT", DataType.Primitives.NULLABLE_STRING.type(), 0),
                    DataType.StructType.Field.from("TABLE_SCHEM", DataType.Primitives.NULLABLE_STRING.type(), 1),
                    DataType.StructType.Field.from("TABLE_NAME", DataType.Primitives.NULLABLE_STRING.type(), 2),
                    DataType.StructType.Field.from("COLUMN_NAME", DataType.Primitives.NULLABLE_STRING.type(), 3),
                    DataType.StructType.Field.from("KEY_SEQ", DataType.Primitives.NULLABLE_INTEGER.type(), 4),
                    DataType.StructType.Field.from("PK_NAME", DataType.Primitives.NULLABLE_STRING.type(), 5)
            ), true);
            return new IteratorResultSet(RelationalStructMetaData.of(primaryKeysStructType), rows.iterator(), 0);
        });
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //we actually can't here, it will violate RecordLayer isolation laws
    public RelationalResultSet getColumns(String database, String schema, String tablePattern, String columnPattern) throws SQLException {
        /*
         * The following are feature blocks, where the SQL spec calls for specific behavior that we either don't support
         * yet (or do not plan to ever support), so we through UNSUPPORTED_OPERATION errors for these. As we add in the
         * feature support, we should remove these checks.
         */
        if (database == null || database.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Cannot scan across Databases yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (schema == null || schema.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Cannot scan across Schemas yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (tablePattern == null || tablePattern.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Table must be specified", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (columnPattern != null) {
            throw new SQLFeatureNotSupportedException("Column filters on getColumns() is not supported yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return conn.runIsolatedInTransactionIfPossible(() -> {
            //TODO(bfines) this is a weird way of doing this, is there a better way?
            RecordMetaData rmd = new CatalogMetaDataProvider(this.catalog, URI.create(database), schema, conn.getTransaction()).getRecordMetaData();
            Descriptors.FileDescriptor fileDesc = rmd.getRecordsDescriptor();
            //verify that it is in fact a table
            try {
                rmd.getRecordType(tablePattern);
            } catch (MetaDataException mde) {
                throw new RelationalException("table <" + tablePattern + "> does not exist", ErrorCode.UNDEFINED_TABLE);
            }
            //now get its column data
            final Descriptors.Descriptor tableDescriptor = fileDesc.findMessageTypeByName(tablePattern);
            final List<Row> columnDefs = tableDescriptor.getFields().stream()
                    .map(field -> {
                        Object[] row = {
                                database,
                                schema,
                                tablePattern,
                                field.getName(),
                                ProtobufDdlUtil.getSqlType(field),
                                ProtobufDdlUtil.getTypeName(field),
                                -1,
                                0,
                                null,
                                null,
                                DatabaseMetaData.columnNullableUnknown, //TODO(bfines) we can probably figure this out
                                null,
                                field.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE ? field.getDefaultValue() : null,
                                -1,
                                -1,
                                -1,
                                field.getIndex() + 1,
                                "YES",
                                null,
                                null,
                                null,
                                null,
                                "NO",
                                "NO"
                        };
                        return new ArrayRow(row);
                    }).collect(Collectors.toList());

            final var columnsStructType = DataType.StructType.from("PRIMARY_KEYS", List.of(
                    DataType.StructType.Field.from("TABLE_CAT", DataType.Primitives.NULLABLE_STRING.type(), 0),
                    DataType.StructType.Field.from("TABLE_SCHEM", DataType.Primitives.NULLABLE_STRING.type(), 1),
                    DataType.StructType.Field.from("TABLE_NAME", DataType.Primitives.NULLABLE_STRING.type(), 2),
                    DataType.StructType.Field.from("COLUMN_NAME", DataType.Primitives.NULLABLE_STRING.type(), 3),
                    DataType.StructType.Field.from("DATA_TYPE", DataType.Primitives.NULLABLE_STRING.type(), 4),
                    DataType.StructType.Field.from("TYPE_NAME", DataType.Primitives.NULLABLE_STRING.type(), 5),
                    DataType.StructType.Field.from("COLUMN_SIZE", DataType.Primitives.NULLABLE_INTEGER.type(), 6),
                    DataType.StructType.Field.from("BUFFER_LENGTH", DataType.Primitives.NULLABLE_INTEGER.type(), 7),
                    DataType.StructType.Field.from("DECIMAL_DIGITS", DataType.Primitives.NULLABLE_INTEGER.type(), 8),
                    DataType.StructType.Field.from("NUM_PREC_RADIX", DataType.Primitives.NULLABLE_INTEGER.type(), 9),
                    DataType.StructType.Field.from("NULLABLE", DataType.Primitives.NULLABLE_INTEGER.type(), 10),
                    DataType.StructType.Field.from("REMARKS", DataType.Primitives.NULLABLE_STRING.type(), 11),
                    DataType.StructType.Field.from("COLUMN_DEF", DataType.Primitives.NULLABLE_STRING.type(), 12),
                    DataType.StructType.Field.from("SQL_DATA_TYPE", DataType.Primitives.NULLABLE_INTEGER.type(), 13),
                    DataType.StructType.Field.from("SQL_DATETIME_SUB", DataType.Primitives.NULLABLE_INTEGER.type(), 14),
                    DataType.StructType.Field.from("CHAR_OCTET_LENGTH", DataType.Primitives.NULLABLE_INTEGER.type(), 15),
                    DataType.StructType.Field.from("ORDINAL_POSITION", DataType.Primitives.NULLABLE_INTEGER.type(), 16),
                    DataType.StructType.Field.from("IS_NULLABLE", DataType.Primitives.NULLABLE_STRING.type(), 17),
                    DataType.StructType.Field.from("SCOPE_CATALOG", DataType.Primitives.NULLABLE_STRING.type(), 18),
                    DataType.StructType.Field.from("SCOPE_SCHEMA", DataType.Primitives.NULLABLE_STRING.type(), 19),
                    DataType.StructType.Field.from("SCOPE_TABLE", DataType.Primitives.NULLABLE_STRING.type(), 20),
                    DataType.StructType.Field.from("SOURCE_DATA_TYPE", DataType.Primitives.NULLABLE_INTEGER.type(), 21),
                    DataType.StructType.Field.from("IS_AUTOINCREMENT", DataType.Primitives.NULLABLE_STRING.type(), 22),
                    DataType.StructType.Field.from("IS_GENERATEDCOLUMN", DataType.Primitives.NULLABLE_STRING.type(), 23)
            ), true);
            return new IteratorResultSet(RelationalStructMetaData.of(columnsStructType), columnDefs.iterator(), 0);
        });
    }

    // note that approximate is ignored
    //TODO(bfines) I suspect that this interface will be insufficiently rich for our index structures, but hey, it's a start
    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //we actually can't here, it will violate RecordLayer isolation laws
    public RelationalResultSet getIndexInfo(String database, String schema, String tablePattern, boolean unique, boolean approximate) throws SQLException {
        /*
         * The following are feature blocks, where the SQL spec calls for specific behavior that we either don't support
         * yet (or do not plan to ever support), so we through UNSUPPORTED_OPERATION errors for these. As we add in the
         * feature support, we should remove these checks.
         */
        if (database == null || database.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Cannot scan across Databases yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (schema == null || schema.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Cannot scan across Schemas yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (tablePattern == null || tablePattern.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Table must be specified", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        return conn.runIsolatedInTransactionIfPossible(() -> {
            RecordMetaData rmd = RecordMetaData.build(loadSchemaMetadata(database, schema));
            //verify that it is in fact a table
            List<Row> indexDefs;
            try {
                final RecordType recordType = rmd.getRecordType(tablePattern);
                final List<Index> indexes = recordType.getIndexes();
                indexDefs = indexes.stream()
                        .map(index -> {
                            Object[] row = {
                                    database,
                                    schema,
                                    recordType.getName(),
                                    index.isUnique(),
                                    index.getType(),
                                    index.getName(),
                                    DatabaseMetaData.tableIndexOther, //default value--create our own for different index types?
                                    -1,
                                    null,
                                    null, // TODO(bfines) get asc/desc order from options maybe?
                                    -1,
                                    -1,
                                    null //TODO(bfines) filter condition? SQL supports index filters?
                            };
                            return new ArrayRow(row);
                        })
                        .collect(Collectors.toList());
            } catch (MetaDataException mde) {
                throw new RelationalException("table <" + tablePattern + "> does not exist", ErrorCode.UNDEFINED_TABLE);
            }

            final var indexInfoStructType = DataType.StructType.from("PRIMARY_KEYS", List.of(
                    DataType.StructType.Field.from("TABLE_CAT", DataType.Primitives.NULLABLE_STRING.type(), 0),
                    DataType.StructType.Field.from("TABLE_SCHEM", DataType.Primitives.NULLABLE_STRING.type(), 1),
                    DataType.StructType.Field.from("TABLE_NAME", DataType.Primitives.NULLABLE_STRING.type(), 2),
                    DataType.StructType.Field.from("NON_UNIQUE", DataType.Primitives.NULLABLE_BOOLEAN.type(), 3),
                    DataType.StructType.Field.from("INDEX_QUALIFIER", DataType.Primitives.NULLABLE_STRING.type(), 4),
                    DataType.StructType.Field.from("INDEX_NAME", DataType.Primitives.NULLABLE_STRING.type(), 5),
                    DataType.StructType.Field.from("TYPE", DataType.Primitives.NULLABLE_STRING.type(), 6),
                    DataType.StructType.Field.from("ORDINAL_POSITION", DataType.Primitives.NULLABLE_INTEGER.type(), 7),
                    DataType.StructType.Field.from("COLUMN_NAME", DataType.Primitives.NULLABLE_STRING.type(), 8),
                    DataType.StructType.Field.from("ASC_OR_DESC", DataType.Primitives.NULLABLE_STRING.type(), 9),
                    DataType.StructType.Field.from("CARDINALITY", DataType.Primitives.NULLABLE_INTEGER.type(), 10),
                    DataType.StructType.Field.from("PAGES", DataType.Primitives.NULLABLE_INTEGER.type(), 11),
                    DataType.StructType.Field.from("FILTER_CONDITION", DataType.Primitives.NULLABLE_STRING.type(), 12)
            ), true);
            return new IteratorResultSet(RelationalStructMetaData.of(indexInfoStructType), indexDefs.iterator(), 0);
        });
    }

    @Nonnull
    private RecordMetaDataProto.MetaData loadSchemaMetadata(@Nonnull final String database, @Nonnull final String schema) throws RelationalException {
        final var recLayerSchema = this.catalog.loadSchema(conn.getTransaction(), URI.create(database), schema);
        Assert.thatUnchecked(recLayerSchema instanceof RecordLayerSchema);
        return (recLayerSchema.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata().toProto());
    }

    //the position in the array is the key sequence, the value is the name of the column
    private String[] keyExpressionToPrimaryKey(RecordKeyExpressionProto.KeyExpression ke) throws UncheckedRelationalException {
        if (ke.hasThen()) {
            final List<RecordKeyExpressionProto.KeyExpression> childList = ke.getThen().getChildList();
            String[] fields = new String[childList.size()];
            int pos = 0;
            for (RecordKeyExpressionProto.KeyExpression childKe : childList) {
                //skip record type keys
                if (!childKe.hasRecordTypeKey()) {
                    if (childKe.hasField()) {
                        //this is a pk field!
                        fields[pos] = childKe.getField().getFieldName();
                        pos++;
                    }
                }
            }
            if (pos < fields.length) {
                fields = Arrays.copyOf(fields, pos);
            }
            return fields;
        } else {
            //TODO(bfines) we should never throw this, we need to check all the different KeyExpression structures
            //that are valid
            throw new OperationUnsupportedException("Unexpected Primary Key structure").toUncheckedWrappedException();
        }
    }
}
