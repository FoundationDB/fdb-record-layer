/*
 * CatalogMetaData.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.CatalogMetaDataProvider;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


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
        return getSchemas(conn.frl.getURI().getPath(), null);
    }

    @Override
    public RelationalResultSet getSchemas(String catalogStr, String schemaPattern) throws SQLException {
        if (catalogStr == null) {
            throw new OperationUnsupportedException("Must use a non-null catalog name currently").toSqlException();
        }
        ensureActiveTransaction();
        try (RelationalResultSet rrs = catalog.listSchemas(conn.transaction, URI.create(catalogStr), Continuation.BEGIN)) {
            //TODO(bfines) we need to transform this live, not materialize like this
            List<Row> simplifiedRows = new ArrayList<>();
            while (rrs.next()) {
                Object[] data = new Object[]{
                        conn.frl.getURI(),
                        rrs.getString("SCHEMA_NAME"),
                };
                simplifiedRows.add(new ArrayRow(data));
            }

            FieldDescription[] fields = new FieldDescription[]{
                    FieldDescription.primitive("TABLE_CATALOG", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable)
            };
            return new IteratorResultSet(new RelationalStructMetaData(fields), simplifiedRows.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
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
        ensureActiveTransaction();
        try {
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

            FieldDescription[] fields = new FieldDescription[]{
                    FieldDescription.primitive("TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_VERSION", Types.BIGINT, DatabaseMetaData.columnNullable)
            };
            return new IteratorResultSet(new RelationalStructMetaData(fields), tableList.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public RelationalResultSet getPrimaryKeys(String database, String schema, String table) throws SQLException {
        ensureActiveTransaction();
        try {
            final RecordMetaDataProto.MetaData schemaInfo = loadSchemaMetadata(database, schema);
            Stream<Row> rows = schemaInfo.getRecordTypesList().stream()
                    .filter(type -> type.getName().equals(table))
                    .map(type -> {
                        RecordMetaDataProto.KeyExpression ke = type.getPrimaryKey();
                        return new AbstractMap.SimpleEntry<>(type.getName(), keyExpressionToPrimaryKey(ke));
                    }).flatMap(pks -> IntStream.range(0, pks.getValue().length)
                    .mapToObj(pos -> new ArrayRow(new Object[]{
                            database,
                            schema,
                            pks.getKey(),
                            pks.getValue()[pos],
                            pos + 1,
                            null
                    })));
            FieldDescription[] fields = new FieldDescription[]{
                    FieldDescription.primitive("TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("KEY_SEQ", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("PK_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
            };
            return new IteratorResultSet(new RelationalStructMetaData(fields), rows.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap().toSqlException();
        }
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
        ensureActiveTransaction();
        try {
            //TODO(bfines) this is a weird way of doing this, is there a better way?
            RecordMetaData rmd = new CatalogMetaDataProvider(this.catalog, URI.create(database), schema, conn.transaction).getRecordMetaData();
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
                        Object[] row = new Object[]{
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

            FieldDescription[] columns = new FieldDescription[]{
                    FieldDescription.primitive("TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("DATA_TYPE", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TYPE_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("COLUMN_SIZE", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("BUFFER_LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("DECIMAL_DIGITS", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("NUM_PREC_RADIX", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("NULLABLE", Types.BOOLEAN, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("REMARKS", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("COLUMN_DEF", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("SQL_DATA_TYPE", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("SQL_DATETIME_SUB", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("CHAR_OCTET_LENGTH", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("ORDINAL_POSITION", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("IS_NULLABLE", Types.BOOLEAN, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("SCOPE_CATALOG", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("SCOPE_SCHEMA", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("SCOPE_TABLE", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("SOURCE_DATA_TYPE", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("IS_AUTOINCREMENT", Types.BOOLEAN, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("IS_GENERATEDCOLUMN", Types.BOOLEAN, DatabaseMetaData.columnNullable)
            };
            return new IteratorResultSet(new RelationalStructMetaData(columns), columnDefs.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
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
        ensureActiveTransaction();
        try {
            RecordMetaData rmd = RecordMetaData.build(loadSchemaMetadata(database, schema));
            //verify that it is in fact a table
            List<Row> indexDefs;
            try {
                final RecordType recordType = rmd.getRecordType(tablePattern);
                final List<Index> indexes = recordType.getIndexes();
                indexDefs = indexes.stream()
                        .map(index -> {
                            Object[] row = new Object[]{
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

            FieldDescription[] columns = new FieldDescription[]{
                    FieldDescription.primitive("TABLE_CAT", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_SCHEM", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("NON_UNIQUE", Types.BOOLEAN, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("INDEX_QUALIFIER", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("INDEX_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("TYPE", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("ORDINAL_POSITION", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("ASC_OR_DESC", Types.VARCHAR, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("CARDINALITY", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("PAGES", Types.INTEGER, DatabaseMetaData.columnNullable),
                    FieldDescription.primitive("FILTER_CONDITION", Types.VARCHAR, DatabaseMetaData.columnNullable)
            };
            return new IteratorResultSet(new RelationalStructMetaData(columns), indexDefs.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    private void ensureActiveTransaction() throws SQLException {
        if (!conn.inActiveTransaction()) {
            conn.beginTransaction();
        }
    }

    @Nonnull
    private RecordMetaDataProto.MetaData loadSchemaMetadata(@Nonnull final String database, @Nonnull final String schema) throws RelationalException {
        final var recLayerSchema = this.catalog.loadSchema(conn.transaction, URI.create(database), schema);
        Assert.thatUnchecked(recLayerSchema instanceof RecordLayerSchema);
        return (recLayerSchema.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata().toProto());
    }

    //the position in the array is the key sequence, the value is the name of the column
    private String[] keyExpressionToPrimaryKey(RecordMetaDataProto.KeyExpression ke) throws UncheckedRelationalException {
        if (ke.hasThen()) {
            final List<RecordMetaDataProto.KeyExpression> childList = ke.getThen().getChildList();
            String[] fields = new String[childList.size()];
            int pos = 0;
            for (RecordMetaDataProto.KeyExpression childKe : childList) {
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
