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
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.apple.foundationdb.relational.recordlayer.catalog.CatalogMetaDataProvider;

import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.URI;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
                        rrs.getString("schema_name"),
                };
                simplifiedRows.add(new ArrayRow(data));
            }

            return new IteratorResultSet(new String[]{"TABLE_CATALOG", "TABLE_SCHEM"}, simplifiedRows.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Nonnull
    @Override
    public RelationalResultSet getTables(String catalog, String schema, String tableName, String[] types) throws SQLException {
        /*
         * The following are feature blocks, where the SQL spec calls for specific behavior that we either don't support
         * yet (or do not plan to ever support), so we through UNSUPPORTED_OPERATION errors for these. As we add in the
         * feature support, we should remove these checks.
         */
        if (catalog != null) {
            throw new SQLFeatureNotSupportedException("Specified catalogs not supported", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
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
            final CatalogData.Schema schemaInfo = this.catalog.loadSchema(conn.transaction, conn.frl.getURI(), schema);
            List<Row> tableList = schemaInfo.getTablesList().stream()
                    .map(tbl -> new String[]{null, schema, tbl.getName()})
                    .map(ArrayRow::new)
                    .collect(Collectors.toList());
            return new IteratorResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"}, tableList.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public RelationalResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        ensureActiveTransaction();
        try {
            final CatalogData.Schema schemaInfo = this.catalog.loadSchema(conn.transaction, URI.create(catalog), schema);
            List<Row> rows = new ArrayList<>();
            for (CatalogData.Table tbl : schemaInfo.getTablesList()) {
                if (tbl.getName().equalsIgnoreCase(table)) {
                    RecordMetaDataProto.KeyExpression pk = RecordMetaDataProto.KeyExpression.parseFrom(tbl.getPrimaryKey());
                    String[] pkInfo = keyExpressionToPrimaryKey(pk);
                    for (int i = 0; i < pkInfo.length; i++) {
                        rows.add(new ArrayRow(new Object[]{
                                catalog,
                                schema,
                                tbl.getName(),
                                pkInfo[i],
                                i + 1,
                                null
                        }));
                    }
                }
            }
            return new IteratorResultSet(new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"}, rows.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        } catch (InvalidProtocolBufferException e) {
            throw new SQLException(e);
        }
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //we actually can't here, it will violate RecordLayer isolation laws
    public RelationalResultSet getColumns(String catalog, String schema, String tablePattern, String columnPattern) throws SQLException {
        /*
         * The following are feature blocks, where the SQL spec calls for specific behavior that we either don't support
         * yet (or do not plan to ever support), so we through UNSUPPORTED_OPERATION errors for these. As we add in the
         * feature support, we should remove these checks.
         */
        if (catalog != null) {
            throw new SQLFeatureNotSupportedException("Specified catalogs not supported", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
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
            RecordMetaData rmd = new CatalogMetaDataProvider(this.catalog, conn.frl.getURI(), schema, conn.transaction).getRecordMetaData();
            Descriptors.FileDescriptor fileDesc = rmd.getRecordsDescriptor();
            //verify that it is in fact a table
            try {
                rmd.getRecordType(tablePattern);
            } catch (MetaDataException mde) {
                throw new RelationalException("table <" + tablePattern + "> does not exist", ErrorCode.TABLE_NOT_FOUND);
            }
            //now get its column data
            final Descriptors.Descriptor tableDescriptor = fileDesc.findMessageTypeByName(tablePattern);
            final List<Row> columnDefs = tableDescriptor.getFields().stream()
                    .map(field -> {
                        Object[] row = new Object[]{
                                conn.frl.getURI(),
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

            String[] columnHeaders = new String[]{
                    "TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "TYPE_NAME",
                    "COLUMN_SIZE",
                    "BUFFER_LENGTH",
                    "DECIMAL_DIGITS",
                    "NUM_PREC_RADIX",
                    "NULLABLE",
                    "REMARKS",
                    "COLUMN_DEF",
                    "SQL_DATA_TYPE",
                    "SQL_DATETIME_SUB",
                    "CHAR_OCTET_LENGTH",
                    "ORDINAL_POSITION",
                    "IS_NULLABLE",
                    "SCOPE_CATALOG",
                    "SCOPE_SCHEMA",
                    "SCOPE_TABLE",
                    "SOURCE_DATA_TYPE",
                    "IS_AUTOINCREMENT",
                    "IS_GENERATEDCOLUMN"
            };
            return new IteratorResultSet(columnHeaders, columnDefs.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    // note that approximate is ignored
    //TODO(bfines) I suspect that this interface will be insufficiently rich for our index structures, but hey, it's a start
    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") //we actually can't here, it will violate RecordLayer isolation laws
    public RelationalResultSet getIndexInfo(String catalog, String schema, String tablePattern, boolean unique, boolean approximate) throws SQLException {
        /*
         * The following are feature blocks, where the SQL spec calls for specific behavior that we either don't support
         * yet (or do not plan to ever support), so we through UNSUPPORTED_OPERATION errors for these. As we add in the
         * feature support, we should remove these checks.
         */
        if (catalog != null) {
            throw new SQLFeatureNotSupportedException("Specified catalogs not supported", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (schema == null || schema.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Cannot scan across Schemas yet", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        if (tablePattern == null || tablePattern.isEmpty()) {
            throw new SQLFeatureNotSupportedException("Table must be specified", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
        ensureActiveTransaction();
        try {
            //TODO(bfines) this is a weird way of doing this, is there a better way?
            RecordMetaData rmd = new CatalogMetaDataProvider(this.catalog, conn.frl.getURI(), schema, conn.transaction).getRecordMetaData();
            //verify that it is in fact a table
            List<Row> indexDefs;
            try {
                final RecordType recordType = rmd.getRecordType(tablePattern);
                final List<Index> indexes = recordType.getIndexes();
                indexDefs = indexes.stream()
                        .map(index -> {
                            Object[] row = new Object[]{
                                    conn.frl.getURI(),
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
                throw new RelationalException("table <" + tablePattern + "> does not exist", ErrorCode.TABLE_NOT_FOUND);
            }

            String[] columnHeaders = new String[]{
                    "TABLE_CAT",
                    "TABLE_SCHEM",
                    "TABLE_NAME",
                    "NON_UNIQUE",
                    "INDEX_QUALIFIER",
                    "INDEX_NAME",
                    "TYPE",
                    "ORDINAL_POSITION",
                    "COLUMN_NAME",
                    "ASC_OR_DESC",
                    "CARDINALITY",
                    "PAGES",
                    "FILTER_CONDITION"
            };
            return new IteratorResultSet(columnHeaders, indexDefs.iterator(), 0);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    private void ensureActiveTransaction() throws SQLException {
        if (!conn.inActiveTransaction()) {
            try {
                conn.beginTransaction();
            } catch (RelationalException e) {
                throw e.toSqlException();
            }
        }
    }

    //the position in the array is the key sequence, the value is the name of the column
    private String[] keyExpressionToPrimaryKey(RecordMetaDataProto.KeyExpression ke) throws RelationalException {
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
            throw new OperationUnsupportedException("Unexpected Primary Key structure");
        }
    }
}
