/*
 * RecordLayerMetaData.java
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

import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.ImmutableKeyValue;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.DirectoryScannable;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.net.URI;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

public class RecordLayerMetaData implements RelationalDatabaseMetaData {
    private final RecordStoreConnection conn;
    private final KeySpace keySpace;

    public RecordLayerMetaData(@Nonnull RecordStoreConnection conn, @Nonnull KeySpace keySpace) {
        this.conn = conn;
        this.keySpace = keySpace;
    }

    @Override
    @Nonnull
    public RelationalResultSet getSchemas() throws SQLException {
        try {
            if (!conn.inActiveTransaction()) {
                conn.beginTransaction();
            }
            URI dbPath = conn.frl.getPath();
            final Scannable scannable = new DirectoryScannable(keySpace, dbPath, new String[]{"TABLE_SCHEM", "TABLE_CATALOG"}, nestableTuple -> {
                // the data looks like (<dbPath>/schema,0), so split this into (schema, <dbPath>)
                final String fullSchemaPath;
                try {
                    fullSchemaPath = nestableTuple.getString(0);
                } catch (InvalidTypeException | InvalidColumnReferenceException e) {
                    throw e.toUncheckedWrappedException();
                }
                int lastSlashIdx = fullSchemaPath.lastIndexOf("/");
                //TODO(bfines) prepending the root's name doesn't feel right--should that be toPathString() instead?
                // but when I do toPathString() I end up with // not /...
                String db = keySpace.getRoot().getName() + fullSchemaPath.substring(0, lastSlashIdx);
                String schema = fullSchemaPath.substring(lastSlashIdx + 1);
                return TupleUtils.toRelationalTuple(new Tuple().add(schema).add(db));
            });
            //TODO: This should return the elements sorted by TABLE_CATALOG,TABLE_SCHEM as per JDBC recommendations
            return new RecordLayerResultSet(scannable, null, null, conn, QueryProperties.DEFAULT, null);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    @Nonnull
    public RelationalResultSet getTables(
            String catalog,
            String schema,
            String tableName,
            String[] types) throws SQLException {
        try {
            if (catalog != null) {
                throw new OperationUnsupportedException("getTables: Non null catalog parameter not supported");
            }
            if (schema == null || schema.isEmpty()) {
                throw new OperationUnsupportedException("getTables: null or empty string schemaPattern parameter not supported");
            }
            if (tableName != null) {
                throw new OperationUnsupportedException("getTables: non null tableNamePattern parameter not supported");
            }
            if (types != null) {
                throw new OperationUnsupportedException("getTables: non null types parameter not supported");
            }
            final List<String> strings = conn.frl.loadSchema(schema, Options.create()).listTables().stream().sorted().collect(Collectors.toUnmodifiableList());
            final Scannable scannable = new IterableScannable<>(
                    strings,
                    s -> new ImmutableKeyValue(new EmptyTuple(), TupleUtils.toRelationalTuple(
                            new Tuple()
                                    .add(conn.frl.getPath().getPath()) // TABLE_CAT
                                    .add(schema) // TABLE_SCHEM
                                    .add(s) // TABLE_NAME
                    )),
                    new String[]{},
                    new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"});
            return new RecordLayerResultSet(scannable, null, null, conn, QueryProperties.DEFAULT, null);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    @Nonnull
    public RelationalResultSet getColumns(
            String catalog,
            String schema,
            String tableName,
            String column) throws SQLException {
        try {
            if (catalog != null) {
                throw new OperationUnsupportedException("getColumns: Non null catalog parameter not supported");
            }
            if (schema == null || schema.isEmpty()) {
                throw new OperationUnsupportedException("getColumns: null or empty string schema parameter not supported");
            }
            if (tableName == null || tableName.isEmpty()) {
                throw new OperationUnsupportedException("getColumns: null or empty string tableName parameter not supported");
            }
            if (column != null) {
                throw new OperationUnsupportedException("getColumns: Non null column parameter not supported");
            }
            final TableMetaData metaData = describeTable(schema, tableName);
            final Descriptors.Descriptor tableTypeDescriptor = metaData.getTableTypeDescriptor();
            List<KeyValue> data = tableTypeDescriptor.getFields()
                    .stream()
                    .map(fd -> new Tuple()
                            .add(conn.frl.getPath().getPath()) // TABLE_CAT
                            .add(schema) // TABLE_SCHEM
                            .add(tableName) // TABLE_NAME
                            .add(fd.getName()) // COLUMN_NAME
                            .add(formatFieldType(fd, fd.getType())) // TYPE_NAME
                            .add(fd.getIndex()) // ORDINAL_POSITION
                            .add(formatOptions(fd.getOptions()))) // BL_OPTIONS (Relational Layer specific)
                    .sorted(Comparator.comparing((Tuple t) -> t.getString(0))
                            .thenComparing((Tuple t) -> t.getString(1))
                            .thenComparing((Tuple t) -> t.getString(2))
                            .thenComparing((Tuple t) -> t.getLong(5)))
                    .map(tuple -> new ImmutableKeyValue(EmptyTuple.INSTANCE, TupleUtils.toRelationalTuple(tuple)))
                    .collect(Collectors.toUnmodifiableList());

            final String[] fieldNames = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME", "ORDINAL_POSITION", "BL_OPTIONS"};

            return new RecordLayerResultSet(new IterableScannable<>(data, keyValue -> keyValue, new String[]{}, fieldNames), null, null, conn, QueryProperties.DEFAULT, null);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Nonnull
    @Override
    public TableMetaData describeTable(@Nonnull String schema, @Nonnull String tableName) throws RelationalException {
        final RecordLayerSchema recordLayerSchema = conn.frl.loadSchema(schema, Options.create());
        final Table table = recordLayerSchema.loadTable(tableName, Options.create());
        return table.getMetaData();
    }

    @Nonnull
    @Override
    public URI getDatabasePath() {
        return conn.frl.getPath();
    }

    private String formatFieldType(Descriptors.FieldDescriptor field, Descriptors.FieldDescriptor.Type t) {
        String typeStr = "";
        if (t != Descriptors.FieldDescriptor.Type.MESSAGE) {
            typeStr = t.name();
        } else {
            final Descriptors.Descriptor messageType = field.getMessageType();
            typeStr = "Message(" + messageType.getFullName().toUpperCase(Locale.ROOT) + ")";
        }
        return typeStr;
    }

    private String formatOptions(DescriptorProtos.FieldOptions options) {
        String optionsStr = "{";
        final Map<Descriptors.FieldDescriptor, Object> allFields = options.getAllFields();
        boolean isFirst = true;
        for (Map.Entry<Descriptors.FieldDescriptor, Object> fieldPair : allFields.entrySet()) {
            if (isFirst) {
                isFirst = false;
            } else {
                optionsStr += ",";
            }
            final Object value = fieldPair.getValue();
            String valueStr = null;
            String keyStr = null;
            if (value instanceof RecordMetaDataOptionsProto.FieldOptions) {
                RecordMetaDataOptionsProto.FieldOptions vfo = (RecordMetaDataOptionsProto.FieldOptions) value;
                if (vfo.hasPrimaryKey()) {
                    keyStr = "primary_key";
                    valueStr = Boolean.toString(vfo.getPrimaryKey());
                } else if (vfo.hasIndex()) {
                    keyStr = "index";
                    valueStr = vfo.getIndex().getType();
                }
            } else {
                keyStr = fieldPair.getKey().getName();
                valueStr = value == null ? "null" : value.toString();
            }
            optionsStr += String.format("%s:%s", keyStr, valueStr);
        }
        optionsStr += "}";
        return optionsStr;
    }
}
