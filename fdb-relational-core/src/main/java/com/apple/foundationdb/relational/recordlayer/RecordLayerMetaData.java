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
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.TableMetaData;
import com.apple.foundationdb.relational.recordlayer.catalog.DirectoryScannable;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class RecordLayerMetaData implements RelationalDatabaseMetaData {
    private final RecordStoreConnection conn;
    private final KeySpace keySpace;

    public RecordLayerMetaData(@Nonnull RecordStoreConnection conn,@Nonnull KeySpace keySpace) {
        this.conn = conn;
        this.keySpace = keySpace;
    }


    @Override
    @Nonnull
    public RelationalResultSet getSchemas() throws RelationalException {
        if(!conn.inActiveTransaction()){
            conn.beginTransaction();
        }
        URI dbPath = conn.frl.getPath();
        final Scannable scannable = new DirectoryScannable(keySpace, dbPath, new String[]{"db_path", "schema_name"}, nestableTuple -> {
            // the data looks like (<dbPath>/schema,0), so split this into (<dbPath>,schema)
            final String fullSchemaPath = nestableTuple.getString(0);
            int lastSlashIdx = fullSchemaPath.lastIndexOf("/");
            //TODO(bfines) prepending the root's name doesn't feel right--should that be toPathString() instead?
            // but when I do toPathString() I end up with // not /...
            String db = keySpace.getRoot().getName()+fullSchemaPath.substring(0,lastSlashIdx);
            String schema = fullSchemaPath.substring(lastSlashIdx+1);
            return TupleUtils.toRelationalTuple(new Tuple().add(db).add(schema));
        });
        return new RecordLayerResultSet(scannable,null,null, conn,QueryProperties.DEFAULT);
    }

    @Override
    @Nonnull
    public RelationalResultSet getTables(@Nonnull String schema) throws RelationalException {
        final Set<String> strings = conn.frl.loadSchema(schema, Options.create()).listTables();
        return new RecordLayerResultSet(new IterableScannable<>(strings,
                s -> new ImmutableKeyValue(new EmptyTuple(), new ValueTuple(s)), new String[]{}, new String[]{"NAME"}),null,null,conn, QueryProperties.DEFAULT);
    }

    @Override
    @Nonnull
    public RelationalResultSet getColumns(@Nonnull String schema,@Nonnull String tableName) throws RelationalException {
        final TableMetaData metaData = describeTable(schema, tableName);
        final Descriptors.Descriptor tableTypeDescriptor = metaData.getTableTypeDescriptor();
        final List<Descriptors.FieldDescriptor> fields = tableTypeDescriptor.getFields();
        List<KeyValue> data = new ArrayList<>(fields.size());
        fields.forEach(fd -> {
            Tuple dataTuple = new Tuple();
            dataTuple = dataTuple.add(fd.getName()).add(fd.getIndex()).add(formatFieldType(fd, fd.getType())).add(formatOptions(fd.getOptions()));
            data.add(new ImmutableKeyValue(EmptyTuple.INSTANCE,TupleUtils.toRelationalTuple(dataTuple)));
        });

        return new RecordLayerResultSet(new IterableScannable<>(data, keyValue -> keyValue, new String[]{},new String[]{"NAME","INDEX","TYPE","OPTIONS"}),null,null,conn,QueryProperties.DEFAULT);
    }

    @Nonnull
    @Override
    public TableMetaData describeTable(@Nonnull String schema,@Nonnull String tableName) throws RelationalException {
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
