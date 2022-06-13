/*
 * JDBCStatement.java
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

package com.apple.foundationdb.relational.compare;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

public class JDBCStatement implements RelationalStatement {
    private final RelationalCatalog catalog;
    private final java.sql.Statement statement;

    public JDBCStatement(RelationalCatalog structure, Statement statement) {
        this.catalog = structure;
        this.statement = statement;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return statement.execute(sql);
    }

    @Override
    public RelationalResultSet executeQuery(@Nonnull String query) throws SQLException {
        final ResultSet resultSet = statement.executeQuery(query);
        return new JDBCRelationalResultSet(resultSet);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return statement.executeUpdate(sql);
    }

    @Nonnull
    @Override
    public RelationalResultSet executeScan(@Nonnull TableScan scan, @Nonnull Options options) throws RelationalException {
        //TODO(bfines) this keyset arrangement isn't going to work for scans over unequal ranges
        String whereClauseBuilder = getWhereClause(scan.getTableName(), new KeySet().setKeyColumns(scan.getStartKey()));

        String tableName = scan.getTableName();
        if (tableName.contains(".")) {
            //strip the schema name off of it
            tableName = tableName.substring(tableName.indexOf(".") + 1);
        }
        RelationalStructure structure = catalog.getStructure(tableName);

        Table t = structure.getTable(tableName);
        Message.Builder topLevelProtobuf = t.getProtobuf();
        String sql;
        if (topLevelProtobuf == null) {
            //someone issued a GET against one of the link tables, so just return it directly

            sql = "SELECT * from " + scan.getTableName() + whereClauseBuilder;
        } else {
            /*
             * We want to pull the columns and add a specific field reference for each column, to make sure
             * that we get it back correctly for nested and repeated types
             */
            sql = createQuery(topLevelProtobuf.getDescriptorForType(), whereClauseBuilder);
        }
        try {
            return executeQuery(sql);
        } catch (SQLException e) {
            throw new RelationalException(e.getMessage(), ErrorCode.get(e.getSQLState()));
        }
    }

    @Nonnull
    @Override
    public RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws RelationalException {
        String whereClauseBuilder = getWhereClause(tableName, key);

        RelationalStructure structure = catalog.getStructure(tableName);

        Table t = structure.getTable(tableName);
        Message.Builder topLevelProtobuf = t.getProtobuf();
        String sql;
        if (topLevelProtobuf == null) {
            //someone issued a GET against one of the link tables, so just return it directly

            sql = "SELECT * from " + tableName + whereClauseBuilder;
        } else {
            /*
             * We want to pull the columns and add a specific field reference for each column, to make sure
             * that we get it back correctly for nested and repeated types
             */
            sql = createQuery(topLevelProtobuf.getDescriptorForType(), whereClauseBuilder);
        }

        System.out.println(sql);
        try {
            RelationalResultSet rrs = executeQuery(sql);
            if (topLevelProtobuf == null) {
                return rrs;
            } else {
                //need to map
                Collection<Message> results = new TabularToProtobufParser(topLevelProtobuf.getDescriptorForType()).parse(rrs);
                String[] fields = topLevelProtobuf.getDescriptorForType().getFields().stream()
                        .map(Descriptors.FieldDescriptor::getName)
                        .collect(Collectors.toList())
                        .toArray(new String[]{});
                if (results.isEmpty()) {
                    return new IteratorResultSet(fields, Collections.emptyIterator(), 0);
                } else {
                    Iterator<com.apple.foundationdb.relational.api.Row> messageIter = results.stream().map(message -> (com.apple.foundationdb.relational.api.Row) new MessageTuple(message)).iterator();
                    return new IteratorResultSet(fields, messageIter, 0);
                }
            }
        } catch (SQLException se) {
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, se);
        }
    }

    @Nonnull
    private String getWhereClause(@Nonnull String tableName, @Nonnull KeySet key) {
        StringBuilder whereClauseBuilder = new StringBuilder("");
        Map<String, Object> paramsMap = key.toMap();
        boolean isFirst = true;
        for (Map.Entry<String, Object> param : paramsMap.entrySet()) {
            if (!isFirst) {
                whereClauseBuilder.append(" AND ");
            } else {
                isFirst = false;
            }
            whereClauseBuilder.append(tableName).append(".").append(param.getKey()).append(" = ");
            final Object value = param.getValue();
            if (value instanceof String) {
                String str = ((String) value).replaceAll("'", "''");
                whereClauseBuilder.append("'").append(str).append("'");
            } else {
                whereClauseBuilder.append(value);
            }
        }
        return whereClauseBuilder.toString();
    }

    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull String typeName) throws RelationalException {
        return catalog.getDataBuilder(typeName);
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data) throws RelationalException {
        RelationalStructure structure = catalog.getStructure(tableName);
        int count = 0;
        Message datum = null;
        try {
            while (data.hasNext()) {
                datum = data.next();
                Map<String, ValuesClause> values = structure.flattenToValues(datum);
                for (Map.Entry<String, ValuesClause> insertClause : values.entrySet()) {
                    final ValuesClause value = insertClause.getValue();
                    if (value.hasValues()) {
                        String insertStatement = String.format("INSERT INTO %s (%s) VALUES %s", insertClause.getKey(), value.columnList(), value.valuesString());
                        statement.execute(insertStatement);
                    }
                }
                count++;
            }
        } catch (SQLException e) {
            System.out.println(datum);
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, e);
        }

        return count;
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys) throws RelationalException {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    private void buildQuery(Descriptors.Descriptor descriptor,
                            Set<String> columns,
                            String tableName,
                            StringBuilder fromClause) {

        //add column to the returned query so that we can put repeated types back together
        columns.add(descriptor.getName() + ".RECORD_ID as " + descriptor.getName() + "_RECORD_ID");
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            if (field.isRepeated()) {
                if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    String parentTableName = tableName;
                    String linkTableName = parentTableName + "_" + field.getName();
                    String nestedTableName = field.getMessageType().getName();
                    //join from the parent to the link table
                    fromClause.append(String.format(" left outer join %2$s on %1$s.%3$s_RECORD_ID = %2$s.PARENT_RECORD_ID", parentTableName, linkTableName, field.getName()));
                    //join from the link table to the data table
                    fromClause.append(String.format(" left outer join %1$s on %1$s.RECORD_ID = %2$s.RECORD_ID", nestedTableName, linkTableName));
                    buildQuery(field.getMessageType(), columns, nestedTableName, fromClause);
                }
            } else {
                if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    buildQuery(field.getMessageType(), columns, field.getName(), fromClause);
                    String joinClause = String.format(" left outer join %1$s on %1$s.RECORD_ID = %2$s.%3$s_id ", field.getMessageType().getName(), tableName, field.getName());
                    fromClause.append(joinClause);
                } else {
                    String columnName = descriptor.getName() + "." + field.getName();
                    columns.add(columnName);
                }
            }
        }
    }

    /**
     * Creates a query on the entire structure based on the where clause.
     */
    private String createQuery(Descriptors.Descriptor topLevelDescriptor, String whereClause) {
        Set<String> columns = new TreeSet<>();
        StringBuilder fromClause = new StringBuilder(topLevelDescriptor.getName());
        buildQuery(topLevelDescriptor, columns, topLevelDescriptor.getName(), fromClause);

        return String.format("SELECT %s from %s where %s", String.join(",", columns), fromClause, whereClause);
    }
}
