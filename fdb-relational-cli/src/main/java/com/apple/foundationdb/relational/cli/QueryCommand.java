/*
 * QueryCommand.java
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

package com.apple.foundationdb.relational.cli;

import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;

import java.io.PrintWriter;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nonnull;

/**
 * Command that executes a SQL statement against the database.
 */
public class QueryCommand extends CommandWithConnectionAndSchema {

    private final String statement;
    private final PrintWriter out;

    public QueryCommand(@Nonnull final DbState dbState, @Nonnull final String statement, @Nonnull final PrintWriter out) {
        super(dbState);
        this.out = out;
        this.statement = statement;
    }

    @Override
    public void callInternal() throws Exception {
        try (RelationalStatement s = dbState.getConnection().createStatement()) {
            boolean hasResults = s.execute(statement);
            if (hasResults) {
                try (ResultSet resultSet = s.getResultSet()) {
                    List<List<String>> results = new ArrayList<>();
                    int colCount = resultSet.getMetaData().getColumnCount();
                    String[] colsMetadata = new String[colCount];
                    final ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 1; i <= colCount; i++) {
                        colsMetadata[i - 1] = metaData.getColumnName(i);
                    }
                    Function<ResultSet, List<String>> converters = rrs -> {
                        List<String> cols = new ArrayList<>();
                        for (int i = 1; i <= colsMetadata.length; i++) {
                            Object col;
                            try {
                                col = rrs.getObject(i);
                                cols.add(stringify(col));
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return cols;
                    };
                    while (resultSet.next()) {
                        results.add(converters.apply(resultSet));
                    }
                    Utils.tabulate(out, dbState.isPrettyPrint(), dbState.getDelimiter(), dbState.isDisplayHeaders(), results, colsMetadata);
                }
            }
        }
    }

    private String stringify(Object value) throws SQLException {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof RelationalStruct) {
            RelationalStruct vs = (RelationalStruct) value;
            StructMetaData smd = vs.getMetadata();
            StringBuilder sb = new StringBuilder("{");
            for (int i = 1; i <= smd.getColumnCount(); i++) {
                if (i != 1) {
                    sb.append(",");
                }
                sb.append("\"").append(smd.getColumnName(i)).append("\"").append(":").append(stringify(vs.getObject(i)));
            }
            return sb.append("}").toString();
        } else if (value instanceof Array) {
            StringBuilder sb = new StringBuilder("[");
            RelationalResultSet rs = (RelationalResultSet) ((Array) value).getResultSet();
            StructMetaData smd = rs.getMetaData().unwrap(StructMetaData.class);
            boolean isFirst = true;
            while (rs.next()) {
                if (!isFirst) {
                    sb.append(",");
                } else {
                    isFirst = false;
                }
                int colCount = smd.getColumnCount();
                if (colCount != 1) {
                    sb.append("{");
                    for (int i = 1; i <= smd.getColumnCount(); i++) {
                        if (i != 1) {
                            sb.append(",");
                        }
                        sb.append("\"").append(smd.getColumnName(i)).append("\"").append(":").append(stringify(rs.getObject(i)));
                    }
                    sb.append("}");
                } else {
                    sb.append(stringify(rs.getObject(1)));
                }
            }
            sb.append("]");
            return sb.toString();
        } else if (value instanceof Number) {
            if (value instanceof Long || value instanceof Integer) {
                return Long.toString(((Number) value).longValue());
            } else {
                return Double.toString(((Number) value).doubleValue());
            }
        } else {
            return "\"" + value + "\"";
        }
    }
}
