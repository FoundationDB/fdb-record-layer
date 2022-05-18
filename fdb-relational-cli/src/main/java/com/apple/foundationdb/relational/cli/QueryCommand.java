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

import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Message;

import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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
                    for (int i = 0; i < colCount; i++) {
                        colsMetadata[i] = metaData.getColumnName(i);
                    }
                    Function<Object, String> columnStringifier = getStringifier();
                    Function<ResultSet, List<String>> converters = rrs -> {
                        List<String> cols = new ArrayList<>();
                        for (int i = 1; i <= colsMetadata.length; i++) {
                            Object col;
                            try {
                                col = rrs.getObject(i);
                                cols.add(columnStringifier.apply(col));
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

    private Function<Object, String> getStringifier() {
        return new Function<>() {
            @Override
            public String apply(Object o) {
                if (o == null) {
                    return "NULL";
                }
                if (o instanceof Message) {
                    try {
                        return Utils.protoToJson((Message) o);
                    } catch (RelationalException e) {
                        throw e.toUncheckedWrappedException();
                    }
                } else if (o instanceof Number) {
                    if (o instanceof Long || o instanceof Integer) {
                        return Long.toString(((Number) o).longValue());
                    } else {
                        return Double.toString(((Number) o).doubleValue());
                    }
                } else if (o instanceof Iterable) {
                    //recursively map the underlying elements
                    return "[" + StreamSupport.stream(((Iterable<?>) o).spliterator(), false).map(this).collect(Collectors.joining(",")) + "]";
                } else if (o instanceof String) {
                    return "\"" + o + "\"";
                } else {
                    return o.toString();
                }
            }
        };
    }

}
