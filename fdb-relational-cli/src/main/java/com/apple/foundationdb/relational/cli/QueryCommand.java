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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Message;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Command that executes a SQL statement against the database.
 */
public class QueryCommand extends CommandWithConnectionAndSchema {

    private final String statement;
    private PrintWriter out;

    public QueryCommand(DbState dbState, String statement, PrintWriter out) {
        super(dbState);
        this.out = out;
        this.statement = statement;
    }

    @Override
    public void callInternal() throws Exception {
        try (RelationalStatement s = dbState.getConnection().createStatement()) {
            try (RelationalResultSet resultSet = s.executeQuery(statement, Options.create(), QueryProperties.DEFAULT)) {
                List<List<String>> results = new ArrayList<>();
                int colCount = resultSet.getMetaData().getColumnCount();
                String[] colsMetadata = new String[colCount];
                for (int i = 0; i < colCount; i++) {
                    colsMetadata[i] = resultSet.getMetaData().getColumnName(i);
                }
                Function<RelationalResultSet, List<String>> converters = rrs -> {
                    List<String> cols = new ArrayList<>();
                    for (int i = 1; i <= colsMetadata.length; i++) {
                        Object col = null;
                        try {
                            col = rrs.getObject(i);
                            if (col instanceof Message) {
                                try {
                                    cols.add(Utils.protoToJson((Message) col));
                                } catch (RelationalException e) {
                                    cols.add(col.toString()); // todo improve this
                                }
                            } else {
                                cols.add(col.toString());
                            }
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
