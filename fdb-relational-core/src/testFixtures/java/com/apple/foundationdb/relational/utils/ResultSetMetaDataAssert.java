/*
 * ResultSetMetaDataAssert.java
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.StructResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;

import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class ResultSetMetaDataAssert extends AbstractAssert<ResultSetMetaDataAssert, RelationalResultSetMetaData> {

    public static ResultSetMetaDataAssert assertThat(RelationalResultSetMetaData actual) {
        return new ResultSetMetaDataAssert(actual);
    }

    protected ResultSetMetaDataAssert(RelationalResultSetMetaData relationalResultSetMetaData) {
        super(relationalResultSetMetaData, ResultSetMetaDataAssert.class);
    }

    @Override
    public ResultSetMetaDataAssert isEqualTo(Object expected) {
        if (expected instanceof RelationalResultSetMetaData) {
            return isEqualTo((RelationalResultSetMetaData) expected);
        } else {
            return super.isEqualTo(expected);
        }
    }

    public ResultSetMetaDataAssert isEqualTo(RelationalResultSetMetaData expectedMetaData) {
        try {
            hasColumnCount(expectedMetaData.getColumnCount());
            SoftAssertions sa = new SoftAssertions();
            for (int i = 1; i <= expectedMetaData.getColumnCount(); i++) {
                String aName = actual.getColumnName(i);
                String eName = expectedMetaData.getColumnName(i);
                sa.assertThat(aName).describedAs("position %d Name", i).isEqualTo(eName);

                int aType = actual.getColumnType(i);
                int eType = expectedMetaData.getColumnType(i);
                sa.assertThat(aType).describedAs("position %d Type", i).isEqualTo(eType);

                if (aType == Types.STRUCT) {
                    StructMetaData aStructMd = actual.getStructMetaData(i);
                    StructMetaData eStructMd = expectedMetaData.getStructMetaData(i);

                    sa.proxy(StructMetaDataAssert.class, StructMetaData.class, aStructMd).isEqualTo(eStructMd);
                }
            }
            sa.assertAll();
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    /**
     * Asserts that the specified columns are contained in the result set, in any order.
     *
     * Note that the returned ResultSet can have <em>more</em> columns than just these. If you want
     * to ensure that <em>exactly</em> these columns are contained use {@link #hasColumnsExactly(String...)}.
     *
     * Note also that this method doesn't do any protection against asking the same column twice--if the same
     * column is in the expected names list, then it will just the same column name twice.
     *
     * @param columnNames the columnNames to check for.
     * @return an assertion useful for chaining assertions
     */
    public ResultSetMetaDataAssert hasColumns(String... columnNames) {
        Set<String> columns = new HashSet<>(Arrays.asList(columnNames));
        return hasColumns(columns);
    }

    public ResultSetMetaDataAssert hasColumns(Set<String> columnNames) {
        try {
            Set<String> missingColumns = new HashSet<>();
            List<String> actualColumns = new ArrayList<>();
            for (String columnName : columnNames) {
                boolean found = false;
                for (int i = 1; i <= actual.getColumnCount(); i++) {
                    String col = actual.getColumnName(i);
                    actualColumns.add(col);
                    if (columnName.equals(col)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    missingColumns.add(columnName);
                }
            }

            if (!missingColumns.isEmpty()) {
                failWithMessage("Missing Columns from ResultSet actual!%n " +
                        "Expected Columns:%n    %s%n " +
                        "MetaData Columns:%n    %s%n" +
                        "Missing Columns:%n    %s",
                        columnNames, actualColumns, missingColumns);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public ResultSetMetaDataAssert hasColumnsExactly(String... columnNames) {
        Set<String> columns = new HashSet<>(Arrays.asList(columnNames));
        return hasColumnsExactly(columns);
    }

    public ResultSetMetaDataAssert hasColumnsExactly(Set<String> columnNames) {
        hasColumns(columnNames); //make sure the resultset isn't missing columns

        //make sure the resultset doesn't have any extras

        try {
            Set<String> extraColumns = new HashSet<>();
            List<String> actualColumns = new ArrayList<>();
            for (int i = 1; i <= actual.getColumnCount(); i++) {
                String col = actual.getColumnName(i);
                actualColumns.add(col);
                if (!columnNames.contains(col)) {
                    extraColumns.add(col);
                }
            }

            if (!extraColumns.isEmpty()) {
                failWithMessage("Unexpected Columns from ResultSet actual!%n " +
                        "Expected Columns:%n    %s%n " +
                        "MetaData Columns:%n    %s%n" +
                        "Extra Columns:%n    %s",
                        columnNames, actualColumns, extraColumns);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return this;
    }

    public ResultSetMetaDataAssert hasColumnsExactlyInOrder(String... columnNames) {
        return hasColumnsExactlyInOrder(Arrays.asList(columnNames));
    }

    /**
     * Verifies that the MetaData columns are <em> exactly</em> the same as the list, in <em>exactly</em>
     * the same order.
     *
     * @param columnNames the names to check
     * @return this if the assertion passes
     */
    public ResultSetMetaDataAssert hasColumnsExactlyInOrder(List<String> columnNames) {
        try {
            Map<Integer, Map.Entry<String, String>> badColumns = new HashMap<>();
            List<String> actualColumns = new ArrayList<>();
            for (int i = 1; i <= actual.getColumnCount(); i++) {
                String metaColumn = actual.getColumnName(i);
                actualColumns.add(metaColumn);
                if (i > columnNames.size()) {
                    badColumns.put(i, new AbstractMap.SimpleEntry<>(null, metaColumn));
                } else {
                    String expectedColumn = columnNames.get(i - 1);
                    if (!metaColumn.equalsIgnoreCase(expectedColumn)) {
                        badColumns.put(i, new AbstractMap.SimpleEntry<>(expectedColumn, metaColumn));
                    }
                }
            }
            if (columnNames.size() > actual.getColumnCount()) {
                for (int p = actual.getColumnCount() + 1; p <= columnNames.size(); p++) {
                    badColumns.put(p, new AbstractMap.SimpleEntry<>(columnNames.get(p - 1), null));
                }
            }

            if (!badColumns.isEmpty()) {
                String badColStr = "[" + badColumns.entrySet().stream().map(entry -> {
                    Map.Entry<String, String> val = entry.getValue();
                    if (val.getKey() == null) {
                        return String.format(Locale.ROOT, "%d->{Extra Column: %s", entry.getKey(), val.getValue());
                    } else if (val.getValue() == null) {
                        return String.format(Locale.ROOT, "%d->{Missing Column: %s", entry.getKey(), val.getKey());
                    } else {
                        return String.format(Locale.ROOT, "%d->{Expected: %s, Actual: %s}", entry.getKey(), val.getKey(), val.getValue());
                    }
                }).collect(Collectors.joining(",")) + "]";

                String msg = String.format(Locale.ROOT, "Incorrect MetaData columns!%n" +
                        "Expected Columns: %n    %s%n" +
                        "MetaData Columns: %n    %s%n" +
                        "Invalid Columns: %n    %s",
                        columnNames, actualColumns, badColStr
                );
                failWithMessage(msg);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public ResultSetMetaDataAssert hasColumnCount(int expectedNumColumns) {
        try {
            Assertions.assertThat(actual.getColumnCount())
                    .describedAs("Incorrect column count!")
                    .isEqualTo(expectedNumColumns);
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public ResultSetMetaDataAssert hasColumnTypes(Map<String, Integer> columnTypes) {
        try {
            Map<String, Integer> metaDataColumns = new HashMap<>();
            for (int i = 1; i <= actual.getColumnCount(); i++) {
                metaDataColumns.put(actual.getColumnName(i), actual.getColumnType(i));
            }

            Map<String, String> errorColumns = new HashMap<>();
            metaDataColumns.forEach((col, type) -> {
                Integer eType = columnTypes.get(col);
                if (eType == null) {
                    errorColumns.put(col, "Unexpected Column");
                } else if (eType.intValue() != type) {
                    errorColumns.put(col, "Incoorect type. Expected: " + eType + ", Actual: " + type);
                }
            });

            if (!errorColumns.isEmpty()) {
                String msg = "Incorrect Column Types! %n" +
                        "Expected Column Types: %n    %s%n" +
                        "Actual Column Types: %n    %s%n" +
                        "Mismatching Columns: %n    %s%n";

                failWithMessage(msg, columnTypes, metaDataColumns, errorColumns);
            }
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public ArrayMetaDataAssert hasArrayMetaData(String column) {
        try {
            for (int i = 1; i <= actual.getColumnCount(); i++) {
                if (actual.getColumnName(i).equalsIgnoreCase(column)) {
                    if (actual.getColumnType(i) != Types.ARRAY) {
                        failWithMessage("Column %s is not an array type!", column);
                    }
                    return new ArrayMetaDataAssert(actual.getArrayMetaData(i));
                }
            }
            throw failure("Missing column %s", column);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSetMetaDataAssert hasStructMetaData(String column) {
        try {
            for (int i = 1; i <= actual.getColumnCount(); i++) {
                if (actual.getColumnName(i).equalsIgnoreCase(column)) {
                    if (actual.getColumnType(i) != Types.STRUCT) {
                        failWithMessage("Column %s is not a struct type!", column);
                    }
                    return new ResultSetMetaDataAssert(new StructResultSetMetaData(actual.getStructMetaData(i)));
                }
            }
            throw failure("Missing column %s", column);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
