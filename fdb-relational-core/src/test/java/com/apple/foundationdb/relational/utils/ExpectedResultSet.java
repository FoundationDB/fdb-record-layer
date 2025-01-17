/*
 * ExpectedResultSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to make building expected result sets easier in tests.
 */
public class ExpectedResultSet {
    private int[] columnTypes;
    private String[] columnNames;

    private List<Object[]> rows = new ArrayList<>();

    public ExpectedResultSet addRow(Object[] elements) {
        if (elements.length != columnTypes.length) {
            throw new RuntimeException("Must specify rows with the same number of columns that are in the result set!");
        }
        this.rows.add(elements);

        return this;
    }

    public ExpectedResultSet addColumn(int position, String columnName, int columnSqlType) {
        if (position > columnTypes.length) {
            columnTypes = Arrays.copyOf(columnTypes, position + 1);
            columnNames = Arrays.copyOf(columnNames, position + 1);
        }
        columnTypes[position] = columnSqlType;
        columnNames[position] = columnName;

        return this;
    }

    public int getColumnCount() {
        return columnTypes.length;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public Map<String, Integer> getColumnTypes() {
        Map<String, Integer> colTypes = new HashMap<>();
        for (int i = 0; i < columnTypes.length; i++) {
            colTypes.put(columnNames[i], columnTypes[i]);
        }
        return colTypes;
    }
}
