/*
 * TableDescription.java
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

package com.apple.foundationdb.relational.autotest;

import java.util.List;
import java.util.Map;

public class TableDescription {
    private final String tableName;
    private final Map<String, String> columnNames;
    private final List<String> pkColumns;

    public TableDescription(String tableName, Map<String, String> columnNames, List<String> pkColumns) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.pkColumns = pkColumns;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, String> getColumnNames() {
        return columnNames;
    }

    public List<String> getPkColumns() {
        return pkColumns;
    }
}
