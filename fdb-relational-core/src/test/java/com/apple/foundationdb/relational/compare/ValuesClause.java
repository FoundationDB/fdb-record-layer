/*
 * ValuesClause.java
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

import java.util.LinkedList;
import java.util.List;

class ValuesClause {
    List<String> columnNames;
    List<String> columnValues;

    public ValuesClause(List<String> columnNames) {
        this.columnNames = columnNames;
        this.columnValues = new LinkedList<>();
    }

    void addValues(String value) {
        columnValues.add(value);
    }

    public String columnList() {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (String columnName : columnNames) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(",");
            }
            sb.append(columnName);
        }
        return sb.toString();
    }

    public String valuesString() {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        int numColumns = columnNames.size();
        int written = 0;
        for (String columnValue : columnValues) {
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(",");
            }
            if (written == 0) {
                sb.append("(");
            }
            sb.append(columnValue);
            written++;
            if (written % numColumns == 0) {
                sb.append(")");
                written = 0;
            }
        }
        String s = sb.toString();
        if (s.startsWith("(")) {
            return s;
        } else {
            return "(" + s + ")";
        }
    }

    public boolean hasValues() {
        return !columnValues.isEmpty();
    }
}
