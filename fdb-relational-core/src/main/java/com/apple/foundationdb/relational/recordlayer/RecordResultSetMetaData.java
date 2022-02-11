/*
 * RecordResultSetMetaData.java
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

import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

public class RecordResultSetMetaData implements RelationalResultSetMetaData {
    private final String[] fieldNames;

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2", justification = "internal class, shouldn't escape proper usage")
    public RecordResultSetMetaData(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    @Override
    public int getColumnCount() {
        return fieldNames.length;
    }

    @Override
    public String getColumnName(int column) {
        return fieldNames[column];
    }

}
