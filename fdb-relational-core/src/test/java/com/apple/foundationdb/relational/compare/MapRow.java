/*
 * MapRow.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import java.sql.SQLException;
import java.util.Map;

class MapRow implements Row {
    private Map<String, Object> rowData;
    private String[] positionToNameMap;

    // the contents of positionToNameMap should not be null
    MapRow(Map<String, Object> rowData, String[] positionToNameMap) {
        this.rowData = rowData;
        this.positionToNameMap = positionToNameMap;
    }

    @Override
    public Object getObject(String name) throws SQLException {
        return rowData.get(name);
    }

    @Override
    public Object getObject(int position) throws SQLException {
        //we assume by construction that the returned name should not be null
        return rowData.get(positionToNameMap[position]);
    }

    @Override
    public int getNumColumns() {
        return positionToNameMap.length;
    }
}
