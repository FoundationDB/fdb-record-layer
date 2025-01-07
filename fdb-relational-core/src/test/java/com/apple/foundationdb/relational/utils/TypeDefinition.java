/*
 * TypeDefinition.java
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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class TypeDefinition {
    private final String name;
    //written this way rather than a map in order to preserve the order of columns in the specified list field
    private final Map<String, String> columnTypes;

    public TypeDefinition(String name, List<String> columnTypes) {
        this.name = name;
        this.columnTypes = new TreeMap<>((k1, k2) -> {
            int colNum1 = Integer.parseInt(k1.substring(3));
            int colNum2 = Integer.parseInt(k2.substring(3));
            return Integer.compare(colNum1, colNum2);
        });
        int p = 0;
        for (String colType : columnTypes) {
            this.columnTypes.put("col" + p, colType);
            p++;
        }
    }

    /**
     * Be aware that the iteration order of the map will be the column order in the type.
     *
     * @param name the name of the type
     * @param columns the column name->type map
     */
    public TypeDefinition(String name, Map<String, String> columns) {
        this.name = name;
        this.columnTypes = columns;
    }

    public String getName() {
        return name;
    }

    public String getDdlDefinition() {
        return name + columnTypeDefinition();
    }

    public String getJson() {
        return "{name: " + name + ", columns: { " + columnJson() + "}}";
    }

    protected String columnTypeDefinition() {
        return "(" +
                columnTypes.entrySet().stream()
                        .map(entry -> entry.getKey() + " " + entry.getValue())
                        .collect(Collectors.joining(",")) +
                ")";
    }

    private String columnJson() {
        return columnTypes.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(Collectors.joining(","));
    }
}
