/*
 * RelationalCatalog.java
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

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.ProtobufDataBuilder;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RelationalCatalog {
    private final Map<String, Set<String>> schemaToTableMapping = new HashMap<>();
    private final Map<String, RelationalStructure> relationalStructures = new HashMap<>();
    private String currentSchema;

    public RelationalStructure loadStructure(String table, Descriptors.Descriptor typeDescriptor) {
        /*
         * we can't recursively update the map because that's a CME, so instead we just collect a set of tables
         * to load and then load them in sequence afterwards
         */
        Map<String, RelationalStructure> nestedTables = new ConcurrentHashMap<>();
        RelationalStructure structure = relationalStructures.computeIfAbsent(table, k -> insertStructure(k, typeDescriptor, nestedTables));
        nestedTables.forEach(relationalStructures::putIfAbsent);
        return structure;
    }

    public RelationalStructure getStructure(String table) {
        return relationalStructures.get(table);
    }

    public void loadSchema(String schemaName, Set<Descriptors.Descriptor> tables) throws SQLException {
        for (Descriptors.Descriptor table :tables) {
            schemaToTableMapping.computeIfAbsent(schemaName, s -> new HashSet<>()).add(table.getName());
            loadStructure(table.getName(), table);
        }
    }

    public void createTables(Statement statement) throws SQLException {
        for (String schema : schemaToTableMapping.keySet()) {
            statement.execute("CREATE SCHEMA " + schema);
            statement.execute("SET SCHEMA " + schema);
        }

        Set<String> createdTables = new HashSet<>();
        for (Map.Entry<String, RelationalStructure> toLoad : relationalStructures.entrySet()) {
            String tableName = toLoad.getKey();
            if (createdTables.contains(tableName)) {
                continue;
            }
            RelationalStructure structure = toLoad.getValue();
            for (Table table : structure.getAllTables()) {
                if (!createdTables.contains(table.getName())) {
                    statement.execute(table.getCreateStatement());
                    createdTables.add(table.getName());
                }
            }
        }
    }

    public void dropTables(Statement statement, String schemaName) throws SQLException {
        Set<String> tablesToDrop = schemaToTableMapping.getOrDefault(schemaName, Collections.emptySet());
        Set<String> droppedTables = new HashSet<>();
        for (String table : tablesToDrop) {
            if (droppedTables.contains(table)) {
                continue;
            }
            RelationalStructure relStruc = relationalStructures.get(table);
            for (Table tbl : relStruc.getAllTables()) {
                if (droppedTables.add(tbl.getName())) {
                    statement.execute("DROP TABLE " + tbl.getName());
                }
            }
        }
    }

    public void dropTables(Statement statement) throws SQLException {
        Set<String> droppedTables = new HashSet<>();
        for (Map.Entry<String, RelationalStructure> toLoad : relationalStructures.entrySet()) {
            String tableName = toLoad.getKey();
            if (droppedTables.contains(tableName)) {
                continue;
            }
            RelationalStructure structure = toLoad.getValue();
            for (Table table : structure.getAllTables()) {
                if (!droppedTables.contains(table.getName())) {
                    statement.execute("DROP TABLE " + table.getName());
                    droppedTables.add(table.getName());
                }
            }
        }

    }

    private RelationalStructure insertStructure(String tableName, Descriptors.Descriptor descriptor, Map<String, RelationalStructure> nestedStructs) {
        final RelationalStructure relStruct = new RelationalStructure(descriptor);
        Collection<Table> tables = relStruct.getAllTables();
        //skip the base table and any link tables
        tables.stream()
                .filter(tbl -> !tbl.getName().equals(tableName) && tbl.getDescriptor() != null)
                .forEach(tbl -> nestedStructs.computeIfAbsent(tbl.getName(), name -> insertStructure(name, tbl.getDescriptor(), nestedStructs)));
        return relStruct;
    }

    public DynamicMessageBuilder getDataBuilder(String typeName) throws RelationalException {
        //go through the relational structures looking for the
        for (RelationalStructure structure : relationalStructures.values()) {
            final Descriptors.Descriptor descriptor = structure.getDescriptor();
            if (descriptor.getName().equals(typeName)) {
                //it's a table! whoo!
                return new ProtobufDataBuilder(structure.getDescriptor());
            } else {
                //look through the struct messages in the descriptor to see if you can find it's nestednedd
                for (Descriptors.Descriptor desc : descriptor.getFile().getMessageTypes()) {
                    if (desc.getName().equals(typeName)) {
                        return new ProtobufDataBuilder(desc);
                    }
                }
            }
        }
        throw new RelationalException("Unknown type: <" + typeName + ">", ErrorCode.UNKNOWN_TYPE);
    }
}
