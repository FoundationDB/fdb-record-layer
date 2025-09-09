/*
 * DirectPrimaryKeyScanTest.java
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

package com.apple.foundationdb.relational.autotest.cases;

import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.autotest.AutomatedTest;
import com.apple.foundationdb.relational.autotest.Connection;
import com.apple.foundationdb.relational.autotest.Connector;
import com.apple.foundationdb.relational.autotest.Data;
import com.apple.foundationdb.relational.autotest.DataSet;
import com.apple.foundationdb.relational.autotest.ParameterizedQuery;
import com.apple.foundationdb.relational.autotest.Query;
import com.apple.foundationdb.relational.autotest.Schema;
import com.apple.foundationdb.relational.autotest.SchemaDescription;
import com.apple.foundationdb.relational.autotest.WorkloadConfig;
import com.apple.foundationdb.relational.autotest.WorkloadConfiguration;
import com.apple.foundationdb.relational.autotest.datagen.RandomDataSet;
import com.apple.foundationdb.relational.autotest.datagen.RandomDataSource;
import com.apple.foundationdb.relational.autotest.datagen.SchemaGenerator;
import com.apple.foundationdb.relational.autotest.datagen.UniformDataSource;
import com.apple.foundationdb.relational.memory.InMemoryCatalog;
import com.apple.foundationdb.relational.memory.InMemoryRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

@AutomatedTest
public class DirectPrimaryKeyScanTest {
    @RegisterExtension
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @WorkloadConfiguration
    public WorkloadConfig config = new WorkloadConfig(
            1L,
            System.getProperty("user.dir") + "/.out/reports/autoTest/" + getClass().getSimpleName(),
            Map.of(
                    "maxStringLength", 10,
                    "maxBytesLength", 10,
                    "maxArrayLength", 10,
                    "maxTables", 1,
                    "maxStructs", 2,
                    "maxColumns", 5,
                    "numSchemas", 2,
                    "recordsPerTable", 10
            ));

    @Connection
    public Connector relationalConnector = new Connector() {
        @Override
        public RelationalConnection connect(URI dbUri) throws SQLException {
            return DriverManager.getConnection("jdbc:embed:" + dbUri.getPath()).unwrap(RelationalConnection.class);
        }

        @Override
        public String getLabel() {
            return "Relational";
        }
    };

    @Connection
    public Connector memoryConnector = new Connector() {
        private final InMemoryCatalog catalog = new InMemoryCatalog();

        @Override
        public RelationalConnection connect(URI dbUri) throws RelationalException, SQLException {
            return new InMemoryRelationalConnection(catalog, dbUri);
        }

        @Override
        public String getLabel() {
            return "InMemory";
        }
    };

    @Schema
    public Stream<SchemaDescription> getSchemas(WorkloadConfig cfg) throws SQLException {
        RandomDataSource rds = new UniformDataSource(cfg.getSeed(),
                cfg.getInt("maxStringLength"),
                cfg.getInt("maxBytesLength"));
        SchemaGenerator generator = new SchemaGenerator(rds, cfg.getInt("maxTables"),
                cfg.getInt("maxStructs"),
                cfg.getInt("maxColumns"));

        String baseTemplateName = this.getClass().getSimpleName() + "_" + String.valueOf(UUID.randomUUID()).replace("-", "_");

        int numSchemasToGen = cfg.getInt("numSchemas");
        List<SchemaDescription> schemas = new ArrayList<>();
        for (int i = 0; i < numSchemasToGen; i++) {
            String templateName = baseTemplateName + "_template_" + i;
            schemas.add(generator.generateSchemaDescription(templateName, baseTemplateName + "_" + i));
        }

        return schemas.stream();
    }

    @Data
    public final DataSet dataSet() {
        return new RandomDataSet(config.getSeed(),
                config.getInt("maxArrayLength"),
                config.getInt("recordsPerTable"),
                config.getInt("maxStringLength"),
                config.getInt("maxBytesLength"));
    }

    @Query(label = "scanKeyEqualsDirect")
    public final Collection<ParameterizedQuery> getPkScanQueries(SchemaDescription schema) {
        //for each table in the schema, want to create a ParameterizedQuery for that table
        List<ParameterizedQuery> queries = new ArrayList<>();
        schema.getTables()
                .forEach(table -> queries.add((statement, params) -> {
                    String fullTableName = schema.getSchemaName() + "." + table.getTableName();
                    RelationalStruct struct = params.get(fullTableName);
                    if (struct == null) {
                        return null; //no test to execute
                    }
                    List<String> pkCols = table.getPkColumns();
                    KeySet keySet = new KeySet();
                    for (String pkCol : pkCols) {
                        keySet.setKeyColumn(pkCol, struct.getObject(pkCol));
                    }
                    return statement.executeScan(fullTableName, keySet, Options.NONE);
                }));
        return queries;
    }

}
