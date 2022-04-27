/*
 * DirectAccessComparisonTest.java
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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.compare.ComparisonTest;
import com.apple.foundationdb.relational.compare.DataGenerator;
import com.apple.foundationdb.relational.compare.QueryTestResult;
import com.apple.foundationdb.relational.compare.RandomDataGenerator;
import com.apple.foundationdb.relational.compare.TestResult;
import com.apple.foundationdb.relational.compare.TransactionAction;
import com.apple.foundationdb.relational.compare.Workload;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

@Disabled("disabled until comparison testing API is further resolved")
public class DirectAccessComparisonTest {
    @RegisterExtension public final EmbeddedRelationalExtension rlEngineRule = new EmbeddedRelationalExtension();

    @Test
    void getWorkload() throws Exception {
        Map<String, PlaceholderCatalog.TableInfo> tables = new HashMap<>();
        tables.put("restaurant", new PlaceholderCatalog.TableInfo(Restaurant.RestaurantRecord.getDescriptor(), Collections.singletonList("rest_no")));
        PlaceholderCatalog.SchemaData schemaData = new PlaceholderCatalog.SchemaData("restaurant", Restaurant.getDescriptor(), tables);
        GetWorkload workload = new GetWorkload(schemaData);
        new ComparisonTest(rlEngineRule.getEngine()).runWorkloadTest(workload);
    }

    private static class GetLoad implements Workload.LoadAction {
        private final Descriptors.Descriptor table;
        private final long randomSeed;
        private final String schema;

        public GetLoad(String schema, Descriptors.Descriptor table, long randomSeed) {
            this.schema = schema;
            this.table = table;
            this.randomSeed = randomSeed;
        }

        @Override
        public String getSchemaName() {
            return schema;
        }

        @Override
        public Descriptors.Descriptor getDataType() {
            return table;
        }

        @Override
        public DataGenerator getDataToLoad() {
            return new RandomDataGenerator(new Random(randomSeed), table, 10, 10, 10, 10);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GetLoad getLoad = (GetLoad) o;
            return table.getName().equalsIgnoreCase(getLoad.table.getName());
        }

        @Override
        public int hashCode() {
            return table.getName().hashCode();
        }
    }

    private static class GetWorkload implements Workload {
        private final PlaceholderCatalog.SchemaData schema;

        public GetWorkload(PlaceholderCatalog.SchemaData schema) {
            this.schema = schema;
        }

        @Override
        public Set<SchemaAction> getSchemaActions() {
            return Collections.singleton(new SchemaAction() {

                @Override
                public Set<Descriptors.Descriptor> getTables() {
                    return schema.getAllTables().stream().map(PlaceholderCatalog.TableInfo::getTableDescriptor).collect(Collectors.toSet());
                }

                @Override
                public String getLoadSchemaStatement() {
                    StringBuilder schemaCreateStatement = new StringBuilder("CREATE SCHEMA TEMPLATE " + schema.getSchemaName() + "_TEMPLATE AS {");
                    final Descriptors.FileDescriptor fileDescriptor = schema.getFileDescriptor();
                    final Descriptors.Descriptor unionDescriptor = fileDescriptor.findMessageTypeByName("UnionDescriptor");
                    Set<String> tables = unionDescriptor.getFields().stream().map(fd -> fd.getName().replace("_", "")).collect(Collectors.toSet());
                    tables.add("UnionDescriptor"); //remove UnionDescriptor type data
                    fileDescriptor.getMessageTypes().stream().filter(type -> !tables.contains(type.getName())).forEach(new Consumer<Descriptors.Descriptor>() {
                        @Override
                        public void accept(Descriptors.Descriptor type) {
                            schemaCreateStatement.append("CREATE STRUCT ").append(type.getName()).append("(");
                            boolean isFirst = true;
                            for (Descriptors.FieldDescriptor field : type.getFields()) {
                                if (isFirst) {
                                    isFirst = false;
                                } else {
                                    schemaCreateStatement.append(",");
                                }
                                schemaCreateStatement.append(field.getName()).append(" ").append(typeForDescriptor(field));
                            }
                            schemaCreateStatement.append(");\n");
                        }
                    });

                    schema.getAllTables().forEach(tableInfo -> schemaCreateStatement.append(tableInfo.getTableStatement()).append(";\n"));

                    schemaCreateStatement.append("};\n");

                    schemaCreateStatement.append("CREATE DATABASE ").append("/").append(getDatabaseName()).append(" WITH {")
                            .append(getSchemaName()).append(" has template ") .append(getSchemaName()).append("_TEMPLATE;").append("};");

                    return schemaCreateStatement.toString();
                }

                @Override
                public String getSchemaName() {
                    return schema.getSchemaName();
                }

                @Override
                public boolean tearDownAfterTest() {
                    return true;
                }

                @Override
                public Descriptors.FileDescriptor getSchemaFileDescriptor() {
                    return schema.getFileDescriptor();
                }
            });
        }

        private String typeForDescriptor(Descriptors.FieldDescriptor field) {
            String type = field.isRepeated() ? "repeated " : "";
            switch (field.getJavaType()) {
                case INT:
                case LONG:
                    return type + "int64";
                case FLOAT:
                case DOUBLE:
                    return type + "double";
                case BOOLEAN:
                    return type + "boolean";
                case STRING:
                    return type + "string";
                case BYTE_STRING:
                    return type + "bytes";
                case MESSAGE:
                    return type + "message " + field.getMessageType().getName();
                case ENUM:
                default:
                    throw new UnsupportedOperationException("Not Implemented in the Relational layer");
            }
        }

        @Override
        public Set<LoadAction> getLoadActions() {
            final Set<PlaceholderCatalog.TableInfo> allTables = schema.getAllTables();
            final Set<LoadAction> loads = new HashSet<>();
            for (PlaceholderCatalog.TableInfo tableInfo :allTables) {
                loads.add(new GetLoad(schema.getSchemaName(), tableInfo.getTableDescriptor(), getRandomSeed()));
            }
            return loads;
        }

        @Override
        public int getSampleSize() {
            return 10;
        }

        @Override
        public Set<TransactionAction> getQueries() {
            Set<TransactionAction> queries = new HashSet<>();
            for (PlaceholderCatalog.TableInfo tableInfo : schema.getAllTables()) {
                Descriptors.Descriptor dataType = tableInfo.getTableDescriptor();
                List<String> pks = tableInfo.getPrimaryKeys();
                List<Descriptors.FieldDescriptor> pkFields = new ArrayList<>();
                for (String pk :pks) {
                    pkFields.add(dataType.findFieldByName(pk));
                }
                GetQuery query = new GetQuery(dataType.getName(), pkFields);
                queries.add(query);
            }
            return queries;
        }

        @Override
        public String getDatabaseName() {
            return "testGetWorkload";
        }
    }

    private static class GetQuery implements TransactionAction {
        private final String tableName;
        private final List<Descriptors.FieldDescriptor> keyFields;

        public GetQuery(String tableName, List<Descriptors.FieldDescriptor> keyFields) {
            this.tableName = tableName;
            this.keyFields = keyFields;
        }

        @Override
        public TestResult execute(@Nonnull ParameterSet parameterSet, @Nonnull RelationalStatement dataSource) throws RelationalException {
            KeySet keySet = new KeySet();
            final Message message = parameterSet.getParameter(tableName);
            if (message != null) {
                for (Descriptors.FieldDescriptor fd : keyFields) {
                    Object o = message.getField(fd);
                    if (o != null) {
                        keySet.setKeyColumn(fd.getName(), o);
                    }
                }
            }

            RelationalResultSet rrs = dataSource.executeGet(tableName, keySet, Options.create());
            return new QueryTestResult(rrs);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GetQuery getQuery = (GetQuery) o;
            return tableName.equals(getQuery.tableName);
        }

        @Override
        public int hashCode() {
            return tableName.hashCode();
        }
    }
}
