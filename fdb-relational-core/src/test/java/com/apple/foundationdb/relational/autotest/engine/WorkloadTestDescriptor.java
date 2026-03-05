/*
 * WorkloadTestDescriptor.java
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

package com.apple.foundationdb.relational.autotest.engine;

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.autotest.Connector;
import com.apple.foundationdb.relational.autotest.DataSet;
import com.apple.foundationdb.relational.autotest.ParameterizedQuery;
import com.apple.foundationdb.relational.autotest.SchemaDescription;
import com.apple.foundationdb.relational.autotest.TableDescription;
import com.apple.foundationdb.relational.autotest.WorkloadConfig;
import com.apple.foundationdb.relational.autotest.datagen.DataSample;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.utils.ReservoirSample;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.junit.jupiter.engine.config.JupiterConfiguration;
import org.junit.jupiter.engine.descriptor.DynamicDescendantFilter;
import org.junit.jupiter.engine.descriptor.JunitUtils;
import org.junit.jupiter.engine.descriptor.JupiterTestDescriptor;
import org.junit.jupiter.engine.descriptor.NestedClassTestDescriptor;
import org.junit.jupiter.engine.execution.JupiterEngineExecutionContext;
import org.junit.jupiter.engine.extension.MutableExtensionRegistry;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.commons.util.UnrecoverableExceptions;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.UniqueId;

import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Note: this class extends TestMethodTestDescriptor, but it doesn't actually have any methods to invoke.
 * This was done because Junit doesn't expose a public constructor for JupiterTestDescriptor, so we needed
 * to subclass a subclass that could be managed more easily, and TestMethodTestDescriptor fits the bill nicely
 */
class WorkloadTestDescriptor extends NestedClassTestDescriptor {
    private final AutoWorkload workload;
    private final QuerySet querySet;

    private final DynamicDescendantFilter dynamicDescendantFilter = new DynamicDescendantFilter();

    public WorkloadTestDescriptor(UniqueId uniqueId,
                                  Class<?> testClass,
                                  TestDescriptor parent,
                                  JupiterConfiguration configuration,
                                  AutoWorkload workload,
                                  QuerySet queries) {
        super(uniqueId, testClass, () -> NestedClassTestDescriptor.getEnclosingTestClasses(parent), configuration);
        this.workload = workload;
        this.querySet = queries;
    }

    @Override
    public boolean mayRegisterTests() {
        return true;
    }

    @Override
    public JupiterEngineExecutionContext execute(JupiterEngineExecutionContext context, DynamicTestExecutor dynamicTestExecutor) throws Exception {
        /*
         * First, create the data
         */
        context.getThrowableCollector().execute(() -> {
            WorkloadReporter reporter = new WorkloadReporter(workload.getConfig().getReportDirectory());
            reporter.publishWorkload(workload);
            try {
                loadSchema(workload);
                DataSample dataSample = loadData(workload);

                TestSource defaultTestSource = getSource().orElseThrow(
                        () -> new JUnitException("Illegal state: TestSource must be present"));

                int index = 1;
                for (ParameterizedQuery query : querySet.getQueries()) {
                    final Iterator<Map<String, RelationalStruct>> sampleIterator = dataSample.getSampleIterator();
                    while (sampleIterator.hasNext()) {
                        Map<String, RelationalStruct> params = sampleIterator.next();

                        final String label = workload.getDisplayName() + "." + querySet.getLabel() + "[" + index + "]";
                        DynamicTest dt = DynamicTest.dynamicTest(label, () -> {
                            WorkloadReporter.TestReporter testReporter = reporter.beginTest(querySet.getLabel());
                            try {
                                runComparisonTest(query, workload.getConnectors(), params, testReporter);
                                testReporter.testSuccess();
                            } catch (Throwable t) {
                                testReporter.testFailed(t, false);
                                throw t;
                            }
                        });
                        Optional<JupiterTestDescriptor> descriptor = JunitUtils.createDynamicDescriptor(this, dt, index++,
                                defaultTestSource, dynamicDescendantFilter, context.getConfiguration());

                        descriptor.ifPresent(dynamicTestExecutor::execute);
                    }
                }
                dynamicTestExecutor.awaitFinished();
            } catch (Throwable t) {
                UnrecoverableExceptions.rethrowIfUnrecoverable(t);
                invokeTestExecutionExceptionHandlers(context.getExtensionRegistry(), context.getExtensionContext(), t);
            } finally {
                dropDatabase(workload);
                reporter.workloadComplete();
            }
        });

        return context;
    }

    @Override
    public ExecutionMode getExecutionMode() {
        return super.getExecutionMode();
    }

    private void invokeTestExecutionExceptionHandlers(MutableExtensionRegistry registry, ExtensionContext context, Throwable t) {
        JunitUtils.invokeExecutionExceptionHandlers(this, TestExecutionExceptionHandler.class, registry, t,
                (handler, handledT) -> handler.handleTestExecutionException(context, handledT));
    }

    @Override
    public void cleanUp(JupiterEngineExecutionContext context) throws Exception {
        super.cleanUp(context);
        context.getThrowableCollector().assertEmpty();
    }

    @Override
    public void nodeSkipped(JupiterEngineExecutionContext context, TestDescriptor testDescriptor, SkipResult result) {
        super.nodeSkipped(context, testDescriptor, result);
    }

    @Override
    public void nodeFinished(JupiterEngineExecutionContext context, TestDescriptor testDescriptor, TestExecutionResult result) {
        super.nodeFinished(context, testDescriptor, result);
    }

    private static class QueryResultSet {
        private final List<Row> rows;
        //TODO(bfines) we might need to make a defensive copy here to avoid lifecycle access problems
        private final StructMetaData metaData;

        public QueryResultSet(List<Row> rows, StructMetaData metaData) {
            this.rows = rows;
            this.metaData = metaData;
        }

        public RelationalResultSet asResultSet() {
            return new IteratorResultSet(metaData, rows.iterator(), 0);
        }
    }

    private void runComparisonTest(ParameterizedQuery query,
                                   List<Connector> connectors,
                                   Map<String, RelationalStruct> params,
                                   WorkloadReporter.TestReporter testReporter) {
        Map<String, QueryResultSet> results = new HashMap<>();
        params.forEach((key, value) -> testReporter.publishEntry("param[" + key + "]", value.toString()));

        for (Connector connector : connectors) {
            try (RelationalConnection conn = connector.connect(workload.getDatabasePath());
                    RelationalStatement vs = conn.createStatement()) {
                try (RelationalResultSet rrs = query.executeQuery(vs, params)) {
                    if (rrs != null) {
                        List<Row> data = new ArrayList<>();
                        while (rrs.next()) {
                            Object[] arr = new Object[rrs.getMetaData().getColumnCount()];
                            for (int i = 0; i < arr.length; i++) {
                                arr[i] = rrs.getObject(i + 1);
                            }
                            data.add(new ArrayRow(arr));
                        }
                        results.put(connector.getLabel(), new QueryResultSet(data, rrs.getMetaData().unwrap(StructMetaData.class)));
                    }
                }
            } catch (RelationalException ve) {
                throw ve.toUncheckedWrappedException();
            } catch (SQLException e) {
                throw new RelationalException("Error while executing query from connection " + connector.getLabel() + ":" + e.getMessage(), e).toUncheckedWrappedException();
            }
        }
        //now verify that all the rows are the same
        for (Map.Entry<String, QueryResultSet> result : results.entrySet()) {
            String resultName1 = result.getKey();
            RelationalResultSet dataSet1 = result.getValue().asResultSet();
            for (Map.Entry<String, QueryResultSet> otherResult : results.entrySet()) {
                if (otherResult.getKey().equals(resultName1)) {
                    continue; //skip comparing to ourselves
                }
                RelationalResultSet dataSet2 = result.getValue().asResultSet();
                ResultSetAssert.assertThat(dataSet1).as(resultName1).isExactlyInAnyOrder(dataSet2);
            }
        }
    }

    private void dropDatabase(AutoWorkload workload) {
        Throwable t = null;
        for (Connector connector : workload.getConnectors()) {
            try (RelationalConnection ddlConn = connector.connect(URI.create("/__SYS"));
                    RelationalStatement ddlStatement = ddlConn.createStatement()) {
                ddlConn.setSchema("CATALOG");

                ddlStatement.execute("DROP DATABASE \"" + workload.getDatabasePath() + "\"");
                ddlConn.commit();
            } catch (RelationalException | SQLException se) {
                if (t != null) {
                    t.addSuppressed(se);
                } else {
                    t = se;
                }
            }
        }
    }

    private DataSample loadData(AutoWorkload workload) {
        List<Connector> connectors = workload.getConnectors();
        final SchemaDescription schema = workload.getSchema();
        DataSet dataSet = workload.getData();
        DataSample sample = new DataSample();

        try {
            String schemaName = schema.getSchemaName();
            List<TableDescription> tables = schema.getTables();
            for (TableDescription table : tables) {
                String fullTableName = schemaName + "." + table.getTableName();

                List<RelationalConnection> vConns = new ArrayList<>();
                List<RelationalStatement> statements = new ArrayList<>();
                try {
                    for (Connector connector : connectors) {
                        final RelationalConnection conn = connector.connect(workload.getDatabasePath());
                        vConns.add(conn);
                        final RelationalStatement statement = conn.createStatement();
                        statements.add(statement);
                    }

                    //TODO(bfines) configure this
                    final WorkloadConfig config = workload.getConfig();
                    ReservoirSample<RelationalStruct> reservoir = new ReservoirSample<>(config.getInt(WorkloadConfig.SAMPLE_SIZE, 100),
                            config.getSeed());
                    try (Stream<RelationalStruct> structs = dataSet.getData(table)) {
                        /*
                         * Read in a batch of records, and insert them to every connector
                         */
                        int batchSize = config.getInt(WorkloadConfig.INSERT_BATCH_SIZE, 10);
                        List<RelationalStruct> batch = new ArrayList<>(batchSize); //TODO(Bfines) make this configurable
                        structs.forEach(struct -> {
                            batch.add(struct);
                            if (batch.size() == batchSize) {
                                insertDataBatch(fullTableName, statements, batch, reservoir);
                            }
                        });
                        if (!batch.isEmpty()) {
                            insertDataBatch(fullTableName, statements, batch, reservoir);
                        }
                    }
                    sample.addSample(fullTableName, reservoir);
                } finally {
                    for (RelationalStatement vs : statements) {
                        vs.close();
                    }
                    for (RelationalConnection vConn : vConns) {
                        vConn.close();
                    }
                }
            }
        } catch (RelationalException ve) {
            throw ve.toUncheckedWrappedException();
        } catch (SQLException se) {
            throw new RelationalException(se).toUncheckedWrappedException();
        }
        return sample;
    }

    void insertDataBatch(String tableName,
                         List<RelationalStatement> statements,
                         List<RelationalStruct> structs,
                         ReservoirSample<RelationalStruct> reservoir) {
        try {
            for (RelationalStatement statement : statements) {
                statement.executeInsert(tableName, structs);
            }
            //add to the sample here, only after we guarantee that the write actually succeeded
            structs.forEach(reservoir::add);
            structs.clear();
        } catch (SQLException e) {
            RelationalException ve = ExceptionUtil.toRelationalException(e);
            //ignore PK violations for now
            //TODO(bfines) better data generation to avoid duplicate keys
            if (!ve.getErrorCode().equals(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION)) {
                throw ve.toUncheckedWrappedException();
            }
        }
    }

    private void loadSchema(AutoWorkload workload) {
        List<Connector> connectors = workload.getConnectors();
        try {
            //load up the schemas
            SchemaDescription schema = workload.getSchema();
            final URI databasePath = workload.getDatabasePath();
            for (Connector connector : connectors) {
                try (RelationalConnection ddlConn = connector.connect(URI.create("/__SYS"));
                        RelationalStatement ddlStatement = ddlConn.createStatement()) {
                    ddlConn.setSchema("CATALOG");
                    String schemaCreateStatement = String.format(Locale.ROOT, "CREATE SCHEMA TEMPLATE \"%s\" %s",
                            schema.getTemplateName(),
                            schema.getTemplateDescription());
                    ddlStatement.execute(schemaCreateStatement);

                    //now create the database
                    ddlStatement.execute("CREATE DATABASE \"" + databasePath.getPath() + "\"");

                    //create the schema
                    String fullSchemaName = "\"" + databasePath.getPath() + "/" + schema.getSchemaName() + "\"";
                    ddlStatement.execute("CREATE SCHEMA " + fullSchemaName + " WITH TEMPLATE \"" + schema.getTemplateName() + "\"");
                }
            }
        } catch (RelationalException | SQLException e) {
            throw new JUnitException("Error constructing schema", e);
        }
    }

}
