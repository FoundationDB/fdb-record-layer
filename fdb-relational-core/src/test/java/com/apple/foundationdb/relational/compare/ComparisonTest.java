/*
 * ComparisonTest.java
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * A Base test environment to use when running a "Comparison Test".
 *
 * Essentially, a Comparison test follows this basic approach:
 *
 * 1. Load data from the Workload into H2
 * 2. Load data from the Workload into Relational
 * 3. Run the Query Loads in H2
 * 4. Run the Query Loads in Relational
 * 5. Compare the results for each query for data correctness
 * 6. Compare the performance of each query--In general, the H2 implementation is not optimal for our data model,
 * so we can reasonably expect H2's performance to be near or much worse than ours for most reasonable workloads. If that
 * is not the case (i.e. if Relational performs more than a few percentage values worse), then likely the Relational implementation
 * has problems, so we can fail the test based on that categorization.
 */
public class ComparisonTest {
    private final EmbeddedRelationalEngine rlRelationalEngine;

    public ComparisonTest(EmbeddedRelationalEngine rlRelationalEngine) {
        this.rlRelationalEngine = rlRelationalEngine;
    }

    public void runWorkloadTest(Workload workload) throws Exception {
        //register Relational stuff
        rlRelationalEngine.registerDriver();
        //configure our RelationalCatalog
        RelationalCatalog catalog = new RelationalCatalog();
        for (Workload.SchemaAction schemaAction : workload.getSchemaActions()) {
            catalog.loadSchema(schemaAction.getSchemaName(), schemaAction.getTables());
        }

        //        final DdlStatement ddlEngine = rlRelationalEngine.getDdlEngine();
        //        for(Workload.SchemaAction schemaAction: workload.getSchemaActions()){
        //            ddlEngine.execute(schemaAction.getLoadSchemaStatement());
        //        }

        //connect to H2
        //register the driver
        Class.forName("org.h2.Driver");

        //<./test> = current working directory/test -- should ultimately be src/test/resources/h2Data or something
        try (final Connection h2Conn = DriverManager.getConnection("jdbc:h2:./src/test/resources/" + workload.getDatabaseName())) {

            try (RelationalConnection jdbcConn = new JDBCDatabaseConnection(catalog, h2Conn);
                    RelationalConnection relationalConn = connectToRelational(workload)) {
                //create the tables in H2
                try (Statement stmt = jdbcConn.createStatement()) {
                    catalog.createTables(stmt);
                }

                // TODO(bfines)create the tables in Relational

                try {
                    Map<String, ReservoirSample<Message>> dataSamples = loadData(workload, jdbcConn, relationalConn);
                    runTransactions(workload, jdbcConn, relationalConn, new Params(dataSamples));
                } finally {
                    try (Statement jdbcStatement = jdbcConn.createStatement()) {
                        for (Workload.SchemaAction action : workload.getSchemaActions()) {
                            if (action.tearDownAfterTest()) {
                                catalog.dropTables(jdbcStatement, action.getSchemaName());
                                dropRelationalTables();
                            }
                        }
                    }
                }
            }
        }
    }

    private void dropRelationalTables() {
        //TODO(bfines) implement

    }

    private void runTransactions(Workload workload,
                                 RelationalConnection jdbcConn,
                                 RelationalConnection relationalConn,
                                 Params params) throws SQLException, RelationalException {
        /*
         * Now run each query. The way this works is that we ask the Workload to give us a ParameterizedQuery,
         * which we then populate over and over again with different values in the sample, and perform the comparison
         */

        final Set<TransactionAction> queries = workload.getQueries();
        for (TransactionAction query : queries) {
            while (params.hasData()) {
                //run it on H2 and Relational, and compare the results
                try (RelationalStatement h2Stmt = jdbcConn.createStatement();
                        RelationalStatement vStmt = relationalConn.createStatement()) {
                    TestResult h2Result = query.execute(params, h2Stmt);
                    TestResult relationalResult = query.execute(params, vStmt);
                    if (h2Result.getQueryResult() != null) {
                        try (RelationalResultSet h2Vrs = h2Result.getQueryResult()) {
                            try (RelationalResultSet relationalVrs = relationalResult.getQueryResult()) {
                                //the results should be the same
                                assertIdenticalResults(h2Vrs, relationalVrs, query.enforceSortOrder());
                            }
                        }
                    }
                }
                params.advance();
            }
        }
    }

    private Map<String, ReservoirSample<Message>> loadData(Workload workload, RelationalConnection jdbcConn, RelationalConnection relationalConn) throws SQLException, RelationalException {
        /*
         * Now load up the data. Because so many tests use parameters based on what's loaded,
         * we maintain a random sample of what we load so that we can pass those parameters to the query
         * workloads
         */
        final Set<Workload.LoadAction> loadActions = workload.getLoadActions();
        Map<String, ReservoirSample<Message>> dataSamples = new HashMap<>();
        //TODO(bfines) loading this in parallel might be a good idea
        for (Workload.LoadAction loadAction :loadActions) {
            jdbcConn.setSchema(loadAction.getSchemaName());
            relationalConn.setSchema(loadAction.getSchemaName());
            final List<Message> batchBuffer = new ArrayList<>(loadAction.getWriteBufferSize());

            final DataGenerator dataToLoad = loadAction.getDataToLoad();
            final ReservoirSample<Message> sample = new ReservoirSample<>(workload.getSampleSize(), workload.getRandomSeed());
            Message message;
            while ((message = dataToLoad.generateNext()) != null) {
                batchBuffer.add(message);
                sample.add(message);
                if (batchBuffer.size() == loadAction.getWriteBufferSize()) {
                    try (RelationalStatement stmt = jdbcConn.createStatement();
                            RelationalStatement vStmt = relationalConn.createStatement()) {
                        stmt.executeInsert(message.getDescriptorForType().getName(), batchBuffer.iterator(), Options.create());
                        vStmt.executeInsert(message.getDescriptorForType().getName(), batchBuffer.iterator(), Options.create());
                    }
                }
            }
            dataSamples.put(loadAction.getDataType().getName(), sample);
        }
        return dataSamples;
    }

    private RelationalConnection connectToRelational(Workload workload) throws RelationalException {
        String dbName = workload.getDatabaseName();
        return Relational.connect(URI.create("jdbc:embed:/" + dbName), Options.create());
    }

    private void assertIdenticalResults(RelationalResultSet h2Vrs, RelationalResultSet relationalVrs, boolean enforceSortOrder) throws SQLException {

        if (enforceSortOrder) {
            Iterator<Row> h2Iter = ResultSetUtils.resultSetIterator(h2Vrs);
            Iterator<Row> relationalIter = ResultSetUtils.resultSetIterator(relationalVrs);

            while (h2Iter.hasNext()) {
                Assertions.assertTrue(relationalIter.hasNext(), "The expected ResultSet contains more records than the test ResultSet");
                Row h2Row = h2Iter.next();
                Row relationalRow = relationalIter.next();
                Assertions.assertTrue(h2Row.equalTo(relationalRow), "Expected row " + h2Row.asString() + " but got " + relationalRow.asString());
            }
        } else {
            Collection<Row> h2Rows = ResultSetUtils.materializeResultSet(h2Vrs);
            Collection<Row> relationalRows = ResultSetUtils.materializeResultSet(relationalVrs);
            Assertions.assertEquals(h2Rows.size(), relationalRows.size(), "Expected result set size <" + h2Rows.size() + ">, but got <" + relationalRows.size() + "> instead");
            for (Row h2Row : h2Rows) {
                boolean found = false;
                for (Row relationalRow : relationalRows) {
                    if (relationalRow.equalTo(h2Row)) {
                        found = true;
                        break;
                    }
                }
                Assertions.assertTrue(found, "Result Set missing expected row " + h2Row);
            }

            for (Row relationalRow : relationalRows) {
                boolean found = false;
                for (Row h2Row : h2Rows) {
                    if (relationalRow.equalTo(h2Row)) {
                        found = true;
                        break;
                    }
                }
                Assertions.assertTrue(found, "Unexpected row in result set: <" + relationalRow + ">");
            }
        }
    }

    private static class Params implements TransactionAction.ParameterSet {
        private final Map<String, MessageParams> paramsIterator;
        boolean hasData;

        public Params(Map<String, ReservoirSample<Message>> data) {
            this.paramsIterator = new HashMap<>();
            data.forEach((name, messageReservoirSample) -> paramsIterator.put(name, new MessageParams(messageReservoirSample.sampleIterator())));
            advance();
        }

        @Override
        @Nullable
        public Message getParameter(String table) {
            return paramsIterator.get(table).current;
        }

        @Override
        public boolean hasData() {
            return hasData;
        }

        private void advance() {
            hasData = false;
            for (MessageParams params : paramsIterator.values()) {
                if (params.message.hasNext()) {
                    hasData = true;
                    params.current = params.message.next();
                } else {
                    params.current = null;
                }
            }
        }
    }

    private static class MessageParams {
        private final Iterator<Message> message;
        private Message current;

        public MessageParams(Iterator<Message> message) {
            this.message = message;
        }
    }
}
