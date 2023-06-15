/*
 * JDBCYamlIntegrationTests.java
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

import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.cli.CliCommand;
import com.apple.foundationdb.relational.cli.CliCommandFactory;
import com.apple.foundationdb.relational.cli.CliConfigCommand;
import com.apple.foundationdb.relational.cli.CommandGroupVisitor;
import com.apple.foundationdb.relational.cli.RelationalScriptProcessor;
import com.apple.foundationdb.relational.jdbc.JDBCURI;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.server.InProcessRelationalServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Disabled;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Like {@link YamlIntegrationTests} only it runs the YAML via the fdb-relational-jdbc client
 * talking to an in-process Relational Server.
 */
public class JDBCYamlIntegrationTests extends YamlIntegrationTests {
    private static final Logger LOG = LogManager.getLogger(JDBCYamlIntegrationTests.class);

    /**
     * Creates an in-process server per invocation.
     */
    private static final class JDBCCommandFactory implements CliCommandFactory {
        /**
         * An inprocess server started on construction and closed out in {@link #close()}.
         */
        final InProcessRelationalServer server;
        /**
         * Loaded Relational JDBC Driver.
         */
        final Driver driver;
        /**
         * Current, live connection.
         * We do one-at-a-time only.
         */
        private RelationalConnection connection;

        // Load up our JDBC Driver. Run all registered ServiceLaoders.
        static {
            // Load ServiceLoader Services.
            for (Driver value : ServiceLoader.load(Driver.class)) {
                // Intentionally empty
            }
        }

        private JDBCCommandFactory() throws SQLException {
            // Use ANY valid URl to get hold of the driver. When we 'connect' we'll
            // more specific about where we want to connect to.
            this.driver = DriverManager.getDriver("jdbc:relational:///");
            try {
                this.server = new InProcessRelationalServer().start();
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }

        @Override
        public void close() throws Exception {
            try {
                closeConnection();
            } finally {
                if (this.server != null) {
                    this.server.close();
                }
            }
        }

        private void closeConnection() throws SQLException {
            if (this.connection != null) {
                this.connection.close();
                this.connection = null;
            }
        }

        @Override
        public CliCommand<Void> getConfigureCommand(String configName, String configValue) {
            return new CliConfigCommand(null, configName, configValue);
        }

        @Override
        public CliCommand<Void> getConnectCommand(@Nonnull URI connectPath) {
            return () -> {
                closeConnection();
                // Add name of the inprocess running server to the connectPath.
                URI connectPathPlusServerName = JDBCURI.addQueryParameter(connectPath,
                        JDBCURI.INPROCESS_URI_QUERY_SERVERNAME_KEY, this.server.getServerName());
                String uriStr = connectPathPlusServerName.toString().replaceFirst("embed:", "relational://");
                LOG.info("Rewrote {} as {}", connectPath, uriStr);
                this.connection = this.driver.connect(uriStr, null)
                        .unwrap(RelationalConnection.class);
                return null;
            };
        }

        @Override
        public CliCommand<RelationalConnection> getConnection() {
            return () -> this.connection;
        }

        @Override
        public CliCommand<ResultSet> getShowConnectionCommand() {
            // Copied from DbStateCommandFactory.
            return () -> {
                List<Row> output;
                if (this.connection == null) {
                    output = Collections.emptyList();
                } else {
                    output = List.of(new ArrayRow(new Object[]{connection.getPath().toString()}));
                }
                StructMetaData smd = new RelationalStructMetaData(
                        FieldDescription.primitive("Path", Types.VARCHAR, DatabaseMetaData.columnNoNulls)
                );

                return new IteratorResultSet(smd, output.iterator(), 0);
            };
        }

        @Override
        public CliCommand<Void> getDisconnectCommand() {
            return () -> {
                closeConnection();
                return null;
            };
        }

        @Override
        public CliCommand<ResultSet> getShowDatabasesCommand() {
            return () -> {
                boolean changeSchema = false;
                String oldSchema = this.connection.getSchema();
                try {
                    changeSchema = !"CATALOG".equals(oldSchema);
                    if (changeSchema) {
                        this.connection.setSchema("CATALOG");
                        changeSchema = true;
                    }
                    try (RelationalStatement vs = this.connection.createStatement()) {
                        return vs.executeQuery("Select * from \"DATABASES\"");
                    }
                } finally {
                    if (changeSchema) {
                        this.connection.setSchema(oldSchema);
                    }
                }
            };
        }

        @Override
        public CliCommand<Void> getSetSchemaCommand(String schemaName) {
            return () -> {
                this.connection.setSchema(schemaName);
                return null;
            };
        }

        @Override
        public CliCommand<ResultSet> getShowSchemasCommand() {
            return () -> this.connection.getMetaData().getSchemas();
        }

        @Override
        public CliCommand<ResultSet> getShowTablesCommand() {
            return () -> {
                if (this.connection.getSchema() == null) {
                    throw new SQLException("Cannot get tables without a Schema",
                            ErrorCode.UNDEFINED_SCHEMA.getErrorCode());
                }
                return this.connection.getMetaData().getTables(this.connection.getPath().getPath(),
                        this.connection.getSchema(), null, null);
            };
        }

        @Override
        public CliCommand<Object> getQueryCommand(String command) {
            return () -> {
                try (RelationalStatement s = this.connection.createStatement()) {
                    boolean hasResults = s.execute(command);
                    return hasResults ? s.getResultSet() : s.getUpdateCount();
                }
            };
        }

        @Override
        public CliCommand<CommandGroupVisitor.CommandGroup> getExecuteCommand(String command) {
            return () -> {
                try {
                    return RelationalScriptProcessor.parseFromFile(null, command);
                } catch (FileNotFoundException fnfe) {
                    throw new SQLException(fnfe.getMessage(), ErrorCode.UNDEFINED_FILE.getErrorCode());
                } catch (IOException e) {
                    //TODO(bfines) have this throw a more precise error code
                    throw new SQLException(e.getMessage(), ErrorCode.INTERNAL_ERROR.getErrorCode(), e);
                }
            };
        }

        @Override
        public <T> CliCommand<T> error(ErrorCode syntaxError, String errorMsg) {
            return () -> {
                throw new RelationalException(errorMsg, syntaxError).toSqlException();
            };
        }
    }

    @Override
    CliCommandFactory createCliCommandFactory() throws RelationalException {
        try {
            return new JDBCCommandFactory();
        } catch (SQLException e) {
            throw new RelationalException(e);
        }
    }

    @Override
    @Disabled("The standard-tests-proto.yaml has 'load schema template' which is not supported")
    public void standardTestsWithProto() throws Exception {
        super.standardTestsWithProto();
    }

    @Override
    @Disabled("The deprecated-fields-tests-proto.yaml has 'load schema template' which is not supported")
    public void deprecatedFieldsTestsWithProto() throws Exception {
        super.deprecatedFieldsTestsWithProto();
    }

    @Override
    @Disabled("The subquery-tests.yaml file has 'inserts' which need DynamicBuilder on client-side; not supported")
    public void subqueryTests() throws Exception {
        super.subqueryTests();
    }

    @Override
    @Disabled("The primary-key-tests.yaml file has 'inserts' which need DynamicBuilder on client-side; not supported")
    public void primaryKey() throws Exception {
        super.primaryKey();
    }

    @Override
    @Disabled("The boolean.yaml file has 'inserts' which need DynamicBuilder on client-side; not supported")
    void booleanTypes() throws Exception {
        super.booleanTypes();
    }

    @Override
    @Disabled("TODO: Flakey")
    public void orderBy() throws Exception {
        super.orderBy();
    }

    @Override
    @Disabled("TODO: Flakey")
    public void scenarioTests() throws Exception {
        super.scenarioTests();
    }

    @Override
    @Disabled("TODO: Need to work on supporting labels")
    public void limit() throws Exception {
        super.limit();
    }

    @Override
    @Disabled("TODO")
    public void selectAStar() throws Exception {
        super.selectAStar();
    }

    @Override
    @Disabled("TODO: Flakey")
    public void aggregateIndexTestsCount() throws Exception {
        super.aggregateIndexTestsCount();
    }

    @Override
    @Disabled("TODO: Flakey")
    public void joinTests() throws Exception {
        super.joinTests();
    }

    @Override
    @Disabled("TODO: Flakey")
    public void nested() throws Exception {
        super.nested();
    }

    @Override
    @Disabled("TODO: Flakey")
    public void showcasingTests() throws Exception {
        super.showcasingTests();
    }
}
