/*
 * EmbeddedJDBCYamlIntegrationTests.java
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
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;

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
 * Like {@link YamlIntegrationTests} only it runs the YAML via the embedded fdb-relational-jdbc driver.
 */
// TODO: Run all yaml files. See below for reasons on why we skip particular yamls, usually because
// waiting on functionality.
public class EmbeddedJDBCYamlIntegrationTests extends YamlIntegrationTests {
    private static final Logger LOG = LogManager.getLogger(EmbeddedJDBCYamlIntegrationTests.class);

    /**
     * Creates an in-process server per invocation.
     */
    private static final class EmbeddedJDBCCommandFactory implements CliCommandFactory {
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

        private EmbeddedJDBCCommandFactory() throws SQLException {
            // Use ANY valid URl to get hold of the driver. When we 'connect' we'll
            // more specific about where we want to connect to.
            this.driver = DriverManager.getDriver("jdbc:embed:/__SYS");
        }

        @Override
        public void close() throws Exception {
            closeConnection();
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
                this.connection = this.driver.connect(connectPath.toString(), null)
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
            return new EmbeddedJDBCCommandFactory();
        } catch (SQLException e) {
            throw new RelationalException(e);
        }
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
    @Disabled("TODO: Need to work on supporting labels")
    public void limit() throws Exception {
        super.limit();
    }
}
