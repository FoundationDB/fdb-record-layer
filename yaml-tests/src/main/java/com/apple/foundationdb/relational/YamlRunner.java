/*
 * YamlRunner.java
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

package com.apple.foundationdb.relational;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.cli.DbState;
import com.apple.foundationdb.relational.cli.DbStateCommandFactory;
import com.apple.foundationdb.relational.recordlayer.ErrorCapturingResultSet;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.relational.Matchers.*;

@SuppressWarnings({"PMD.GuardLogStatement"}) // It already is, but PMD is confused and reporting error in unrelated locations.
public final class YamlRunner implements AutoCloseable {

    private static final Logger LOG = LogManager.getLogger(YamlRunner.class);

    @Nonnull
    final String resourcePath;

    @Nonnull
    private final InputStream inputStream;

    @Nonnull
    private final DbState dbState;

    @Nonnull
    private final DbStateCommandFactory commandFactory;

    private YamlRunner(@Nonnull String resourcePath, @Nonnull final InputStream inputStream,
                       @Nonnull final DbState dbState, @Nonnull DbStateCommandFactory commandFactory) {
        this.resourcePath = resourcePath;
        this.inputStream = inputStream;
        this.dbState = dbState;
        this.commandFactory = commandFactory;
    }

    private static class CustomTagsInject extends SafeConstructor {

        private boolean recursing;

        public CustomTagsInject() {
            yamlConstructors.put(new Tag("!dc"), new CustomTagsInject.ConstructDontCare());
            yamlConstructors.put(new Tag("!l"), new CustomTagsInject.ConstructLong());
        }

        @Override
        protected Object constructObject(Node node) {
            if (recursing) {
                return super.constructObject(node);
            } else {
                recursing = true;
                Object o = super.constructObject(node);
                recursing = false;
                return new LinedObject(o, node.getStartMark());
            }
        }

        private static final class LinedObject {
            private final Object object;

            private final Mark startMark;

            private LinedObject(final Object object, final Mark startMark) {
                this.object = object;
                this.startMark = startMark;
            }

            public Object getObject() {
                return object;
            }

            public Mark getStartMark() {
                return startMark;
            }
        }

        private static class ConstructDontCare extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                return YamlRunner.DontCare.INSTANCE;
            }
        }

        private static class ConstructLong extends AbstractConstruct {
            @Override
            public Object construct(Node node) {
                if (!(node instanceof ScalarNode)) {
                    Assert.failUnchecked(String.format("The value of the long (!l) tag must be a scalar, however '%s' is found!", node));
                }
                return Long.valueOf(((ScalarNode) node).getValue());
            }
        }
    }

    static final class DontCare {
        static final DontCare INSTANCE = new DontCare();

        private DontCare() {
        }

        @Override
        public String toString() {
            return "!dc";
        }
    }

    private enum Command {
        CONNECT("connect") {
        @Override
        public void invoke(@Nonnull final List<?> region, @Nonnull final DbStateCommandFactory factory, @Nonnull final DbState dbState) throws Exception {
            final var uri = string(firstEntry(first(region, "connect"), "connect").getValue(), "connect");
            debug(String.format("connecting to '%s'", uri));
            final var connectionFun = factory.getConnectCommand(URI.create(notNull(uri, "connection URI")));
            connectionFun.call();
            debug(String.format("connected to '%s'", uri));
        }
    },
        QUERY("query") {
            private void sanitizeConfigurations(@Nonnull final Set<QueryConfig> configMap) throws RelationalException {
                if (configMap.contains(QueryConfig.ERROR) && configMap.contains(QueryConfig.RESULT)) {
                    Assert.fail(String.format("‼️ could not have both %s and %s as configuration", QueryConfig.ERROR.label, QueryConfig.RESULT.label));
                }
            }

            @Override
            public void invoke(@Nonnull final List<?> region, @Nonnull final DbStateCommandFactory factory, @Nonnull final DbState dbState) throws Exception {
                final var queryString = string(notNull(firstEntry(first(region), "query string").getValue(), "query string"), "query string");
                // parse configurations, for now the only configuration we support is checking the exact results, in future we could add more configuration such as
                // checking the error message codes.
                final var queryConfigs = new HashMap<QueryConfig, Object>();
                for (final var config : region.stream().skip(1).collect(Collectors.toList())) {
                    final var queryConfigurationAndValue = firstEntry(config, "query configuration");
                    final var queryConfigurationStr = notNull(string(notNull(queryConfigurationAndValue, "query configuration").getKey(), "query configuration"), "query configuration");
                    queryConfigs.put(QueryConfig.resolve(queryConfigurationStr), notNull(queryConfigurationAndValue, "query configuration").getValue());
                }
                sanitizeConfigurations(queryConfigs.keySet());
                debug(String.format("executing query '%s'", queryString));
                Object queryResults = null;
                SQLException sqlException = null;
                try {
                    queryResults = factory.getQueryCommand(queryString).call();
                } catch (SQLException se) {
                    sqlException = se;
                }
                debug(String.format("finished executing query '%s'", queryString));
                if (queryConfigs.isEmpty() && sqlException != null) {
                    error(String.format("‼️ statement failed with the following error:%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                            "%s%n" +
                            "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            sqlException.getMessage()));
                    throw sqlException;
                }
                for (final var config : queryConfigs.entrySet()) {
                    switch (config.getKey()) {
                        case RESULT:
                            if (sqlException != null) {
                                error(String.format("‼️ expecting statement to return results, however received an error instead:%n" +
                                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                        "%s%n" +
                                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                                        sqlException.getMessage()));
                                throw sqlException; // propagate.
                            }
                            debug(String.format("matching results of query '%s'", queryString));
                            Assert.that(queryResults instanceof ErrorCapturingResultSet, String.format("‼️ unexpected query result of type '%s' (expecting '%s')",
                                    queryResults.getClass().getSimpleName(),
                                    ErrorCapturingResultSet.class.getSimpleName()));
                            final var resultSet = (ErrorCapturingResultSet) queryResults;
                            final var expectedResults = config.getValue();
                            final var matchResult = matchResultSet(expectedResults, resultSet);
                            if (!matchResult.equals(ResultSetMatchResult.success())) {
                                Assert.fail(String.format("‼️ result mismatch:%n" +
                                        notNull(matchResult.getExplanation(), "failure error message") + "%n" +
                                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                        "↪ expected result:%n" +
                                        (expectedResults == null ? "<NULL>" : expectedResults.toString()) + "%n" +
                                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                        "↩ actual result:%n" +
                                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                        notNull(matchResult.getResultSetPrinter(), "failure error actual result set")));
                            } else {
                                debug("✔️ results match!");
                            }
                            break;
                        case ERROR:
                            if (queryResults != null) {
                                ResultSetPrettyPrinter resultSetPrettyPrinter = new ResultSetPrettyPrinter();
                                Assert.that(queryResults instanceof ErrorCapturingResultSet, String.format("‼️ another error is enountered while trying to print to" +
                                        "print unexpected query result!%n" +
                                        "unexpected query result of type '%s' (expecting '%s')",
                                        queryResults.getClass().getSimpleName(),
                                        ErrorCapturingResultSet.class.getSimpleName()));
                                printRemaining((ErrorCapturingResultSet) queryResults, resultSetPrettyPrinter);
                                Assert.fail(String.format("‼️ expecting statement to throw an error, however it returned a result set%n" +
                                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                        "%s%n" +
                                        "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                                        resultSetPrettyPrinter)
                                );
                            }
                            debug(String.format("checking error code resulted from executing '%s'", queryString));
                            if (sqlException == null) {
                                Assert.fail("‼️ unexpected NULL SQLException!");
                            }
                            final var errorCode = string(config.getValue(), "expected error code");
                            if (!sqlException.getSQLState().equals(errorCode)) {
                                Assert.fail(String.format("‼️ expecting '%s' error code, got '%s' instead!", errorCode, sqlException.getSQLState()));
                            } else {
                                debug(String.format("✔️ error codes '%s' match!", errorCode));
                            }
                            break;
                        default:
                            Assert.fail(String.format("‼️ no handler for query configuration '%s'", config.getKey().label));
                    }
                }
            }
        },
        INSERT("insert") {
            @Override
            public void invoke(@Nonnull final List<?> region, @Nonnull final DbStateCommandFactory factory, @Nonnull final DbState dbState) throws Exception {
                final var tableEntry = firstEntry(second(region), "table name");
                matches(notNull(string(notNull(tableEntry, "table name").getKey(), "table name"), "table name"), "table");
                final var tableName = notNull(string(notNull(tableEntry, "table name").getValue(), "table name"), "table name");
                final var connection = notNull(dbState.getConnection(), "database connection");
                try (var statement = connection.createStatement()) {
                    debug("parsing YAML input into PB Message(s)");
                    final var yamlData = notNull(firstEntry(first(region), "insert data").getValue(), "insert data");
                    final DynamicMessageBuilder tableRowBuilder = notNull(statement.getDataBuilder(tableName), String.format("table '%s' message builder", tableName));
                    final var dataList = Generators.yamlToDynamicMessage(yamlData, tableRowBuilder);
                    if (dataList.isEmpty()) {
                        debug(String.format("⚠️ parsed 0 rows, skipping insert into '%s'", tableName));
                        return;
                    }
                    debug(String.format("inserting %d row(s) in '%s'", dataList.size(), tableName));
                    statement.executeInsert(tableName, dataList); // todo: affected rows.
                    debug(String.format("inserting %d row(s) in '%s'", dataList.size(), tableName));
                }
            }
        };

        // in Java-11, it is not possible to define enum inside enum-value, so we do it here.
        private enum QueryConfig {
            RESULT("result"),
            ERROR("error");

            @Nonnull
            private final String label;

            QueryConfig(@Nonnull final String label) {
                this.label = label;
            }

            @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
            @Nonnull
            public static QueryConfig resolve(@Nonnull final String label) throws Exception {
                for (final var c : values()) {
                    if (c.label.equals(label)) {
                        return c;
                    }
                }
                Assert.fail(String.format("‼️ '%s' is not a valid configuration of '%s', available configuration(s): '%s'",
                        label,
                        QUERY.label,
                        Arrays.stream(values()).map(v -> v.label).collect(Collectors.joining(","))));
                return null;
            }
        }

        private final String label;

        public abstract void invoke(@Nonnull final List<?> region,
                                    @Nonnull final DbStateCommandFactory factory,
                                    @Nonnull final DbState dbState) throws Exception;

        Command(@Nonnull final String label) {
            this.label = label;
        }

        @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
        @Nonnull
        public static Command resolve(@Nonnull final String label) throws Exception {
            for (final var c : values()) {
                if (c.label.equals(label)) {
                    return c;
                }
            }
            Assert.failUnchecked(String.format("‼️ could not find command '%s'", label));
            return null;
        }
    }

    private static void debug(@Nonnull final String message) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(message);
        }
    }

    private static void error(@Nonnull final String message) {
        if (LOG.isErrorEnabled()) {
            LOG.error(message);
        }
    }

    @Nonnull
    private Command resolveCommand(@Nonnull final List<?> commandAndConfiguration) throws Exception {
        final var commandAndArgument = firstEntry(first(commandAndConfiguration, "command list"), "command list");
        final var commandStr = notNull(string(notNull(notNull(commandAndArgument, "command").getKey(), "command"), "command"), "command");
        return Command.resolve(commandStr);
    }

    public void run() throws Exception {
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(true);
        final var yaml = new Yaml(new CustomTagsInject(), new Representer(), new DumperOptions(), options, new Resolver());
        int currentLine = 0;
        for (final var region : yaml.loadAll(inputStream)) {
            final var regionWithLines = (CustomTagsInject.LinedObject) region;
            currentLine = regionWithLines.getStartMark().getLine() + 1;
            debug(String.format("📍 executing at line %s of '%s'", currentLine, resourcePath));
            final var commandObject = regionWithLines.getObject();
            final var commandAndConfiguration = arrayList(commandObject, "test commands");
            final var command = resolveCommand(commandAndConfiguration);
            try {
                command.invoke(commandAndConfiguration, commandFactory, dbState);
            } catch (Exception e) {
                addYamlFileStackFrameToException(e, resourcePath, currentLine);
                throw e;
            }
        }
        debug(String.format("🏁 executed all tests in '%s' successfully!", resourcePath));
    }

    private static void addYamlFileStackFrameToException(@Nonnull final Exception exception, @Nonnull final String path, int line) {
        final StackTraceElement[] stackTrace = exception.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[stackTrace.length + 1];
        newStackTrace[0] = new StackTraceElement("<YAML FILE>", "", path, line);
        System.arraycopy(stackTrace, 0, newStackTrace, 1, stackTrace.length);
        exception.setStackTrace(newStackTrace);
    }

    @Override
    public void close() throws Exception {
        dbState.close();
        inputStream.close();
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static YamlRunner create(@Nonnull final String resourcePath) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
        Assert.notNull(inputStream, String.format("could not find '%s' in resources bundle", resourcePath));
        try {
            final var dbState = new DbState();
            return new YamlRunner(resourcePath, inputStream, dbState, new DbStateCommandFactory(dbState));
        } catch (RelationalException ve) {
            Assert.fail(String.format("failed to create a '%s' object.", DbState.class.getSimpleName()));
        }
        return null;
    }
}
