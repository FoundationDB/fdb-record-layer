/*
 * QueryConfig.java
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

package com.apple.foundationdb.relational.yamltests.command;

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.recordlayer.ErrorCapturingResultSet;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.Matchers;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.block.PreambleBlock;
import com.apple.foundationdb.relational.yamltests.command.queryconfigs.CheckExplainConfig;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import com.apple.foundationdb.relational.yamltests.server.SupportedVersionCheck;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.opentest4j.AssertionFailedError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.apple.foundationdb.relational.yamltests.command.QueryCommand.reportTestFailure;

/**
 * A {@link QueryConfig} defines how the query is to be run and how the output of the query execution or the error
 * (in case of failure in query execution) is to be tested. To do so, it utilizes 2 methods
 * <ul>
 *     <li>{@code decorateQuery}: takes in the canonical query string and changes it as is needed to be run</li>
 *     <li>{@code checkResult}: checks for the result from the query execution.</li>
 *     <li>{@code checkError}: checks for the error.</li>
 * </ul>
 */
@SuppressWarnings({"PMD.GuardLogStatement", "PMD.AvoidCatchingThrowable"})
public abstract class QueryConfig {
    private static final Logger logger = LogManager.getLogger(QueryConfig.class);

    public static final String QUERY_CONFIG_RESULT = "result";
    public static final String QUERY_CONFIG_UNORDERED_RESULT = "unorderedResult";
    public static final String QUERY_CONFIG_EXPLAIN = "explain";
    public static final String QUERY_CONFIG_EXPLAIN_CONTAINS = "explainContains";
    public static final String QUERY_CONFIG_COUNT = "count";
    public static final String QUERY_CONFIG_ERROR = "error";
    public static final String QUERY_CONFIG_PLAN_HASH = "planHash";
    public static final String QUERY_CONFIG_NO_CHECKS = "noChecks";
    public static final String QUERY_CONFIG_MAX_ROWS = "maxRows";
    public static final String QUERY_CONFIG_SUPPORTED_VERSION = PreambleBlock.PREAMBLE_BLOCK_SUPPORTED_VERSION;
    public static final String QUERY_CONFIG_INITIAL_VERSION_AT_LEAST = "initialVersionAtLeast";
    public static final String QUERY_CONFIG_INITIAL_VERSION_LESS_THAN = "initialVersionLessThan";
    public static final String QUERY_CONFIG_NO_OP = "noOp";
    public static final String QUERY_CONFIG_SETUP = "setup";
    public static final String QUERY_CONFIG_SETUP_REFERENCE = "setupReference";
    public static final String QUERY_CONFIG_DEBUGGER = "debugger";

    private static final Set<String> RESULT_CONFIGS = ImmutableSet.of(QUERY_CONFIG_ERROR, QUERY_CONFIG_COUNT, QUERY_CONFIG_RESULT, QUERY_CONFIG_UNORDERED_RESULT);
    private static final Set<String> VERSION_DEPENDENT_RESULT_CONFIGS = ImmutableSet.of(QUERY_CONFIG_INITIAL_VERSION_AT_LEAST, QUERY_CONFIG_INITIAL_VERSION_LESS_THAN);

    @Nullable private final Object value;
    @Nonnull private final YamlReference reference;
    @Nullable private final String configName;

    protected QueryConfig(@Nullable String configName, @Nullable Object value, @Nonnull final YamlReference reference) {
        this.configName = configName;
        this.value = value;
        this.reference = reference;
    }

    @Nonnull
    protected YamlReference getReference() {
        return reference;
    }

    @Nullable
    protected Object getVal() {
        return value;
    }

    @Nullable
    String getConfigName() {
        return configName;
    }

    protected String getValueString() {
        if (value instanceof byte[]) {
            return ByteArrayUtil2.loggable((byte[]) value);
        } else if (value != null) {
            return value.toString();
        } else {
            return "";
        }
    }

    protected String decorateQuery(@Nonnull String query) {
        return query;
    }

    final void checkResult(@Nonnull String currentQuery, @Nonnull Object actual, @Nonnull String queryDescription,
                           @Nonnull YamlConnection connection, @Nonnull List<String> setups) {
        try {
            checkResultInternal(currentQuery, actual, queryDescription, setups);
        } catch (AssertionFailedError e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️Check result failed in config at " + getReference() + " against connection for versions " + connection.getVersions(),
                    "config [" + getConfigName() + ": " + getVal() + "] ", getReference());
        } catch (Throwable e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️Failed to test config at " + getReference() + " against connection for versions " + connection.getVersions(),
                    "config [" + getConfigName() + ": " + getVal() + "] ", getReference());
        }
    }

    final void checkError(@Nonnull SQLException actual, @Nonnull String queryDescription, final YamlConnection connection) {
        try {
            checkErrorInternal(actual, queryDescription);
        } catch (AssertionFailedError e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️Check result failed in config at " + getReference() + " against connection for versions " + connection.getVersions(),
                    "config [" + getConfigName() + ": " + getVal() + "] ", getReference());
        } catch (Throwable e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️Failed to test config at " + getReference() + " against connection for versions " + connection.getVersions(),
                    "config [" + getConfigName() + ": " + getVal() + "] ", getReference());
        }
    }

    protected abstract void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                                @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException;

    void checkErrorInternal(@Nonnull SQLException e, @Nonnull String queryDescription) throws SQLException {
        final var diffMessage = "‼️ statement failed with the following error at " + getReference() + ":\n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n" +
                e.getMessage() + "\n" +
                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n";
        logger.error(diffMessage);
        throw e;

    }

    private static QueryConfig getCheckResultConfig(boolean isExpectedOrdered, @Nullable String configName,
                                                    @Nullable Object value, @Nonnull final YamlReference reference) {
        return new QueryConfig(configName, value, reference) {

            @Override
            protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                               @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException {
                logger.debug("⛳️ Matching results of query '{}'", queryDescription);
                try (RelationalResultSet resultSet = (RelationalResultSet)actual) {
                    final var matchResult = Matchers.matchResultSet(getVal(), resultSet, isExpectedOrdered);
                    if (!matchResult.getLeft().equals(Matchers.ResultSetMatchResult.success())) {
                        var toReport = "‼️ result mismatch at " + getReference() + ":\n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n" +
                                Matchers.notNull(matchResult.getLeft().getExplanation(), "failure error message") + "\n" +
                                "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n";
                        final var valueString = getValueString();
                        if (!valueString.isEmpty()) {
                            toReport += "↪ expected result:\n" +
                                    getValueString() + "\n";
                        }
                        if (matchResult.getRight() != null) {
                            toReport += "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n" +
                                    "↩ actual result left to be matched:\n" +
                                    Matchers.notNull(matchResult.getRight(), "failure error actual result set");
                        }
                        reportTestFailure(toReport);
                    } else {
                        logger.debug("✅ results match!");
                    }
                }
            }
        };
    }

    private static QueryConfig getCheckExplainConfig(boolean isExact, @Nonnull String blockName,
                                                     @Nonnull String configName, @Nullable Object value,
                                                     @Nonnull final YamlReference reference, @Nonnull YamlExecutionContext executionContext) {
        return new CheckExplainConfig(configName, value, reference, executionContext, isExact, blockName);
    }

    private static QueryConfig getCheckErrorConfig(@Nullable Object value, @Nonnull final YamlReference reference) {
        return new QueryConfig(QUERY_CONFIG_ERROR, value, reference) {

            @Override
            protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                               @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException {
                Matchers.ResultSetPrettyPrinter resultSetPrettyPrinter = new Matchers.ResultSetPrettyPrinter();
                if (actual instanceof ErrorCapturingResultSet) {
                    Matchers.printRemaining((ErrorCapturingResultSet) actual, resultSetPrettyPrinter);
                    reportTestFailure(String.format(Locale.ROOT,
                            "‼️ expecting statement to throw an error, however it returned a result set at %s%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n" +
                                    "%s%n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤%n",
                            getReference(), resultSetPrettyPrinter));
                } else if (actual instanceof Integer) {
                    reportTestFailure("‼️ expecting statement to throw an error, however it returned a count at " + getReference() + "\n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n" +
                                    actual + "\n" +
                                    "⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n");
                } else {
                    reportTestFailure("‼️ unexpected query result of type '" + actual.getClass().getSimpleName() + "' (expecting '" + ErrorCapturingResultSet.class.getSimpleName() + "') at " + getReference() + "\n");
                }
            }

            @Override
            void checkErrorInternal(@Nonnull SQLException e, @Nonnull String queryDescription) {
                logger.debug("⛳️ Checking error code resulted from executing '{}'", queryDescription);
                if (!e.getSQLState().equals(getVal())) {
                    reportTestFailure("‼️ expecting '" + getVal() + "' error code, got '" + e.getSQLState() + "' instead at " + getReference() + "!", e);
                } else {
                    logger.debug("✅ error codes '{}' match!", getVal());
                }
            }
        };
    }

    private static QueryConfig getCheckCountConfig(@Nullable Object value, @Nonnull final YamlReference reference) {
        return new QueryConfig(QUERY_CONFIG_COUNT, value, reference) {

            @Override
            protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                               @Nonnull String queryDescription, @Nonnull List<String> setups) {
                logger.debug("⛳️ Matching count of update query '{}'", queryDescription);
                if (!Matchers.matches(getVal(), actual)) {
                    reportTestFailure("‼️ Expected count value " + getVal() + ", but got " + actual + " at line " + getReference());
                } else {
                    logger.debug("✅ Results match!");
                }
            }
        };
    }

    private static QueryConfig getCheckPlanHashConfig(@Nullable Object value, @Nonnull YamlReference reference) {
        return new QueryConfig(QUERY_CONFIG_PLAN_HASH, value, reference) {

            @Override
            protected String decorateQuery(@Nonnull String query) {
                return "EXPLAIN " + query;
            }

            @SuppressWarnings("PMD.CloseResource") // lifetime of autocloseable persists beyond method
            @Override
            protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                               @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException {
                logger.debug("⛳️ Matching plan hash of query '{}'", queryDescription);
                final var resultSet = (RelationalResultSet) actual;
                resultSet.next();
                final var actualPlanHash = resultSet.getInt(2);
                if (!Matchers.matches(getVal(), actualPlanHash)) {
                    reportTestFailure("‼️ Incorrect plan hash: expecting " + getVal() + " got " + actualPlanHash + " at " + getReference());
                }
                logger.debug("✅️ Plan hash matches!");
            }
        };
    }

    public static QueryConfig getNoCheckConfig(@Nonnull final YamlReference reference) {
        return new QueryConfig(QUERY_CONFIG_NO_CHECKS, null, reference) {
            @SuppressWarnings("PMD.CloseResource") // lifetime of autocloseable persists beyond method
            @Override
            protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                               @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException {
                if (actual instanceof RelationalResultSet) {
                    final var resultSet = (RelationalResultSet) actual;
                    // slurp
                    boolean valid = true;
                    while (valid) { // suppress check style
                        valid = ((RelationalResultSet) actual).next();
                    }
                    resultSet.close();
                }
            }
        };
    }

    private static QueryConfig getMaxRowConfig(@Nonnull Object value, @Nonnull final YamlReference reference) {
        return new QueryConfig(QUERY_CONFIG_MAX_ROWS, value, reference) {
            @Override
            protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                               @Nonnull String queryDescription, @Nonnull List<String> setups) {
                Assert.failUnchecked("No results to check on a maxRow config");
            }
        };
    }

    private static QueryConfig getSetupConfig(final Object value, @Nonnull final YamlReference reference) {
        return new QueryConfig(QUERY_CONFIG_SETUP, value, reference) {
            @Override
            protected void checkResultInternal(@Nonnull final String currentQuery, @Nonnull final Object actual,
                                               @Nonnull final String queryDescription, @Nonnull List<String> setups) {
                Assert.failUnchecked("No results to check on a setup config");
            }
        };
    }

    private static QueryConfig getDebuggerConfig(@Nonnull Object value, @Nonnull final YamlReference reference) {
        return new QueryConfig(QUERY_CONFIG_DEBUGGER, DebuggerImplementation.valueOf(((String)value).toUpperCase(Locale.ROOT)),
                reference) {
            @Override
            protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                               @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException {
                Assert.failUnchecked("No results to check on a debugger config");
            }
        };
    }

    @Nonnull
    public static QueryConfig getSupportedVersionConfig(Object rawVersion, @Nonnull final YamlReference reference, final YamlExecutionContext executionContext) {
        final SupportedVersionCheck check = SupportedVersionCheck.parse(rawVersion, executionContext.getConnectionFactory().getVersionsUnderTest());
        if (!check.isSupported()) {
            return new SkipConfig(QUERY_CONFIG_SUPPORTED_VERSION, rawVersion, reference, check.getMessage());
        }
        return new QueryConfig(QUERY_CONFIG_SUPPORTED_VERSION, rawVersion, reference) {
            @Override
            protected void checkResultInternal(@Nonnull final String currentQuery, @Nonnull final Object actual,
                                               @Nonnull final String queryDescription, @Nonnull final List<String> setups) {
                // Nothing to do, this query is supported
                // SupportedVersion configs are not executed
                Assertions.fail("Supported version configs are not meant to be executed.");
            }
        };
    }

    /**
     * Return a NoOp config - a config that does nothing.
     * This config can be executed but will perform no action. Use in cases where we need to continue running (e.g.
     * the command and config are legal and supprted) but some conditions make execution unneeded.
     * @param reference the {@link YamlReference} in the test file
     * @return an instance of a NoOp config
     */
    public static QueryConfig getNoOpConfig(@Nonnull final YamlReference reference) {
        return new QueryConfig(QUERY_CONFIG_NO_OP, null, reference) {
            @SuppressWarnings("PMD.CloseResource") // lifetime of autocloseable persists beyond method
            @Override
            protected void checkResultInternal(@Nonnull String currentQuery, @Nonnull Object actual,
                                               @Nonnull String queryDescription, @Nonnull List<String> setups) throws SQLException {
                // This should not be executed
                Assertions.fail("NoOp Config should not be executed");
            }
        };
    }

    @Nonnull
    public static List<QueryConfig> parseConfigs(String blockName, @Nonnull final YamlReference commandReference,
                                                 @Nonnull List<?> objects, @Nonnull YamlExecutionContext executionContext) {
        List<QueryConfig> configs = new ArrayList<>();
        // After the first result config, require all future results are also result configs. That is, we should
        // not interleave explain, maxRows, etc., and result configurations, and the results should be the last
        boolean requireResults = false;

        for (Object object : objects) {
            final var configEntry = Matchers.notNull(Matchers.firstEntry(object, "query configuration"), "query configuration");
            final var linedObject = CustomYamlConstructor.LinedObject.cast(configEntry.getKey(), () -> "Invalid config key-value pair: " + configEntry);
            final var reference = commandReference.getResource().withLineNumber(linedObject.getLineNumber());
            try {
                final var key = Matchers.notNull(Matchers.string(linedObject.getObject(), "query configuration"), "query configuration");
                final var value = Matchers.notNull(configEntry, "query configuration").getValue();
                boolean resultOrVersionConfig = RESULT_CONFIGS.contains(key) || VERSION_DEPENDENT_RESULT_CONFIGS.contains(key);
                if (requireResults && !resultOrVersionConfig) {
                    throw new IllegalArgumentException("Only result configurations can follow first result or version specification config");
                }
                requireResults |= resultOrVersionConfig;
                configs.add(parseConfig(blockName, key, value, reference, executionContext));
            } catch (Exception e) {
                throw YamlExecutionContext.wrapContext(e, () -> "‼️ Error parsing the query config at " + reference, "config", reference);
            }
        }

        validateConfigs(configs, commandReference);
        return configs;
    }

    private static QueryConfig getInitialVersionCheckConfig(Object key, Object value, @Nonnull final YamlReference reference) {
        try {
            SemanticVersion versionArgument = PreambleBlock.parseVersion(value);
            if (QUERY_CONFIG_INITIAL_VERSION_AT_LEAST.equals(key)) {
                return new InitialVersionCheckConfig(QueryConfig.QUERY_CONFIG_INITIAL_VERSION_AT_LEAST, value, reference,
                        versionArgument, SemanticVersion.max());
            } else if (QUERY_CONFIG_INITIAL_VERSION_LESS_THAN.equals(key)) {
                return new InitialVersionCheckConfig(QueryConfig.QUERY_CONFIG_INITIAL_VERSION_LESS_THAN, value, reference,
                        SemanticVersion.min(), versionArgument);
            } else {
                throw new IllegalArgumentException("Unknown version constraint " + key);
            }
        } catch (Exception e) {
            throw YamlExecutionContext.wrapContext(e, () -> "‼️ Unable to parse version constraint information at " + reference, "version constraint", reference);
        }
    }

    private static QueryConfig parseConfig(String blockName, String key, Object value, @Nonnull final YamlReference reference, YamlExecutionContext executionContext) {
        if (QUERY_CONFIG_SUPPORTED_VERSION.equals(key)) {
            return getSupportedVersionConfig(value, reference, executionContext);
        } else if (VERSION_DEPENDENT_RESULT_CONFIGS.contains(key)) {
            return getInitialVersionCheckConfig(key, value, reference);
        } else if (QUERY_CONFIG_COUNT.equals(key)) {
            return getCheckCountConfig(value, reference);
        } else if (QUERY_CONFIG_ERROR.equals(key)) {
            return getCheckErrorConfig(value, reference);
        } else if (QUERY_CONFIG_EXPLAIN.equals(key)) {
            if (shouldExecuteExplain(executionContext)) {
                return getCheckExplainConfig(true, blockName, key, value, reference, executionContext);
            } else {
                return getNoOpConfig(reference);
            }
        } else if (QUERY_CONFIG_EXPLAIN_CONTAINS.equals(key)) {
            if (shouldExecuteExplain(executionContext)) {
                return getCheckExplainConfig(false, blockName, key, value, reference, executionContext);
            } else {
                return getNoOpConfig(reference);
            }
        } else if (QUERY_CONFIG_PLAN_HASH.equals(key)) {
            if (shouldExecuteExplain(executionContext)) {
                return getCheckPlanHashConfig(value, reference);
            } else {
                return getNoOpConfig(reference);
            }
        } else if (QUERY_CONFIG_RESULT.equals(key)) {
            return getCheckResultConfig(true, key, value, reference);
        } else if (QUERY_CONFIG_UNORDERED_RESULT.equals(key)) {
            return getCheckResultConfig(false, key, value, reference);
        } else if (QUERY_CONFIG_MAX_ROWS.equals(key)) {
            return getMaxRowConfig(value, reference);
        } else if (QUERY_CONFIG_SETUP.equals(key)) {
            return getSetupConfig(value, reference);
        } else if (QUERY_CONFIG_SETUP_REFERENCE.equals(key)) {
            return getSetupConfig(executionContext.getTransactionSetup(value), reference);
        } else if (QUERY_CONFIG_DEBUGGER.equals(key)) {
            return getDebuggerConfig(value, reference);
        } else {
            throw Assert.failUnchecked("‼️ '" + key + "' is not a valid configuration");
        }
    }

    private static void validateConfigs(List<QueryConfig> configs, @Nonnull final YamlReference reference) {
        Assert.thatUnchecked(configs.stream().skip(1)
                        .noneMatch(config -> QueryConfig.QUERY_CONFIG_SUPPORTED_VERSION.equals(config.getConfigName())),
                "supported_version must be the first config in a query (after the query itself)");

        // Validate that the results check each version comprehensively by making sure the set of
        // covered ranges spans the range [MIN_VERSION, MAX_VERSION)
        if (configs.stream().anyMatch(config -> config instanceof QueryConfig.InitialVersionCheckConfig)) {
            // Creating an interval set including each covered range
            RangeSet<SemanticVersion> rangeSet = TreeRangeSet.create();
            configs.stream().filter(config -> config instanceof QueryConfig.InitialVersionCheckConfig)
                    .map(config -> (QueryConfig.InitialVersionCheckConfig)config)
                    .forEach(config -> rangeSet.add(Range.closedOpen(config.getMinVersion(), config.getMaxVersion())));
            // Get the set of uncovered ranges that span over [MIN_VERSION, MAX_VERSION)
            Set<Range<SemanticVersion>> uncovered = rangeSet.complement()
                    .subRangeSet(Range.closedOpen(SemanticVersion.min(), SemanticVersion.max()))
                    .asRanges();
            if (!uncovered.isEmpty()) {
                IllegalArgumentException e = new IllegalArgumentException("Test case does not cover complete set of versions as it is missing: " + uncovered);
                throw YamlExecutionContext.wrapContext(e, () -> "‼️ Non-comprehensive test case found at " + reference, "config", reference);
            }
        }
    }

    public static class SkipConfig extends QueryConfig {
        private final String message;

        public SkipConfig(final String configMap, final Object value, @Nonnull final YamlReference reference, final String message) {
            super(configMap, value, reference);
            this.message = message;
        }

        @Override
        protected void checkResultInternal(@Nonnull final String currentQuery, @Nonnull final Object actual,
                                           @Nonnull final String queryDescription, @Nonnull final List<String> setups) {
            Assertions.fail("Skipped config should not be executed: at " + getReference() + " " + message);
        }

        public String getMessage() {
            return message;
        }
    }

    public static class InitialVersionCheckConfig extends QueryConfig {
        private final SemanticVersion minVersion;
        private final SemanticVersion maxVersion;

        public InitialVersionCheckConfig(final String configName, final Object value, @Nonnull final YamlReference reference,
                                         SemanticVersion minVersion, SemanticVersion maxVersion) {
            super(configName, value, reference);
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
        }

        public boolean shouldExecute(YamlConnection connection) {
            SemanticVersion initialVersion = connection.getInitialVersion();
            return initialVersion.compareTo(minVersion) >= 0 && initialVersion.compareTo(maxVersion) < 0;
        }

        @Override
        protected void checkResultInternal(@Nonnull final String currentQuery, @Nonnull final Object actual,
                                           @Nonnull final String queryDescription, @Nonnull final List<String> setups) throws SQLException {
            Assertions.fail("Check version config should not be executed: at " + getReference());
        }

        @Nonnull
        public SemanticVersion getMinVersion() {
            return minVersion;
        }

        @Nonnull
        public SemanticVersion getMaxVersion() {
            return maxVersion;
        }
    }

    private static boolean shouldExecuteExplain(final YamlExecutionContext executionContext) {
        return (! executionContext.getConnectionFactory().isMultiServer());
    }

}
