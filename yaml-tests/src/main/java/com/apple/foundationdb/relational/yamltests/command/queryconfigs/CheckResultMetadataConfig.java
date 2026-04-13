/*
 * CheckResultMetadataConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.command.queryconfigs;

import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.yamltests.CustomYamlConstructor;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opentest4j.AssertionFailedError;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * QueryConfig associated with {@link QueryConfig#QUERY_CONFIG_RESULT_METADATA} that validates the column
 * metadata (column names and SQL type names) of a query's result set.
 * <p>
 *     The expected metadata is specified in the YAMSQL file as a list of column descriptors in
 *     {@code {name: type}} format. Column names are compared case-insensitively.
 * </p>
 * <p>
 *     Example YAMSQL usage:
 *     <pre>
 *     - query: SELECT id, col1 FROM t1
 *       resultMetadata:
 *         - {ID: BIGINT}
 *         - {COL1: BIGINT}
 *       result: [{!l 10, !l 20}]
 *     </pre>
 * </p>
 * <p>
 *     When {@link YamlExecutionContext#OPTION_CORRECT_RESULT_METADATA} is set (i.e., running under
 *     {@link com.apple.foundationdb.relational.yamltests.configs.CorrectResultMetadata}), mismatches cause the
 *     YAMSQL source file to be updated with the actual column metadata rather than failing the test.
 * </p>
 */
@SuppressWarnings("PMD.GuardLogStatement")
public class CheckResultMetadataConfig extends QueryConfig {
    private static final Logger logger = LogManager.getLogger(CheckResultMetadataConfig.class);

    @Nonnull
    private final YamlExecutionContext executionContext;

    public CheckResultMetadataConfig(@Nonnull final String configName, @Nullable final Object value,
                                     @Nonnull final YamlReference reference,
                                     @Nonnull final YamlExecutionContext executionContext) {
        super(configName, value, reference);
        this.executionContext = executionContext;
    }

    /**
     * Extract column descriptors from result-set metadata without consuming any rows.
     * This is a pure read of the schema; the result set remains positioned before the first row.
     */
    @Nonnull
    public static List<ColumnDescriptor> extractDescriptors(@Nonnull final RelationalResultSetMetaData metaData)
            throws SQLException {
        final int count = metaData.getColumnCount();
        final List<ColumnDescriptor> descriptors = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            descriptors.add(new ColumnDescriptor(metaData.getColumnName(i), metaData.getColumnTypeName(i)));
        }
        return descriptors;
    }

    /**
     * Inline metadata check: compare the given descriptors (already extracted from a live result set) against the
     * expected metadata declared in the YAMSQL file, without executing the query again.
     */
    public void checkInline(@Nonnull final List<ColumnDescriptor> actualDescriptors,
                            @Nonnull final String queryDescription,
                            @Nonnull final YamlConnection connection) {
        try {
            checkDescriptorsInternal(actualDescriptors, queryDescription);
        } catch (AssertionFailedError e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️Check result failed in config at " + getReference() + " against connection for versions " + connection.getVersions(),
                    "config [" + QueryConfig.QUERY_CONFIG_RESULT_METADATA + ": " + getVal() + "] ", getReference());
        } catch (Throwable e) {
            throw YamlExecutionContext.wrapContext(e,
                    () -> "‼️Failed to test config at " + getReference() + " against connection for versions " + connection.getVersions(),
                    "config [" + QueryConfig.QUERY_CONFIG_RESULT_METADATA + ": " + getVal() + "] ", getReference());
        }
    }

    @Override
    @SuppressWarnings({"PMD.CloseResource", "PMD.EmptyWhileStmt"})
    protected void checkResultInternal(@Nonnull final String currentQuery, @Nonnull final Object actual,
                                       @Nonnull final String queryDescription,
                                       @Nonnull final List<String> setups) throws SQLException {
        if (!(actual instanceof RelationalResultSet)) {
            logger.warn("⚠️ resultMetadata check skipped: query returned a non-ResultSet result at {}", getReference());
            return;
        }
        try (RelationalResultSet rs = (RelationalResultSet) actual) {
            // Read metadata before consuming rows (metadata is available from the open result set)
            final List<ColumnDescriptor> actualDescriptors = extractDescriptors(rs.getMetaData());
            // Exhaust the result set: getContinuation() requires the result set to be fully iterated
            // noinspection StatementWithEmptyBody
            while (rs.next()) {
            }
            checkDescriptorsInternal(actualDescriptors, queryDescription);
        }
    }

    /**
     * Core comparison and correction logic, shared between independent and inline execution modes.
     * Expects descriptors already extracted; does not touch the result set.
     */
    @SuppressWarnings("unchecked")
    private void checkDescriptorsInternal(@Nonnull final List<ColumnDescriptor> actualDescriptors,
                                          @Nonnull final String queryDescription) {
        final Object val = getVal();
        final List<Map<?, ?>> expectedColumns = val == null ? List.of() : (List<Map<?, ?>>) val;

        logger.debug("⛳️ Checking result metadata for query '{}'", queryDescription);
        if (!matchesExpected(expectedColumns, actualDescriptors)) {
            if (executionContext.shouldCorrectResultMetadata()) {
                correctMetadata(actualDescriptors);
            } else {
                reportMetadataMismatch(expectedColumns, actualDescriptors);
            }
        } else {
            logger.debug("✅ result metadata matches!");
        }
    }

    private void correctMetadata(@Nonnull final List<ColumnDescriptor> actualDescriptors) {
        if (!executionContext.correctResultMetadata(getReference(), actualDescriptors)) {
            QueryCommand.reportTestFailure("‼️ Cannot correct resultMetadata at " + getReference());
        } else {
            logger.debug(() -> "⭐️ Successfully corrected resultMetadata at " + getReference());
        }
    }

    private void reportMetadataMismatch(@Nonnull final List<Map<?, ?>> expectedColumns,
                                        @Nonnull final List<ColumnDescriptor> actualDescriptors) {
        final StringBuilder sb = new StringBuilder();
        sb.append("‼️ result metadata mismatch at ").append(getReference()).append(":\n")
          .append("⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n")
          .append("↪ expected columns (").append(expectedColumns.size()).append("):\n");
        for (Map<?, ?> col : expectedColumns) {
            final Map.Entry<?, ?> entry = CustomYamlConstructor.LinedObject.unlineKeys(col).entrySet().iterator().next();
            sb.append("    ").append(entry.getKey())
              .append(": ").append(entry.getValue()).append('\n');
        }
        sb.append("⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n")
          .append("↩ actual columns (").append(actualDescriptors.size()).append("):\n");
        for (ColumnDescriptor desc : actualDescriptors) {
            sb.append("    ").append(desc.name).append(": ").append(desc.typeName).append('\n');
        }
        QueryCommand.reportTestFailure(sb.toString());
    }

    private static boolean matchesExpected(@Nonnull final List<Map<?, ?>> expected,
                                           @Nonnull final List<ColumnDescriptor> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            final Map<?, ?> expectedCol = CustomYamlConstructor.LinedObject.unlineKeys(expected.get(i));
            final ColumnDescriptor actualCol = actual.get(i);
            final Map.Entry<?, ?> entry = expectedCol.entrySet().iterator().next();
            final String expectedName = entry.getKey().toString();
            final String expectedType = entry.getValue() == null ? null : entry.getValue().toString();
            if (!expectedName.equalsIgnoreCase(actualCol.name)) {
                return false;
            }
            if (expectedType != null && !expectedType.equalsIgnoreCase(actualCol.typeName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Descriptor for a single result-set column, capturing its name and SQL type name as reported by the driver.
     */
    public static final class ColumnDescriptor {
        @Nonnull
        public final String name;
        @Nonnull
        public final String typeName;

        ColumnDescriptor(@Nonnull final String name, @Nonnull final String typeName) {
            this.name = name;
            this.typeName = typeName;
        }
    }
}
