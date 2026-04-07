/*
 * CheckResultMetadataConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;
import com.apple.foundationdb.relational.yamltests.YamlReference;
import com.apple.foundationdb.relational.yamltests.command.QueryCommand;
import com.apple.foundationdb.relational.yamltests.command.QueryConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
 *     The expected metadata is specified in the YAMSQL file as a list of column descriptors, each with optional
 *     {@code name} and {@code type} fields. Column names are compared case-insensitively. Either field may be
 *     omitted to skip checking that attribute for a given column.
 * </p>
 * <p>
 *     Example YAMSQL usage:
 *     <pre>
 *     - query: SELECT id, col1 FROM t1
 *       resultMetadata:
 *         - {name: ID, type: BIGINT}
 *         - {name: COL1, type: BIGINT}
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

    @Override
    @SuppressWarnings({"PMD.CloseResource", "unchecked"})
    protected void checkResultInternal(@Nonnull final String currentQuery, @Nonnull final Object actual,
                                       @Nonnull final String queryDescription,
                                       @Nonnull final List<String> setups) throws SQLException {
        if (!(actual instanceof RelationalResultSet)) {
            logger.warn("⚠️ resultMetadata check skipped: query returned a non-ResultSet result at {}", getReference());
            return;
        }
        try (RelationalResultSet rs = (RelationalResultSet) actual) {
            // Collect actual column metadata while the result set is open
            final RelationalResultSetMetaData meta = rs.getMetaData();
            final int actualColumnCount = meta.getColumnCount();
            final List<ColumnDescriptor> actualDescriptors = new ArrayList<>(actualColumnCount);
            for (int i = 1; i <= actualColumnCount; i++) {
                actualDescriptors.add(new ColumnDescriptor(meta.getColumnName(i), meta.getColumnTypeName(i)));
            }

            // Slurp remaining rows to fully release any server-side cursor resources
            //noinspection StatementWithEmptyBody
            while (rs.next()) {
                // consume
            }

            final Object val = getVal();
            final List<Map<?, ?>> expectedColumns = val == null ? List.of() : (List<Map<?, ?>>) val;

            if (executionContext.shouldCorrectResultMetadata()) {
                if (!matchesExpected(expectedColumns, actualDescriptors)) {
                    if (!executionContext.correctResultMetadata(getReference(), actualDescriptors)) {
                        QueryCommand.reportTestFailure("‼️ Cannot correct resultMetadata at " + getReference());
                    } else {
                        logger.debug(() -> "⭐️ Successfully corrected resultMetadata at " + getReference());
                    }
                }
                return;
            }

            logger.debug("⛳️ Checking result metadata for query '{}'", queryDescription);
            if (!matchesExpected(expectedColumns, actualDescriptors)) {
                final StringBuilder sb = new StringBuilder();
                sb.append("‼️ result metadata mismatch at ").append(getReference()).append(":\n")
                  .append("⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n")
                  .append("↪ expected columns (").append(expectedColumns.size()).append("):\n");
                for (Map<?, ?> col : expectedColumns) {
                    sb.append("    name=").append(getField(col, "name"))
                      .append(", type=").append(getField(col, "type")).append('\n');
                }
                sb.append("⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n")
                  .append("↩ actual columns (").append(actualColumnCount).append("):\n");
                for (ColumnDescriptor desc : actualDescriptors) {
                    sb.append("    name=").append(desc.name)
                      .append(", type=").append(desc.typeName).append('\n');
                }
                QueryCommand.reportTestFailure(sb.toString());
            } else {
                logger.debug("✅ result metadata matches!");
            }
        }
    }

    private static boolean matchesExpected(@Nonnull final List<Map<?, ?>> expected,
                                           @Nonnull final List<ColumnDescriptor> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            final Map<?, ?> expectedCol = expected.get(i);
            final ColumnDescriptor actualCol = actual.get(i);
            final String expectedName = getField(expectedCol, "name");
            final String expectedType = getField(expectedCol, "type");
            if (expectedName != null && !expectedName.equalsIgnoreCase(actualCol.name)) {
                return false;
            }
            if (expectedType != null && !expectedType.equalsIgnoreCase(actualCol.typeName)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extract a named field from a column descriptor map. Keys may be plain Strings or
     * {@link CustomYamlConstructor.LinedObject}-wrapped Strings.
     */
    @Nullable
    static String getField(@Nonnull final Map<?, ?> col, @Nonnull final String fieldName) {
        for (Map.Entry<?, ?> entry : col.entrySet()) {
            Object key = entry.getKey();
            if (key instanceof CustomYamlConstructor.LinedObject) {
                key = ((CustomYamlConstructor.LinedObject) key).getObject();
            }
            if (fieldName.equals(key)) {
                return entry.getValue() == null ? null : entry.getValue().toString();
            }
        }
        return null;
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
