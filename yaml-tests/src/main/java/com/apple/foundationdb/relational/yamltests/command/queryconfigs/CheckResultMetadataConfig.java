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

import com.apple.foundationdb.relational.api.ArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.StructMetaData;
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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
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
        return extractDescriptors((StructMetaData)metaData);
    }

    @Nonnull
    private static List<ColumnDescriptor> extractDescriptors(@Nonnull final StructMetaData metaData)
            throws SQLException {
        final int count = metaData.getColumnCount();
        final List<ColumnDescriptor> descriptors = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            final int type = metaData.getColumnType(i);
            if (type == Types.STRUCT) {
                final StructMetaData structMeta = metaData.getStructMetaData(i);
                descriptors.add(new ColumnDescriptor(metaData.getColumnName(i), "STRUCT",
                        structMeta.getTypeName(), extractDescriptors(structMeta), false));
            } else if (type == Types.ARRAY) {
                final ArrayMetaData arrayMeta = metaData.getArrayMetaData(i);
                if (arrayMeta.getElementType() == Types.STRUCT) {
                    final StructMetaData elemStructMeta = arrayMeta.getElementStructMetaData();
                    descriptors.add(new ColumnDescriptor(metaData.getColumnName(i), "ARRAY(STRUCT)",
                            elemStructMeta.getTypeName(), extractDescriptors(elemStructMeta), true));
                } else {
                    descriptors.add(new ColumnDescriptor(metaData.getColumnName(i),
                            buildArrayTypeName(arrayMeta)));
                }
            } else {
                descriptors.add(new ColumnDescriptor(metaData.getColumnName(i), metaData.getColumnTypeName(i)));
            }
        }
        return descriptors;
    }

    @Nonnull
    private static String buildArrayTypeName(@Nonnull final ArrayMetaData arrayMeta) throws SQLException {
        final String elementTypeName;
        if (arrayMeta.getElementType() == Types.ARRAY) {
            elementTypeName = buildArrayTypeName(arrayMeta.getElementArrayMetaData());
        } else {
            elementTypeName = arrayMeta.getElementTypeName();
        }
        return "ARRAY(" + elementTypeName + ")";
    }

    /**
     * Case-insensitive lookup of the {@code "array"} key in an {@code {array: ...}} map.
     * Returns the associated value, or {@code null} if no such key exists.
     */
    @Nullable
    private static Object getArrayValue(@Nonnull final Map<?, ?> map) {
        for (final Map.Entry<?, ?> e : map.entrySet()) {
            if (e.getKey() instanceof String && "array".equalsIgnoreCase((String)e.getKey())) {
                return e.getValue();
            }
        }
        return null;
    }

    /**
     * Converts an {@code {array: ...}} map (from the YAML expected metadata) into the equivalent SQL type-name
     * string so it can be compared against {@link ColumnDescriptor#typeName}.
     * <ul>
     *   <li>{@code {array: "INTEGER"}} → {@code "ARRAY(INTEGER)"}</li>
     *   <li>{@code {array: {array: "INTEGER"}}} → {@code "ARRAY(ARRAY(INTEGER))"}</li>
     * </ul>
     * The list case ({@code {array: [fields...]}}) is handled upstream in
     * {@link #matchesExpected} and never reaches this method.
     */
    @Nonnull
    private static String buildExpectedArrayTypeName(@Nonnull final Map<?, ?> arrayMap) {
        final Object arrayValue = getArrayValue(arrayMap);
        if (arrayValue instanceof String) {
            return "ARRAY(" + arrayValue + ")";
        } else if (arrayValue instanceof Map) {
            @SuppressWarnings("unchecked") final String inner = buildExpectedArrayTypeName((Map<?, ?>)arrayValue);
            return "ARRAY(" + inner + ")";
        } else {
            return "ARRAY(null)";
        }
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
        } catch (RuntimeException e) {
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
        try (RelationalResultSet rs = (RelationalResultSet)actual) {
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
        final Object rawExpectedValue = getVal();
        logger.debug("⛳️ Checking result metadata for query '{}'", queryDescription);

        // If the server returned no column metadata (e.g. an older server that omits metadata for empty result
        // sets), we cannot perform the check — skip with a warning rather than reporting a false mismatch.
        // A valid SELECT always produces ≥ 1 column descriptor; empty actual metadata means a driver limitation.
        // remove this workaround once all external server versions in multi-server test configs are updated
        // to a version that includes the TypeConversion.toProtobuf() fix (metadata always set before row
        // iteration, so empty result sets also carry column metadata).
        if (actualDescriptors.isEmpty() && (rawExpectedValue == null || !((List<?>)rawExpectedValue).isEmpty())) {
            logger.warn("⚠️ resultMetadata check skipped at {}: server returned no column metadata (possibly an older server version)", getReference());
            return;
        }

        // Synthetic config (rawExpectedValue == null) injected when OPTION_ADD_RESULT_METADATA is set:
        // write the actual metadata to the file without comparing.
        if (rawExpectedValue == null && executionContext.shouldAddResultMetadata()) {
            addResultMetadata(actualDescriptors);
            return;
        }

        final List<Map<?, ?>> expectedColumns = rawExpectedValue == null ? List.of() : (List<Map<?, ?>>)rawExpectedValue;

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

    private void addResultMetadata(@Nonnull final List<ColumnDescriptor> actualDescriptors) {
        if (!executionContext.addResultMetadata(getReference(), actualDescriptors)) {
            QueryCommand.reportTestFailure("‼️ Cannot add resultMetadata at " + getReference());
        } else {
            logger.debug(() -> "⭐️ Successfully added resultMetadata at " + getReference());
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
            appendDescriptorToMessage(sb, desc, "    ");
        }
        QueryCommand.reportTestFailure(sb.toString());
    }

    private static void appendDescriptorToMessage(@Nonnull final StringBuilder sb,
                                                  @Nonnull final ColumnDescriptor desc,
                                                  @Nonnull final String prefix) {
        sb.append(prefix).append(desc.name).append(": ").append(desc.typeName);
        if (desc.structTypeName != null) {
            sb.append('(').append(desc.structTypeName).append(')');
        }
        sb.append('\n');
        if (desc.fields != null) {
            for (final ColumnDescriptor field : desc.fields) {
                appendDescriptorToMessage(sb, field, prefix + "    ");
            }
        }
    }

    /**
     * Checks the optional struct type name prefix in a field list.
     * <p>
     * If the first element of {@code valueList} is a {@link String}, it is treated as the expected struct type name
     * and compared case-insensitively against {@code actualCol.structTypeName}.
     * Returns the tail of the list (field descriptors only) on a match, or {@code null} on a mismatch.
     * If the first element is not a {@link String} (i.e., no type name was specified), returns {@code valueList}
     * unchanged — the type name is not checked.
     * </p>
     */
    @Nullable
    private static List<?> checkStructTypeName(@Nonnull final List<?> valueList,
                                               @Nonnull final ColumnDescriptor actualCol) {
        if (!valueList.isEmpty() && valueList.get(0) instanceof String) {
            final String expectedTypeName = (String)valueList.get(0);
            if (actualCol.structTypeName == null || !expectedTypeName.equalsIgnoreCase(actualCol.structTypeName)) {
                return null;
            }
            return valueList.subList(1, valueList.size());
        }
        return valueList;
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
            if (!expectedName.equalsIgnoreCase(actualCol.name)) {
                return false;
            }
            if (entry.getValue() instanceof List) {
                // plain struct column: value is [{field: type}, ...] or [struct_name, {field: type}, ...]
                if (actualCol.fields == null || actualCol.isArray) {
                    return false;
                }
                @SuppressWarnings("unchecked")
                List<?> valueList = (List<?>)entry.getValue();
                valueList = checkStructTypeName(valueList, actualCol);
                if (valueList == null) {
                    return false;
                }
                @SuppressWarnings("unchecked") final List<Map<?, ?>> fieldMaps = (List<Map<?, ?>>)valueList;
                if (!matchesExpected(fieldMaps, actualCol.fields)) {
                    return false;
                }
            } else if (entry.getValue() instanceof Map) {
                // {array: VALUE} — array type (primitive or struct)
                @SuppressWarnings("unchecked") final Map<?, ?> arrayMap = (Map<?, ?>)entry.getValue();
                final Object arrayValue = getArrayValue(arrayMap);
                if (arrayValue instanceof List) {
                    // {array: [fields...]} — array of struct
                    if (actualCol.fields == null || !actualCol.isArray) {
                        return false;
                    }
                    @SuppressWarnings("unchecked")
                    List<?> valueList = (List<?>)arrayValue;
                    valueList = checkStructTypeName(valueList, actualCol);
                    if (valueList == null) {
                        return false;
                    }
                    @SuppressWarnings("unchecked") final List<Map<?, ?>> fieldMaps = (List<Map<?, ?>>)valueList;
                    if (!matchesExpected(fieldMaps, actualCol.fields)) {
                        return false;
                    }
                } else {
                    // {array: typeName} or {array: {array: typeName}} — primitive/nested array
                    if (actualCol.fields != null) {
                        return false;
                    }
                    final String expectedType = buildExpectedArrayTypeName(arrayMap);
                    if (!expectedType.equalsIgnoreCase(actualCol.typeName)) {
                        return false;
                    }
                }
            } else {
                // scalar column: value must be a type name string
                if (!(entry.getValue() instanceof String)) {
                    return false;
                }
                if (!((String)entry.getValue()).equalsIgnoreCase(actualCol.typeName)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Descriptor for a single result-set column, capturing its name, SQL type name, and (for struct
     * and array-of-struct columns) the nested field descriptors.
     */
    public static final class ColumnDescriptor {
        @Nonnull
        public final String name;
        /**
         * SQL type name: e.g. {@code "BIGINT"}, {@code "STRUCT"}, {@code "ARRAY(INTEGER)"},
         * {@code "ARRAY(STRUCT)"}.
         */
        @Nonnull
        public final String typeName;
        /**
         * Declared struct type name (e.g. {@code "point"} from {@code CREATE TYPE AS STRUCT point(...)}).
         * Non-null for struct and array-of-struct columns; {@code null} for scalars and array-of-scalar columns.
         */
        @Nullable
        public final String structTypeName;
        /**
         * Non-null for struct and array-of-struct columns; contains the struct's field descriptors
         * (or the element struct's field descriptors when {@link #isArray} is {@code true}).
         */
        @Nullable
        public final List<ColumnDescriptor> fields;
        /**
         * {@code true} when this column is an array whose element type is a struct;
         * {@code false} for plain struct columns and all scalar/array-of-scalar columns.
         */
        public final boolean isArray;

        ColumnDescriptor(@Nonnull final String name, @Nonnull final String typeName,
                         @Nullable final String structTypeName,
                         @Nonnull final List<ColumnDescriptor> fields, boolean isArray) {
            this.name = name;
            this.typeName = typeName;
            this.structTypeName = structTypeName;
            this.fields = Collections.unmodifiableList(fields);
            this.isArray = isArray;
        }

        ColumnDescriptor(@Nonnull final String name, @Nonnull final String typeName) {
            this.name = name;
            this.typeName = typeName;
            this.structTypeName = null;
            this.fields = null;
            this.isArray = false;
        }
    }
}
