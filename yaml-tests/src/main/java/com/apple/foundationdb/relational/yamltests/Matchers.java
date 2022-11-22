/*
 * Matchers.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.protobuf.Message;
import de.vandermeer.asciitable.AsciiTable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.fail;

public class Matchers {

    @Nonnull
    public static List<?> arrayList(@Nonnull final Object obj) {
        return arrayList(obj, obj.toString());
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static List<?> arrayList(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof List) {
            return (List<?>) obj;
        }
        fail(String.format("Expecting '%s' to be of type '%s'", desc, List.class.getSimpleName()));
        return null;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static Map<?, ?> map(@Nonnull final Object obj) {
        if (obj instanceof Map<?, ?>) {
            return (Map<?, ?>) obj;
        }
        fail(String.format("Expecting %s to be of type %s", obj, Map.class.getSimpleName()));
        return null;
    }

    public static Object first(@Nonnull final List<?> obj) {
        return first(obj, obj.toString());
    }

    public static Object first(@Nonnull final List<?> obj, @Nonnull final String desc) {
        if (obj.isEmpty()) {
            fail(String.format("Expecting %s to contain at least one element, however it is empty", desc));
        }
        return obj.get(0);
    }

    public static Object second(@Nonnull final List<?> obj) {
        if (obj.size() <= 1) {
            fail(String.format("Expecting %s to contain at least two elements, however it contains %s element", obj, obj.size()));
        }
        return obj.get(1);
    }

    public static String string(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof String) {
            // <NULL> should return null maybe?
            return (String) obj;
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s", desc, String.class.getSimpleName(), obj.getClass().getSimpleName()));
        return null;
    }

    public static void matches(@Nonnull final Object expected, @Nonnull final Object actual) {
        if (!Objects.equals(expected, actual)) {
            fail(String.format("expected to find '%s', however '%s' is found", expected, actual));
        }
    }

    public static String string(@Nonnull final Object obj) {
        return string(obj, obj.toString());
    }

    public static boolean isString(@Nonnull final Object obj) {
        return obj instanceof String;
    }

    public static boolean isBoolean(@Nonnull final Object obj) {
        return obj instanceof Boolean;
    }

    public static boolean bool(@Nonnull final Object obj) {
        if (obj instanceof Boolean) {
            // <NULL> should return null maybe?
            return (Boolean) obj;
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s", obj, Boolean.class.getSimpleName(), obj.getClass().getSimpleName()));
        return false; // never reached.
    }

    public static boolean isLong(@Nonnull final Object obj) {
        return obj instanceof Long;
    }

    public static long longValue(@Nonnull final Object obj) {
        if (obj instanceof Long) {
            return (Long) obj;
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s", obj, Long.class.getSimpleName(), obj.getClass().getSimpleName()));
        return -1; // never reached.
    }

    public static boolean isInt(@Nonnull final Object obj) {
        return obj instanceof Integer;
    }

    public static long intValue(@Nonnull final Object obj) {
        if (obj instanceof Integer) {
            return (Integer) obj;
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s", obj, Integer.class.getSimpleName(), obj.getClass().getSimpleName()));
        return -1; // never reached.
    }

    public static boolean isDouble(@Nonnull final Object object) {
        return object instanceof Double;
    }

    public static double doubleValue(@Nonnull final Object obj) {
        if (obj instanceof Double) {
            return (Double) obj;
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s", obj, Double.class.getSimpleName(), obj.getClass().getSimpleName()));
        return -1; // never reached.
    }

    public static boolean isArray(@Nonnull final Object object) {
        return object instanceof List;
    }

    public static boolean isMap(@Nonnull final Object obj) {
        return obj instanceof Map<?, ?>;
    }

    public static boolean isNull(@Nullable final Object obj) {
        return obj == null;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static Message message(@Nonnull final Object obj) {
        if (obj instanceof Message) {
            return (Message) obj;
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s.", obj, Message.class.getSimpleName(), obj.getClass().getSimpleName()));
        return null;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static <T> T notNull(@Nullable final T object, @Nonnull final String desc) {
        if (object == null) {
            fail(String.format("unexpected %s to be null", desc));
        }
        return object;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static Map.Entry<?, ?> firstEntry(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof Map) {
            return ((Map<?, ?>) obj).entrySet().iterator().next();
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s.", desc, Map.class.getSimpleName(), obj.getClass().getSimpleName()));
        return null;
    }

    static final class ResultSetMatchResult {

        private static final ResultSetMatchResult SUCCESS = new ResultSetMatchResult(true, null, null);

        private final boolean isSuccess;

        @Nullable
        private final String explanation;

        @Nullable
        private final ResultSetPrettyPrinter printer;

        private ResultSetMatchResult(final boolean isSuccess, @Nullable final String explanation, @Nullable ResultSetPrettyPrinter printer) {
            this.isSuccess = isSuccess;
            this.explanation = explanation;
            this.printer = printer;
        }

        @Nullable
        public String getExplanation() {
            return explanation;
        }

        @Nullable
        public ResultSetPrettyPrinter getResultSetPrinter() {
            return printer;
        }

        public boolean isSuccess() {
            return isSuccess;
        }

        public static ResultSetMatchResult success() {
            return SUCCESS;
        }

        public static ResultSetMatchResult fail(@Nonnull final String explanation, @Nonnull ResultSetPrettyPrinter printer) {
            return new ResultSetMatchResult(false, explanation, printer);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (!(obj instanceof ResultSetMatchResult)) {
                return false;
            }

            final var other = (ResultSetMatchResult) obj;

            return Objects.equals(isSuccess, other.isSuccess) && Objects.equals(printer, other.printer) && Objects.equals(explanation, other.explanation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(isSuccess, explanation, printer);
        }
    }

    static class ResultSetPrettyPrinter {

        @Nonnull
        private final List<List<String>> resultSet;

        ResultSetPrettyPrinter() {
            this.resultSet = new ArrayList<>();
        }

        public void addCell(@Nullable final Object cell) {
            resultSet.get(resultSet.size() - 1).add(cell == null ? "<NULL>" : cell.toString());
        }

        public void newRow() {
            resultSet.add(new ArrayList<>());
        }

        public int getRowCount() {
            return resultSet.size();
        }

        @Override
        public String toString() {
            if (resultSet.isEmpty()) {
                return "<EMPTY>";
            }
            final AsciiTable at = new AsciiTable();
            at.addRule();
            for (final var row : resultSet) {
                if (row.isEmpty()) {
                    // strange behavior from record-layer, sometimes it returns a result set with 0 columns
                    // I am not sure yet why, could it be related to NULL parsing semantics?
                    row.add("<EMPTY_ROW_RETURNED_FROM_RECORD_LAYER>");
                }
            }
            for (final var row : resultSet) {
                at.addRow(row);
                at.addRule();
            }
            if (at.getRawContent().size() == 1) { //workaround for /0 bug in AsciiTable
                return "<EMPTY>";
            } else {
                return at.render();
            }
        }
    }

    public static ResultSetMatchResult matchResultSet(final Object expected, final RelationalResultSet actual) throws SQLException {
        final ResultSetPrettyPrinter resultSetPrettyPrinter = new ResultSetPrettyPrinter();

        if (expected instanceof YamlRunner.DontCare) {
            return ResultSetMatchResult.success();
        }
        if (expected == null && actual == null) {
            return ResultSetMatchResult.success();
        }
        if (expected == null || actual == null) {
            if (expected == null) {
                return ResultSetMatchResult.fail("actual result set is non-NULL, expecting NULL result set", resultSetPrettyPrinter);
            } else {
                return ResultSetMatchResult.fail("actual result set is NULL, expecting non-NULL result set", resultSetPrettyPrinter);
            }
        }
        if (expected instanceof YamlRunner.StringContains) {
            return ((YamlRunner.StringContains) expected).matchWith(actual, resultSetPrettyPrinter);
        }
        if (isMap(expected)) {
            if (!actual.next()) {
                return ResultSetMatchResult.fail("actual result set is empty", resultSetPrettyPrinter);
            }
            resultSetPrettyPrinter.newRow();
            final var expectedMap = notNull(map(expected), "expected result set");
            return matchMessage(expectedMap, actual, resultSetPrettyPrinter);
        } else {
            if (!isArray(expected)) {
                printRemaining(actual, resultSetPrettyPrinter);
                return ResultSetMatchResult.fail("unknown format of expected result set", resultSetPrettyPrinter);
            }
            final var expectedArray = arrayList(expected);
            for (final var expectedRow : expectedArray) {
                if (!actual.next()) {
                    return ResultSetMatchResult.fail("actual result set is empty", resultSetPrettyPrinter);
                }
                resultSetPrettyPrinter.newRow();
                if (!isMap(expectedRow)) { // I think it should be possible to expect a result set like: [[1,2,3], [4,5,6]]. But ok for now.
                    printRemaining(actual, resultSetPrettyPrinter);
                    return ResultSetMatchResult.fail("unknown format of expected result set", resultSetPrettyPrinter);
                }
                final var expectedMap = map(expectedRow);
                final var matchResult = matchMessage(expectedMap, actual, resultSetPrettyPrinter);
                if (!matchResult.equals(ResultSetMatchResult.success())) {
                    printRemaining(actual, resultSetPrettyPrinter);
                    return matchResult; // fail.
                }
            }
        }
        final var expectedRowCount = resultSetPrettyPrinter.getRowCount();
        final boolean thereWasRemaining = printRemaining(actual, resultSetPrettyPrinter);
        if (thereWasRemaining) {
            final var actualRowCount = resultSetPrettyPrinter.getRowCount();
            return ResultSetMatchResult.fail(String.format("too many rows in actual result set! expected %d rows, got %d.", expectedRowCount, actualRowCount), resultSetPrettyPrinter);
        }
        return ResultSetMatchResult.success();
    }

    public static boolean printRemaining(@Nonnull final RelationalResultSet resultSet, @Nonnull final ResultSetPrettyPrinter printer) throws SQLException {
        boolean thereWasRemainingRows = false;
        final var colCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            thereWasRemainingRows = true;
            printer.newRow();
            for (int i = 1; i <= colCount; i++) {
                printer.addCell(resultSet.getObject(i));
            }
        }
        return thereWasRemainingRows;
    }

    private static void printRemainingCells(@Nonnull final RelationalResultSet resultSet, @Nonnull final ResultSetPrettyPrinter printer, int startIndex) throws SQLException {
        final var colCount = resultSet.getMetaData().getColumnCount();
        for (int i = startIndex; i <= colCount; i++) {
            printer.addCell(resultSet.getObject(i));
        }
    }

    private static ResultSetMatchResult matchMessage(@Nonnull final Map<?, ?> expected,
                                                     @Nonnull final RelationalResultSet currentRow,
                                                     @Nonnull final ResultSetPrettyPrinter resultSetPrettyPrinter) throws SQLException {
        int counter = 1;
        final var actualColCount = currentRow.getMetaData().getColumnCount();
        final var expectedColCount = expected.entrySet().size();
        if (actualColCount != expectedColCount) {
            printRemainingCells(currentRow, resultSetPrettyPrinter, counter);
            printRemaining(currentRow, resultSetPrettyPrinter);
            return ResultSetMatchResult.fail(String.format("column mismatch! expected a row comprising %d column(s), received %d column(s) instead.", expectedColCount, actualColCount), resultSetPrettyPrinter);
        }
        for (final var entry : expected.entrySet()) {
            if (entry.getValue() == null) { // unnamed, match by field number
                final var expectedField = entry.getKey();
                final var actualField = currentRow.getObject(counter);
                resultSetPrettyPrinter.addCell(actualField);
                final var matchResult = matchField(expectedField, actualField, resultSetPrettyPrinter);
                if (!matchResult.equals(ResultSetMatchResult.success())) {
                    printRemainingCells(currentRow, resultSetPrettyPrinter, counter + 1);
                    printRemaining(currentRow, resultSetPrettyPrinter);
                    return matchResult; // fail.
                }
            } else { // named field, check the name exists, then match values, maybe we should enable either-or (i.e. named XOR unnamed)
                final var fieldName = string(entry.getKey());
                final var expectedField = entry.getValue();
                final var actualField = currentRow.getObject(fieldName);
                resultSetPrettyPrinter.addCell(actualField);
                final var matchResult = matchField(expectedField, actualField, resultSetPrettyPrinter);
                if (!matchResult.equals(ResultSetMatchResult.success())) {
                    printRemainingCells(currentRow, resultSetPrettyPrinter, counter + 1);
                    printRemaining(currentRow, resultSetPrettyPrinter);
                    return matchResult; // fail.
                }
            }
            counter++;
        }
        return ResultSetMatchResult.success();
    }

    private static ResultSetMatchResult matchField(final Object expected,
                                                   final Object actual,
                                                   @Nonnull final ResultSetPrettyPrinter printer) throws SQLException {
        if (expected instanceof YamlRunner.DontCare) {
            return ResultSetMatchResult.success();
        }
        if (expected == null && actual == null) {
            return ResultSetMatchResult.success();
        }
        if (expected == null || actual == null) {
            if (expected == null) {
                return ResultSetMatchResult.fail("actual result set is non-NULL, expecting NULL result set", printer);
            } else {
                return ResultSetMatchResult.fail("actual result set is NULL, expecting non-NULL result set", printer);
            }
        }
        if (expected instanceof YamlRunner.StringContains) {
            return ((YamlRunner.StringContains) expected).matchWith(actual, printer);
        }

        if (expected instanceof Map<?, ?>) {
            final var expectedMap = map(expected);
            if (!(actual instanceof RelationalStruct)) {
                return ResultSetMatchResult.fail(String.format("cell mismatch at row %d! expected 🟢 to match a struct, got 🟡 instead.%n🟢 %s%n🟡 %s", printer.getRowCount(), expected, actual), printer);
            }
            final var struct = (RelationalStruct) (actual);
            int counter = 1;
            if (struct.getAttributes().length != expectedMap.size()) {
                return ResultSetMatchResult.fail(String.format("cell mismatch at row %d! expected 🟢 (containing %d fields) does not match 🟡 (containing %d fields).%n🟢 %s%n🟡 %s",
                        printer.getRowCount(), expectedMap.size(), ((RelationalStruct) actual).getAttributes().length, expected, actual), printer);
            }
            for (final var entry : expectedMap.entrySet()) {
                if (entry.getValue() == null) { // unnamed, match by field number
                    final var expectedField = entry.getKey();
                    final var actualField = struct.getObject(counter);
                    final var matchResult = matchField(expectedField, actualField, printer);
                    if (!matchResult.equals(ResultSetMatchResult.success())) {
                        return matchResult; // fail.
                    }
                } else { // named field, check the name exists, then match values.
                    final var fieldName = string(entry.getKey());
                    final var expectedField = entry.getValue();
                    final var actualField = struct.getObject(fieldName);
                    final var matchResult = matchField(expectedField, actualField, printer);
                    if (!matchResult.equals(ResultSetMatchResult.success())) {
                        return matchResult; // fail.
                    }
                }
                counter++;
            }
            return ResultSetMatchResult.success();
        }
        if (expected instanceof List<?>) {
            final var expectedArray = (List<?>) (expected);
            if (!(actual instanceof RelationalArray)) {
                return ResultSetMatchResult.fail(String.format("cell mismatch at row %d! expected 🟢 to match an array, got 🟡 instead.%n🟢 %s%n🟡 %s", printer.getRowCount(), expected, actual), printer);
            }
            final var actualArray = (RelationalArray) (actual);
            final var actualArrayContent = actualArray.getResultSet();
            for (int i = 0; i < expectedArray.size(); i++) {
                if (!actualArrayContent.next()) {
                    return ResultSetMatchResult.fail(String.format("cell mismatch at row %d! expected 🟢 (containing %d array items) does not match 🟡 (containing %d array items).%n🟢 %s%n🟡 %s",
                            printer.getRowCount(), expectedArray.size(), i, expected, actual), printer);
                }
                final var actualObject = actualArrayContent.getObject(i);
                final var matchResult = matchField(expectedArray.get(i), actualObject, printer);
                if (!matchResult.equals(ResultSetMatchResult.success())) {
                    return matchResult; // fail.
                }
            }
        }
        if (expected instanceof Integer && actual instanceof Long && actual.equals(((Integer) expected).longValue()) ||
                Objects.equals(expected, actual)) {
            return ResultSetMatchResult.success();
        } else {
            return ResultSetMatchResult.fail(String.format("cell mismatch at row %d! expected 🟢 does not match 🟡.%n🟢 %s%n🟡 %s", printer.getRowCount(), expected, actual), printer);
        }
    }
}
