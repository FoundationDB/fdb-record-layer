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

import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.recordlayer.query.ParserUtils;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.collect.HashMultiset;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import de.vandermeer.asciitable.AsciiTable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    public static Object third(@Nonnull final List<?> obj) {
        if (obj.size() <= 2) {
            fail(String.format("Expecting %s to contain at least three elements, however it contains %s element", obj, obj.size()));
        }
        return obj.get(2);
    }

    public static String string(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof String) {
            // <NULL> should return null maybe?
            return (String) obj;
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s", desc, String.class.getSimpleName(), obj.getClass().getSimpleName()));
        return null;
    }

    public static boolean matches(@Nonnull final Object expected, @Nonnull final Object actual) {
        return Objects.equals(expected, actual);
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
        if (obj instanceof Integer) {
            return Long.valueOf((Integer) obj);
        }
        fail(String.format("Expecting %s to be of type %s, however it is of type %s", obj, Long.class.getSimpleName(), obj.getClass().getSimpleName()));
        return -1; // never reached.
    }

    public static boolean isInt(@Nonnull final Object obj) {
        return obj instanceof Integer;
    }

    public static int intValue(@Nonnull final Object obj) {
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

    @Nonnull
    public static Object keyOrValue(@Nonnull final Map.Entry<?, ?> entry) {
        if (isNull(entry.getKey()) && isNull(entry.getValue())) {
            fail(String.format("encountered YAML-style 'null' which is not supported, consider using '%s' instead", CustomTag.NullPlaceholder.INSTANCE));
        }
        return (entry.getValue() == null) ? entry.getKey() : entry.getValue();
    }

    @Nonnull
    public static Function<Integer, Object> valueByIndex(@Nonnull final RelationalResultSet resultSet) {
        return i -> {
            try {
                return resultSet.getObject(i);
            } catch (SQLException e) {
                fail(e.getMessage(), e);
            }
            return null;
        };
    }

    @Nonnull
    public static Function<Integer, Object> valueByIndex(@Nonnull final RelationalStruct resultSet) {
        return i -> {
            try {
                return resultSet.getObject(i);
            } catch (SQLException e) {
                fail(e.getMessage(), e);
            }
            return null;
        };
    }

    @Nonnull
    public static Function<String, Object> valueByName(@Nonnull final RelationalResultSet resultSet) {
        return i -> {
            try {
                return resultSet.getObject(i);
            } catch (SQLException e) {
                fail(e.getMessage(), e);
            }
            return null;
        };
    }

    @Nonnull
    public static Function<String, Object> valueByName(@Nonnull final RelationalStruct resultSet) {
        return i -> {
            try {
                return resultSet.getObject(i);
            } catch (SQLException e) {
                fail(e.getMessage(), e);
            }
            return null;
        };
    }

    public static final class ResultSetMatchResult {

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

    public static class ResultSetPrettyPrinter {

        @Nonnull
        private final List<List<String>> resultSet;

        public ResultSetPrettyPrinter() {
            this.resultSet = new ArrayList<>();
        }

        public void addCell(@Nullable final Object cell) {
            String cellString;
            if (cell == null) {
                cellString = "<NULL>";
            } else if (cell instanceof byte[]) {
                cellString = ByteArrayUtil2.loggable((byte[]) cell);
            } else {
                cellString = cell.toString();
            }
            resultSet.get(resultSet.size() - 1).add(cellString);
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
                at.addRow(row.stream().map(String::trim).collect(Collectors.toList()));
                at.addRule();
            }
            if (at.getRawContent().size() == 1) { //workaround for /0 bug in AsciiTable
                return "<EMPTY>";
            } else {
                return at.render();
            }
        }
    }

    public static ResultSetMatchResult matchResultSet(final Object expected, final RelationalResultSet actual, final boolean isExpectedOrdered) throws SQLException {
        final ResultSetPrettyPrinter resultSetPrettyPrinter = new ResultSetPrettyPrinter();

        if (expected instanceof CustomTag.Ignore) {
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
        if (expected instanceof CustomTag.StringContains) {
            return ((CustomTag.StringContains) expected).matchWith(actual, resultSetPrettyPrinter);
        }
        if (isMap(expected)) {
            if (!actual.next()) {
                return ResultSetMatchResult.fail("actual result set is empty", resultSetPrettyPrinter);
            }
            resultSetPrettyPrinter.newRow();
            final var expectedMap = notNull(map(expected), "expected result set");
            final var matchResult = matchMap(expectedMap, actual.getMetaData().getColumnCount(), valueByName(actual), valueByIndex(actual), resultSetPrettyPrinter, true);
            if (!matchResult.equals(ResultSetMatchResult.SUCCESS)) {
                printRemaining(actual, resultSetPrettyPrinter);
            }
        } else {
            if (!isArray(expected)) {
                printRemaining(actual, resultSetPrettyPrinter);
                return ResultSetMatchResult.fail("unknown format of expected result set", resultSetPrettyPrinter);
            }
            final var expectedAsList = arrayList(expected);
            if (isExpectedOrdered) {
                for (final var expectedRow : expectedAsList) {
                    if (!actual.next()) {
                        return ResultSetMatchResult.fail("actual result set is empty", resultSetPrettyPrinter);
                    }
                    resultSetPrettyPrinter.newRow();
                    if (!isMap(expectedRow)) { // I think it should be possible to expect a result set like: [[1,2,3], [4,5,6]]. But ok for now.
                        printRemaining(actual, resultSetPrettyPrinter);
                        return ResultSetMatchResult.fail("unknown format of expected result set", resultSetPrettyPrinter);
                    }
                    final var matchResult = matchMap(map(expectedRow), actual.getMetaData().getColumnCount(), valueByName(actual), valueByIndex(actual), resultSetPrettyPrinter, true);
                    if (!matchResult.equals(ResultSetMatchResult.success())) {
                        printRemaining(actual, resultSetPrettyPrinter);
                        return matchResult; // fail.
                    }
                }
            } else {
                // O(n^2) -- we all got M1s
                final var expectedAsMultiSet = HashMultiset.create(expectedAsList);

                while (actual.next()) {
                    boolean found = false;
                    for (final var expectedRow : expectedAsMultiSet) {
                        resultSetPrettyPrinter.newRow();
                        if (!isMap(expectedRow)) { // I think it should be possible to expect a result set like: [[1,2,3], [4,5,6]]. But ok for now.
                            printRemaining(actual, resultSetPrettyPrinter);
                            return ResultSetMatchResult.fail("unknown format of expected result set", resultSetPrettyPrinter);
                        }
                        final var matchResult = matchMap(map(expectedRow), actual.getMetaData().getColumnCount(), valueByName(actual), valueByIndex(actual), resultSetPrettyPrinter, true);
                        if (matchResult.equals(ResultSetMatchResult.success())) {
                            found = true;
                            expectedAsMultiSet.remove(expectedRow);
                            break;
                        }
                    }

                    if (!found) {
                        printRemaining(actual, resultSetPrettyPrinter);
                        return ResultSetMatchResult.fail("result row does not match any expected records", resultSetPrettyPrinter);
                    }
                }

                if (!expectedAsMultiSet.isEmpty()) {
                    return ResultSetMatchResult.fail("result does not contain all expected rows", resultSetPrettyPrinter);
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

    @Nonnull
    private static ResultSetMatchResult matchMap(@Nonnull final Map<?, ?> expected,
                                                 final int actualEntriesCount,
                                                 @Nonnull final Function<String, Object> entryByNameAccessor,
                                                 @Nonnull final Function<Integer, Object> entryByNumberAccessor,
                                                 @Nonnull final ResultSetPrettyPrinter printer,
                                                 boolean addCellToPrinter) throws SQLException {
        int counter = 1;
        final var expectedColCount = expected.entrySet().size();
        if (actualEntriesCount != expectedColCount) {
            if (addCellToPrinter) {
                for (int i = 1; i <= actualEntriesCount; i++) {
                    printer.addCell(entryByNumberAccessor.apply(i));
                }
            }
            return ResultSetMatchResult.fail(String.format("column mismatch! expected a row comprising %d column(s), received %d column(s) instead.", expectedColCount, actualEntriesCount), printer);
        }
        for (final var entry : expected.entrySet()) {
            final var expectedField = keyOrValue(entry);
            final var actualField = (entry.getValue() == null) ? entryByNumberAccessor.apply(counter) : entryByNameAccessor.apply(string(entry.getKey()));
            final var matchResult = matchField(expectedField, actualField, printer);
            if (!matchResult.equals(ResultSetMatchResult.success())) {
                if (addCellToPrinter) {
                    for (int i = counter; i <= actualEntriesCount; i++) {
                        printer.addCell(entryByNumberAccessor.apply(i));
                    }
                }
                return matchResult; // propagate failure.
            }
            if (addCellToPrinter) {
                printer.addCell(actualField);
            }
            counter++;
        }
        return ResultSetMatchResult.success();
    }

    private static ResultSetMatchResult matchField(@Nullable final Object expected,
                                                   @Nullable final Object actual,
                                                   @Nonnull final ResultSetPrettyPrinter printer) throws SQLException {
        // the test does not care about the incoming value.
        if (expected instanceof CustomTag.Ignore) {
            return ResultSetMatchResult.success();
        }
        final var expectedIsNull = expected instanceof CustomTag.NullPlaceholder;

        if (expectedIsNull && actual == null) {
            return ResultSetMatchResult.success();
        }
        if (expectedIsNull || actual == null) {
            if (expectedIsNull) {
                return ResultSetMatchResult.fail("actual result set is non-NULL, expecting NULL result set", printer);
            } else {
                return ResultSetMatchResult.fail("actual result set is NULL, expecting non-NULL result set", printer);
            }
        }
        if (expected instanceof CustomTag.NotNull) {
            // Actual value is not null, which is all the test cares about
            return ResultSetMatchResult.success();
        }
        if (expected instanceof CustomTag.StringContains) {
            return ((CustomTag.StringContains) expected).matchWith(actual, printer);
        }

        // (nested) message
        if (expected instanceof Map<?, ?>) {
            if (!(actual instanceof RelationalStruct)) {
                return ResultSetMatchResult.fail(String.format("cell mismatch at row %d! expected 🟢 to match a struct, got 🟡 instead.%n🟢 %s%n🟡 %s", printer.getRowCount(), expected, actual), printer);
            }
            final var struct = (RelationalStruct) (actual);
            return matchMap(map(expected), struct.getAttributes().length, valueByName(struct), valueByIndex(struct), printer, false);
        }

        // (nested) array
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
                if (isMap(expectedArray.get(i))) {
                    final var matchResult = matchMap(map(expectedArray.get(i)), actualArrayContent.getMetaData().getStructMetaData(2).getColumnCount(),
                            valueByName(actualArrayContent.getStruct(2)), valueByIndex(actualArrayContent.getStruct(2)), printer, false);
                    if (!matchResult.equals(ResultSetMatchResult.success())) {
                        return matchResult; // propagate failure.
                    }
                } else {
                    final var actualObject = actualArrayContent.getObject(2);
                    final var matchResult = matchField(expectedArray.get(i), actualObject, printer);
                    if (!matchResult.equals(ResultSetMatchResult.success())) {
                        return matchResult; // propagate failure.
                    }
                }
            }
            return ResultSetMatchResult.success();
        }

        // Enum comparison
        if (expected instanceof String && actual instanceof Descriptors.EnumValueDescriptor) {
            final var actualEnumDescriptor = (Descriptors.EnumValueDescriptor) actual;
            if (expected.equals(actualEnumDescriptor.getName())) {
                return ResultSetMatchResult.success();
            }
        }

        // integer comparison (with possible promotion)
        if (expected instanceof Integer) {
            return matchIntField((Integer) expected, actual, printer);
        }

        if (expected instanceof String && actual instanceof byte[]) {
            if (Objects.equals(expected, new String((byte[]) actual, StandardCharsets.UTF_8))) {
                return ResultSetMatchResult.success();
            } else if (((String) expected).toLowerCase(Locale.ROOT).startsWith("x'") && ((String) expected).endsWith("'") &&
                    Arrays.equals(ParserUtils.parseBytes((String) expected), (byte[]) actual)) {
                return ResultSetMatchResult.success();
            }
        }

        // exact comparison.
        if (Objects.equals(expected, actual)) {
            return ResultSetMatchResult.success();
        } else {
            return ResultSetMatchResult.fail(String.format("cell mismatch at row %d! expected 🟢 does not match 🟡.%n🟢 %s%n🟡 %s", printer.getRowCount(), expected, actual), printer);
        }
    }

    /**
     * Performs integer matching against integer, or against long (with promotion).
     * @param expected expected value.
     * @param actual actual value.
     * @return {@code true} if {@code expected} matches {@code actual}, otherwise {@code false}.
     */
    @Nonnull
    private static ResultSetMatchResult matchIntField(@Nonnull final Integer expected, @Nonnull final Object actual, @Nonnull final ResultSetPrettyPrinter printer) {
        if (actual instanceof Integer) {
            if (Objects.equals(expected, actual)) {
                return ResultSetMatchResult.success();
            }
        }
        if (actual instanceof Long) {
            if (Objects.equals(expected.longValue(), actual)) {
                return ResultSetMatchResult.success();
            }
        }
        return ResultSetMatchResult.fail(String.format("cell mismatch at row %d! expected 🟢 does not match 🟡.%n🟢 %s%n🟡 %s", printer.getRowCount(), expected, actual), printer);
    }
}
