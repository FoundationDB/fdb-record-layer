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

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.util.pair.ImmutablePair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.tags.PosTag;
import com.apple.foundationdb.relational.yamltests.tags.IgnoreTag;
import com.apple.foundationdb.relational.yamltests.tags.Matchable;
import com.apple.foundationdb.relational.yamltests.tags.IsNullTag;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.recordlayer.query.ParseHelpers;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import de.vandermeer.asciitable.AsciiTable;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.SequenceNode;

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
        fail(String.format(Locale.ROOT, "Expecting '%s' to be of type '%s'", desc, List.class.getSimpleName()));
        return null;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static Map<?, ?> map(@Nonnull final Object obj) {
        return map(obj, obj.toString());
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static Map<?, ?> map(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof Map<?, ?>) {
            return (Map<?, ?>) obj;
        }
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s", desc, Map.class.getSimpleName()));
        return null;
    }

    public static Map.Entry<?, ?> mapEntry(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof Map.Entry<?, ?>) {
            return (Map.Entry<?, ?>) obj;
        }
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s", desc, Map.Entry.class.getSimpleName()));
        return null;
    }

    public static Object first(@Nonnull final List<?> obj) {
        return first(obj, obj.toString());
    }

    public static Object first(@Nonnull final List<?> obj, @Nonnull final String desc) {
        if (obj.isEmpty()) {
            fail(String.format(Locale.ROOT, "Expecting %s to contain at least one element, however it is empty", desc));
        }
        return obj.get(0);
    }

    public static Object second(@Nonnull final List<?> obj) {
        if (obj.size() <= 1) {
            fail(String.format(Locale.ROOT, "Expecting %s to contain at least two elements, however it contains %s element", obj, obj.size()));
        }
        return obj.get(1);
    }

    public static Object third(@Nonnull final List<?> obj) {
        if (obj.size() <= 2) {
            fail(String.format(Locale.ROOT, "Expecting %s to contain at least three elements, however it contains %s element", obj, obj.size()));
        }
        return obj.get(2);
    }

    public static String string(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof String) {
            // <NULL> should return null maybe?
            return (String) obj;
        }
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s, however it is of type %s", desc, String.class.getSimpleName(), obj.getClass().getSimpleName()));
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
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s, however it is of type %s", obj, Boolean.class.getSimpleName(), obj.getClass().getSimpleName()));
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
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s, however it is of type %s", obj, Long.class.getSimpleName(), obj.getClass().getSimpleName()));
        return -1; // never reached.
    }

    public static boolean isInt(@Nonnull final Object obj) {
        return obj instanceof Integer;
    }

    public static int intValue(@Nonnull final Object obj) {
        if (obj instanceof Integer) {
            return (Integer) obj;
        }
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s, however it is of type %s", obj, Integer.class.getSimpleName(), obj.getClass().getSimpleName()));
        return -1; // never reached.
    }

    public static boolean isDouble(@Nonnull final Object object) {
        return object instanceof Double;
    }

    public static double doubleValue(@Nonnull final Object obj) {
        if (obj instanceof Double) {
            return (Double) obj;
        }
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s, however it is of type %s", obj, Double.class.getSimpleName(), obj.getClass().getSimpleName()));
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
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s, however it is of type %s.", obj, Message.class.getSimpleName(), obj.getClass().getSimpleName()));
        return null;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static <T> T notNull(@Nullable final T object, @Nonnull final String desc) {
        if (object == null) {
            fail(String.format(Locale.ROOT, "unexpected %s to be null", desc));
        }
        return object;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static Map.Entry<?, ?> firstEntry(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof Map) {
            return ((Map<?, ?>) obj).entrySet().iterator().next();
        }
        fail(String.format(Locale.ROOT, "Expecting %s to be of type %s, however it is of type %s.", desc, Map.class.getSimpleName(), obj.getClass().getSimpleName()));
        return null;
    }

    @SpotBugsSuppressWarnings(value = "NP_NONNULL_RETURN_VIOLATION", justification = "should never happen, fail throws")
    @Nonnull
    public static Map.Entry<?, ?> onlyEntry(@Nonnull final Object obj, @Nonnull final String desc) {
        if (obj instanceof Map) {
            final var map = ((Map<?, ?>) obj);
            if (map.size() != 1) {
                fail(String.format(Locale.ROOT, "Expecting map %s to have a single element, however it has %s elements.", desc, map.size()));
            }
            return ((Map<?, ?>) obj).entrySet().iterator().next();
        }
        return fail(String.format(Locale.ROOT, "Expecting %s to be of type %s, however it is of type %s.", desc, Map.class.getSimpleName(), obj.getClass().getSimpleName()));
    }

    @Nonnull
    public static Object valueElseKey(@Nonnull final Map.Entry<?, ?> entry) {
        if (isNull(entry.getKey()) && isNull(entry.getValue())) {
            fail(String.format(Locale.ROOT, "encountered YAML-style 'null' which is not supported, consider using '%s' instead", IsNullTag.usage()));
        }
        return entry.getValue() == null ? entry.getKey() : entry.getValue();
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

        private static final ResultSetMatchResult SUCCESS = new ResultSetMatchResult(true, null);

        private final boolean isSuccess;

        @Nullable
        private final String explanation;

        private ResultSetMatchResult(final boolean isSuccess, @Nullable final String explanation) {
            this.isSuccess = isSuccess;
            this.explanation = explanation;
        }

        @Nullable
        public String getExplanation() {
            return explanation;
        }

        public boolean isSuccess() {
            return isSuccess;
        }

        public static ResultSetMatchResult success() {
            return SUCCESS;
        }

        public static ResultSetMatchResult fail(@Nonnull final String explanation) {
            return new ResultSetMatchResult(false, explanation);
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

            return Objects.equals(isSuccess, other.isSuccess) && Objects.equals(explanation, other.explanation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(isSuccess, explanation);
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

    public static Pair<ResultSetMatchResult, ResultSetPrettyPrinter> matchResultSet(final Object expected, final RelationalResultSet actual, final boolean isExpectedOrdered) throws SQLException {
        if (expected instanceof IgnoreTag) {
            return ImmutablePair.of(ResultSetMatchResult.success(), null);
        }
        if (expected == null) {
            if (actual == null) {
                return ImmutablePair.of(ResultSetMatchResult.success(), null);
            } else {
                return ImmutablePair.of(ResultSetMatchResult.fail("actual result set is non-NULL, expecting NULL result set"), null);
            }
        }
        if (actual == null) {
            return ImmutablePair.of(ResultSetMatchResult.fail("actual result set is NULL, expecting non-NULL result set"), null);
        }
        if (!isArray(expected)) {
            final ResultSetPrettyPrinter resultSetPrettyPrinter = new ResultSetPrettyPrinter();
            printRemaining(actual, resultSetPrettyPrinter);
            return ImmutablePair.of(ResultSetMatchResult.fail("unknown format of expected result set"), resultSetPrettyPrinter);
        }
        if (isExpectedOrdered) {
            return matchOrderedResultSet(actual, arrayList(expected));
        } else {
            return matchUnorderedResultSet(actual, HashMultiset.create(arrayList(expected)));
        }
    }

    private static ImmutablePair<ResultSetMatchResult, ResultSetPrettyPrinter> matchOrderedResultSet(final RelationalResultSet actual, final List<?> expectedAsList) throws SQLException {
        final ResultSetPrettyPrinter resultSetPrettyPrinter = new ResultSetPrettyPrinter();
        var i = 1;
        for (final var expectedRow : expectedAsList) {
            if (!actual.next()) {
                printCurrentAndRemaining(actual, resultSetPrettyPrinter);
                return ImmutablePair.of(ResultSetMatchResult.fail(String.format(Locale.ROOT, "result does not contain all expected rows! expected %d rows, got %d", expectedAsList.size(), i - 1)), null);
            }
            final var matchResult = matchRow(expectedRow, actual.getMetaData().getColumnCount(), valueByName(actual), valueByIndex(actual), i++);
            if (!matchResult.equals(ResultSetMatchResult.success())) {
                printCurrentAndRemaining(actual, resultSetPrettyPrinter);
                return ImmutablePair.of(matchResult, resultSetPrettyPrinter); // fail.
            }
        }
        if (printRemaining(actual, resultSetPrettyPrinter)) {
            final var leftActualRowCount = resultSetPrettyPrinter.getRowCount();
            return ImmutablePair.of(ResultSetMatchResult.fail(String.format(Locale.ROOT, "too many rows in actual result set! expected %d rows, got %d.", expectedAsList.size(), expectedAsList.size() + leftActualRowCount)), resultSetPrettyPrinter);
        }
        return ImmutablePair.of(ResultSetMatchResult.success(), null);
    }

    private static ImmutablePair<ResultSetMatchResult, ResultSetPrettyPrinter> matchUnorderedResultSet(final RelationalResultSet actual, final @Nonnull Multiset<?> expectedAsMultiSet) throws SQLException {
        final ResultSetPrettyPrinter resultSetPrettyPrinter = new ResultSetPrettyPrinter();
        final var expectedRowCount = expectedAsMultiSet.size();
        var actualRowsCounter = 1;
        while (actual.next()) {
            boolean found = false;
            if (expectedAsMultiSet.isEmpty()) {
                printCurrentAndRemaining(actual, resultSetPrettyPrinter);
                // count the remaining actual rows.
                while (actual.next()) {
                    actualRowsCounter++;
                }
                return ImmutablePair.of(ResultSetMatchResult.fail(String.format(Locale.ROOT, "too many rows in actual result set! expected %d row(s), got %d row(s) instead.", expectedRowCount, actualRowsCounter - 1)), resultSetPrettyPrinter);
            }
            for (final var expectedRow : expectedAsMultiSet.elementSet()) {
                final var matchResult = matchRow(expectedRow, actual.getMetaData().getColumnCount(), valueByName(actual), valueByIndex(actual), actualRowsCounter);
                if (matchResult.equals(ResultSetMatchResult.success())) {
                    found = true;
                    expectedAsMultiSet.remove(expectedRow);
                    break;
                }
            }
            if (!found) {
                printCurrentAndRemaining(actual, resultSetPrettyPrinter);
                return ImmutablePair.of(ResultSetMatchResult.fail(String.format(Locale.ROOT, "actual row at %d does not match any expected records", actualRowsCounter)), resultSetPrettyPrinter);
            }
            actualRowsCounter++;
        }
        if (!expectedAsMultiSet.isEmpty()) {
            return ImmutablePair.of(ResultSetMatchResult.fail(String.format(Locale.ROOT, "result does not contain all expected rows, expected %d row(s), got %d row(s) instead.", expectedRowCount, actualRowsCounter - 1)), null);
        }
        return ImmutablePair.of(ResultSetMatchResult.success(), null);
    }

    public static void printCurrentAndRemaining(@Nonnull final RelationalResultSet resultSet, @Nonnull final ResultSetPrettyPrinter printer) throws SQLException {
        final var colCount = resultSet.getMetaData().getColumnCount();
        printer.newRow();
        for (int i = 1; i <= colCount; i++) {
            printer.addCell(resultSet.getObject(i));
        }
        printRemaining(resultSet, printer);
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
    private static ResultSetMatchResult matchRow(@Nonnull final Object expected,
                                                 final int actualEntriesCount,
                                                 @Nonnull final Function<String, Object> entryByNameAccessor,
                                                 @Nonnull final Function<Integer, Object> entryByNumberAccessor,
                                                 int rowNumber) throws SQLException {
        if (expected instanceof Map) {
            return matchMap((Map<?, ?>)expected, actualEntriesCount, entryByNameAccessor, entryByNumberAccessor, rowNumber, "");
        }
        if (expected instanceof List) {
            return matchArray((List<?>)expected, actualEntriesCount, entryByNumberAccessor, rowNumber, "");
        }
        return ResultSetMatchResult.fail(String.format(Locale.ROOT, "Expecting %s to be of type Map or List, however it is of type %s", expected.toString(), expected.getClass().getSimpleName()));
    }

    @Nonnull
    private static ResultSetMatchResult matchArray(@Nonnull final List<?> expected,
                                                 final int actualEntriesCount,
                                                 @Nonnull final Function<Integer, Object> entryByNumberAccessor,
                                                 int rowNumber,
                                                 @Nonnull String cellRef) throws SQLException {
        final var expectedColCount = expected.size();
        if (actualEntriesCount != expectedColCount) {
            return ResultSetMatchResult.fail(String.format(Locale.ROOT, "! expected a row comprising %d column(s), received %d column(s) instead.", expectedColCount, actualEntriesCount));
        }
        int counter = 1;
        for (final var expectedField : expected) {
            final var actualField = entryByNumberAccessor.apply(counter);
            final var currentCellRef = "pos<" + counter + ">";
            final var matchResult = matchField(expectedField, actualField, rowNumber, cellRef + (cellRef.isEmpty() ? "" : ".") + currentCellRef);
            if (!matchResult.equals(ResultSetMatchResult.success())) {
                return matchResult; // propagate failure.
            }
            counter++;
        }
        return ResultSetMatchResult.success();
    }

    @Nonnull
    private static ResultSetMatchResult matchMap(@Nonnull final Map<?, ?> expected,
                                                 final int actualEntriesCount,
                                                 @Nonnull final Function<String, Object> entryByNameAccessor,
                                                 @Nonnull final Function<Integer, Object> entryByNumberAccessor,
                                                 int rowNumber,
                                                 @Nonnull String cellRef) throws SQLException {
        int counter = 1;
        final var expectedColCount = expected.entrySet().size();
        if (actualEntriesCount != expectedColCount) {
            return ResultSetMatchResult.fail(String.format(Locale.ROOT, "! expected a row comprising %d column(s), received %d column(s) instead.", expectedColCount, actualEntriesCount));
        }
        for (final var entry : expected.entrySet()) {
            final var expectedField = valueElseKey(entry);
            final var isUserDefinedColPos = entry.getKey() instanceof PosTag.ColumnPosition;
            final var effectiveColumnPos = isUserDefinedColPos ? ((PosTag.ColumnPosition)entry.getKey()).getValue() : counter;
            final var actualField = entry.getValue() == null || isUserDefinedColPos ?
                                    entryByNumberAccessor.apply(effectiveColumnPos) : entryByNameAccessor.apply(string(entry.getKey()));
            final var currentCellRef = entry.getValue() == null ? "pos<" + effectiveColumnPos + ">" : entry.getKey().toString();
            final var matchResult = matchField(expectedField, actualField, rowNumber, cellRef + (cellRef.isEmpty() ? "" : ".") + currentCellRef);
            if (!matchResult.equals(ResultSetMatchResult.success())) {
                return matchResult; // propagate failure.
            }
            counter++;
        }
        return ResultSetMatchResult.success();
    }

    private static ResultSetMatchResult matchField(@Nullable final Object expected,
                                                   @Nullable final Object actual,
                                                   int rowNumber,
                                                   @Nonnull String cellRef) throws SQLException {
        if (expected instanceof IgnoreTag.IgnoreMatcher) {
            return ResultSetMatchResult.success();
        }

        if (expected == null && actual == null) {
            return ResultSetMatchResult.success();
        }
        if (expected == null) {
            return ResultSetMatchResult.fail("actual result set is non-NULL, expecting NULL result set");
        }
        if (!(expected instanceof IsNullTag.IsNullMatcher) && actual == null) {
            return ResultSetMatchResult.fail("actual result set is NULL, expecting non-NULL result set");
        }

        if (expected instanceof Matchable) {
            return ((Matchable)expected).matches(actual, rowNumber, cellRef);
        }

        // (nested) message
        if (expected instanceof Map<?, ?>) {
            if (!(actual instanceof RelationalStruct)) {
                return ResultSetMatchResult.fail(String.format(Locale.ROOT, "cell mismatch at row: %d cellRef: %s%n expected 🟢 to match a struct, got 🟡 instead.%n🟢 %s (Struct)%n🟡 %s (%s)", rowNumber, cellRef, expected, actual, actual.getClass().getSimpleName()));
            }
            final var struct = (RelationalStruct) (actual);
            return matchMap(map(expected), struct.getAttributes().length, valueByName(struct), valueByIndex(struct), rowNumber, cellRef);
        }

        // (nested) array
        if (expected instanceof List<?>) {
            final var expectedArray = (List<?>) (expected);
            if (!(actual instanceof RelationalArray)) {
                return ResultSetMatchResult.fail(String.format(Locale.ROOT, "cell mismatch at row: %d cellRef: %s%n expected 🟢 to match an array, got 🟡 instead.%n🟢 %s (Array)%n🟡 %s (%s)", rowNumber, cellRef, expected, actual, actual.getClass().getSimpleName()));
            }
            final var actualArray = (RelationalArray) (actual);
            final var actualArrayContent = actualArray.getResultSet();
            for (int i = 0; i < expectedArray.size(); i++) {
                if (!actualArrayContent.next()) {
                    return ResultSetMatchResult.fail(String.format(Locale.ROOT, "cell mismatch at row: %d cellRef: %s%n expected 🟢 (containing %d array items) does not match 🟡 (containing %d array items).%n🟢 %s%n🟡 %s",
                            rowNumber, cellRef, expectedArray.size(), i, expected, actual));
                }
                if (isMap(expectedArray.get(i))) {
                    final var matchResult = matchMap(map(expectedArray.get(i)), actualArrayContent.getMetaData().getStructMetaData(2).getColumnCount(),
                            valueByName(actualArrayContent.getStruct(2)), valueByIndex(actualArrayContent.getStruct(2)), rowNumber, cellRef + "[" + i + "]");
                    if (!matchResult.equals(ResultSetMatchResult.success())) {
                        return matchResult; // propagate failure.
                    }
                } else {
                    final var actualObject = actualArrayContent.getObject(2);
                    final var matchResult = matchField(expectedArray.get(i), actualObject, rowNumber, cellRef + "[" + i + "]");
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
            return matchIntField((Integer) expected, actual, rowNumber, cellRef);
        }

        if (expected instanceof String && actual instanceof byte[]) {
            if (Objects.equals(expected, new String((byte[]) actual, StandardCharsets.UTF_8))) {
                return ResultSetMatchResult.success();
            } else if (((String) expected).toLowerCase(Locale.ROOT).startsWith("xstartswith_") && ((String) expected).endsWith("'")) {
                byte[] parsedByteArray = ParseHelpers.parseBytes((String) expected);
                byte[] actualByteArray = (byte[]) actual;
                int index1 = ((String) expected).indexOf("_");
                int index2 = ((String) expected).indexOf("'");
                int expectedByteArrayLength = Integer.parseInt(((String) expected).substring(index1 + 1, index2));
                if (actualByteArray.length == expectedByteArrayLength && parsedByteArray.length <= actualByteArray.length && Arrays.equals(Arrays.copyOf(parsedByteArray, actualByteArray.length), actualByteArray)) {
                    return ResultSetMatchResult.success();
                }
            } else if (((String) expected).toLowerCase(Locale.ROOT).startsWith("x'") && ((String) expected).endsWith("'") &&
                    Arrays.equals(ParseHelpers.parseBytes((String) expected), (byte[]) actual)) {
                return ResultSetMatchResult.success();
            }
        }

        // exact comparison.
        if (Objects.equals(expected, actual)) {
            return ResultSetMatchResult.success();
        } else {
            return ResultSetMatchResult.fail(String.format(Locale.ROOT, "cell mismatch at row: %d cellRef: %s%n expected 🟢 does not match 🟡.%n🟢 %s (%s)%n🟡 %s (%s)", rowNumber, cellRef, expected, expected.getClass().getSimpleName(), actual, actual.getClass().getSimpleName()));
        }
    }

    /**
     * Performs integer matching against integer, or against long (with promotion).
     * @param expected expected value.
     * @param actual actual value.
     * @return {@code true} if {@code expected} matches {@code actual}, otherwise {@code false}.
     */
    @Nonnull
    private static ResultSetMatchResult matchIntField(@Nonnull final Integer expected, @Nonnull final Object actual, int rowNumber, @Nonnull String cellRef) {
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
        return ResultSetMatchResult.fail(String.format(Locale.ROOT, "cell mismatch at row: %d cellRef: %s%n expected 🟢 does not match 🟡.%n🟢 %s (Integer) %n🟡 %s (%s)", rowNumber, cellRef, expected, actual, actual.getClass().getSimpleName()));
    }

    @Nonnull
    public static RealVector constructVectorFromString(int precision, @Nonnull final SequenceNode yamlElementsNode) {
        final var elements = yamlElementsNode.getValue().stream().map(v -> Assert.castUnchecked(v, ScalarNode.class).getValue())
                .collect(ImmutableList.toImmutableList());

        // Handle empty vector case
        if (elements.isEmpty()) {
            switch (precision) {
                case 16:
                    return new HalfRealVector(new double[0]);
                case 32:
                    return new FloatRealVector(new double[0]);
                case 64:
                    return new DoubleRealVector(new double[0]);
                default:
                    throw new IllegalArgumentException("Unsupported vector precision: " + precision + ". Expected 16, 32, or 64.");
            }
        }

        // Split by comma and parse each element as double
        final double[] values = elements.stream().map(Double::parseDouble).mapToDouble(d -> d).toArray();

        // Create appropriate vector based on precision
        switch (precision) {
            case 16:
                return new HalfRealVector(values);
            case 32:
                return new FloatRealVector(values);
            case 64:
                return new DoubleRealVector(values);
            default:
                throw new IllegalArgumentException("Unsupported vector precision: " + precision + ". Expected 16, 32, or 64.");
        }
    }
}
