/*
 * ResultSetPrettyPrinterTest.java
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

package com.apple.foundationdb.relational.yamltests;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResultSetPrettyPrinterTest {

    @Test
    void toStringOnEmptyResultSetReturnsEmptyMarker() {
        final var printer = new Matchers.ResultSetPrettyPrinter();

        assertThat(printer.toString()).isEqualTo("<EMPTY>");
    }

    @Test
    void toStringOnSingleRowRendersTableWithRules() {
        final var printer = new Matchers.ResultSetPrettyPrinter();
        printer.newRow();
        printer.addCell("a");
        printer.addCell("bb");

        assertThat(printer.toString()).isEqualTo("""
                +---+----+
                | a | bb |
                +---+----+
                """);
    }

    @Test
    void toStringOnMultipleRowsSizesColumnsToWidestCell() {
        final var printer = new Matchers.ResultSetPrettyPrinter();
        printer.newRow();
        printer.addCell("a");
        printer.addCell("y");
        printer.newRow();
        printer.addCell("aaaa");
        printer.addCell("y");

        assertThat(printer.toString()).isEqualTo("""
                +------+---+
                | a    | y |
                +------+---+
                | aaaa | y |
                +------+---+
                """);
    }

    @Test
    void toStringOnRaggedRowsPadsShorterRowsWithEmptyCells() {
        final var printer = new Matchers.ResultSetPrettyPrinter();
        printer.newRow();
        printer.addCell("aaa");
        printer.addCell("b");
        printer.newRow();
        printer.addCell("c");

        assertThat(printer.toString()).isEqualTo("""
                +-----+---+
                | aaa | b |
                +-----+---+
                | c   |   |
                +-----+---+
                """);
    }

    @Test
    void toStringOnCellWithLeadingOrTrailingWhitespacePreservesIt() {
        // Whitespace is preserved (not trimmed) so that whitespace-only mismatches between
        // expected and actual values remain visible when diagnosing a failed match.
        final var printer = new Matchers.ResultSetPrettyPrinter();
        printer.newRow();
        printer.addCell(" a ");
        printer.addCell("b");

        assertThat(printer.toString()).isEqualTo("""
                +-----+---+
                |  a  | b |
                +-----+---+
                """);
    }

    @Test
    void addCellOnNullValueRendersNullMarker() {
        final var printer = new Matchers.ResultSetPrettyPrinter();
        printer.newRow();
        printer.addCell(null);

        assertThat(printer.toString()).isEqualTo("""
                +--------+
                | <NULL> |
                +--------+
                """);
    }

    @Test
    void toStringOnEmptyRowRendersRecordLayerMarker() {
        final var printer = new Matchers.ResultSetPrettyPrinter();
        printer.newRow();

        assertThat(printer.toString()).isEqualTo("""
                +----------------------------------------+
                | <EMPTY_ROW_RETURNED_FROM_RECORD_LAYER> |
                +----------------------------------------+
                """);
    }

    @Test
    void getRowCountReturnsNumberOfRowsAdded() {
        final var printer = new Matchers.ResultSetPrettyPrinter();
        printer.newRow();
        printer.addCell("a");
        printer.newRow();
        printer.addCell("b");

        assertThat(printer.getRowCount()).isEqualTo(2);
    }
}
