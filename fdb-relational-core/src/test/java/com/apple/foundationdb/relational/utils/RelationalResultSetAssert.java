/*
 * RelationalResultSetAssert.java
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;

import com.google.protobuf.Message;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.MapAssert;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RelationalResultSetAssert extends AbstractAssert<RelationalResultSetAssert, RelationalResultSet> {
    protected RelationalResultSetAssert(RelationalResultSet relationalResultSet) {
        super(relationalResultSet, RelationalResultSetAssert.class);
    }

    public RelationalResultSetAssert nextRowMatches(Row expectedNextRow) {
        isNotNull();
        //make sure that there is at least one row
        hasNextRow();
        extracting(rs -> {
            try {
                return ResultSetTestUtils.currentRow(rs);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, RowAssert::new).isEqualTo(expectedNextRow);

        return this;
    }

    public RelationalResultSetAssert nextRowMatches(Map<String, Object> theExpectedRow) {
        hasNextRow().extracting(relationalResultSet -> {
            Map<String, Object> theRowMap = new HashMap<>();
            try {
                for (String expectedKey : theExpectedRow.keySet()) {
                    theRowMap.put(expectedKey, relationalResultSet.getObject(expectedKey));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return theRowMap;
        }, MapAssert::new).containsAllEntriesOf(theExpectedRow);

        return this;
    }

    public RelationalResultSetAssert hasExactly(Row theExpectedRow) {
        isNotNull();
        return nextRowMatches(theExpectedRow).hasNoNextRow();
    }

    public RelationalResultSetAssert hasExactly(Iterable<Row> theExpectedRow) {
        isNotNull();
        for (Row row : theExpectedRow) {
            nextRowMatches(row);
        }
        return hasNoNextRow();
    }

    public RelationalResultSetAssert hasExactly(Map<String, Object> theExpectedRow) {
        return isNotNull().nextRowMatches(theExpectedRow).hasNoNextRow();
    }

    public RelationalResultSetAssert hasNextRow() {
        extracting(this::advance, Assertions::assertThat).isTrue();
        return this;
    }

    public RelationalResultSetAssert hasNoNextRow() {
        extracting(this::advance, Assertions::assertThat).isFalse();
        return this;
    }

    public RelationalResultSetAssert hasExactlyInAnyOrder(Collection<Row> expectedRows) throws SQLException, RelationalException {
        Collection<Row> actualRows = new ArrayList<>(expectedRows.size());
        while (actual.next()) {
            actualRows.add(ResultSetTestUtils.currentRow(actual));
        }

        Assertions.assertThat(actualRows.size()).describedAs("Row size").isEqualTo(expectedRows.size());

        for (Row expectedRow : expectedRows) {
            boolean found = false;
            for (Row actualRow : actualRows) {
                if (expectedRow.equals(actualRow)) {
                    found = true;
                    break;
                }
            }
            Assertions.assertThat(found).describedAs("Row %s", expectedRow).withFailMessage("Was not found!").isTrue();
        }

        for (Row actualRow : actualRows) {
            boolean found = false;
            for (Row expectedRow : expectedRows) {
                if (expectedRow.equals(actualRow)) {
                    found = true;
                    break;
                }
            }
            Assertions.assertThat(found).describedAs("Row %s", actualRow).withFailMessage("Unexpected row returned!").isTrue();
        }
        return this;
    }

    public RelationalResultSetAssert hasExactlyInAnyOrder(Iterable<?> expectedRows) throws SQLException, RelationalException {
        Iterator<?> iter = expectedRows.iterator();
        if (!iter.hasNext()) {
            return hasNoNextRow();
        } else {
            Object o = iter.next();
            if (o instanceof Row) {
                return hasExactlyInAnyOrder(StreamSupport.stream(expectedRows.spliterator(), false)
                        .map(t -> (Row) t)
                        .collect(Collectors.toList())
                );
            } else if (o instanceof RelationalStruct) {
                return hasExactlyInAnyOrder(StreamSupport.stream(expectedRows.spliterator(), false)
                        .map(t -> ResultSetTestUtils.structToRow((RelationalStruct) o))
                        .collect(Collectors.toList())
                );
            } else if (o instanceof Message) {
                return hasExactlyInAnyOrder(StreamSupport.stream(expectedRows.spliterator(), false)
                        .map(t -> new MessageTuple((Message) o)).collect(Collectors.toList()));
            }
            Assertions.fail("Unable to process expected results of class " + o.getClass());
            return this;
        }
    }

    private boolean advance(RelationalResultSet rrs) {
        //A simple wrapper function to throw the SQLException as a RuntimeException so that we can have cleaner asserts
        try {
            return rrs.next();
        } catch (SQLException se) {
            throw new RuntimeException();
        }
    }

    public RelationalResultSetAssert isEqualToInAnyOrder(ResultSet expectedResults) {
        Collection<Row> expectedRows = new ArrayList<>();
        try {
            while (expectedResults.next()) {
                expectedRows.add(ResultSetTestUtils.currentRow(expectedResults));
            }
            return hasExactlyInAnyOrder(expectedRows);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class ResultSetMetaDataAssert extends AbstractAssert<ResultSetMetaDataAssert, ResultSetMetaData> {

        protected ResultSetMetaDataAssert(ResultSetMetaData resultSetMetaData) {
            super(resultSetMetaData, ResultSetMetaDataAssert.class);
        }

        ResultSetMetaDataAssert hasColumns(String[] columns) {
            extracting(metaData -> {
                try {
                    int colCount = metaData.getColumnCount();
                    String[] actualColumns = new String[colCount];
                    for (int i = 1; i < colCount; i++) {
                        actualColumns[i - 1] = metaData.getColumnLabel(i);
                    }
                    return actualColumns;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }, Assertions::assertThat).describedAs("columns").containsExactly(columns);
            return this;
        }
    }

}
