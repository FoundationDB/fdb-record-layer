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
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;

import com.google.protobuf.Message;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.IterableAssert;
import org.assertj.core.api.MapAssert;
import org.assertj.core.api.SoftAssertions;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class RelationalResultSetAssert extends AbstractAssert<RelationalResultSetAssert, RelationalResultSet> {
    protected RelationalResultSetAssert(RelationalResultSet relationalResultSet) {
        super(relationalResultSet, RelationalResultSetAssert.class);
    }

    public RelationalResultSetAssert hasColumns(String[] columns) throws SQLException {
        isNotNull();
        extracting(relationalResultSet -> {
            try {
                return relationalResultSet.getMetaData();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, ResultSetMetaDataAssert::new).hasColumns(columns);

        return this;
    }

    public RelationalResultSetAssert nextRowMatches(Row expectedNextRow) {
        isNotNull();
        //make sure that there is at least one row
        hasNextRow();
        extracting(this::parseRow, RowAssert::new).isEqualTo(expectedNextRow);

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

    public RelationalResultSetAssert hasExactlyInAnyOrder(Collection<Row> expectedRows) throws SQLException {
        Collection<Row> actualRows = new ArrayList<>(expectedRows.size());
        while (actual.next()) {
            actualRows.add(parseRow(actual));
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
            Assertions.assertThat(found).describedAs("Row %s").withFailMessage("Unexpected row returned!").isTrue();
        }
        return this;
    }

    private boolean advance(RelationalResultSet rrs) {
        //A simple wrapper function to throw the SQLException as a RuntimeException so that we can have cleaner asserts
        try {
            return rrs.next();
        } catch (SQLException se) {
            throw new RuntimeException();
        }
    }

    private Row parseRow(RelationalResultSet rrs) {
        try {
            final ResultSetMetaData metaData = rrs.getMetaData();
            Object[] theRow = new Object[metaData.getColumnCount()];
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                theRow[i - 1] = rrs.getObject(i);
            }
            return new ArrayRow(theRow);
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static class RowAssert extends AbstractAssert<RowAssert, Row> {
        protected RowAssert(Row row) {
            super(row, RowAssert.class);
        }

        @Override
        public RowAssert isEqualTo(Object expected) {
            if (expected instanceof Row) {
                //do row comparison
                Row other = (Row) expected;
                //make sure that they have the same number of fields
                extracting(Row::getNumFields, Assertions::assertThat).isEqualTo(other.getNumFields());
                for (int i = 0; i < other.getNumFields(); i++) {
                    try {
                        Object actualO = getObject(actual, i);
                        Object otherO = other.getObject(i);
                        extractAssert(actualO).isEqualTo(otherO);
                        //                        final int p = i; //use a temporary final variable so that the lambda doesn't object
                        //                        extracting(row -> getObject(row,p), this::extractAssert).isEqualTo(other.getObject(i));
                    } catch (InvalidColumnReferenceException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                return super.isEqualTo(expected);
            }
            return this;
        }

        private AbstractAssert<? extends AbstractAssert<?, ?>, ?> extractAssert(Object o) {
            if (o instanceof Message) {
                return new MessageAssert((Message) o);
            } else if (o instanceof Iterable) {
                Iterable<?> objects = (Iterable<?>) o;

                return new IterableAssert<Object>(objects) {
                    @Override
                    public IterableAssert<Object> isEqualTo(Object expected) {
                        if (!(expected instanceof Iterable)) {
                            failWithMessage("Unexpected iterable. Expected type " + expected.getClass().getName());
                        }
                        Iterable<?> expectedObjects = (Iterable<?>) expected;
                        for (Object expectedObj : expectedObjects) {
                            Predicate<? super Object> searchPredicate = expectedObj instanceof Message ? o1 -> {
                                if (!(o1 instanceof Message)) {
                                    return false;
                                }
                                SoftAssertions assertions = new SoftAssertions();
                                return MessageAssert.messagesMatch((Message) expectedObj, (Message) o1, assertions).wasSuccess();
                            } : expectedObj::equals;

                            anyMatch(searchPredicate);
                        }
                        return this;
                    }
                };

            } else {
                return Assertions.assertThat(o);
            }
        }

        private Object getObject(Row row, int position) {
            //wrapper for the runtime exception handling so that our assertions are prettier
            try {
                return row.getObject(position);
            } catch (InvalidColumnReferenceException e) {
                //shouldn't happen, but you never know
                throw new RuntimeException(e);
            }
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
