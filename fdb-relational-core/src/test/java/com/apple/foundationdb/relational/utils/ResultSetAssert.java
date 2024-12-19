/*
 * ResultSetAssert.java
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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.MutableRowStruct;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.assertj.core.api.SoftAssertions;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

public class ResultSetAssert extends AbstractAssert<ResultSetAssert, RelationalResultSet> {

    public static ResultSetAssert assertThat(RelationalResultSet resultSet) {
        return new ResultSetAssert(resultSet);
    }

    public ResultSetAssert(RelationalResultSet resultSet) {
        super(resultSet, ResultSetAssert.class);
    }

    public void meetsForAllRows(Condition<RelationalResultSet> perRowCondition) {
        try {
            while (actual.next()) {
                has(perRowCondition);
            }
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    public static interface SqlPredicate {

        boolean test(RelationalResultSet rs) throws SQLException;
    }

    public static Condition<RelationalResultSet> perRowCondition(SqlPredicate predicate, String description, Object... args) {
        return new Condition<>(relationalResultSet -> {
            try {
                return predicate.test(relationalResultSet);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, description, args);
    }

    public ResultSetAssert hasNextRow() {
        isNotNull();
        try {
            Assertions.assertThat(actual.next()).isTrue();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public ResultSetAssert hasNoNextRow() {
        isNotNull();
        try {
            Assertions.assertThat(actual.next()).isFalse();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public RelationalStructAssert row() {
        try {
            RelationalResultSetMetaData metaData = actual.getMetaData();
            Object[] row = new Object[metaData.getColumnCount()];
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                row[i - 1] = readColumn(actual, i, metaData.getColumnType(i));
            }

            //this should _generally_ work, but probably not ideal
            StructMetaData smd = metaData.unwrap(StructMetaData.class);
            RelationalStruct struct = new ImmutableRowStruct(new ArrayRow(row), smd);

            return new RelationalStructAssert(struct);

        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private Object readColumn(RelationalResultSet rs, int position, int columnType) throws SQLException {
        //calls the correct ResultSet method for the given column type
        // this SHOULD fail if the type doesn't match the metadata's expected type
        switch (columnType) {
            case Types.BOOLEAN:
                return rs.getBoolean(position);
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return rs.getLong(position);
            case Types.FLOAT:
                return rs.getFloat(position);
            case Types.DOUBLE:
                return rs.getDouble(position);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
                return rs.getString(position);
            case Types.STRUCT:
                return rs.getStruct(position);
            case Types.ARRAY:
                return rs.getArray(position);
            case Types.BINARY:
                return rs.getBytes(position);
            default:
                return rs.getObject(position);
        }
    }

    public ResultSetAssert hasRow(Map<String, Object> colValues) {
        colValues.forEach(this::hasColumn);
        return this;
    }

    public ResultSetAssert hasRowExactly(Object... colValues) {
        try {
            final RelationalResultSetMetaData metaData = actual.getMetaData();
            Assertions.assertThat(metaData.getColumnCount()).isGreaterThanOrEqualTo(colValues.length);
            for (int i = 0; i < colValues.length; i++) {
                Object o = actual.getObject(i + 1);
                Object expected = colValues[i];
                if (expected instanceof RelationalStruct) {
                    Assertions.assertThat(o).isInstanceOf(RelationalStruct.class);
                    RelationalStructAssert.assertThat((RelationalStruct) o).isEqualTo((RelationalStruct) expected);
                } else if (expected instanceof Array) {
                    Assertions.assertThat(o).isInstanceOf(Array.class);
                    ArrayAssert.assertThat((Array) o).isEqualTo((Array) expected);
                } else {
                    Assertions.assertThat(o).as("checking column %d (zero-based) of expected row (%s)", i,
                            Arrays.stream(colValues).map(colValue -> colValue == null ? "<NULL>" : colValue.toString()).collect(Collectors.joining(","))).isEqualTo(expected);
                }
            }
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }

        return this;
    }

    public ResultSetAssert hasRowExactly(Map<String, Object> colValues) {
        hasRow(colValues);
        //now make sure that there aren't any others
        metaData().hasColumnsExactly(colValues.keySet());
        return this;
    }

    public ResultSetAssert hasColumn(String colName, Object colValue) {
        try {
            Object o = actual.getObject(colName);
            if (colValue instanceof RelationalStruct) {
                Assertions.assertThat(o).isInstanceOf(RelationalStruct.class);
                RelationalStruct actualStruct = (RelationalStruct) o;
                RelationalStruct expectedStruct = (RelationalStruct) colValue;

                RelationalStructAssert.assertThat(actualStruct).isEqualTo(expectedStruct);
            } else if (colValue instanceof Array) {
                Assertions.assertThat(o).isInstanceOf(Array.class);

                ArrayAssert.assertThat((Array) o).isEqualTo((Array) colValue);
            } else if (colValue instanceof Message) {
                Type.Record record = ProtobufDdlUtil.recordFromDescriptor(((Message) colValue).getDescriptorForType());
                final StructMetaData metaData = SqlTypeSupport.recordToMetaData(record);
                Row row = new MessageTuple((Message) colValue);

                Assertions.assertThat(o).isInstanceOf(RelationalStruct.class);
                RelationalStruct actualStruct = (RelationalStruct) o;
                RelationalStructAssert.assertThat(new ImmutableRowStruct(row, metaData)).isEqualTo(actualStruct);
            } else {
                Assertions.assertThat(o).isEqualTo(colValue);
            }
        } catch (SQLException | RelationalException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public ResultSetAssert hasRow(Message message) {
        final Descriptors.Descriptor descriptor = message.getDescriptorForType();
        try {
            Type.Record record = ProtobufDdlUtil.recordFromDescriptor(descriptor);
            final StructMetaData structMetaData = SqlTypeSupport.recordToMetaData(record);
            Row row = new MessageTuple(message);

            Row actualRow = ResultSetTestUtils.currentRow(actual);
            StructMetaData actualSMetaData = actual.getMetaData().unwrap(StructMetaData.class);
            RelationalStructAssert.assertThat(new ImmutableRowStruct(actualRow, actualSMetaData))
                    .isEqualTo(new ImmutableRowStruct(row, structMetaData));
        } catch (SQLException | RelationalException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public ResultSetAssert containsRowsExactly(Iterable<? extends Message> rows) {
        /*
         * We are operating on the assumption that all rows have the same metadata, because otherwise
         * the result set would break anyway
         */
        Iterator<? extends Message> iter = rows.iterator();
        if (!iter.hasNext()) {
            return isEmpty();
        }
        Message first = iter.next();
        try {
            Type.Record record = ProtobufDdlUtil.recordFromDescriptor(first.getDescriptorForType());
            StructMetaData expectedMetaData = SqlTypeSupport.recordToMetaData(record);
            List<Row> expectedRows = StreamSupport.stream(rows.spliterator(), false)
                    .map(MessageTuple::new)
                    .collect(Collectors.toList());
            RelationalResultSet expectedResultSet = new IteratorResultSet(expectedMetaData, expectedRows.iterator(), 0);
            return isExactlyInAnyOrder(expectedResultSet);
        } catch (RelationalException se) {
            throw new RuntimeException(se);
        }
    }

    public ResultSetAssert containsRowsExactly(Message... rows) {
        if (rows.length == 0) {
            return hasNoNextRow();
        }
        /*
         * We are operating on the assumption that all rows have the same metadata, because otherwise
         * the result set would break anyway
         */
        Message first = rows[0];
        try {
            Type.Record record = ProtobufDdlUtil.recordFromDescriptor(first.getDescriptorForType());
            StructMetaData expectedMetaData = SqlTypeSupport.recordToMetaData(record);
            List<Row> expectedRows = Arrays.stream(rows)
                    .map(MessageTuple::new)
                    .collect(Collectors.toList());
            RelationalResultSet expectedResultSet = new IteratorResultSet(expectedMetaData, expectedRows.iterator(), 0);
            return isExactlyInAnyOrder(expectedResultSet);
        } catch (RelationalException se) {
            throw new RuntimeException(se);
        }
    }

    public ResultSetAssert containsRowsExactly(Collection<Object[]> rows) {
        //assume that the struct metadata is the same
        try {
            StructMetaData metaData = actual.getMetaData().unwrap(StructMetaData.class);
            Iterable<Row> expectedData = rows.stream()
                    .map(ArrayRow::new)
                    .collect(Collectors.toList());
            RelationalResultSet expected = new IteratorResultSet(metaData, expectedData.iterator(), 0);
            return isExactlyInAnyOrder(expected);
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    public ResultSetAssert isExactlyInAnyOrder(RelationalResultSet expectedResultSet) {
        try {
            //verify the metadata
            ResultSetMetaDataAssert.assertThat(actual.getMetaData()).isEqualTo(expectedResultSet.getMetaData());

            List<Row> expectedRows = new ArrayList<>();
            while (expectedResultSet.next()) {
                expectedRows.add(ResultSetTestUtils.currentRow(expectedResultSet));
            }

            List<Row> actualRows = new ArrayList<>();
            while (actual.next()) {
                actualRows.add(ResultSetTestUtils.currentRow(actual));
            }

            Assertions.assertThat(actualRows.size()).describedAs("ResultSet size").isEqualTo(expectedRows.size());

            MutableRowStruct expectedStruct = new MutableRowStruct(expectedResultSet.getMetaData().unwrap(StructMetaData.class));
            int p = 0;
            SoftAssertions caughtAssertions = new SoftAssertions();
            StructMetaData actualMetaData = expectedResultSet.getMetaData().unwrap(StructMetaData.class);
            for (Row expectedRow : expectedRows) {
                expectedStruct.setRow(expectedRow);
                final Iterator<RelationalStruct> iterator = getRelationalStructIterator(actualMetaData, actualRows);
                caughtAssertions.proxy(RelationalStructAssert.class, RelationalStruct.class, expectedStruct)
                        .as(expectedRow.toString()).isContainedIn(iterator);
                p++;
            }

            //check if there are no missing rows
            caughtAssertions.assertAll();

            p = 0;
            StructMetaData expectedMetaData = expectedResultSet.getMetaData().unwrap(StructMetaData.class);
            MutableRowStruct actualStruct = new MutableRowStruct(expectedResultSet.getMetaData().unwrap(StructMetaData.class));
            for (Row actualRow : actualRows) {
                actualStruct.setRow(actualRow);

                final Iterator<RelationalStruct> iterator = getRelationalStructIterator(expectedMetaData, expectedRows);
                caughtAssertions.proxy(RelationalStructAssert.class, RelationalStruct.class, actualStruct)
                        .as("Has Row %d", p).isContainedIn(iterator);
                p++;
            }

            //check if there are no extra rows
            caughtAssertions.assertAll();

        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public ResultSetAssert is(RelationalResultSet expectedResultSet) {
        isNotNull();
        try {
            //verify the metadata
            ResultSetMetaDataAssert.assertThat(actual.getMetaData()).isEqualTo(expectedResultSet.getMetaData());
            RelationalResultSetMetaData metaData = actual.getMetaData();
            while (actual.next()) {
                Assertions.assertThat(expectedResultSet.next()).isTrue();

                SoftAssertions rowAssert = new SoftAssertions();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    Object actualO = readColumn(actual, i, metaData.getColumnType(i));
                    Object expectedO = readColumn(expectedResultSet, i, metaData.getColumnType(i));

                    if (actualO instanceof RelationalStruct) {
                        rowAssert.proxy(RelationalStructAssert.class, RelationalStruct.class, (RelationalStruct) actualO).isEqualTo(expectedO);
                    } else if (actualO instanceof Array) {
                        ResultSet rs = ((Array) actualO).getResultSet();
                        rowAssert.assertThat(rs).isInstanceOf(RelationalResultSet.class);
                        RelationalResultSet actualArrRs = (RelationalResultSet) rs;
                        rowAssert.assertThat(expectedO).isInstanceOf(Array.class);
                        ResultSet ers = ((Array) expectedO).getResultSet();
                        rowAssert.assertThat(ers).isInstanceOf(RelationalResultSet.class);
                        RelationalResultSet expectedArrRs = (RelationalResultSet) ers;

                        rowAssert.proxy(ResultSetAssert.class, RelationalResultSet.class, actualArrRs).isExactlyInAnyOrder(expectedArrRs);
                    } else {
                        rowAssert.assertThat(actualO).isEqualTo(expectedO);
                    }
                }
                rowAssert.assertAll();
            }
            Assertions.assertThat(expectedResultSet.next()).as("Should have no more expected rows").isFalse();

        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public ResultSetMetaDataAssert metaData() {
        isNotNull();
        try {
            RelationalResultSetMetaData metaData = actual.getMetaData();
            return ResultSetMetaDataAssert.assertThat(metaData).isNotNull();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ResultSetAssert isEmpty() {
        isNotNull();
        try {
            if (actual.next()) {
                failWithMessage("Expected ResultSet#next() to return false");
            }
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    @Nonnull
    private Iterator<RelationalStruct> getRelationalStructIterator(StructMetaData metaData, List<Row> expectedRows) {
        return new Iterator<>() {
            private final MutableRowStruct structWrapper = new MutableRowStruct(metaData);
            private final Iterator<Row> rowIter = expectedRows.iterator();

            @Override
            public boolean hasNext() {
                return rowIter.hasNext();
            }

            @Override
            public RelationalStruct next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                structWrapper.setRow(rowIter.next());
                return structWrapper;
            }
        };
    }

}
