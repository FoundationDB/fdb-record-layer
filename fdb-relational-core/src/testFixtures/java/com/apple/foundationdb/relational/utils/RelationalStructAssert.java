/*
 * RelationalStructAssert.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.google.protobuf.Descriptors;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.assertj.core.api.SoftAssertions;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.Map;

@API(API.Status.EXPERIMENTAL)
public class RelationalStructAssert extends AbstractAssert<RelationalStructAssert, RelationalStruct> {
    RelationalStructAssert(RelationalStruct row) {
        super(row, RelationalStructAssert.class);
    }

    public static RelationalStructAssert assertThat(RelationalStruct actualStruct) {
        return new RelationalStructAssert(actualStruct);
    }

    @Override
    public RelationalStructAssert isEqualTo(Object expected) {
        if (expected instanceof RelationalStruct) {
            return isEqualTo((RelationalStruct) expected);
        } else {
            return super.isEqualTo(expected);
        }
    }

    public RelationalStructAssert containsColumnsByPosition(Map<Integer, Object> expectedColumns) {
        isNotNull();
        SoftAssertions assertions = new SoftAssertions();
        expectedColumns.forEach((colPosition, colValue) -> {
            try {
                Object actualO = actual.getObject(colPosition);
                if (actualO instanceof RelationalStruct) {
                    assertions.proxy(RelationalStructAssert.class, RelationalStruct.class, (RelationalStruct) actualO).isEqualTo(colValue);
                } else if (actualO instanceof Array) {
                    ResultSet rs = ((Array) actualO).getResultSet();
                    assertions.proxy(ResultSetAssert.class, RelationalResultSet.class, (RelationalResultSet) rs).isEqualTo(colValue);
                } else {
                    Assertions.assertThat(actualO).isEqualTo(colValue);
                }
            } catch (SQLException se) {
                throw new RuntimeException(se);
            }
        });
        assertions.assertAll();
        return this;
    }

    public RelationalStructAssert containsColumnsByName(Map<String, Object> expectedColumns) throws SQLException {
        isNotNull();
        StructMetaData actualMetaData = actual.getMetaData();
        SoftAssertions assertions = new SoftAssertions();
        StructMetaDataAssert metaDataAssert = assertions.proxy(StructMetaDataAssert.class, StructMetaData.class, actualMetaData);
        expectedColumns.forEach((colName, colValue) -> {
            metaDataAssert.hasColumn(colName);
            try {
                Object actualO = actual.getObject(colName);
                if (actualO instanceof RelationalStruct) {
                    assertions.proxy(RelationalStructAssert.class, RelationalStruct.class, (RelationalStruct) actualO).isEqualTo(colValue);
                } else if (actualO instanceof Array) {
                    RelationalResultSet rs = (RelationalResultSet) ((Array) actualO).getResultSet();
                    Assertions.assertThat(colValue).withFailMessage("Unexpected array for column %s", colName).isInstanceOf(Array.class);
                    RelationalResultSet expectedRs = (RelationalResultSet) ((Array) colValue).getResultSet();
                    assertions.proxy(ResultSetAssert.class, RelationalResultSet.class, rs).isExactlyInAnyOrder(expectedRs);
                } else {
                    Assertions.assertThat(actualO).isEqualTo(colValue);
                }
            } catch (SQLException se) {
                throw new RuntimeException(se);
            }
        });
        assertions.assertAll();
        return this;
    }

    public void isContainedIn(Iterator<RelationalStruct> expected) {
        while (expected.hasNext()) {
            if (checkEquals(actual, expected.next())) {
                return;
            }
        }
        throw failure("Is not contained in data set");
    }

    public void isPartlyContainedIn(Iterator<RelationalStruct> list) {
        while (list.hasNext()) {
            if (checkPartlyEquals(list.next(), actual)) {
                return;
            }
        }
        throw failure("Is not contained in data set");
    }

    @SuppressWarnings("checkstyle:EmptyCatchBlock")
    private static boolean checkEquals(RelationalStruct actual, RelationalStruct expected) {

        try {
            StructMetaData actualMetaData = actual.getMetaData();
            StructMetaData expectedMetaData = expected.getMetaData();
            if (actualMetaData.getColumnCount() != expectedMetaData.getColumnCount()) {
                return false;
            }

            for (int i = 1; i <= actualMetaData.getColumnCount(); i++) {
                int actualSqlType = actualMetaData.getColumnType(i);
                int expectedSqlType = expectedMetaData.getColumnType(i);
                if (actualSqlType != expectedSqlType) {
                    return false;
                }
                if (!actualMetaData.getColumnName(i).equalsIgnoreCase(expectedMetaData.getColumnName(i))) {
                    return false;
                }

                boolean fieldEquals = false;
                switch (actualSqlType) {
                    case Types.BOOLEAN:
                        fieldEquals = actual.getBoolean(i) == expected.getBoolean(i);
                        break;
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        fieldEquals = actual.getLong(i) == expected.getLong(i);
                        break;
                    case Types.FLOAT:
                        fieldEquals = actual.getFloat(i) == expected.getFloat(i);
                        break;
                    case Types.DOUBLE:
                        fieldEquals = actual.getDouble(i) == expected.getDouble(i);
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.NCHAR:
                    case Types.NVARCHAR:
                        fieldEquals = actual.getString(i).equals(expected.getString(i));
                        break;
                    case Types.STRUCT:
                        fieldEquals = RelationalStructAssert.checkEquals(actual.getStruct(i), expected.getStruct(i));
                        break;
                    case Types.ARRAY:
                        fieldEquals = ArrayAssert.checkEquals(actual.getArray(i), expected.getArray(i));
                        break;
                    case Types.BINARY:
                        try {
                            Assertions.assertThat(actual.getBytes(i)).containsExactly(expected.getBytes(i));
                            fieldEquals = true;
                        } catch (AssertionError ignored) {
                        }
                        break;
                    default:
                        fieldEquals = actual.getObject(i).equals(expected.getObject(i));
                }
                if (!fieldEquals) {
                    return false;
                }
            }
            return true;
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }

    }


    /**
     * Checks if the expected {@link RelationalStruct} is partial overlap of the {@link RelationalStruct}. It follows the
     * following rules:
     * <ul>
     *     <li>The expected struct should have at least 1 column.</li>
     *     <li>The ordered list of columns in expected struct is required to be a ordered sub-list of actual struct</li>
     *     <li>The column name of expected struct should match column name of actual struct. For the matched name, their
     *     sql type should match</li>
     *     <li>If the column is of primitive or array type, the values should match completely. However, if it is a struct,
     *     the matching can be partial.</li>
     * </ul>
     * @param actual the actual {@link RelationalStruct}.
     * @param expected the actual {@link RelationalStruct}.
     *
     * @return if the actual and expected is the same.
     */
    @SuppressWarnings("checkstyle:EmptyCatchBlock")
    private static boolean checkPartlyEquals(RelationalStruct actual, RelationalStruct expected) {
        try {
            StructMetaData actualMetaData = actual.getMetaData();
            StructMetaData expectedMetaData = expected.getMetaData();
            if (actualMetaData.getColumnCount() < expectedMetaData.getColumnCount() || expectedMetaData.getColumnCount() == 0) {
                return false;
            }

            var expectedIdx = 1;
            for (int actualIdx = 1; actualIdx <= actualMetaData.getColumnCount(); actualIdx++) {
                final var actualName = actualMetaData.getColumnName(actualIdx);
                if (expectedIdx > expectedMetaData.getColumnCount() || !actualName.equalsIgnoreCase(expectedMetaData.getColumnName(expectedIdx))) {
                    continue;
                }
                int actualSqlType = actualMetaData.getColumnType(actualIdx);
                int expectedSqlType = expectedMetaData.getColumnType(actualIdx);
                if (actualSqlType != expectedSqlType) {
                    return false;
                }

                boolean fieldEquals = false;
                switch (actualSqlType) {
                    case Types.BOOLEAN:
                        fieldEquals = actual.getBoolean(actualIdx) == expected.getBoolean(expectedIdx);
                        break;
                    case Types.SMALLINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        fieldEquals = actual.getLong(actualIdx) == expected.getLong(expectedIdx);
                        break;
                    case Types.FLOAT:
                        fieldEquals = actual.getFloat(actualIdx) == expected.getFloat(expectedIdx);
                        break;
                    case Types.DOUBLE:
                        fieldEquals = actual.getDouble(actualIdx) == expected.getDouble(expectedIdx);
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.NCHAR:
                    case Types.NVARCHAR:
                        fieldEquals = actual.getString(actualIdx).equals(expected.getString(expectedIdx));
                        break;
                    case Types.STRUCT:
                        fieldEquals = RelationalStructAssert.checkPartlyEquals(actual.getStruct(actualIdx), expected.getStruct(expectedIdx));
                        break;
                    case Types.ARRAY:
                        fieldEquals = ArrayAssert.checkEquals(actual.getArray(actualIdx), expected.getArray(expectedIdx));
                        break;
                    case Types.BINARY:
                        try {
                            Assertions.assertThat(actual.getBytes(actualIdx)).containsExactly(expected.getBytes(expectedIdx));
                            fieldEquals = true;
                        } catch (AssertionError ignored) {
                        }
                        break;
                    default:
                        fieldEquals = actual.getObject(actualIdx).equals(expected.getObject(expectedIdx));
                }
                if (!fieldEquals) {
                    return false;
                }
                expectedIdx++;
            }
            return expectedIdx > expectedMetaData.getColumnCount();
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    @SuppressWarnings("checkstyle:EmptyCatchBlock")
    public RelationalStructAssert isPartlyEqualTo(RelationalStruct expected) {
        if (actual == null) {
            Assertions.assertThat(expected).isNull();
            return this;
        }
        isNotNull();
        try {
            StructMetaData actualMetaData = actual.getMetaData();
            StructMetaData expectedMetaData = expected.getMetaData();
            Assertions.assertThat(actualMetaData.getColumnCount()).isGreaterThanOrEqualTo(expectedMetaData.getColumnCount());
            Assumptions.assumeThat(expectedMetaData.getColumnCount()).isGreaterThan(0);

            var expectedIdx = 1;
            for (int actualIdx = 1; actualIdx <= actualMetaData.getColumnCount(); actualIdx++) {
                final var actualName = actualMetaData.getColumnName(actualIdx);
                if (expectedIdx > expectedMetaData.getColumnCount() || !actualName.equalsIgnoreCase(expectedMetaData.getColumnName(expectedIdx))) {
                    // ignoring columns that are not in expected.
                    continue;
                }
                int actualSqlType = actualMetaData.getColumnType(actualIdx);
                int expectedSqlType = expectedMetaData.getColumnType(expectedIdx);
                Assertions.assertThat(actualSqlType).isEqualTo(expectedSqlType)
                        .describedAs("expected type: " + SqlTypeNamesSupport.getSqlTypeName(expectedSqlType) + ", but found: " + SqlTypeNamesSupport.getSqlTypeName(actualSqlType));

                switch (actualSqlType) {
                    case Types.BOOLEAN:
                        Assertions.assertThat(actual.getBoolean(actualIdx)).isEqualTo(expected.getBoolean(expectedIdx));
                        break;
                    case Types.SMALLINT:
                    case Types.INTEGER:
                        Assertions.assertThat(actual.getInt(actualIdx)).isEqualTo(expected.getInt(expectedIdx));
                        break;
                    case Types.BIGINT:
                        Assertions.assertThat(actual.getLong(actualIdx)).isEqualTo(expected.getLong(expectedIdx));
                        break;
                    case Types.FLOAT:
                        Assertions.assertThat(actual.getFloat(actualIdx)).isEqualTo(expected.getFloat(expectedIdx));
                        break;
                    case Types.DOUBLE:
                        Assertions.assertThat(actual.getDouble(actualIdx)).isEqualTo(expected.getDouble(expectedIdx));
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.NCHAR:
                    case Types.NVARCHAR:
                        Assertions.assertThat(actual.getString(actualIdx)).isEqualTo(expected.getString(expectedIdx));
                        break;
                    case Types.STRUCT:
                        RelationalStructAssert.assertThat(actual.getStruct(actualIdx)).isPartlyEqualTo(expected.getStruct(expectedIdx));
                        break;
                    case Types.ARRAY:
                        ArrayAssert.assertThat(actual.getArray(actualIdx)).isEqualTo(expected.getArray(expectedIdx));
                        break;
                    case Types.BINARY:
                        Assertions.assertThat(actual.getBytes(actualIdx)).containsExactly(expected.getBytes(actualIdx));
                        break;
                    default:
                        Assertions.assertThat(actual.getObject(actualIdx)).isEqualTo(expected.getObject(actualIdx));
                }
                expectedIdx++;
            }
            Assertions.assertThat(expectedIdx).isGreaterThanOrEqualTo(expectedMetaData.getColumnCount());
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }

    public RelationalStructAssert isEqualTo(RelationalStruct expected) {
        if (actual == null) {
            Assertions.assertThat(expected).isNull();
            return this;
        }
        isNotNull();
        SoftAssertions assertions = new SoftAssertions();
        /*
         * Here we are reproducing some of the work in StructMetaDataAssert, but that's
         * because we are _also_ checking row values each time. It's a little non-ideal,
         * in that the metadata comparisons are likely going to be highly repetitive (being checked
         * against every row, when they aren't likely to change at all from row to row), but OTOH,
         * it allows us the safety of easily ensuring that every step of the check is valid and correct.
         */
        try {
            StructMetaData actualMetaData = actual.getMetaData();
            StructMetaData expectedMetaData = expected.getMetaData();
            assertions.assertThat(actualMetaData.getColumnCount())
                    .describedAs("Struct column count")
                    .isEqualTo(expectedMetaData.getColumnCount());

            for (int i = 1; i <= actualMetaData.getColumnCount(); i++) {
                int actualSqlType = actualMetaData.getColumnType(i);
                int expectedSqlType = expectedMetaData.getColumnType(i);
                assertions.assertThat(actualSqlType)
                        .describedAs(actualMetaData.getColumnName(i) + "(" + i + ") SqlType")
                        .isEqualTo(expectedSqlType);
                assertions.assertThat(actualMetaData.getColumnName(i))
                        .describedAs("%s (%d) Name", actualMetaData.getColumnName(i), i)
                        .isEqualTo(expectedMetaData.getColumnName(i));

                switch (actualSqlType) {
                    case Types.BOOLEAN:
                        assertions.assertThat(actual.getBoolean(i)).isEqualTo(expected.getBoolean(i));
                        break;
                    case Types.SMALLINT:
                    case Types.INTEGER:
                        assertions.assertThat(actual.getInt(i)).isEqualTo(expected.getInt(i));
                        break;
                    case Types.BIGINT:
                        assertions.assertThat(actual.getLong(i)).isEqualTo(expected.getLong(i));
                        break;
                    case Types.FLOAT:
                        assertions.assertThat(actual.getFloat(i)).isEqualTo(expected.getFloat(i));
                        break;
                    case Types.DOUBLE:
                        assertions.assertThat(actual.getDouble(i)).isEqualTo(expected.getDouble(i));
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                    case Types.NCHAR:
                    case Types.NVARCHAR:
                        assertions.assertThat(actual.getString(i)).isEqualTo(expected.getString(i));
                        break;
                    case Types.STRUCT:
                        assertions.proxy(RelationalStructAssert.class, RelationalStruct.class, actual.getStruct(i)).isEqualTo(expected.getStruct(i));
                        break;
                    case Types.ARRAY:
                        assertions.proxy(ArrayAssert.class, Array.class, actual.getArray(i)).isEqualTo(expected.getArray(i));
                        break;
                    case Types.BINARY:
                        assertions.assertThat(actual.getBytes(i)).containsExactly(expected.getBytes(i));
                        break;
                    default:
                        final var actualValue = actual.getObject(i);
                        if (actualSqlType == Types.OTHER) {
                            // maybe an ENUM value or a UUID
                            assertions.assertThat(actualValue.toString()).isEqualTo(expected.getObject(i).toString());
                        } else {
                            assertions.assertThat(actual.getObject(i)).isEqualTo(expected.getObject(i));
                        }
                }
            }
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        assertions.assertAll();
        return this;
    }

    @SuppressWarnings("unchecked")
    public RelationalStructAssert hasValue(String columnName, Object value) {
        try {
            final Object object = actual.getObject(columnName);
            if (value instanceof Map) {
                //this is the same type of check as a struct, but made convenient for the tester
                Assertions.assertThat(object).isInstanceOf(RelationalStruct.class);
                RelationalStructAssert.assertThat((RelationalStruct) object).containsColumnsByName((Map<String, Object>) value);
            } else if (object instanceof RelationalStruct) {
                RelationalStructAssert.assertThat((RelationalStruct) object).isEqualTo(value);
            } else if (object instanceof Array) {
                ArrayAssert.assertThat((Array) object).isEqualTo(value);
            } else if (object instanceof Descriptors.EnumValueDescriptor) {
                Assertions.assertThat(((Descriptors.EnumValueDescriptor) object).getName()).isEqualTo(value);
            } else {
                Assertions.assertThat(object).isEqualTo(value);
            }
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
        return this;
    }
}
