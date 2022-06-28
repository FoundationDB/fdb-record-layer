/*
 * RecordLayerResultSetTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.RelationalAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class RecordLayerResultSetTest {

    RecordLayerResultSet resultSet;

    ResumableIterator<Row> cursor;

    @SuppressWarnings("unchecked")
    RecordLayerResultSetTest() throws RelationalException {
        cursor = (ResumableIterator<Row>) Mockito.mock(ResumableIterator.class);
        StructMetaData smd = new RelationalStructMetaData(
                FieldDescription.primitive("a", Types.INTEGER, true),
                FieldDescription.primitive("b", Types.VARCHAR, true)
        );
        resultSet = new RecordLayerResultSet(
                smd,
                cursor,
                Mockito.mock(EmbeddedRelationalConnection.class));
    }

    private void mockNext(boolean next, Row keyValue) throws RelationalException {
        Mockito.when(cursor.hasNext()).thenReturn(next);
        if (next) {
            Mockito.when(cursor.next()).thenReturn(keyValue);
        }
    }

    private void mockNext(boolean next) throws RelationalException {
        mockNext(next, new ImmutableKeyValue(new ValueTuple(1L), new ValueTuple(2L)));
    }

    @Test
    void nextTrue() throws RelationalException, SQLException {
        mockNext(true);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertTrue(resultSet.next());
    }

    @Test
    void nextFalse() throws RelationalException, SQLException {
        mockNext(false);
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    void closeBeforeDoingAnything() {
        Assertions.assertDoesNotThrow(() -> resultSet.close());
    }

    @Test
    void closeDoesCloseUnderlying() throws RelationalException, SQLException {
        mockNext(true);
        resultSet.next();
        resultSet.close();
        Mockito.verify(cursor, Mockito.times(1)).close();
    }

    @Test
    void closeThrows() throws RelationalException, SQLException {
        mockNext(true);
        resultSet.next();
        Mockito.doThrow(new RelationalException("fake exception", ErrorCode.INTERNAL_ERROR)).when(cursor).close();
        RelationalAssertions.assertThrowsSqlException(() -> resultSet.close()).hasErrorCode(ErrorCode.INTERNAL_ERROR);
    }

    @Test
    void getObjectBeforeNextCall() {
        RelationalAssertions.assertThrowsSqlException(
                () -> resultSet.getObject(1))
                .hasErrorCode(ErrorCode.INVALID_CURSOR_STATE);
    }

    @Test
    void getObjectInvalidPosition() throws SQLException, RelationalException {
        mockNext(true);
        Assertions.assertTrue(resultSet.next());
        RelationalAssertions.assertThrowsSqlException(
                () -> resultSet.getObject(0))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);

        RelationalAssertions.assertThrowsSqlException(
                () -> resultSet.getObject(1000))
                .hasErrorCode(ErrorCode.INVALID_COLUMN_REFERENCE);
    }

    @Test
    void getFieldNames() throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        assertThat(metaData.getColumnName(1)).isEqualToIgnoringCase("A");
        assertThat(metaData.getColumnName(2)).isEqualToIgnoringCase("B");
    }
}
