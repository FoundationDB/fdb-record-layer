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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordLayerResultSetTest {

    RecordLayerResultSet resultSet;

    ResumableIterator<Row> cursor;

    RecordLayerResultSetTest() throws RelationalException {
        Scannable scannable = Mockito.mock(Scannable.class);
        Mockito.when(scannable.getFieldNames()).thenReturn(new String[]{"a", "b"});

        resultSet = new RecordLayerResultSet(
                scannable,
                Mockito.mock(Row.class),
                Mockito.mock(Row.class),
                Mockito.mock(RecordStoreConnection.class),
                Mockito.mock(QueryProperties.class),
                Mockito.mock(Continuation.class));
    }

    @SuppressWarnings("unchecked")
    private void mockNext(boolean next, Row keyValue) throws RelationalException {
        cursor = (ResumableIterator<Row>) Mockito.mock(ResumableIterator.class);
        Mockito.when(cursor.hasNext()).thenReturn(next);
        if (next) {
            Mockito.when(cursor.next()).thenReturn(keyValue);
        }
        Mockito.when(resultSet.scannable.openScan(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(cursor);
    }

    private void mockNext(boolean next) throws RelationalException {
        mockNext(next, new ImmutableKeyValue(new ValueTuple(1L), new ValueTuple(2L)));
    }

    @Test
    void nextTrue() throws RelationalException, SQLException {
        mockNext(true);
        assertTrue(resultSet.next());
        assertTrue(resultSet.next());
    }

    @Test
    void nextFalse() throws RelationalException, SQLException {
        mockNext(false);
        assertFalse(resultSet.next());
    }

    @Test
    void nextThrow() throws RelationalException {
        Mockito.when(resultSet.scannable.openScan(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenThrow(new RelationalException("fake exception", ErrorCode.UNKNOWN_SCHEMA));
        RelationalAssertions.assertThrowsSqlException(() -> resultSet.next(), ErrorCode.UNKNOWN_SCHEMA);
    }

    @Test
    void closeBeforeDoingAnything() {
        assertDoesNotThrow(() -> resultSet.close());
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
        RelationalAssertions.assertThrowsSqlException(() -> resultSet.close(), ErrorCode.INTERNAL_ERROR);
    }

    @Test
    void getObjectBeforeNextCall() {
        RelationalAssertions.assertThrowsSqlException(
                () -> resultSet.getObject(1),
                ErrorCode.INVALID_CURSOR_STATE);
    }

    @Test
    void getObjectInvalidPosition() throws SQLException, RelationalException {
        mockNext(true);
        Assertions.assertTrue(resultSet.next());
        RelationalAssertions.assertThrowsSqlException(
                () -> resultSet.getObject(0),
                ErrorCode.INVALID_COLUMN_REFERENCE);

        RelationalAssertions.assertThrowsSqlException(
                () -> resultSet.getObject(1000),
                ErrorCode.INVALID_COLUMN_REFERENCE);
    }

    @Test
    void getFieldNames() {
        assertThat(resultSet.getFieldNames()).containsExactly("a", "b");
    }

    @Test
    void parseMessageFail() {
        RelationalAssertions.assertThrowsSqlException(() -> resultSet.parseMessage(), ErrorCode.UNSUPPORTED_OPERATION);
    }

    @Test
    void getNumFieldsFail() {
        RelationalAssertions.assertThrowsSqlException(() -> resultSet.getNumFields(), ErrorCode.INVALID_CURSOR_STATE);
    }

    @Test
    void getNumFieldsMessageParsingNotSupported() throws RelationalException, SQLException {
        mockNext(true);
        resultSet.next();
        assertThat(resultSet.getNumFields()).isEqualTo(2);
    }

    @Test
    void getNumFieldsMessageParsingSupported() throws RelationalException, SQLException {
        mockNext(true, new MessageTuple(Restaurant.RestaurantRecord.newBuilder().setRestNo(42).build()));
        resultSet.next();
        assertThat(resultSet.getNumFields()).isEqualTo(6);
    }
}
