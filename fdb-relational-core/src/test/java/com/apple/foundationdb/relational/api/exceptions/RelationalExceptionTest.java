/*
 * RelationalExceptionTest.java
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

package com.apple.foundationdb.relational.api.exceptions;

import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

class RelationalExceptionTest {
    RelationalException relationalException = new RelationalException("message", ErrorCode.INTERNAL_ERROR);
    SQLException sqlException = new SQLException("message", ErrorCode.INTERNAL_ERROR.getErrorCode());
    Exception exception = new Exception("message");

    @Test
    void convert() {
        assertThat(ExceptionUtil.toRelationalException(relationalException)).isSameAs(relationalException);

        assertThat(ExceptionUtil.toRelationalException(sqlException))
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.INTERNAL_ERROR);

        assertThat(ExceptionUtil.toRelationalException(exception))
                .isInstanceOf(RelationalException.class)
                .hasCause(exception)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.UNKNOWN);
    }

    @Test
    void toSqlException() {
        assertThat((Throwable) ExceptionUtil.toRelationalException(sqlException).toSqlException())
                .isSameAs(sqlException);

        assertThat((Throwable) relationalException.toSqlException())
                .isInstanceOf(SQLException.class)
                .hasMessage("message")
                .hasCause(relationalException)
                .extracting("SQLState")
                .isEqualTo(ErrorCode.INTERNAL_ERROR.getErrorCode());
    }

    @Test
    void toUncheckedWrappedException() {
        assertThat(relationalException.toUncheckedWrappedException())
                .isInstanceOf(UncheckedRelationalException.class)
                .hasCause(relationalException);
    }

    @Test
    void getErrorCode() {
        assertThat(relationalException.getErrorCode()).isEqualTo(ErrorCode.INTERNAL_ERROR);
    }

    @Test
    void unwrap() {
        assertThat(new UncheckedRelationalException(relationalException).unwrap()).isSameAs(relationalException);
    }
}
