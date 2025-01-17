/*
 * RelationalExceptionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

class RelationalExceptionTest {
    RelationalException relationalException = new RelationalException("message", ErrorCode.INTERNAL_ERROR);
    SQLException sqlException = new SQLException("message", ErrorCode.INTERNAL_ERROR.getErrorCode());
    Exception exception = new Exception("message");

    @Test
    void convert() {
        Assertions.assertThat(toRelationalException(relationalException)).isSameAs(relationalException);

        Assertions.assertThat(toRelationalException(sqlException))
                .isInstanceOf(RelationalException.class)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.INTERNAL_ERROR);

        Assertions.assertThat(toRelationalException(exception))
                .isInstanceOf(RelationalException.class)
                .hasCause(exception)
                .extracting("errorCode")
                .isEqualTo(ErrorCode.UNKNOWN);
    }

    @Test
    void toSqlException() {
        Assertions.assertThat((Throwable) toRelationalException(sqlException).toSqlException())
                .isSameAs(sqlException);

        Assertions.assertThat((Throwable) relationalException.toSqlException())
                .isInstanceOf(SQLException.class)
                .hasMessage("message")
                .hasCause(relationalException)
                .extracting("SQLState")
                .isEqualTo(ErrorCode.INTERNAL_ERROR.getErrorCode());
    }

    @Test
    void toUncheckedWrappedException() {
        Assertions.assertThat(relationalException.toUncheckedWrappedException())
                .isInstanceOf(UncheckedRelationalException.class)
                .hasCause(relationalException);
    }

    @Test
    void getErrorCode() {
        Assertions.assertThat(relationalException.getErrorCode()).isEqualTo(ErrorCode.INTERNAL_ERROR);
    }

    @Test
    void unwrap() {
        Assertions.assertThat(new UncheckedRelationalException(relationalException).unwrap()).isSameAs(relationalException);
    }

    /**
     * Main part of the body copied from com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil
     * so we did not have to add dependency on recordlayer here in this api module.
     */
    private static RelationalException toRelationalException(Throwable re) {
        if (re instanceof RelationalException) {
            return (RelationalException) re;
        } else if (re instanceof SQLException) {
            return new RelationalException(re.getMessage(), ErrorCode.get(((SQLException) re).getSQLState()), re);
        }
        return new RelationalException(ErrorCode.UNKNOWN, re);
    }
}
