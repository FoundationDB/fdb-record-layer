/*
 * RelationalAssertions.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.IntegerAssert;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.StringAssert;
import org.assertj.core.api.ThrowableAssert;

import java.sql.SQLException;

/**
 * A collection of entry points for various Relational-specific assertJ assertion matchers.
 */
@API(API.Status.EXPERIMENTAL)
public final class RelationalAssertions {

    public static RelationalAssert assertThat(Object o) {
        return new RelationalAssert(o);
    }

    public static RelationalExceptionAssert assertThrows(ThrowableAssert.ThrowingCallable shouldThrow) {
        RelationalException ve = Assertions.catchThrowableOfType(RelationalException.class, shouldThrow);
        return new RelationalExceptionAssert(ve);
    }

    public static SQLExceptionAssert assertThrowsSqlException(ThrowableAssert.ThrowingCallable shouldThrow) {
        SQLException se = Assertions.catchThrowableOfType(SQLException.class, shouldThrow);
        if (se == null) {
            Assertions.fail("expected an exception of type " + SQLException.class.getSimpleName() + " to be thrown, but no exception was thrown");
        }
        return new SQLExceptionAssert(se);
    }

    public static class RelationalExceptionAssert extends AbstractThrowableAssert<RelationalExceptionAssert, RelationalException> {

        protected RelationalExceptionAssert(RelationalException e) {
            super(e, RelationalExceptionAssert.class);
        }

        public RelationalExceptionAssert hasErrorCode(ErrorCode expected) {
            extracting(RelationalException::getErrorCode, ObjectAssert::new).isEqualTo(expected);
            return this;
        }

        public RelationalExceptionAssert containsInMessage(String text) {
            extracting(RelationalException::getMessage, StringAssert::new).contains(text);
            return this;
        }

        public RelationalExceptionAssert doesNotContainInMessage(String text) {
            extracting(RelationalException::getMessage, StringAssert::new).doesNotContain(text);
            return this;
        }
    }

    public static class SQLExceptionAssert extends AbstractThrowableAssert<SQLExceptionAssert, SQLException> {

        protected SQLExceptionAssert(SQLException e) {
            super(e, SQLExceptionAssert.class);
        }

        public SQLExceptionAssert hasErrorCode(int expected) {
            extracting(SQLException::getErrorCode, IntegerAssert::new).isEqualTo(expected);
            return this;
        }

        public SQLExceptionAssert hasErrorCode(ErrorCode errorCode) {
            extracting(se -> ErrorCode.get(se.getSQLState())).isEqualTo(errorCode);
            return this;
        }

        public SQLExceptionAssert containsInMessage(String text) {
            extracting(SQLException::getMessage, StringAssert::new).contains(text);
            return this;
        }

        public SQLExceptionAssert doesNotContainInMessage(String text) {
            extracting(SQLException::getMessage, StringAssert::new).doesNotContain(text);
            return this;
        }

    }

    private RelationalAssertions() {
    }
}
