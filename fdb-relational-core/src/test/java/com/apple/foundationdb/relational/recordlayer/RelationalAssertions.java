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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

import java.sql.SQLException;

public final class RelationalAssertions {
    public static <T extends Throwable> void assertThrowsSqlException(Executable executable, ErrorCode expectedError) {
        SQLException exception = Assertions.assertThrows(SQLException.class, executable);
        Assertions.assertEquals(expectedError.getErrorCode(), exception.getSQLState(),
                String.format("Invalid SQL State. Expected: %s but was: %s", prettySqlState(expectedError.getErrorCode()), prettySqlState(exception.getSQLState())));
    }

    public static <T extends Throwable> void assertThrowsRelationalException(Executable executable, ErrorCode expectedErrorCode) {
        RelationalException exception = Assertions.assertThrows(RelationalException.class, executable);
        Assertions.assertEquals(expectedErrorCode, exception.getErrorCode(),
                String.format("Invalid Error Code. Expected: %s but was: %s", prettySqlState(expectedErrorCode.getErrorCode()), prettySqlState(exception.getErrorCode().name())));
    }

    private static String prettySqlState(String state) {
        ErrorCode errorCode = ErrorCode.get(state);
        if (errorCode == null) {
            return String.format("UnknownErrorCode<%s>", state);
        } else {
            return String.format("%s<%s>", errorCode.name(), state);
        }

    }

    private RelationalAssertions() {
    }
}
