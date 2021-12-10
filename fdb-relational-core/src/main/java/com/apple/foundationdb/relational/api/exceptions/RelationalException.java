/*
 * RelationalException.java
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

public class RelationalException extends RuntimeException {
    private final ErrorCode errorCode;

    public RelationalException(String message, ErrorCode errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public RelationalException(String message, ErrorCode errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public RelationalException(ErrorCode errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }

    public static RelationalException convert(Throwable re) {
        if (re instanceof RelationalException) {
            return (RelationalException) re;
        }
        return new RelationalException(ErrorCode.UNKNOWN, re);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * An enumeration form of the different error codes that Relational makes use of.
     * <p>
     * Error codes in Relational follow the SQLSTATE format wherever possible (to maximize interoperability and
     * ease of understanding). To that end, all error codes are 5-digit codes, where the first 2 characters are the
     * Class of the error, and the remaining 3 characters describe the error itself.
     * <p>
     * It is important to note that not every error in Relational directly translates to a known standard SQLSTATE
     * code. In those cases, you'll want to look here for the technical definition of those error codes.
     * <p>
     * The Class codes are as follows:
     * <p>
     * 00 | Success
     * 01 | Warning
     * 08 | Connection Exception
     * 0A | Unsupported Operation
     * 22 | Data exception
     * 23 | Integrity Constraint Violation
     * 25 | Transaction State
     * 40 | Transaction Rollback
     * 42 | Syntax Error
     * 53 | Insufficient Resources
     * 58 | System Error (errors external to Relational)
     * XX | Internal Error
     * <p>
     * To add a new Error Code:
     * <p>
     * First, look at https://en.wikipedia.org/wiki/SQLSTATE to see if the standard has already
     * defined an error code that would be applicable for your purpose. If so, use that one (this means that we
     * are in-line with published SQL codes whenever possible). However, often you'll find that the standard does
     * not have an error code that matches what you're trying to do. In that case, choose a class from
     * the above table, and then define a unique code--do not define a duplicate!
     */
    public enum ErrorCode {
        /**
         * Indicates a successful operation.
         */
        SUCCESS("00000"),
        UNSUPPORTED_OPERATION("0A000"),
        CANNOT_CONVERT_TYPE("22000"),
        INVALID_PARAMETER("22023"),
        UNIQUE_CONSTRAINT_VIOLATION("23505"),
        INVALID_CURSOR_STATE("24000"),
        TRANSACTION_INACTIVE("25F01"),
        SCHEMA_NOT_FOUND("4200Q"),
        UNDEFINED_TABLE("42F01"),
        UNDEFINED_DATABASE("42F02"),
        UNKNOWN_SCHEMA("42P03"),
        UNKNOWN_INDEX("42F04"),
        UNKNOWN_SCHEMA_TEMPLATE("42P05"),
        /**
         * Indicates that a schema with the given name is already mapped to a schema template.
         */
        SCHEMA_MAPPING_ALREADY_EXISTS("42F06"),
        /**
         * Indicates that a query has timed out during execution.
         */
        INVALID_COLUMN_REFERENCE("42F10"), //no field of specified name in result set
        CANNOT_CONVERT_TO_MESSAGE("42P12"), //cannot convert a ResultRow to a Message object
        INVALID_PATH("42PAT"),
        QUERY_TIMEOUT("53QTO"),
        /**
         * Used to represent an error that we don't know more details about. This is a backup in case
         * the error handling system can't find a more accurate representation, and shouldn't be used in general.
         */
        UNKNOWN("XXXXX"),
        UNKNOWN_SCHEME("08001"),
        /**
         * Used for the exceptions due to internal issue of Relational, which is caused by neither clients nor upstream
         * system.
         */
        INTERNAL_ERROR("XX000");

        private final String errorCode;

        ErrorCode(String errorCode) {
            this.errorCode = errorCode;
        }

        String getCodeString() {
            return errorCode;
        }
    }
}
