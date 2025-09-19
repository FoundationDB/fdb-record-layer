/*
 * ErrorCode.java
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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
 * 02 | No Data
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
 * First, look at <a href="https://en.wikipedia.org/wiki/SQLSTATE">Wikipedia</a> to see if the standard has already
 * defined an error code that would be applicable for your purpose. If so, use that one (this means that we
 * are in-line with published SQL codes whenever possible). However, often you'll find that the standard does
 * not have an error code that matches what you're trying to do. In that case, choose a class from
 * the above table, and then define a unique code.
 * Newly introduced error codes follow the pattern "..F..". For example: 08F01
 * */
public enum ErrorCode {
    // Class 00 - Successful Completion
    SUCCESS("00000"),

    // Class 02 - No data
    NO_RESULT_SET("02F01"),

    // Class 08 - Connection Exception
    UNABLE_TO_ESTABLISH_SQL_CONNECTION("08001"),
    CONNECTION_DOES_NOT_EXIST("08003"),
    INVALID_PATH("08F01"),
    CANNOT_COMMIT_ROLLBACK_WITH_AUTOCOMMIT("08F02"),

    // Class 0A - Feature not supported
    UNSUPPORTED_OPERATION("0A000"),
    UNSUPPORTED_QUERY("0AF00"),

    // Class 22 - Data Exception
    CANNOT_CONVERT_TYPE("22000"),
    INVALID_ROW_COUNT_IN_LIMIT_CLAUSE("2201W"),
    INVALID_PARAMETER("22023"),
    ARRAY_ELEMENT_ERROR("2202E"),
    INVALID_BINARY_REPRESENTATION("22F03"),
    INVALID_ARGUMENT_FOR_FUNCTION("22F00"),

    // Class 23 - Integrity Constraint Violation
    NOT_NULL_VIOLATION("23502"),
    UNIQUE_CONSTRAINT_VIOLATION("23505"),

    // Class 24 - Invalid Cursor State
    INVALID_CURSOR_STATE("24000"),
    INVALID_CONTINUATION("24F00"),

    // Class 25 - Invalid Transaction State
    INVALID_TRANSACTION_STATE("25000"),
    TRANSACTION_INACTIVE("25F01"),

    // Class 40 - Transaction Rollback
    SERIALIZATION_FAILURE("40001"),

    // Class 42 - Syntax Error or Access Rule Violation
    SYNTAX_OR_ACCESS_VIOLATION("42000"),
    INSUFFICIENT_PRIVILEGE("42501"),
    SYNTAX_ERROR("42601"),
    INVALID_NAME("42602"),
    COLUMN_ALREADY_EXISTS("42701"),
    AMBIGUOUS_COLUMN("42702"),
    UNDEFINED_COLUMN("42703"),
    DUPLICATE_ALIAS("42712"),
    DUPLICATE_FUNCTION("42723"),
    GROUPING_ERROR("42803"),
    DATATYPE_MISMATCH("42804"),
    WRONG_OBJECT_TYPE("42809"),
    UNDEFINED_FUNCTION("42883"),
    UNDEFINED_DATABASE("42F00"),
    UNDEFINED_TABLE("42F01"),
    UNDEFINED_PARAMETER("42F02"),
    DATABASE_ALREADY_EXISTS("42F04"),
    SCHEMA_ALREADY_EXISTS("42F06"),
    TABLE_ALREADY_EXISTS("42F07"),
    INVALID_COLUMN_REFERENCE("42F10"), //no field of specified name in the result set
    INVALID_FUNCTION_DEFINITION("42F13"),
    INVALID_TABLE_DEFINITION("42F16"),
    UNKNOWN_TYPE("42F18"),
    INVALID_RECURSION("42F19"),
    INCOMPATIBLE_TABLE_ALIAS("42F20"),
    /**
     * Indicates that a schema with the given name is already mapped to a schema template.
     */
    SCHEMA_MAPPING_ALREADY_EXISTS("42F50"),
    UNDEFINED_SCHEMA("42F51"),
    UNDEFINED_INDEX("42F54"),
    UNKNOWN_SCHEMA_TEMPLATE("42F55"),
    ANNOTATION_ALREADY_EXISTS("42F56"),
    INDEX_ALREADY_EXISTS("42F57"),
    INCORRECT_METADATA_TABLE_VERSION("42F58"),
    INVALID_SCHEMA_TEMPLATE("42F59"),
    INVALID_PREPARED_STATEMENT_PARAMETER("42F60"),
    EXECUTE_UPDATE_RETURNED_RESULT_SET("42F61"),
    DUPLICATE_SCHEMA_TEMPLATE("42F62"),
    UNKNOWN_DATABASE("42F63"),
    UNION_INCORRECT_COLUMN_COUNT("42F64"),
    UNION_INCOMPATIBLE_COLUMNS("42F65"),
    INVALID_DATABASE("42F66"),
    // Class 53 - Insufficient Resources
    TRANSACTION_TIMEOUT("53F00"),
    // Class 54 Program Limit Exceeded
    TOO_MANY_COLUMNS("54011"),
    EXECUTION_LIMIT_REACHED("54F01"),

    // Class 55 - Object Not In Prerequisite State
    STATEMENT_CLOSED("55F00"),

    // Class 58 - System Error
    UNDEFINED_FILE("58F01"),

    // Class XX - Internal Error
    /**
     * Used to represent an error that we don't know more details about. This is a backup in case
     * the error handling system can't find a more accurate representation, and shouldn't be used in general.
     */
    UNKNOWN("XXXXX"),
    /**
     * Used for the exceptions due to internal issue of Relational, which is caused by neither clients nor upstream
     * system.
     */
    INTERNAL_ERROR("XX000"),
    DESERIALIZATION_FAILURE("XXF01");

    private final String errorCode;

    private static final Map<String, ErrorCode> ENUM_MAP = Stream.of(ErrorCode.values())
            .collect(Collectors.toUnmodifiableMap(ErrorCode::getErrorCode, Function.identity()));

    ErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public static ErrorCode get(String errorCode) {
        if (errorCode == null) {
            return ErrorCode.UNKNOWN;
        }
        return ENUM_MAP.getOrDefault(errorCode, ErrorCode.UNKNOWN);
    }

    public String getErrorCode() {
        return errorCode;
    }
}
