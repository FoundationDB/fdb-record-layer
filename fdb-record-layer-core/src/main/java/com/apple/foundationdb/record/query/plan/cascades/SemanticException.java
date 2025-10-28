/*
 * SemanticException.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Semantic exceptions that could occur e.g. to illegal type conversions, ... etc.
 */
public class SemanticException extends RecordCoreException {
    private static final long serialVersionUID = 101714053557545076L;

    /**
     * Semantic error codes.
     */
    public enum ErrorCode {
        UNKNOWN(-1, "unknown reason"),

        // generic
        INCOMPATIBLE_TYPE(1, "A value cannot be assigned to a variable because the type of the value does not match the type of the variable and cannot be promoted to the type of the variable."),
        NULL_ASSIGNMENT(2, "A null value cannot be assigned to a variable that is of a non-nullable type."),
        FIELD_ACCESS_INPUT_NON_RECORD_TYPE(3, "A field is accessed on an input that is not of a record type."),
        RECORD_DOES_NOT_CONTAIN_FIELD(4, "A non-existing field is accessed in a record."),
        COMPARAND_TO_COMPARISON_IS_OF_COMPLEX_TYPE(5, "The comparand to a comparison expecting an argument of a primitive type, is invoked with an argument of a complex type, e.g. an array or a record."),
        ARGUMENT_TO_ARITHMETIC_OPERATOR_IS_OF_COMPLEX_TYPE(6, "The argument to an arithmetic operator expecting an argument of a primitive type, is invoked with an argument of a complex type, e.g. an array or a record."),
        OPERAND_OF_LIKE_OPERATOR_IS_NOT_STRING(7, "The like operator expects string operands but was invoked with an operand of another type."),
        ESCAPE_CHAR_OF_LIKE_OPERATOR_IS_NOT_SINGLE_CHAR(8, "The like operator expects an escape character of length 1."),
        FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES(9, "The function is not defined for the given argument types"),
        ORDERING_IS_OF_INCOMPATIBLE_TYPE(10, "The specified ordering expecting an argument of a primitive or record type, is invoked with an argument of an array type or other complex type."),
        ARGUMENT_TO_COLLATE_IS_OF_COMPLEX_TYPE(11, "The argument to a collate expression expecting an argument of a primitive type, is invoked with an argument of a complex type, e.g. an array or a record."),
        INVALID_ENUM_VALUE(12, "Invalid enum value for the enum type"),
        INVALID_UUID_VALUE(13, "Invalid UUID value for the UUID type"),
        INVALID_CAST(14, "Invalid cast operation"),

        // insert, update, deletes
        UPDATE_TRANSFORM_AMBIGUOUS(1_000, "The transformations used in an UPDATE statement are ambiguous."),

        //
        UNSUPPORTED(10_000, "The action is currently unsupported");

        final int numericCode;
        final String message;

        ErrorCode(final int numericCode, final String message) {
            this.numericCode = numericCode;
            this.message = message;
        }

        public int getNumericCode() {
            return numericCode;
        }

        public String getMessage() {
            return message;
        }
    }

    @Nonnull
    private final ErrorCode errorCode;
    @Nullable
    private final String additionalErrorMessage;

    private SemanticException(@Nonnull final ErrorCode errorCode, @Nullable final String additionalErrorMessage) {
        super(errorCode.getMessage() + (additionalErrorMessage == null ? "" : " " + additionalErrorMessage));
        this.errorCode = errorCode;
        this.additionalErrorMessage = additionalErrorMessage;
    }

    public SemanticException(@Nonnull final ErrorCode errorCode, @Nullable final String additionalErrorMessage, final Throwable cause) {
        super(errorCode.getMessage() + (additionalErrorMessage == null ? "" : " " + additionalErrorMessage), cause);
        this.errorCode = errorCode;
        this.additionalErrorMessage = additionalErrorMessage;
    }

    @Nonnull
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Nullable
    public String getAdditionalErrorMessage() {
        return additionalErrorMessage;
    }

    public static void check(final boolean condition, @Nonnull final ErrorCode message) {
        if (!condition) {
            throw new SemanticException(message, null);
        }
    }

    public static void check(final boolean condition, @Nonnull final ErrorCode message, @Nonnull final String additionalErrorMessage) {
        if (!condition) {
            fail(message, additionalErrorMessage);
        }
    }

    public static void fail(@Nonnull final ErrorCode message, @Nonnull final String additionalErrorMessage) {
        throw new SemanticException(message, additionalErrorMessage);
    }
}
