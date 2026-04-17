/*
 * StructTypeValidator.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Utility class for validating struct type compatibility.
 * Provides centralized logic for comparing struct types, with support for
 * ignoring nullability differences and recursive validation of nested structs.
 */
@API(API.Status.EXPERIMENTAL)
public final class StructTypeValidator {

    private StructTypeValidator() {
        // Utility class - prevent instantiation
    }

    /**
     * Check if two struct types are compatible, ignoring nullability differences.
     * Two struct types are considered compatible if:
     * - They have the same number of fields
     * - Each corresponding field has the same type code (ignoring nullability)
     * - If recursive=true, nested struct fields are recursively validated
     *
     * @param expected The expected struct type
     * @param provided The provided struct type
     * @param recursive If true, recursively validate nested struct types
     * @return true if the struct types are compatible, false otherwise
     */
    public static boolean areStructTypesCompatible(@Nonnull DataType.StructType expected,
                                                   @Nonnull DataType.StructType provided,
                                                   boolean recursive) {
        final var expectedFields = expected.getFields();
        final var providedFields = provided.getFields();

        // Check field count
        if (!Integer.valueOf(expectedFields.size()).equals(providedFields.size())) {
            return false;
        }

        // Check each field type
        for (int i = 0; i < expectedFields.size(); i++) {
            final var expectedFieldType = expectedFields.get(i).getType();
            final var providedFieldType = providedFields.get(i).getType();

            // Compare type codes (ignoring nullability)
            if (!expectedFieldType.getCode().equals(providedFieldType.getCode())) {
                return false;
            }

            // Recursively validate nested structs if requested
            if (recursive && expectedFieldType instanceof DataType.StructType && providedFieldType instanceof DataType.StructType) {
                if (!areStructTypesCompatible((DataType.StructType) expectedFieldType,
                        (DataType.StructType) providedFieldType,
                        true)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Validate that two struct types are compatible, throwing an exception if they are not.
     * This is a wrapper around {@link #areStructTypesCompatible} that throws an exception
     * with a detailed error message if the types are incompatible.
     *
     * @param expected The expected struct type
     * @param provided The provided struct type
     * @param structName The name of the struct being validated (for error messages)
     * @param recursive If true, recursively validate nested struct types
     * @throws com.apple.foundationdb.relational.api.exceptions.RelationalException if the types are incompatible
     */
    public static void validateStructTypesCompatible(@Nonnull DataType.StructType expected,
                                                     @Nonnull DataType.StructType provided,
                                                     @Nonnull String structName,
                                                     boolean recursive) {
        final var expectedFields = expected.getFields();
        final var providedFields = provided.getFields();

        // Check field count
        if (!Integer.valueOf(expectedFields.size()).equals(providedFields.size())) {
            Assert.failUnchecked(ErrorCode.CANNOT_CONVERT_TYPE,
                    String.format(Locale.ROOT,
                            "Struct type '%s' has incompatible signatures: expected %d fields but got %d fields",
                            structName, expectedFields.size(), providedFields.size()));
        }

        // Check each field type
        for (int i = 0; i < expectedFields.size(); i++) {
            final var expectedFieldType = expectedFields.get(i).getType();
            final var providedFieldType = providedFields.get(i).getType();

            // Compare type codes (ignoring nullability)
            if (!expectedFieldType.getCode().equals(providedFieldType.getCode())) {
                Assert.failUnchecked(ErrorCode.CANNOT_CONVERT_TYPE,
                        String.format(Locale.ROOT,
                                "Struct type '%s' has incompatible field at position %d: expected %s but got %s",
                                structName, i + 1, expectedFieldType.getCode(), providedFieldType.getCode()));
            }

            // Recursively validate nested structs if requested
            if (recursive && expectedFieldType instanceof DataType.StructType && providedFieldType instanceof DataType.StructType) {
                // StructType extends Named, so we can always get the name
                final var expectedStructName = ((DataType.StructType) expectedFieldType).getName();
                validateStructTypesCompatible((DataType.StructType) expectedFieldType,
                        (DataType.StructType) providedFieldType,
                        expectedStructName,
                        true);
            }
        }
    }
}
