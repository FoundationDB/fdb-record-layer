/*
 * RecordValidationResult.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A record validation result.
 * This would be returned from a {@link RecordValidator#validateRecordAsync(Tuple)} call with the results of the validation
 * operation. The results should have the following fields populated:
 * <ul>
 *     <li>primaryKey: The key of the record validated</li>
 *     <li>isValid: an overall result of the validation operation</li>
 *     <li>errorCode: A unique key representing the result of the validation. Use {@link #CODE_VALID} for valid result</li>
 *     <li>message: A message describing the result of the validation</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class RecordValidationResult {
    public static final String CODE_VALID = "valid";

    @Nonnull
    private final Tuple primaryKey;
    private final boolean isValid;
    @Nonnull
    private final String errorCode;
    @Nullable
    private final String message;

    private RecordValidationResult(@Nonnull final Tuple primaryKey, final boolean isValid, @Nonnull final String errorCode, @Nullable final String message) {
        this.primaryKey = primaryKey;
        this.isValid = isValid;
        this.errorCode = errorCode;
        this.message = message;
    }

    public static RecordValidationResult valid(Tuple primaryKey) {
        return new RecordValidationResult(primaryKey, true, CODE_VALID, null);
    }

    public static RecordValidationResult invalid(Tuple primaryKey, String error, String message) {
        return new RecordValidationResult(primaryKey, false, error, message);
    }

    @Nonnull
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    public boolean isValid() {
        return isValid;
    }

    @Nonnull
    public String getErrorCode() {
        return errorCode;
    }

    @Nullable
    public String getMessage() {
        return message;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordValidationResult)) {
            return false;
        }
        final RecordValidationResult that = (RecordValidationResult)o;
        return isValid == that.isValid && Objects.equals(primaryKey, that.primaryKey) && Objects.equals(errorCode, that.errorCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKey, isValid, errorCode);
    }
}
