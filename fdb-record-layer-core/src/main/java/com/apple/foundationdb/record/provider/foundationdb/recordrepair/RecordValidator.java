/*
 * RecordValidator.java
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
import java.util.concurrent.CompletableFuture;

/**
 * An interface to be implemented by record validation and repair operators.
 * A Record Validator should be able to perform two functions: Validate a record (given a primary key) and repair a broken
 * record. The intent for these is to be done in sequence, that is, a {@link #validateRecordAsync(Tuple)} call is to be
 * followed by an optional call to {@link #repairRecordAsync(RecordValidationResult)}. These are separate methods in
 * order to allow a "dry run" such that validate alone is called as well as to allow specialization by subclassing
 * the validator and overriding the {@link #repairRecordAsync(RecordValidationResult)} or {@link #validateRecordAsync(Tuple)}
 * methods.
 * <p>
 * Each call to {@link #validateRecordAsync(Tuple)} will return a {@link RecordValidationResult} that has an error code
 * {@link RecordValidationResult#getErrorCode()}. When the error code is {@link RecordValidationResult#CODE_VALID} the
 * {@link RecordValidationResult#isValid()} should return {@code true}. When the {@link RecordValidationResult#isValid()}
 * returns false, the error code is validator dependant.
 * <p>
 * Each call to {@link #repairRecordAsync(RecordValidationResult)} takes a {@link RecordValidationResult}, presumably
 * returned by a previous call to {@link #validateRecordAsync(Tuple)}. Mixing results among different validators may result
 * in unpredictable behavior.
 * Following a call to {@link #repairRecordAsync(RecordValidationResult)} the result's {@link RecordValidationResult#isRepaired()}
 * should return {@code true} and the {@link RecordValidationResult#getRepairCode()} should return information about the repair.
 * <p>
 * Ideally, the two calls (validate and repair) would be called within the same transaction. Spanning them across transactions
 * may allow the database state to change in between the calls and create a repair operation that is incorrect.
 * Do not store validations results across transactions.
 *
 */
@API(API.Status.INTERNAL)
public interface RecordValidator {
    /**
     * Validate a record with the given primary key.
     * @param primaryKey the primary key of the record
     * @return a future to be completed with the validation result
     */
    CompletableFuture<RecordValidationResult> validateRecordAsync(@Nonnull Tuple primaryKey);

    /**
     * Repair a record based on the previously executed validation.
     * @param validationResult the result of the previously executed validation
     * @return a future to be completed with the repair result
     */
    CompletableFuture<RecordValidationResult> repairRecordAsync(@Nonnull RecordValidationResult validationResult);
}
