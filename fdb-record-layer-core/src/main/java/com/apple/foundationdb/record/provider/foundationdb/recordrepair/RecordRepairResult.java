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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A record validation and repair result.
 * This would be returned from a {@link RecordRepairRunner#runValidationAndRepair(FDBRecordStore.Builder, RecordRepairRunner.ValidationKind, boolean)} call with the results of the validation
 * operation. The results should have the following fields populated:
 * <ul>
 *     <li>primaryKey: The key of the record validated</li>
 *     <li>isValid: an overall result of the validation operation</li>
 *     <li>errorCode: A unique key representing the result of the validation. Use {@link #CODE_VALID} for valid result</li>
 *     <li>message: A message describing the result of the validation</li>
 *     <li>isRepaired: whether an attempt to repair the record was made</li>
 *     <li>repairCode (Optional): The repair action taken</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class RecordRepairResult {
    /** (error code) Validation did not find any issue with the record. */
    public static final String CODE_VALID = "Valid";
    /** (error code) Record splits issue (missing split/out of order). */
    public static final String CODE_SPLIT_ERROR = "RecordValueSplitError";
    /** (error code) Record could not be serialized (corrupt data, missing split). */
    public static final String CODE_DESERIALIZE_ERROR = "RecordValueDeserializeError";
    /** (error code) Record version is missing. */
    public static final String CODE_VERSION_MISSING_ERROR = "RecordVersionMissingError";
    /** (error code) Record is missing (so cannot verify version existence). */
    public static final String CODE_RECORD_MISSING_ERROR = "RecordMissingError";

    /** (repair code) Repair attempted for the record but the record was valid, so no action was taken. */
    public static final String REPAIR_NOT_NEEDED = "RepairNotNeeded";
    /** (repair code) Repair was attempted for the record but the validation error code was not recognized. */
    public static final String REPAIR_UNKNOWN_VALIDATION_CODE = "UnknownCode";
    /** (repair code) Record data was deleted as a repair. */
    public static final String REPAIR_RECORD_DELETED = "RecordValueRecordDeletedRepair";
    /** (repair code) Record version was created and added to the record. */
    public static final String REPAIR_VERSION_CREATED = "RecordVersionCreatedRepair";

    @Nonnull
    private final Tuple primaryKey;
    private final boolean isValid;
    @Nonnull
    private final String errorCode;
    @Nullable
    private final String message;
    private final boolean isRepaired;
    @Nullable
    private final String repairCode;

    private RecordRepairResult(@Nonnull final Tuple primaryKey, final boolean isValid, @Nonnull final String errorCode, @Nullable final String message) {
        this(primaryKey, isValid, errorCode, message, false, null);
    }

    private RecordRepairResult(@Nonnull final Tuple primaryKey, final boolean isValid, @Nonnull final String errorCode, @Nullable final String message, boolean isRepaired, String repairCode) {
        this.primaryKey = primaryKey;
        this.isValid = isValid;
        this.errorCode = errorCode;
        this.message = message;
        this.isRepaired = isRepaired;
        this.repairCode = repairCode;
    }

    public static RecordRepairResult valid(Tuple primaryKey) {
        return new RecordRepairResult(primaryKey, true, CODE_VALID, null);
    }

    public static RecordRepairResult invalid(Tuple primaryKey, String error, String message) {
        return new RecordRepairResult(primaryKey, false, error, message);
    }

    @Nonnull
    public RecordRepairResult withRepair(@Nonnull String repairCode) {
        return new RecordRepairResult(primaryKey, isValid, errorCode, message, true, repairCode);
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

    public boolean isRepaired() {
        return isRepaired;
    }

    @Nullable
    public String getRepairCode() {
        return repairCode;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordRepairResult)) {
            return false;
        }
        final RecordRepairResult that = (RecordRepairResult)o;
        return isValid == that.isValid && isRepaired == that.isRepaired && Objects.equals(primaryKey, that.primaryKey) && Objects.equals(errorCode, that.errorCode) && Objects.equals(repairCode, that.repairCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKey, isValid, errorCode, isRepaired, repairCode);
    }

    @Override
    public String toString() {
        return "RecordValidationResult{" +
                "primaryKey=" + primaryKey +
                ", isValid=" + isValid +
                ", errorCode='" + errorCode + '\'' +
                ", message='" + message + '\'' +
                ", isRepaired=" + isRepaired +
                ", repairCode='" + repairCode + '\'' +
                '}';
    }
}
