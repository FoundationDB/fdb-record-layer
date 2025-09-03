/*
 * RepairValidationResults.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Repair results for the call to {@link RecordRepairValidateRunner#run}.
 * Holds the results of the execution of the validation and repair operation.
 * Note that the result may be incomplete: If the call to run() restricted the number of results returned
 * (via {@link RecordRepair.Builder#withMaxResultsReturned(int)} - e.g. to control the size of the list returned)
 * then this list would contain at most that many results and another call may be necessary to continue iterating through
 * the records.
 */
@API(API.Status.EXPERIMENTAL)
public class RepairValidationResults {
    private final boolean isComplete;
    @Nullable
    private final Throwable caughtException;
    @Nonnull
    private final List<RecordRepairResult> invalidResults;
    private final int validResultCount;

    public RepairValidationResults(final boolean isComplete, @Nullable final Throwable caughtException, @Nonnull final List<RecordRepairResult> invalidResults, final int validResultCount) {
        this.isComplete = isComplete;
        this.caughtException = caughtException;
        this.invalidResults = invalidResults;
        this.validResultCount = validResultCount;
    }

    /**
     * Whether the repair operation completed the iteration through all the records or not.
     * The operation can return before completing the iteration either in the case of an error (in which case the
     * {@link #caughtException} will be set or by reaching the maximum number of results allowed.
     * @return TRUE if the operation completed iterating through all the records.
     */
    public boolean isComplete() {
        return isComplete;
    }

    /**
     * The exception caught by the operation, if any.
     * @return the throwable caught by the operation, if any.
     */
    @Nullable
    public Throwable getCaughtException() {
        return caughtException;
    }

    /**
     * The list of validation and repair results for all records that results in non-valid validation code.
     * Note that this list can be limited in size by calling {@link RecordRepair.Builder#withMaxResultsReturned(int)}
     * @return the list of record validation results that ended up with non-valid code.
     */
    @Nonnull
    public List<RecordRepairResult> getInvalidResults() {
        return invalidResults;
    }

    /**
     * Return the number of record validation results that had a valid return code during the repair run.
     * This returns the valid count only. Under normal circumstances, this number will be much greater than the number of non-valid
     * results, and a count only will reduce resource usage.
     * @return the number of records that returned a valid results during the repair run.
     */
    public int getValidResultCount() {
        return validResultCount;
    }
}
