/*
 * RecordVersionValidator.java
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

package com.apple.foundationdb.record.provider.foundationdb.recordvalidation;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

@API(API.Status.UNSTABLE)
public class RecordVersionValidator implements RecordValidator {
    public static final String CODE_VERSION_MISSING_ERROR = "VersionMissingError";
    public static final String CODE_RECORD_MISSING_ERROR = "RecordMissingError";

    @Nonnull
    private FDBRecordStore store;

    public RecordVersionValidator(@Nonnull final FDBRecordStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<RecordValidationResult> validateRecordAsync(final Tuple primaryKey) {
        return store.loadRecordAsync(primaryKey).thenApply(rec -> {
            if (rec == null) {
                return RecordValidationResult.invalid(primaryKey, CODE_RECORD_MISSING_ERROR, "Record cannot be found");
            }
            if (!rec.hasVersion()) {
                return RecordValidationResult.invalid(primaryKey, CODE_VERSION_MISSING_ERROR, "Record version is missing");
            }
            return RecordValidationResult.valid(primaryKey);
        });
    }

    @Override
    public CompletableFuture<Void> repairRecordAsync(final Tuple primaryKey, final CompletableFuture<RecordValidationResult> validationResult) {
        throw new UnsupportedOperationException("Repair is not yet supported");
    }
}
