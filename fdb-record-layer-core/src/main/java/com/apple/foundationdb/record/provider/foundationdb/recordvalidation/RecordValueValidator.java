/*
 * RecordValueValidator.java
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
import com.apple.foundationdb.record.provider.foundationdb.RecordDeserializationException;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@API(API.Status.UNSTABLE)
public class RecordValueValidator implements RecordValidator {
    public static final String CODE_SPLIT_ERROR = "SplitError";
    public static final String CODE_DESERIALIZE_ERROR = "DeserializeError";

    @Nonnull
    private FDBRecordStore store;

    public RecordValueValidator(@Nonnull final FDBRecordStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<RecordValidationResult> validateRecordAsync(final Tuple primaryKey) {
        return store.loadRecordAsync(primaryKey).handle((rec, exception) -> {
            if (exception != null) {
                if (exception instanceof CompletionException) {
                    exception = exception.getCause();
                }
                if (exception instanceof SplitHelper.FoundSplitWithoutStartException) {
                    return RecordValidationResult.invalid(primaryKey, CODE_SPLIT_ERROR, "Found split record without start");
                }
                if (exception instanceof SplitHelper.FoundSplitOutOfOrderException) {
                    return RecordValidationResult.invalid(primaryKey, CODE_SPLIT_ERROR, "Split record segments out of order");
                }
                if (exception instanceof RecordDeserializationException) {
                    return RecordValidationResult.invalid(primaryKey, CODE_DESERIALIZE_ERROR, "Record cannot be deseralized");
                }
                throw new UnknownValidationException("Unknown exception caught", exception);
            } else {
                return RecordValidationResult.valid(primaryKey);
            }
        });
    }

    @Override
    public CompletableFuture<Void> repairRecordAsync(final Tuple primaryKey, final CompletableFuture<RecordValidationResult> validationResult) {
        throw new UnsupportedOperationException("Repair is not yet supported");
    }
}
