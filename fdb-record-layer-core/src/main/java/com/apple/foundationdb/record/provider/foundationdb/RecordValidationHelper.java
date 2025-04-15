/*
 * RecordValidationHelper.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

// TODO: Add logs

public class RecordValidationHelper {
    public static CompletableFuture<EnumSet<FDBRecordStoreBase.RecordValidationOptions>> validateRecordAsync(
            final FDBRecordStore store,
            final Tuple primaryKey,
            final EnumSet<FDBRecordStoreBase.RecordValidationOptions> options,
            final boolean allowRepair) {

        if (allowRepair) {
            throw new UnsupportedOperationException("Allow repair is not yet supported");
        }

        // TODO: Do we want to load the raw record and deserialize separately?
        return store.loadRecordAsync(primaryKey).handle((rec, exception) -> {
            EnumSet<FDBRecordStoreBase.RecordValidationOptions> result = EnumSet.noneOf(FDBRecordStoreBase.RecordValidationOptions.class);
            if (exception != null) {
                if (exception instanceof CompletionException) {
                    exception = exception.getCause();
                }
                if ((exception instanceof SplitHelper.FoundSplitWithoutStartException) ||
                        (exception instanceof SplitHelper.FoundSplitOutOfOrderException) ||
                        (exception instanceof FDBRecordStore.RecordDeserializationException)) {
                    if (options.contains(FDBRecordStoreBase.RecordValidationOptions.VALID_VALUE)) {
                        result.add(FDBRecordStoreBase.RecordValidationOptions.VALID_VALUE);
                    }
                } else {
                    throw new UnknownValidationException("Unknown exception caught", exception);
                }
            } else {
                if (rec == null) {
                    if (options.contains(FDBRecordStoreBase.RecordValidationOptions.RECORD_EXISTS)) {
                        result.add(FDBRecordStoreBase.RecordValidationOptions.RECORD_EXISTS);
                    }
                } else if ( ! rec.hasVersion()) {
                    if (options.contains(FDBRecordStoreBase.RecordValidationOptions.VALID_VERSION)) {
                        result.add(FDBRecordStoreBase.RecordValidationOptions.VALID_VERSION);
                    }
                }
            }
            return result;
        });
    }

    @SuppressWarnings({"serial"})
    public static class UnknownValidationException extends RecordCoreException {
        public UnknownValidationException(@Nonnull final String msg, @Nullable final Throwable cause) {
            super(msg, cause);
        }
    }
}
