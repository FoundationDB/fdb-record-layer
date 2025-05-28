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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.RecordDeserializationException;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A record validator that can handle value deserialization.
 * This validator will ensure that the record pointed to by the primary key has a valid value.
 * <p>
 * Note that an empty record is valid that way.
 * <p>
 * A record that is valid according to this validator has a split set that is legal (either 0 or 1..n) - or is not split -
 * and a payload that can be serialized with the store's schema.
 */
@API(API.Status.INTERNAL)
public class RecordValueValidator implements RecordValidator {
    public static final String CODE_SPLIT_ERROR = "RecordValueSplitError";
    public static final String CODE_DESERIALIZE_ERROR = "RecordValueDeserializeError";
    public static final String REPAIR_RECORD_DELETED = "RecordValueRecordDeletedRepair";

    private static final Logger logger = LoggerFactory.getLogger(RecordValueValidator.class);

    @Nonnull
    private final FDBRecordStore store;

    public RecordValueValidator(@Nonnull final FDBRecordStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<RecordValidationResult> validateRecordAsync(@Nonnull final Tuple primaryKey) {
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
                if (exception instanceof RecordCoreException) {
                    // In order to facilitate error handling for out of band (and other known errors) by the caller, allow
                    // RecordCoreExceptions to flow through
                    throw (RecordCoreException)exception;
                }
                throw new UnknownValidationException("Unknown exception caught", exception);
            } else {
                return RecordValidationResult.valid(primaryKey);
            }
        });
    }

    @Override
    public CompletableFuture<RecordValidationResult> repairRecordAsync(@Nonnull RecordValidationResult validationResult) {
        if (validationResult.isValid()) {
            // do nothing
            return CompletableFuture.completedFuture(validationResult.withRepair(RecordValidationResult.REPAIR_NOT_NEEDED));
        }
        switch (validationResult.getErrorCode()) {
            case CODE_SPLIT_ERROR:
            case CODE_DESERIALIZE_ERROR:
                // Delete record subspace
                store.deleteRecordSplits(validationResult.getPrimaryKey(), false, null, store.getRecordMetaData());
                if (logger.isInfoEnabled()) {
                    logger.info(KeyValueLogMessage.of("Record repair: Record deleted", LogMessageKeys.PRIMARY_KEY, validationResult.getPrimaryKey()));
                }
                return CompletableFuture.completedFuture(validationResult.withRepair(REPAIR_RECORD_DELETED));
            default:
                // Unknown code
                if (logger.isWarnEnabled()) {
                    logger.warn(KeyValueLogMessage.of("Record repair: Unknown code",
                            LogMessageKeys.PRIMARY_KEY, validationResult.getPrimaryKey(),
                            LogMessageKeys.CODE, validationResult.getErrorCode()));
                }
                return CompletableFuture.completedFuture(validationResult.withRepair(RecordValidationResult.REPAIR_UNKNOWN_VALIDATION_CODE));
        }
    }
}
