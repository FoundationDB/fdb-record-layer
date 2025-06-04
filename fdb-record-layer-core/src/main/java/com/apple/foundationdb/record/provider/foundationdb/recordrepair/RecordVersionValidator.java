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

package com.apple.foundationdb.record.provider.foundationdb.recordrepair;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * A record validator that ensures the record has a valid version.
 * A record has to have a valid value to be validated with this validator. It also has to exist and have a version.
 * As a general rule, the metadata has a {@link RecordMetaData#isStoreRecordVersions()} property that is used for making that decision:
 * If the metadata declares that the store does not store data then the validator will mark the record as "valid".
 */
@API(API.Status.INTERNAL)
public class RecordVersionValidator implements RecordValidator {
    private static final Logger logger = LoggerFactory.getLogger(RecordVersionValidator.class);

    @Nonnull
    private final FDBRecordStore store;

    public RecordVersionValidator(@Nonnull final FDBRecordStore store) {
        this.store = store;
    }

    @Override
    public CompletableFuture<RecordRepairResult> validateRecordAsync(@Nonnull final Tuple primaryKey) {
        // In case the metadata says to not store versions, we will not actually check to see if a version exists
        if ( ! store.getRecordMetaData().isStoreRecordVersions()) {
            return CompletableFuture.completedFuture(RecordRepairResult.valid(primaryKey));
        }

        return store.loadRecordAsync(primaryKey).thenApply(rec -> {
            if (rec == null) {
                return RecordRepairResult.invalid(primaryKey, RecordRepairResult.CODE_RECORD_MISSING_ERROR, "Record cannot be found");
            }
            if (!rec.hasVersion()) {
                return RecordRepairResult.invalid(primaryKey, RecordRepairResult.CODE_VERSION_MISSING_ERROR, "Record version is missing");
            }
            return RecordRepairResult.valid(primaryKey);
        });
    }

    @Override
    public CompletableFuture<RecordRepairResult> repairRecordAsync(@Nonnull final RecordRepairResult validationResult) {
        if (validationResult.isValid()) {
            // do nothing
            return CompletableFuture.completedFuture(validationResult.withRepair(RecordRepairResult.REPAIR_NOT_NEEDED));
        }
        switch (validationResult.getErrorCode()) {
            case RecordRepairResult.CODE_RECORD_MISSING_ERROR:
                // Nothing to do
                return CompletableFuture.completedFuture(validationResult.withRepair(RecordRepairResult.REPAIR_NOT_NEEDED));
            case RecordRepairResult.CODE_VERSION_MISSING_ERROR:
                if (logger.isDebugEnabled()) {
                    logger.debug(KeyValueLogMessage.of("Record repair: Version created",
                            LogMessageKeys.PRIMARY_KEY, validationResult.getPrimaryKey(),
                            LogMessageKeys.CODE, validationResult.getErrorCode()));
                }
                // Create a new version
                // Save the record with the existing data and the new version
                // This uses the DEFAULT behavior to follow the metadata direction on whether to store the version
                final FDBRecordVersion newVersion = FDBRecordVersion.incomplete(store.getContext().claimLocalVersion());
                return store.loadRecordAsync(validationResult.getPrimaryKey())
                        .thenCompose(rec ->
                                store.saveRecordAsync(rec.getRecord(), newVersion, FDBRecordStoreBase.VersionstampSaveBehavior.DEFAULT))
                        .thenApply(ignore ->
                                validationResult.withRepair(RecordRepairResult.REPAIR_VERSION_CREATED));
            default:
                // Unknown code
                if (logger.isWarnEnabled()) {
                    logger.warn(KeyValueLogMessage.of("Record version repair: Unknown code",
                            LogMessageKeys.PRIMARY_KEY, validationResult.getPrimaryKey(),
                            LogMessageKeys.CODE, validationResult.getErrorCode()));
                }
                return CompletableFuture.completedFuture(validationResult.withRepair(RecordRepairResult.REPAIR_UNKNOWN_VALIDATION_CODE));
        }
    }
}
