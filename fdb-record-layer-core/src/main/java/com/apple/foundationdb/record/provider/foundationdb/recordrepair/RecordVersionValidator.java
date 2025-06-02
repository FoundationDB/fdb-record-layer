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
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * A record validator that ensures the record has a valid version.
 * A record has to have a valid value to be validated with this validator. It also has to exist and have a version.
 * Since version creation for a record can be done on a per-record basis (when the record is saved), it is the responsibility
 * of the user of the validator to decide whether a version should be present or not. As a general rule, the metadata
 * has a {@link RecordMetaData#isStoreRecordVersions()} property that is used as the default value for making that decision,
 * but the {@link FDBRecordStore#saveRecordAsync(Message, FDBRecordVersion, FDBRecordStoreBase.VersionstampSaveBehavior)}
 * can override this.
 * Once this validator is used, it assumes that versions are to be saved with the records and will flag a record that has
 * no version. The repair operation will create a new version for that record.
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
                if (logger.isInfoEnabled()) {
                    logger.info(KeyValueLogMessage.of("Record repair: Version created",
                            LogMessageKeys.PRIMARY_KEY, validationResult.getPrimaryKey(),
                            LogMessageKeys.CODE, validationResult.getErrorCode()));
                }
                // Create a new version
                // Save the record with the existing data and the new version
                // This uses the WITH_VERSION behavior to force a version even if the metadata says otherwise, since this
                // is the assumption of the validator
                final FDBRecordVersion newVersion = FDBRecordVersion.incomplete(store.getContext().claimLocalVersion());
                return store.loadRecordAsync(validationResult.getPrimaryKey())
                        .thenCompose(rec ->
                                store.saveRecordAsync(rec.getRecord(), newVersion, FDBRecordStoreBase.VersionstampSaveBehavior.WITH_VERSION))
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
