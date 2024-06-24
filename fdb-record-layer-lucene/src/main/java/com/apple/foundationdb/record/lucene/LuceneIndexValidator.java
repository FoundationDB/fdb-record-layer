/*
 * LuceneIndexMaintainerValidator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Validator for Lucene indexes.
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneIndexValidator extends IndexValidator {
    public LuceneIndexValidator(@Nonnull final Index index) {
        super(index);
    }

    @Override
    public void validate(@Nonnull MetaDataValidator metaDataValidator) {
        super.validate(metaDataValidator);
        validateNotVersion();
        final var recordMetadata = metaDataValidator.getRecordMetaData();
        for (RecordType recordType : recordMetadata.recordTypesForIndex(index)) {
            LuceneIndexExpressions.validate(index.getRootExpression(), recordType.getDescriptor());
        }

        validateIndexOptions(index, recordMetadata);
    }

    @VisibleForTesting
    public static void validateIndexOptions(@Nonnull Index index, @Nonnull RecordMetaData recordMetaData) {
        validateAnalyzerNamePerFieldOption(LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, index);
        validateAnalyzerNamePerFieldOption(LuceneIndexOptions.AUTO_COMPLETE_ANALYZER_NAME_PER_FIELD_OPTION, index);
        validatePartitionOptions(index);
        validatePrimaryKeyOptions(index.getOptions());
    }

    private static void validatePartitionOptions(@Nonnull Index index) {
        String lowWatermarkOption = index.getOption(LuceneIndexOptions.INDEX_PARTITION_LOW_WATERMARK);
        String highWatermarkOption = index.getOption(LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK);
        Integer highWatermark = null;
        int lowWatermark;
        if (highWatermarkOption != null) {
            highWatermark = Integer.parseInt(highWatermarkOption);
            if (highWatermark < 1) {
                throw new MetaDataException(
                        "Invalid value for " + LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK + ": must be > 1");
            }
        }

        if (lowWatermarkOption != null) {
            lowWatermark = Integer.parseInt(lowWatermarkOption);
            if (lowWatermark < 0) {
                throw new MetaDataException(
                        "Invalid value for " + LuceneIndexOptions.INDEX_PARTITION_LOW_WATERMARK +
                                ": must be > 0");
            } else {
                int actualHighWatermark = highWatermark == null ? LucenePartitioner.DEFAULT_PARTITION_HIGH_WATERMARK : highWatermark;
                if (lowWatermark >= actualHighWatermark) {
                    throw new MetaDataException(
                            "Invalid value for " + LuceneIndexOptions.INDEX_PARTITION_LOW_WATERMARK +
                                    ": less than " + LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK + ": " + actualHighWatermark);
                }
            }
        }
    }

    private static void validatePrimaryKeyOptions(final Map<String, String> options) {
        if (Boolean.parseBoolean(options.get(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED))) {
            if (Boolean.parseBoolean(options.get(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED))) {
                throw new MetaDataException(
                        "Index cannot enable both " + LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED + " and "
                                + LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED);
            }
            if (options.get(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED) != null) {
                throw new MetaDataException(
                        "The index option " + LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED +
                                " is implied by " +
                                LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED +
                                " and cannot be controlled independently");
            }
        }
    }

    private static void validateAnalyzerNamePerFieldOption(@Nonnull String optionKey, @Nonnull Index index) {
        String analyzerNamePerFieldOption = index.getOption(optionKey);
        if (analyzerNamePerFieldOption != null) {
            LuceneIndexOptions.validateKeyValuePairOptionValue(analyzerNamePerFieldOption,
                    new MetaDataException("Index has invalid option value")
                            .addLogInfo(LogMessageKeys.INDEX_NAME, index.getName())
                            .addLogInfo(LogMessageKeys.INDEX_OPTION, optionKey)
                            .addLogInfo(LogMessageKeys.VALUE, analyzerNamePerFieldOption)
            );
        }
    }
}
