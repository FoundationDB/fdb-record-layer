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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;

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
        validateAutoCompleteExcludedFields(index, recordMetaData);
    }

    private static void validateAnalyzerNamePerFieldOption(@Nonnull String optionKey, @Nonnull Index index) {
        String analyzerNamePerFieldOption = index.getOption(optionKey);
        if (analyzerNamePerFieldOption != null) {
            LuceneIndexOptions.validateKeyValuePairOptionValue(analyzerNamePerFieldOption,
                    new MetaDataException("Index " + index.getName() + " has invalid option value for " + optionKey + ": " + analyzerNamePerFieldOption));
        }
    }

    private static void validateAutoCompleteExcludedFields(@Nonnull Index index, @Nonnull RecordMetaData recordMetaData) {
        String autoCompleteExcludedFieldsOption = index.getOption(LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS);
        if (autoCompleteExcludedFieldsOption != null) {
            LuceneIndexOptions.validateMultipleElementsOptionValue(autoCompleteExcludedFieldsOption,
                    new MetaDataException("Index " + index.getName() + " has invalid option value for " + LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS + ": " + autoCompleteExcludedFieldsOption));
            final var excludedFieldNames = LuceneIndexOptions.parseMultipleElementsOptionValue(autoCompleteExcludedFieldsOption);
            final var documentFields = LuceneIndexExpressions.getDocumentFieldDerivations(index, recordMetaData);
            excludedFieldNames.forEach(excluded  -> {
                final var excludedDocumentField = excluded.replace('.', '_');
                if (!documentFields.containsKey(excludedDocumentField)) {
                    throw new MetaDataException("Index " + index.getName() + " has invalid field name value for " + LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS + ": " + excluded);
                }
            });
        }
    }
}
