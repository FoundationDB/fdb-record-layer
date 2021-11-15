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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;

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
        for (RecordType recordType : metaDataValidator.getRecordMetaData().recordTypesForIndex(index)) {
            LuceneIndexExpressions.validate(index.getRootExpression(), recordType.getDescriptor());
        }
    }
}
