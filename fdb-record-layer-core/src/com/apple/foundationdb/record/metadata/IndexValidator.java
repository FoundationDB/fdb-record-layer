/*
 * IndexValidator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;

import javax.annotation.Nonnull;

/**
 * Validate an index according to the constraints of the index's type.
 *
 * @see Index
 * @see com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory
 */
public class IndexValidator {
    @Nonnull
    protected final Index index;

    public IndexValidator(@Nonnull Index index) {
        this.index = index;
    }

    public void validate(@Nonnull MetaDataValidator metaDataValidator) {
        metaDataValidator.validateIndexForRecordTypes(index, this);
    }

    public void validateIndexForRecordType(@Nonnull RecordType recordType, @Nonnull MetaDataValidator metaDataValidator) {
        metaDataValidator.validateIndexForRecordType(index, recordType);
    }

    protected void validateGrouping(int minGrouped) {
        KeyExpression key = index.getRootExpression();
        if (key instanceof GroupingKeyExpression) {
            if (((GroupingKeyExpression) key).getGroupedCount() < minGrouped) {
                throw new KeyExpression.InvalidExpressionException(
                        String.format("%s index requires grouping at least %d fields", index.getType(), minGrouped),
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        LogMessageKeys.INDEX_KEY, key);
            }
        } else {
            throw new KeyExpression.InvalidExpressionException(
                    String.format("%s index requires grouping", index.getType()),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, key);
        }
    }

    protected void validateNotGrouping() {
        KeyExpression key = index.getRootExpression();
        if (key instanceof GroupingKeyExpression) {
            throw new KeyExpression.InvalidExpressionException(
                    String.format("grouping not possible in %s index", index.getType()),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, key);
        }
    }

    protected void validateStoresRecordVersions(@Nonnull RecordMetaDataProvider metaDataProvider) {
        if (!metaDataProvider.getRecordMetaData().isStoreRecordVersions()) {
            throw new MetaDataException(
                    String.format("%s index requires metadata store record version", index.getType()),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }

    protected void validateVersionKey() {
        KeyExpression key = index.getRootExpression();
        if (key.versionColumns() != 1) {
            throw new KeyExpression.InvalidExpressionException(
                    String.format("there must be exactly 1 version entry in %s index",
                            index.getType()),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, key);
        }
    }

    protected void validateNotUnique() {
        if (index.isUnique()) {
            throw new MetaDataException(String.format(
                    "%s index does not allow unique indexes", index.getType()),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }

    protected void validateNotVersion() {
        KeyExpression key = index.getRootExpression();
        if (key.versionColumns() > 0) {
            throw new KeyExpression.InvalidExpressionException(
                    String.format("version key not possible in %s index", index.getType()),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }

    protected void validateNoValue() {
        KeyExpression key = index.getRootExpression();
        if (key instanceof KeyWithValueExpression) {
            throw new KeyExpression.InvalidExpressionException(
                    String.format("no value expression allowed in %s index", index.getType()),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }
}
