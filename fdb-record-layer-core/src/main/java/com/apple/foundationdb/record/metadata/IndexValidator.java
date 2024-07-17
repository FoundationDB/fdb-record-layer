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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Validate an index according to the constraints of the index's type.
 *
 * @see Index
 * @see com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory
 */
@API(API.Status.MAINTAINED)
public class IndexValidator {
    @Nonnull
    protected final Index index;

    public IndexValidator(@Nonnull Index index) {
        this.index = index;
    }

    public void validate(@Nonnull MetaDataValidator metaDataValidator) {
        metaDataValidator.validateIndexForRecordTypes(index, this);
        if (index.getAddedVersion() > index.getLastModifiedVersion()) {
            throw new MetaDataException("Index " + index.getName() + " has added version " + index.getAddedVersion() +
                                        " which is greater than the last modified version " + index.getLastModifiedVersion());
        }
    }

    public void validateIndexForRecordType(@Nonnull RecordType recordType, @Nonnull MetaDataValidator metaDataValidator) {
        metaDataValidator.validateIndexForRecordType(index, recordType);
    }

    protected void validateGrouping(int minGrouped) {
        KeyExpression key = index.getRootExpression();
        if (key instanceof GroupingKeyExpression) {
            if (((GroupingKeyExpression) key).getGroupedCount() < minGrouped) {
                throw new KeyExpression.InvalidExpressionException(
                        "index type requires grouping at least " + minGrouped + " fields",
                        LogMessageKeys.INDEX_TYPE, index.getType(),
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        LogMessageKeys.INDEX_KEY, key);
            }
        } else {
            throw new KeyExpression.InvalidExpressionException(
                    "index type requires grouping",
                    LogMessageKeys.INDEX_TYPE, index.getType(),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, key);
        }
    }

    protected void validateNotGrouping() {
        KeyExpression key = index.getRootExpression();
        if (key instanceof GroupingKeyExpression) {
            throw new KeyExpression.InvalidExpressionException(
                    "grouping not possible in index type",
                    LogMessageKeys.INDEX_TYPE, index.getType(),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, key);
        }
    }

    protected void validateStoresRecordVersions(@Nonnull RecordMetaDataProvider metaDataProvider) {
        if (!metaDataProvider.getRecordMetaData().isStoreRecordVersions()) {
            throw new MetaDataException(
                    "index type requires metadata store record version",
                    LogMessageKeys.INDEX_TYPE, index.getType(),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }

    protected void validateVersionKey() {
        KeyExpression key = index.getRootExpression();
        if (key.versionColumns() != 1) {
            throw new KeyExpression.InvalidExpressionException(
                    "there must be exactly 1 version entry in index",
                    LogMessageKeys.INDEX_TYPE, index.getType(),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, key);
        }
    }

    protected void validateVersionInGroupedKeys() {
        KeyExpression key = index.getRootExpression();
        validateGrouping(1);
        if (key instanceof GroupingKeyExpression) {
            GroupingKeyExpression grouping = (GroupingKeyExpression)key;
            KeyExpression groupingKey = grouping.getGroupingSubKey();
            if (groupingKey.versionColumns() != 0) {
                throw new KeyExpression.InvalidExpressionException(
                        "there must be no version entries in grouping key in index",
                        LogMessageKeys.INDEX_TYPE, index.getType(),
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        LogMessageKeys.INDEX_KEY, key);
            }
            final KeyExpression groupedKey = grouping.getGroupedSubKey();
            if (groupedKey.versionColumns() != 1) {
                throw new KeyExpression.InvalidExpressionException(
                        "there must be exactly 1 version entry in grouped key in index",
                        LogMessageKeys.INDEX_TYPE, index.getType(),
                        LogMessageKeys.INDEX_NAME, index.getName(),
                        LogMessageKeys.INDEX_KEY, key);
            }
        }
    }

    protected void validateNotUnique() {
        if (index.isUnique()) {
            throw new MetaDataException(
                    "index type does not allow unique indexes",
                    LogMessageKeys.INDEX_TYPE, index.getType(),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }

    protected void validateNotVersion() {
        KeyExpression key = index.getRootExpression();
        if (key.versionColumns() > 0) {
            throw new KeyExpression.InvalidExpressionException(
                    "version key not possible in index type",
                    LogMessageKeys.INDEX_TYPE, index.getType(),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }

    protected void validateNoValue() {
        KeyExpression key = index.getRootExpression();
        if (key instanceof KeyWithValueExpression) {
            throw new KeyExpression.InvalidExpressionException(
                    "no value expression allowed in index type",
                    LogMessageKeys.INDEX_TYPE, index.getType(),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }

    /**
     * Get a set containing all changed index options. This compares the options map of the
     * index associated with this validator with the options map of an older version of that
     * index. Any option whose value has been changed (including options that have been added
     * or removed) are added to the set.
     *
     * @param oldIndex an older version of the index associated with this validator
     * @return a set of option names that have had their value changed
     * @see #validateChangedOptions(Index)
     */
    @Nonnull
    private Set<String> getChangedOptions(@Nonnull Index oldIndex) {
        Set<String> changedOptions = new HashSet<>();
        for (Map.Entry<String, String> oldOptionEntry : oldIndex.getOptions().entrySet()) {
            final String optionName = oldOptionEntry.getKey();
            final String newOptionValue = index.getOption(optionName);
            if (!Objects.equals(oldOptionEntry.getValue(), newOptionValue)) {
                changedOptions.add(optionName);
            }
        }
        for (Map.Entry<String, String> newOptionEntry : index.getOptions().entrySet()) {
            final String optionName = newOptionEntry.getKey();
            if (!oldIndex.getOptions().containsKey(optionName) && newOptionEntry.getValue() != null) {
                changedOptions.add(optionName);
            }
        }
        return changedOptions;
    }

    /**
     * Validate any option changes included in the set of changed options provided.
     * This is a natural extension point for subclasses which may wish to override how
     * certain options are handled. This function should look at <em>only</em> the options
     * included in the {@code changedOptions} parameter even if additional options in this
     * validator's index and {@code oldIndex}. The {@code changedOptions} parameter is also
     * guaranteed to be safe to mutate. A common pattern for extending subclasses for
     * specific index types might therefore be something like:
     *
     * <ol>
     *     <li>Validate that any options specific to this index have been changed responsibly.</li>
     *     <li>Remove those options from {@code changedOptions}.</li>
     *     <li>Call {@code super.validateChangedOptions(oldIndex, changedOptions} to handle common options.</li>
     * </ol>
     *
     * @param oldIndex an older version of this validator's index
     * @param changedOptions the set of changed options to inspect
     */
    @API(API.Status.EXPERIMENTAL)
    protected void validateChangedOptions(@Nonnull Index oldIndex, @Nonnull Set<String> changedOptions) {
        for (String changedOption : changedOptions) {
            if (changedOption.startsWith(IndexOptions.REPLACED_BY_OPTION_PREFIX)) {
                // The set of replacement indexes can be safely added or removed on existing indexes as it
                // does not affect the validity of the data for any READABLE index, only whether an index
                // will be dropped (or not dropped).
                continue;
            }
            switch (changedOption) {
                case IndexOptions.ALLOWED_FOR_QUERY_OPTION:
                    // This option affects only runtime behavior and can be changed safely.
                    break;
                case IndexOptions.UNIQUE_OPTION:
                    // It is safe to drop a uniqueness constraint but not to add one.
                    if (!oldIndex.isUnique() && index.isUnique()) {
                        throw new MetaDataException("index adds uniqueness constraint",
                                LogMessageKeys.INDEX_NAME, index.getName());
                    }
                    break;
                default:
                    final String oldValue = oldIndex.getOption(changedOption);
                    final String newValue = index.getOption(changedOption);
                    throw new MetaDataException("index option changed",
                            LogMessageKeys.INDEX_NAME, index.getName(),
                            LogMessageKeys.INDEX_OPTION, changedOption,
                            LogMessageKeys.OLD_OPTION, oldValue,
                            LogMessageKeys.NEW_OPTION, newValue);
            }
        }
    }

    /**
     * Validate any options that have changed. This should inspect the options of {@code oldIndex},
     * which will be an older version of this index, and determine if all index option changes
     * are valid. In particular, this should validate that none of the changes necessitate any
     * on-disk changes or index rebuilds. For example, it is generally legal to drop a
     * {@linkplain IndexOptions#UNIQUE_OPTION uniqueness} constraint from an index, but it is
     * not legal to change the {@linkplain IndexOptions#TEXT_TOKENIZER_NAME_OPTION tokenizer}
     * of a text index.
     *
     * <p>
     * The default behavior is to allow the index to go from having a uniqueness constraint to not
     * having one as well as allowing any change to the option specifying whether the index may be
     * {@linkplain IndexOptions#ALLOWED_FOR_QUERY_OPTION used for queries}, but all other changes are
     * rejected. If an index type would like to different behavior because some options specific
     * to it may be safely changed, the validator for that type should either override this method
     * or {@link #validateChangedOptions(Index, Set)}.
     * </p>
     *
     * @param oldIndex an older version of this validator's index
     * @see #validateChangedOptions(Index, Set)
     */
    @API(API.Status.EXPERIMENTAL)
    public void validateChangedOptions(@Nonnull Index oldIndex) {
        validateChangedOptions(oldIndex, getChangedOptions(oldIndex));
    }
}
