/*
 * TextIndexMaintainerFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Supplier of {@link TextIndexMaintainer}s, that is, of index maintainers for the full text
 * index type.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.EXPERIMENTAL)
public class TextIndexMaintainerFactory implements IndexMaintainerFactory {
    @Nonnull
    private static final List<String> TYPES = Collections.singletonList(IndexTypes.TEXT);
    @Nonnull
    private static final Set<String> TEXT_OPTIONS = ImmutableSet.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION,
            IndexOptions.TEXT_TOKENIZER_VERSION_OPTION,
            IndexOptions.TEXT_OMIT_POSITIONS_OPTION,
            IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION
    );

    /**
     * A list containing only the name of the "{@value IndexTypes#TEXT}" index type.
     * The maintainers produced by this factory only support that one type.
     *
     * @return a list containing only the supported index type
     */
    @Override
    @Nonnull
    public Iterable<String> getIndexTypes() {
        return TYPES;
    }

    /**
     * Validates that the index provided is valid for text indexes. This means that
     * the index must:
     *
     * <ul>
     *     <li>Not be a unique index.</li>
     *     <li>Not include a {@link com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression#VERSION} expression in its root expression.</li>
     *     <li>Have a key expression whose first column is of type <code>string</code> (possibly with grouping columns
     *         before the tokenized text column) and is not repeated.</li> <!--Maybe we should support FanType.Concatenate?-->
     *     <li>Specify a valid tokenizer and tokenizer version through the index options (possibly using the defaults).</li>
     *     <li>Not define a value expression.</li>
     * </ul>
     *
     * @param index the index to validate
     * @return a validator to run against the index
     * @throws KeyExpression.InvalidExpressionException if the expression does not contain a string as its first ungrouped column
     * @throws com.apple.foundationdb.record.metadata.MetaDataException if the tokenizer is not defined, if the tokenizer version
     *                                                    is out of range, or if the index is marked as unique
     */
    @Nonnull
    @Override
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index) {
            @Override
            public void validate(@Nonnull MetaDataValidator metaDataValidator) {
                super.validate(metaDataValidator);
                validateNotVersion();
                validateNotUnique();
                validateNoValue(); // TODO: allow value expressions for covering text indexes

                // Validate that the tokenizer exists and that the version is in a valid range.
                TextTokenizer tokenizer = TextIndexMaintainer.getTokenizer(index);
                int tokenizerVersion = TextIndexMaintainer.getIndexTokenizerVersion(index);
                tokenizer.validateVersion(tokenizerVersion);
            }

            @Override
            public void validateIndexForRecordType(@Nonnull RecordType recordType, @Nonnull MetaDataValidator metaDataValidator) {
                final List<Descriptors.FieldDescriptor> fields = metaDataValidator.validateIndexForRecordType(index, recordType);
                int textFieldPosition = TextIndexMaintainer.textFieldPosition(index.getRootExpression());
                if (textFieldPosition > fields.size()) {
                    throw new KeyExpression.InvalidExpressionException("text index does not have text field after grouped fields");
                } else {
                    Descriptors.FieldDescriptor textFieldDescriptor = fields.get(textFieldPosition);
                    if (!textFieldDescriptor.getType().equals(Descriptors.FieldDescriptor.Type.STRING)) {
                        throw new KeyExpression.InvalidExpressionException("text index has non-string type as text field")
                                .addLogInfo(LogMessageKeys.EXPECTED_TYPE, Descriptors.FieldDescriptor.Type.STRING)
                                .addLogInfo(LogMessageKeys.ACTUAL_TYPE, textFieldDescriptor.getLiteJavaType());
                    }
                    if (textFieldDescriptor.isRepeated()) {
                        throw new KeyExpression.InvalidExpressionException("text index does not allow a repeated field for text body");
                    }
                }
            }

            /**
             * Validate any options that have changed. There are several options unique to text indexes which
             * may change without requiring the index be rebuilt. They are:
             *
             * <ul>
             *     <li>{@link IndexOptions#TEXT_TOKENIZER_VERSION_OPTION} which can be increased (but not decreased)</li>
             *     <li>{@link IndexOptions#TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION} which only affects what conflict ranges
             *          are added at index update time and thus has no impact on the on-disk representation</li>
             *     <li>{@link IndexOptions#TEXT_OMIT_POSITIONS_OPTION} which changes whether the position lists are included
             *          in index entries</li>
             * </ul>
             *
             * <p>
             * Note that the {@link IndexOptions#TEXT_TOKENIZER_NAME_OPTION} is <em>not</em> allowed to change
             * (without rebuilding the index).
             * </p>
             *
             * @param oldIndex an older version of this index
             * @param changedOptions the set of changed options
             */
            @Override
            protected void validateChangedOptions(@Nonnull Index oldIndex, @Nonnull Set<String> changedOptions) {
                for (String changedOption : changedOptions) {
                    switch (changedOption) {
                        case IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION:
                        case IndexOptions.TEXT_OMIT_POSITIONS_OPTION:
                            // These options either don't affect the on-disk format or can be changed
                            // without breaking compatibility.
                            break;
                        case IndexOptions.TEXT_TOKENIZER_NAME_OPTION:
                            String oldTokenizerName = TextIndexMaintainer.getTokenizer(oldIndex).getName();
                            String newTokenizerName = TextIndexMaintainer.getTokenizer(index).getName();
                            if (!oldTokenizerName.equals(newTokenizerName)) {
                                throw new MetaDataException("text tokenizer changed",
                                        LogMessageKeys.INDEX_NAME, index.getName());
                            }
                            break;
                        case IndexOptions.TEXT_TOKENIZER_VERSION_OPTION:
                            // The tokenizer version should always go up.
                            int oldTokenizerVersion = TextIndexMaintainer.getIndexTokenizerVersion(oldIndex);
                            int newTokenizerVersion = TextIndexMaintainer.getIndexTokenizerVersion(index);
                            if (oldTokenizerVersion > newTokenizerVersion) {
                                throw new MetaDataException("text tokenizer version downgraded",
                                        LogMessageKeys.INDEX_NAME, index.getName(),
                                        LogMessageKeys.OLD_VERSION, oldTokenizerVersion,
                                        LogMessageKeys.NEW_VERSION, newTokenizerVersion);
                            }
                            break;
                        default:
                            // Changed options that are not text options will be handled by super class
                            if (TEXT_OPTIONS.contains(changedOption)) {
                                throw new MetaDataException("index option changed",
                                        LogMessageKeys.INDEX_NAME, index.getName(),
                                        LogMessageKeys.INDEX_OPTION, changedOption,
                                        LogMessageKeys.OLD_OPTION, oldIndex.getOption(changedOption),
                                        LogMessageKeys.NEW_OPTION, index.getOption(changedOption));
                            }
                    }
                }
                changedOptions.removeAll(TEXT_OPTIONS);
                super.validateChangedOptions(oldIndex, changedOptions);
            }
        };
    }

    @Nonnull
    @Override
    public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
        return new TextIndexMaintainer(state);
    }
}
