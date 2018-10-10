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

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

/**
 * Supplier of {@link TextIndexMaintainer}s, that is, of index maintainers for the full text
 * index type.
 */
@AutoService(IndexMaintainerFactory.class)
public class TextIndexMaintainerFactory implements IndexMaintainerFactory {
    private static final List<String> TYPES = Collections.singletonList(IndexTypes.TEXT);

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
                        throw new KeyExpression.InvalidExpressionException(String.format("text index has non-string type %s as text field", textFieldDescriptor.getLiteJavaType()));
                    }
                    if (textFieldDescriptor.isRepeated()) {
                        throw new KeyExpression.InvalidExpressionException("text index does not allow a repeated field for text body");
                    }
                }
            }
        };
    }

    @Nonnull
    @Override
    public <M extends Message> IndexMaintainer<M> getIndexMaintainer(IndexMaintainerState<M> state) {
        return new TextIndexMaintainer<>(state);
    }
}
