/*
 * AtomicMutationIndexMaintainerFactory.java
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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.auto.service.AutoService;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

/**
 * A factory for {@link AtomicMutationIndexMaintainer} indexes.
 *
 * Implements the following index types:
 * <ul>
 *     <li>{@link IndexTypes#COUNT COUNT}</li>
 *     <li>{@link IndexTypes#COUNT_UPDATES COUNT_UPDATES}</li>
 *     <li>{@link IndexTypes#COUNT_NOT_NULL COUNT_NOT_NULL}</li>
 *     <li>{@link IndexTypes#SUM SUM}</li>
 *     <li>{@link IndexTypes#MIN_EVER_TUPLE MIN_EVER_TUPLE}</li>
 *     <li>{@link IndexTypes#MAX_EVER_TUPLE MAX_EVER_TUPLE}</li>
 *     <li>{@link IndexTypes#MIN_EVER_LONG MIN_EVER_LONG}</li>
 *     <li>{@link IndexTypes#MAX_EVER_LONG MAX_EVER_LONG}</li>
 *     <li>{@link IndexTypes#MAX_EVER_VERSION MAX_EVER_VERSION}</li>
 * </ul>
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.MAINTAINED)
public class AtomicMutationIndexMaintainerFactory implements IndexMaintainerFactory {
    @SuppressWarnings({"deprecation", "squid:CallToDeprecatedMethod"}) // Support the deprecated names for compatibility.
    static final String[] TYPES = {
            IndexTypes.COUNT, IndexTypes.COUNT_UPDATES, IndexTypes.COUNT_NOT_NULL, IndexTypes.SUM,
            IndexTypes.MIN_EVER_TUPLE, IndexTypes.MAX_EVER_TUPLE,
            IndexTypes.MIN_EVER_LONG, IndexTypes.MAX_EVER_LONG,
            IndexTypes.MAX_EVER_VERSION,
            IndexTypes.MIN_EVER, IndexTypes.MAX_EVER
    };

    @Override
    @Nonnull
    public Iterable<String> getIndexTypes() {
        return Arrays.asList(TYPES);
    }

    @Override
    @Nonnull
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index) {
            final AtomicMutation mutation = AtomicMutationIndexMaintainer.getAtomicMutation(index);

            private int getGroupedCount() {
                return ((GroupingKeyExpression)index.getRootExpression()).getGroupedCount();
            }

            @Override
            public void validate(@Nonnull MetaDataValidator metaDataValidator) {
                super.validate(metaDataValidator);
                if (!mutation.hasValues()) {
                    validateGrouping(0);
                    if (getGroupedCount() != 0) {
                        throw new KeyExpression.InvalidExpressionException("index type does not support non-group fields; use COUNT_NOT_NULL",
                                LogMessageKeys.INDEX_TYPE, index.getType(),
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_KEY, index.getRootExpression());
                    }
                } else if (!mutation.hasSingleValue()) {
                    validateGrouping(1);
                } else {
                    validateGrouping(1);
                    if (getGroupedCount() != 1) {
                        throw new KeyExpression.InvalidExpressionException("index type only supports single field",
                                LogMessageKeys.INDEX_TYPE, index.getType(),
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_KEY, index.getRootExpression());
                    }
                }
                if (AtomicMutation.Standard.MAX_EVER_VERSION.equals(mutation)) {
                    validateVersionInGroupedKeys();
                } else {
                    validateNotVersion();
                }
                if (AtomicMutationIndexMaintainer.getClearWhenZero(index) && mutation.getCompareAndClearParam() == null) {
                    throw new MetaDataException("index type does not support clearWhenZero").addLogInfo(LogMessageKeys.INDEX_TYPE, index.getType());
                }
            }

            // NOTE: There is no override of validateChangedOptions for CLEAR_WHEN_ZERO.
            // Turning it on does not immediately clear to zero without a rebuild.
            // But it is still valid, provided one understands the option to mean that the clear happens at decrement time.
            // A system requiring that it become clear immediately can arrange for the index to be rebuilt.

            @Override
            public void validateIndexForRecordType(@Nonnull RecordType recordType, @Nonnull MetaDataValidator metaDataValidator) {
                final List<Descriptors.FieldDescriptor> fields = metaDataValidator.validateIndexForRecordType(index, recordType);
                if (mutation.hasLongValue()) {
                    switch (fields.get(fields.size() - 1).getType()) {
                        case INT64:
                        case UINT64:
                        case INT32:
                        case UINT32:
                        case SINT32:
                        case SINT64:
                            break;
                        default:
                            throw new KeyExpression.InvalidExpressionException("index type only supports integer field",
                                    LogMessageKeys.INDEX_TYPE, index.getType(),
                                    LogMessageKeys.INDEX_NAME, index.getName(),
                                    LogMessageKeys.INDEX_KEY, index.getRootExpression(),
                                    "record_type", recordType.getName());
                    }
                }
            }
        };
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
        return new AtomicMutationIndexMaintainer(state);
    }

}
