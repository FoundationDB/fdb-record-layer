/*
 * BitmapValueIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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
import java.util.Collections;
import java.util.List;

/**
 * A factory for {@link BitmapValueIndexMaintainer}.
 */
@AutoService(IndexMaintainerFactory.class)
@API(API.Status.EXPERIMENTAL)
public class BitmapValueIndexMaintainerFactory implements IndexMaintainerFactory {
    @Nonnull
    private static final List<String> TYPES = Collections.singletonList(IndexTypes.BITMAP_VALUE);

    @Override
    @Nonnull
    public Iterable<String> getIndexTypes() {
        return TYPES;
    }

    @Override
    @Nonnull
    public IndexValidator getIndexValidator(Index index) {
        return new IndexValidator(index) {
            @Override
            public void validate(@Nonnull MetaDataValidator metaDataValidator) {
                super.validate(metaDataValidator);
                validateGrouping(1);
                final GroupingKeyExpression group = (GroupingKeyExpression)index.getRootExpression();
                if (group.getGroupedCount() != 1) {
                    throw new KeyExpression.InvalidExpressionException("index type needs grouped position",
                                                                       LogMessageKeys.INDEX_TYPE, index.getType(),
                                                                       LogMessageKeys.INDEX_NAME, index.getName(),
                                                                       LogMessageKeys.INDEX_KEY, index.getRootExpression());
                }
                validateNotVersion();
            }

            @Override
            @SuppressWarnings("fallthrough")
            public void validateIndexForRecordType(@Nonnull RecordType recordType, @Nonnull MetaDataValidator metaDataValidator) {
                final List<Descriptors.FieldDescriptor> fields = metaDataValidator.validateIndexForRecordType(index, recordType);
                switch (fields.get(fields.size() - 1).getType()) {
                    case INT64:
                    case UINT64:
                    case INT32:
                    case UINT32:
                    case SINT32:
                    case SINT64:
                    case FIXED32:
                    case FIXED64:
                    case SFIXED32:
                    case SFIXED64:
                        break;
                    default:
                        throw new KeyExpression.InvalidExpressionException("index type only supports integer position key",
                                LogMessageKeys.INDEX_TYPE, index.getType(),
                                LogMessageKeys.INDEX_NAME, index.getName(),
                                LogMessageKeys.INDEX_KEY, index.getRootExpression(),
                                "record_type", recordType.getName());
                }
            }
        };
    }

    @Override
    @Nonnull
    public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
        return new BitmapValueIndexMaintainer(state);
    }
}
