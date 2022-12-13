/*
 * RecordMetadataSerializer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata.serde;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.SkeletonVisitor;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.Map;

public class RecordMetadataSerializer extends SkeletonVisitor {

    @Nonnull
    private final RecordMetaDataBuilder builder;

    private int recordTypeCounter;

    public RecordMetadataSerializer(@Nonnull final Descriptors.FileDescriptor fileDescriptor) {
        this(RecordMetaData.newBuilder().setRecords(fileDescriptor));
    }

    public RecordMetadataSerializer(@Nonnull final RecordMetaDataBuilder builder) {
        this.builder = builder;
        this.recordTypeCounter = 0;
    }

    @Override
    public void visit(@Nonnull Table table) {
        Assert.thatUnchecked(table instanceof RecordLayerTable);
        final var recLayerTable = (RecordLayerTable) table;
        final KeyExpression keyExpression = recLayerTable.getPrimaryKey();
        final RecordTypeBuilder recordType = getBuilder().getRecordType(table.getName());
        recordType.setRecordTypeKey(recordTypeCounter++);
        recordType.setPrimaryKey(keyExpression);
    }

    @Override
    public void visit(@Nonnull com.apple.foundationdb.relational.api.metadata.Index index) {
        Assert.thatUnchecked(index instanceof RecordLayerIndex);
        getBuilder().addIndex(index.getTableName(), new Index(index.getName(),
                ((RecordLayerIndex) index).getKeyExpression(),
                index.getIndexType(),
                Map.of(IndexOptions.UNIQUE_OPTION, Boolean.toString(index.isUnique()))));
    }

    @Nonnull
    public RecordMetaDataBuilder getBuilder() {
        return builder;
    }
}
