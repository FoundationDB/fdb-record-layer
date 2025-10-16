/*
 * RecordMetadataSerializer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.api.metadata.View;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerView;
import com.apple.foundationdb.relational.recordlayer.metadata.SkeletonVisitor;
import com.apple.foundationdb.relational.util.Assert;

import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;

@API(API.Status.EXPERIMENTAL)
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
        // Note: this does not preserve the index added and lest modified version, necessary
        // correctly handling index rebuilds when the template is updated. This also results
        // in the RecordMetaData builder updating its version, so the resulting meta-data will not
        // have a version that matches the schema template's version
        // See: TODO (Relational index misses version information)
        Assert.thatUnchecked(index instanceof RecordLayerIndex);
        final var recLayerIndex = (RecordLayerIndex) index;
        getBuilder().addIndex(index.getTableName(),
                new Index(index.getName(),
                        recLayerIndex.getKeyExpression(),
                        index.getIndexType(),
                        recLayerIndex.getOptions(),
                        recLayerIndex.getPredicate() == null ? null : IndexPredicate.fromProto(recLayerIndex.getPredicate())));
    }

    @Override
    public void visit(@Nonnull final InvokedRoutine invokedRoutine) {
        // do not serialize temporary routines in the record metadata.
        if (invokedRoutine.isTemporary()) {
            return;
        }
        final var recordLayerInvokedRoutine = Assert.castUnchecked(invokedRoutine, RecordLayerInvokedRoutine.class);
        getBuilder().addUserDefinedFunction(recordLayerInvokedRoutine.asRawFunction());
    }

    @Override
    public void visit(@Nonnull final View view) {
        Assert.thatUnchecked(view instanceof RecordLayerView);
        getBuilder().addView(((RecordLayerView)view).asRawView());
    }

    @Override
    public void visit(@Nonnull SchemaTemplate schemaTemplate) {
        Assert.thatUnchecked(schemaTemplate instanceof RecordLayerSchemaTemplate);
        getBuilder().setSplitLongRecords(schemaTemplate.isEnableLongRows());
        getBuilder().setStoreRecordVersions(schemaTemplate.isStoreRowVersions());
        getBuilder().setVersion(schemaTemplate.getVersion());
    }

    @Nonnull
    public RecordMetaDataBuilder getBuilder() {
        return builder;
    }
}
