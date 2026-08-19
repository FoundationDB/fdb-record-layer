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
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.metadata.InvokedRoutine;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.Table;
import com.apple.foundationdb.relational.api.metadata.View;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerUnnestedSyntheticTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerView;
import com.apple.foundationdb.relational.recordlayer.metadata.SkeletonVisitor;
import com.apple.foundationdb.relational.util.Assert;

import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;

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
        final RecordTypeBuilder recordType = getBuilder().getRecordType(recLayerTable.getType().getStorageName());
        recordType.setRecordTypeKey(recordTypeCounter++);
        recordType.setPrimaryKey(keyExpression);
    }

    public void visit(@Nonnull final RecordLayerUnnestedSyntheticTable unnestedType) {
        final UnnestedRecordTypeBuilder typeBuilder =
                getBuilder().addUnnestedRecordType(unnestedType.getName());
        final RecordTypeBuilder recordTypeBuilder = getBuilder().getRecordType(unnestedType.getParentTableStorageName());
        typeBuilder.addParentConstituent(unnestedType.getAlias(), recordTypeBuilder);
        // The descriptor an array field is looked up in depends on which constituent owns it: the stored
        // record for a first-level unnesting, and the enclosing constituent's element type for a chained
        // one. Constituents are registered parent-before-child, so the map is always populated in time.
        final Map<String, Descriptors.Descriptor> descriptorsByAlias = new LinkedHashMap<>();
        descriptorsByAlias.put(unnestedType.getAlias(), recordTypeBuilder.getDescriptor());
        for (final RecordLayerUnnestedSyntheticTable.NestedConstituent nested : unnestedType.getConstituents()) {
            final Descriptors.Descriptor owningProto = descriptorsByAlias.get(nested.getParentAlias());
            Assert.notNullUnchecked(owningProto, "unknown parent constituent '" + nested.getParentAlias()
                    + "' for constituent '" + nested.getAlias() + "'");
            // The nesting expression is carried verbatim, so the element type is found by walking the field
            // path it navigates. That handles either array storage form — a plain repeated field, or a nullable
            // array wrapped as { repeated T values; } — as well as any deeper nesting, without special-casing.
            Descriptors.Descriptor constituentDescriptor = owningProto;
            for (final String fieldName : nested.getFieldPath()) {
                final Descriptors.FieldDescriptor arrayField = constituentDescriptor.findFieldByName(fieldName);
                Assert.notNullUnchecked(arrayField, "array field '" + fieldName + "' not found on '"
                        + constituentDescriptor.getName() + "'");
                Assert.thatUnchecked(arrayField.getType() == Descriptors.FieldDescriptor.Type.MESSAGE,
                        "unnested index constituent must be a struct array, scalar arrays are not supported");
                constituentDescriptor = arrayField.getMessageType();
            }
            typeBuilder.addNestedConstituent(nested.getAlias(), constituentDescriptor,
                    nested.getParentAlias(), nested.getNestingExpression());
            descriptorsByAlias.put(nested.getAlias(), constituentDescriptor);
        }
    }

    @Override
    public void visit(@Nonnull com.apple.foundationdb.relational.api.metadata.Index index) {
        // Note: this does not preserve the index added and lest modified version, necessary
        // correctly handling index rebuilds when the template is updated. This also results
        // in the RecordMetaData builder updating its version, so the resulting meta-data will not
        // have a version that matches the schema template's version
        // See: TODO (Relational index misses version information)
        Assert.thatUnchecked(index instanceof RecordLayerIndex);
        final RecordLayerIndex recLayerIndex = (RecordLayerIndex) index;
        getBuilder().addIndex(recLayerIndex.getTableStorageName(),
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
        getBuilder().addUserDefinedFunction(recordLayerInvokedRoutine.asSerializableFunction());
    }

    @Override
    public void visit(@Nonnull final View view) {
        if (view instanceof RecordLayerUnnestedSyntheticTable unnestedSyntheticTable) {
            visit(unnestedSyntheticTable);
        } else if (view instanceof RecordLayerView recordLayerView) {
            getBuilder().addView(recordLayerView.asRawView());
        } else {
            Assert.failUnchecked("view not supported");
        }
    }

    @Override
    public void visit(@Nonnull SchemaTemplate schemaTemplate) {
        Assert.thatUnchecked(schemaTemplate instanceof RecordLayerSchemaTemplate);
        final var recLayerSchemaTemplate = (RecordLayerSchemaTemplate) schemaTemplate;
        getBuilder().setSplitLongRecords(schemaTemplate.isEnableLongRows());
        getBuilder().setStoreRecordVersions(schemaTemplate.isStoreRowVersions());
        getBuilder().setVersion(schemaTemplate.getVersion());
        for (final var entry : recLayerSchemaTemplate.getStoredQueries().entrySet()) {
            final var storedQuery = entry.getValue();
            getBuilder().addStoredQuery(entry.getKey(), storedQuery.getQuery(), storedQuery.getTempFunctions());
        }
    }

    @Nonnull
    public RecordMetaDataBuilder getBuilder() {
        return builder;
    }
}
