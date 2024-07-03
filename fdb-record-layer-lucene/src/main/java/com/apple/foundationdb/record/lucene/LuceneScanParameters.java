/*
 * LuceneScanParameters.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.planprotos.PLuceneScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Base class for {@link IndexScanParameters} used by {@code LUCENE} indexes.
 * Stores any group comparisons used to determine the directory location.
 */
@API(API.Status.UNSTABLE)
public abstract class LuceneScanParameters implements IndexScanParameters {
    @Nonnull
    protected final IndexScanType scanType;
    @Nonnull
    protected final ScanComparisons groupComparisons;

    protected LuceneScanParameters(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PLuceneScanParameters luceneScanParametersProto) {
        this(IndexScanType.fromProto(serializationContext, Objects.requireNonNull(luceneScanParametersProto.getScanType())),
                ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(luceneScanParametersProto.getGroupComparisons())));
    }

    protected LuceneScanParameters(@Nonnull final IndexScanType scanType, @Nonnull final ScanComparisons groupComparisons) {
        this.scanType = scanType;
        this.groupComparisons = groupComparisons;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return scanType;
    }

    @Nonnull
    @Override
    public abstract LuceneScanBounds bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context);

    @Nonnull
    public ScanComparisons getGroupComparisons() {
        return groupComparisons;
    }

    @Nonnull
    protected static List<String> indexTextFields(@Nonnull Index index, @Nonnull RecordMetaData metaData) {
        final List<String> textFields = new ArrayList<>();
        for (RecordType recordType : metaData.recordTypesForIndex(index)) {
            for (LuceneIndexExpressions.DocumentFieldDerivation documentField : LuceneIndexExpressions.getDocumentFieldDerivations(index.getRootExpression(), recordType.getDescriptor()).values()) {
                if (documentField.getType() == LuceneIndexExpressions.DocumentFieldType.TEXT) {
                    textFields.add(documentField.getDocumentField());
                }
            }
        }
        return textFields;
    }

    @Override
    public boolean isUnique(@Nonnull Index index) {
        return false;
    }

    protected String getGroupScanDetails() {
        if (groupComparisons.isEmpty()) {
            return "";
        } else {
            return groupComparisons + " ";
        }
    }

    @Nonnull
    protected Tuple getGroupKey(@Nullable FDBRecordStoreBase<?> store, @Nullable EvaluationContext context) {
        TupleRange tupleRange = groupComparisons.toTupleRange(store, context);
        if (TupleRange.ALL.equals(tupleRange)) {
            return TupleHelpers.EMPTY;
        }
        if (!tupleRange.isEquals()) {
            throw new RecordCoreException("group comparisons did not result in equality");
        }
        return tupleRange.getLow();
    }

    @Nullable
    public Tuple getGroupKeyWithoutContext() {
        try {
            return getGroupKey(null, null);
        } catch (Comparisons.EvaluationContextRequiredException ex) {
            return null;
        }
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        detailsBuilder.add("scan type: {{scanType}}");
        attributeMapBuilder.put("scanType", Attribute.gml(scanType.toString()));

        if (!groupComparisons.isEmpty()) {
            @Nullable final Tuple groupKey = getGroupKeyWithoutContext();
            if (groupKey != null) {
                detailsBuilder.add("group: {{range}}");
                attributeMapBuilder.put("range", Attribute.gml(groupKey.toString()));
            } else {
                detailsBuilder.add("group: {{comparisons}}");
                attributeMapBuilder.put("comparisons", Attribute.gml(groupComparisons.toString()));
            }
        }
    }

    @Override
    public String toString() {
        return scanType + ":" + groupComparisons;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final LuceneScanParameters that = (LuceneScanParameters)o;

        if (!scanType.equals(that.scanType)) {
            return false;
        }
        return groupComparisons.equals(that.groupComparisons);
    }

    @Override
    public int hashCode() {
        int result = scanType.hashCode();
        result = 31 * result + groupComparisons.hashCode();
        return result;
    }

    @Nonnull
    public PLuceneScanParameters toLuceneScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PLuceneScanParameters.newBuilder()
                .setScanType(scanType.toProto(serializationContext))
                .setGroupComparisons(groupComparisons.toProto(serializationContext))
                .build();
    }
}
