/*
 * IndexScanComparisons.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.planprotos.PIndexScanComparisons;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * {@link ScanComparisons} for use in an index scan.
 */
@API(API.Status.MAINTAINED)
public class IndexScanComparisons implements IndexScanParameters {
    @Nonnull
    private final IndexScanType scanType;
    @Nonnull
    private final ScanComparisons scanComparisons;

    protected IndexScanComparisons(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PIndexScanComparisons indexScanComparisonsProto) {
        this(IndexScanType.fromProto(serializationContext, Objects.requireNonNull(indexScanComparisonsProto.getScanType())),
                ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(indexScanComparisonsProto.getScanComparisons())));
    }

    public IndexScanComparisons(@Nonnull final IndexScanType scanType, @Nonnull final ScanComparisons scanComparisons) {
        this.scanType = scanType;
        this.scanComparisons = scanComparisons;
    }

    @Nonnull
    public static IndexScanComparisons byValue() {
        return byValue(null);
    }

    @Nonnull
    public static IndexScanComparisons byValue(@Nullable ScanComparisons scanComparisons) {
        return byValue(scanComparisons, IndexScanType.BY_VALUE);
    }

    @Nonnull
    public static IndexScanComparisons byValue(@Nullable ScanComparisons scanComparisons, @Nonnull IndexScanType scanType) {
        if (scanComparisons == null) {
            scanComparisons = ScanComparisons.EMPTY;
        }
        return new IndexScanComparisons(scanType, scanComparisons);
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return scanType;
    }

    @Nonnull
    public ScanComparisons getComparisons() {
        return scanComparisons;
    }

    @Nonnull
    @Override
    public IndexScanRange bind(@Nonnull FDBRecordStoreBase<?> store, @Nonnull Index index, @Nonnull EvaluationContext context) {
        return new IndexScanRange(scanType, scanComparisons.toTupleRange(store, context));
    }

    @Override
    public int planHash(@Nonnull PlanHashMode mode) {
        return scanType.planHash(mode) + scanComparisons.planHash(mode);
    }

    @Override
    public boolean isUnique(@Nonnull Index index) {
        return scanComparisons.isEquality() && scanComparisons.size() == index.getColumnSize();
    }

    @Nonnull
    @Override
    public String getScanDetails() {
        @Nullable final TupleRange tupleRange = scanComparisons.toTupleRangeWithoutContext();
        return tupleRange == null ? scanComparisons.toString() : tupleRange.toString();
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull ImmutableList.Builder<String> detailsBuilder, @Nonnull ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        if (!scanType.equals(IndexScanType.BY_VALUE)) {
            detailsBuilder.add("scan type: {{scanType}}");
            attributeMapBuilder.put("scanType", Attribute.gml(scanType.toString()));
        }

        @Nullable final TupleRange tupleRange = scanComparisons.toTupleRangeWithoutContext();
        if (tupleRange != null) {
            detailsBuilder.add("range: " + tupleRange.getLowEndpoint().toString(false) + "{{low}}, {{high}}" + tupleRange.getHighEndpoint().toString(true));
            attributeMapBuilder.put("low", Attribute.gml(tupleRange.getLow() == null ? "-∞" : tupleRange.getLow().toString()));
            attributeMapBuilder.put("high", Attribute.gml(tupleRange.getHigh() == null ? "∞" : tupleRange.getHigh().toString()));
        } else {
            detailsBuilder.add("comparisons: {{comparisons}}");
            attributeMapBuilder.put("comparisons", Attribute.gml(scanComparisons.toString()));
        }
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return scanComparisons.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public IndexScanParameters rebase(@Nonnull final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap));
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final IndexScanComparisons that = (IndexScanComparisons)other;

        if (!scanType.equals(that.scanType)) {
            return false;
        }
        return scanComparisons.semanticEquals(that.scanComparisons, aliasMap);
    }

    @Override
    public int semanticHashCode() {
        int result = scanType.hashCode();
        result = 31 * result + scanComparisons.semanticHashCode();
        return result;
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public IndexScanParameters translateCorrelations(@Nonnull final TranslationMap translationMap) {
        final var translatedScanComparisons = scanComparisons.translateCorrelations(translationMap);
        if (translatedScanComparisons != scanComparisons) {
            return withScanComparisons(translatedScanComparisons);
        }
        return this;
    }

    @Nonnull
    protected IndexScanParameters withScanComparisons(@Nonnull final ScanComparisons newScanComparisons) {
        return new IndexScanComparisons(scanType, newScanComparisons);
    }

    @Override
    public String toString() {
        return scanType + ":" + scanComparisons;
    }

    @Override
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return semanticEquals(o, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return toIndexScanComparisonsProto(serializationContext);
    }

    @Nonnull
    public PIndexScanComparisons toIndexScanComparisonsProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexScanComparisons.newBuilder()
                .setScanType(scanType.toProto(serializationContext))
                .setScanComparisons(scanComparisons.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexScanParameters.newBuilder().setIndexScanComparisons(toIndexScanComparisonsProto(serializationContext)).build();
    }

    @Nonnull
    public static IndexScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PIndexScanComparisons indexScanComparisonsProto) {
        return new IndexScanComparisons(serializationContext, indexScanComparisonsProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PIndexScanComparisons, IndexScanComparisons> {
        @Nonnull
        @Override
        public Class<PIndexScanComparisons> getProtoMessageClass() {
            return PIndexScanComparisons.class;
        }

        @Nonnull
        @Override
        public IndexScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PIndexScanComparisons indexScanComparisonsProto) {
            return IndexScanComparisons.fromProto(serializationContext, indexScanComparisonsProto);
        }
    }
}
