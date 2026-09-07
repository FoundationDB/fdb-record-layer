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
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Set;

/**
 * {@link ScanComparisons} for use in an index scan.
 */
@API(API.Status.UNSTABLE)
public class IndexScanComparisons implements IndexScanParameters {
    private final IndexScanType scanType;
    private final ScanComparisons scanComparisons;

    protected IndexScanComparisons(final PlanSerializationContext serializationContext,
                                   final PIndexScanComparisons indexScanComparisonsProto) {
        this(IndexScanType.fromProto(serializationContext, Objects.requireNonNull(indexScanComparisonsProto.getScanType())),
                ScanComparisons.fromProto(serializationContext, Objects.requireNonNull(indexScanComparisonsProto.getScanComparisons())));
    }

    public IndexScanComparisons(final IndexScanType scanType, final ScanComparisons scanComparisons) {
        this.scanType = scanType;
        this.scanComparisons = scanComparisons;
    }

    public static IndexScanComparisons byValue() {
        return byValue(null);
    }

    public static IndexScanComparisons byValue(@Nullable ScanComparisons scanComparisons) {
        return byValue(scanComparisons, IndexScanType.BY_VALUE);
    }

    public static IndexScanComparisons byValue(@Nullable ScanComparisons scanComparisons, IndexScanType scanType) {
        if (scanComparisons == null) {
            scanComparisons = ScanComparisons.EMPTY;
        }
        return new IndexScanComparisons(scanType, scanComparisons);
    }

    @Override
    public IndexScanType getScanType() {
        return scanType;
    }

    @Override
    public boolean hasScanComparisons() {
        return true;
    }

    @Override
    public ScanComparisons getScanComparisons() {
        return scanComparisons;
    }

    @Override
    public IndexScanRange bind(FDBRecordStoreBase<?> store, Index index, EvaluationContext context) {
        return new IndexScanRange(scanType, scanComparisons.toTupleRange(store, context));
    }

    @Override
    public int planHash(PlanHashMode mode) {
        return scanType.planHash(mode) + scanComparisons.planHash(mode);
    }

    @Override
    public boolean isUnique(Index index) {
        return scanComparisons.isEquality() && scanComparisons.size() == index.getColumnSize();
    }

    @Override
    public ExplainTokensWithPrecedence explain() {
        @Nullable final TupleRange tupleRange = scanComparisons.toTupleRangeWithoutContext();
        return tupleRange == null ? scanComparisons.explain() : ExplainTokensWithPrecedence.of(new ExplainTokens().addToString(tupleRange));
    }

    @Override
    public void getPlannerGraphDetails(ImmutableList.Builder<String> detailsBuilder, ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
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

    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return scanComparisons.getCorrelatedTo();
    }

    @Override
    public IndexScanParameters rebase(final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap), false);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, final AliasMap aliasMap) {
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

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public IndexScanParameters translateCorrelations(final TranslationMap translationMap,
                                                     final boolean shouldSimplifyValues) {
        final var translatedScanComparisons =
                scanComparisons.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedScanComparisons != scanComparisons) {
            return withScanComparisons(translatedScanComparisons);
        }
        return this;
    }

    protected IndexScanParameters withScanComparisons(final ScanComparisons newScanComparisons) {
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

    @Override
    public Message toProto(final PlanSerializationContext serializationContext) {
        return toIndexScanComparisonsProto(serializationContext);
    }

    public PIndexScanComparisons toIndexScanComparisonsProto(final PlanSerializationContext serializationContext) {
        return PIndexScanComparisons.newBuilder()
                .setScanType(scanType.toProto(serializationContext))
                .setScanComparisons(scanComparisons.toProto(serializationContext))
                .build();
    }

    @Override
    public PIndexScanParameters toIndexScanParametersProto(final PlanSerializationContext serializationContext) {
        return PIndexScanParameters.newBuilder().setIndexScanComparisons(toIndexScanComparisonsProto(serializationContext)).build();
    }

    public static IndexScanComparisons fromProto(final PlanSerializationContext serializationContext,
                                                 final PIndexScanComparisons indexScanComparisonsProto) {
        return new IndexScanComparisons(serializationContext, indexScanComparisonsProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PIndexScanComparisons, IndexScanComparisons> {
        @Override
        public Class<PIndexScanComparisons> getProtoMessageClass() {
            return PIndexScanComparisons.class;
        }

        @Override
        public IndexScanComparisons fromProto(final PlanSerializationContext serializationContext,
                                              final PIndexScanComparisons indexScanComparisonsProto) {
            return IndexScanComparisons.fromProto(serializationContext, indexScanComparisonsProto);
        }
    }
}
