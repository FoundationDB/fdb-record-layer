/*
 * RankScanComparisons.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.planprotos.PRankScanComparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * {@link IndexScanParameters} binding to {@link RankScanBounds}, which is what lets a scan ask for the rank of each
 * entry to be reported rather than merely used as a bound.
 *
 * <p>A {@code BY_RANK} scan converts its rank range into a score range and then scans by value, so the entries it hands
 * back carry no rank of their own. A plan that projects the window function therefore has nothing to decode that
 * column from unless it asks for it here, which is what {@code includeRankAsValue} does: the rank is placed in the
 * entry's value, which a rank index otherwise leaves empty.
 *
 * <p>This delegates to an {@link IndexScanComparisons} rather than extending it, the way
 * {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowScanComparisons} does, because
 * {@code IndexScanComparisons.bind} is declared to return an {@link IndexScanRange} and {@link RankScanBounds} is not
 * one.
 */
@API(API.Status.INTERNAL)
public class RankScanComparisons implements IndexScanParameters {
    @Nonnull
    private final IndexScanComparisons delegate;
    private final boolean includeRankAsValue;

    public RankScanComparisons(@Nonnull final IndexScanType scanType, @Nonnull final ScanComparisons scanComparisons,
                               final boolean includeRankAsValue) {
        this(new IndexScanComparisons(scanType, scanComparisons), includeRankAsValue);
    }

    private RankScanComparisons(@Nonnull final IndexScanComparisons delegate, final boolean includeRankAsValue) {
        this.delegate = delegate;
        this.includeRankAsValue = includeRankAsValue;
    }

    public boolean isIncludeRankAsValue() {
        return includeRankAsValue;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return delegate.getScanType();
    }

    @Nonnull
    @Override
    public RankScanBounds bind(@Nonnull final FDBRecordStoreBase<?> store, @Nonnull final Index index,
                               @Nonnull final EvaluationContext context) {
        return new RankScanBounds(getScanType(), delegate.bind(store, index, context).getScanRange(),
                includeRankAsValue);
    }

    @Override
    public boolean isUnique(@Nonnull final Index index) {
        return delegate.isUnique(index);
    }

    @Override
    public boolean hasScanComparisons() {
        return true;
    }

    @Nonnull
    @Override
    public ScanComparisons getScanComparisons() {
        return delegate.getScanComparisons();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return delegate.planHash(mode) + (includeRankAsValue ? 1 : 0);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain() {
        final var explainTokens = delegate.explain().getExplainTokens();
        if (includeRankAsValue) {
            explainTokens.addToString("+rank");
        }
        return ExplainTokensWithPrecedence.of(explainTokens);
    }

    @Override
    public void getPlannerGraphDetails(@Nonnull final ImmutableList.Builder<String> detailsBuilder,
                                       @Nonnull final ImmutableMap.Builder<String, Attribute> attributeMapBuilder) {
        delegate.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);
        if (includeRankAsValue) {
            detailsBuilder.add("+rank()");
        }
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return delegate.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public IndexScanParameters rebase(@Nonnull final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap), false);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public IndexScanParameters translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                     final boolean shouldSimplifyValues) {
        final var translatedDelegate = delegate.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedDelegate == delegate) {
            return this;
        }
        return new RankScanComparisons((IndexScanComparisons)translatedDelegate, includeRankAsValue);
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
        final RankScanComparisons that = (RankScanComparisons)other;
        return includeRankAsValue == that.includeRankAsValue
                && delegate.semanticEquals(that.delegate, aliasMap);
    }

    @Override
    public int semanticHashCode() {
        return 31 * delegate.semanticHashCode() + (includeRankAsValue ? 1 : 0);
    }

    @Override
    public String toString() {
        return delegate + (includeRankAsValue ? "+rank" : "");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RankScanComparisons that = (RankScanComparisons)o;
        return includeRankAsValue == that.includeRankAsValue && delegate.equals(that.delegate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(delegate, includeRankAsValue);
    }

    @Nonnull
    @Override
    public PRankScanComparisons toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRankScanComparisons.newBuilder()
                .setSuper(delegate.toIndexScanComparisonsProto(serializationContext))
                .setIncludeRankAsValue(includeRankAsValue)
                .build();
    }

    @Nonnull
    @Override
    public PIndexScanParameters toIndexScanParametersProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PIndexScanParameters.newBuilder().setRankScanComparisons(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RankScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRankScanComparisons rankScanComparisonsProto) {
        return new RankScanComparisons(
                IndexScanComparisons.fromProto(serializationContext,
                        Objects.requireNonNull(rankScanComparisonsProto.getSuper())),
                rankScanComparisonsProto.getIncludeRankAsValue());
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRankScanComparisons, RankScanComparisons> {
        @Nonnull
        @Override
        public Class<PRankScanComparisons> getProtoMessageClass() {
            return PRankScanComparisons.class;
        }

        @Nonnull
        @Override
        public RankScanComparisons fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PRankScanComparisons rankScanComparisonsProto) {
            return RankScanComparisons.fromProto(serializationContext, rankScanComparisonsProto);
        }
    }
}
