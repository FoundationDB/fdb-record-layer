/*
 * SpatialObjectQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.spatial.geophile;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithNoChildren;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.geophile.z.SpatialIndex;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;
import com.geophile.z.index.RecordWithSpatialObject;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Base class for query plans that execute a spatial join between a single spatial object and a spatial index.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class GeophileSpatialObjectQueryPlan implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithIndex {
    @Nonnull
    private final String indexName;
    @Nonnull
    private final ScanComparisons prefixComparisons;

    protected GeophileSpatialObjectQueryPlan(@Nonnull String indexName, @Nonnull ScanComparisons prefixComparisons) {
        this.indexName = indexName;
        this.prefixComparisons = prefixComparisons;
    }

    /**
     * Get the spatial object with which to join spatial index entries.
     * @param context query context containing parameter bindings
     * @return a spatial object to use in spatial join or {@code null} if some bound parameter is null.
     */
    @Nullable
    protected abstract SpatialObject getSpatialObject(@Nonnull EvaluationContext context);

    /**
     * Get a optional filter to eliminate false positives from the spatial join.
     *
     * The filter will have access only to the {@link SpatialObject} for the join and the {@link GeophileRecordImpl} for
     * the index entry. If the index is covering, {@link GeophileRecordImpl#spatialObject} will then be available.
     *
     * If there is no filter, or the filter is not entirely effective, some {@link com.apple.foundationdb.record.query.expressions.QueryComponent}
     * form of the original predicate will almost certainly be needed downstream of the {@code SpatialObjectQueryPlan}.
     * @param context query context containing parameter bindings
     * @return a spatial join filter or {@code null} if none is possible
     */
    @Nullable
    @SuppressWarnings("PMD.EmptyMethodInAbstractClassShouldBeAbstract") // null is a reasonable default
    protected SpatialJoin.Filter<RecordWithSpatialObject, GeophileRecordImpl> getFilter(@Nonnull EvaluationContext context) {
        return null;
    }

    /**
     * Get the function to use for mapping {@link IndexEntry} to {@link GeophileRecordImpl}.
     *
     * If the index is covering, the indexed {@link SpatialObject} may be recoverable from the value part of the index entry.
     * @return a function that creates new {@code GeophileRecordImpl} instances
     */
    protected BiFunction<IndexEntry, Tuple, GeophileRecordImpl> getRecordFunction() {
        return GeophileRecordImpl::new;
    }

    @Nonnull
    public ScanComparisons getPrefixComparisons() {
        return prefixComparisons;
    }

    @Nonnull
    @Override
    public String getIndexName() {
        return indexName;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return GeophileScanTypes.GO_TO_Z;
    }

    @Nonnull
    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateOptional() {
        return Optional.empty();
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull FDBRecordStoreBase<M> store,
                                                                       @Nonnull EvaluationContext context,
                                                                       @Nullable byte[] continuation,
                                                                       @Nonnull ExecuteProperties executeProperties) {
        if (continuation != null) {
            throw new RecordCoreException("continuations are not yet supported");
        }
        final SpatialObject spatialObject = getSpatialObject(context);
        if (spatialObject == null) {
            return RecordCursor.empty();
        }
        final SpatialJoin spatialJoin = SpatialJoin.newSpatialJoin(SpatialJoin.Duplicates.INCLUDE, getFilter(context));
        final GeophileSpatialJoin geophileSpatialJoin = new GeophileSpatialJoin(spatialJoin, store.getUntypedRecordStore(), context);
        final SpatialIndex<GeophileRecordImpl> spatialIndex = geophileSpatialJoin.getSpatialIndex(indexName, prefixComparisons, getRecordFunction());
        return geophileSpatialJoin.recordCursor(spatialObject, spatialIndex);
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return this.indexName.equals(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return Collections.singleton(indexName);
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_INDEX);
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public GeophileSpatialObjectQueryPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                                @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return this;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final GeophileSpatialObjectQueryPlan that = (GeophileSpatialObjectQueryPlan)otherExpression;
        return indexName.equals(that.indexName) &&
               prefixComparisons.equals(that.prefixComparisons);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(indexName, prefixComparisons);
    }
}
