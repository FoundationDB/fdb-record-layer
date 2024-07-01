/*
 * RecordQueryLoadByKeysPlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordScanLimiter;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that returns records whose primary keys are taken from some list.
 */
@API(API.Status.INTERNAL)
public class RecordQueryLoadByKeysPlan implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Load-By-Keys-Plan");

    @Nonnull
    private final KeysSource keysSource;

    public RecordQueryLoadByKeysPlan(@Nonnull KeysSource keysSource) {
        this.keysSource = keysSource;
    }

    public RecordQueryLoadByKeysPlan(@Nonnull List<Tuple> primaryKeys) {
        this(new PrimaryKeysKeySource(primaryKeys));
    }

    public RecordQueryLoadByKeysPlan(@Nonnull String parameter) {
        this(new ParameterKeySource(parameter));
    }
                                                                                
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        // Cannot pass down limit(s) because we skip keys that don't load.
        RecordScanLimiter recordScanLimiter = executeProperties.getState().getRecordScanLimiter();
        return RecordCursor.fromList(store.getExecutor(), getKeysSource().getPrimaryKeys(context), continuation)
                .mapPipelined(key -> {
                    // TODO: Implement continuation handling and record scan limit for RecordQueryLoadByKeysPlan (https://github.com/FoundationDB/fdb-record-layer/issues/6)
                    if (recordScanLimiter != null) {
                        recordScanLimiter.tryRecordScan();
                    }
                    return store.loadRecordAsync(key);
                }, store.getPipelineSize(PipelineOperation.KEY_TO_RECORD))
                .filter(Objects::nonNull)
                .map(store::queriedRecord)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit())
                .map(QueryResult::fromQueriedRecord);
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
        return false;
    }

    @Nonnull
    public KeysSource getKeysSource() {
        return keysSource;
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return new HashSet<>();
    }

    @Override
    public int maxCardinality(@Nonnull RecordMetaData metaData) {
        return keysSource.maxCardinality();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public boolean hasLoadBykeys() {
        return true;
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of(); // TODO this should be reconsidered when we have selects
    }

    @Nonnull
    @Override
    public RecordQueryLoadByKeysPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return this;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue();
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

        return Objects.equals(getKeysSource(), ((RecordQueryLoadByKeysPlan)otherExpression).getKeysSource());
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
        return Objects.hash(getKeysSource());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return getKeysSource().planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.planHash(mode, BASE_HASH, getKeysSource());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_LOAD_BY_KEYS);
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.LOAD_BY_KEYS_OPERATOR,
                        ImmutableList.of("BY KEYS {{keysSource}}"),
                        ImmutableMap.of("keysSource", Attribute.gml(getKeysSource().toString()))),
                childGraphs);
    }

    @Nonnull
    @Override
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("serialization of this plan is not supported");
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("serialization of this plan is not supported");
    }

    private static class PrimaryKeysKeySource implements KeysSource {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Primary-Keys-Key-Source");

        private final List<Tuple> primaryKeys;

        public PrimaryKeysKeySource(List<Tuple> primaryKeys) {
            this.primaryKeys = primaryKeys;
        }

        @Override
        public List<Tuple> getPrimaryKeys(@Nonnull EvaluationContext context) {
            return primaryKeys;
        }

        @Override
        public int maxCardinality() {
            return primaryKeys.size();
        }

        @Override
        public String toString() {
            return primaryKeys.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PrimaryKeysKeySource that = (PrimaryKeysKeySource) o;
            return Objects.equals(primaryKeys, that.primaryKeys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(primaryKeys);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return hashCode();
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, primaryKeys);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }
    }

    /**
     * A source for the primary keys for records.
     */
    public interface KeysSource extends PlanHashable {
        List<Tuple> getPrimaryKeys(@Nonnull EvaluationContext context);

        int maxCardinality();
    }

    private static class ParameterKeySource implements KeysSource {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Parameter-Key-Source");

        private final String parameter;

        public ParameterKeySource(String parameter) {
            this.parameter = parameter;
        }

        @Override
        @SuppressWarnings("unchecked")
        public List<Tuple> getPrimaryKeys(@Nonnull EvaluationContext context) {
            return (List<Tuple>)context.getBinding(parameter);
        }

        @Override
        public int maxCardinality() {
            return UNKNOWN_MAX_CARDINALITY;
        }

        @Override
        public String toString() {
            return "$" + parameter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ParameterKeySource that = (ParameterKeySource) o;
            return Objects.equals(parameter, that.parameter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(parameter);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return hashCode();
                case FOR_CONTINUATION:
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, parameter);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }
    }
}
