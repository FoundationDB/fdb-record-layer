/*
 * TempTableScanPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.ListCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PTempTableScanPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithoutChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Scans records from a table-valued correlation, corresponding for example to a temporary in-memory buffer {@link TempTable}.
 */
@API(API.Status.INTERNAL)
public class TempTableScanPlan extends AbstractRelationalExpressionWithoutChildren implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Temp-Table-Scan-Plan");

    private final Value tempTableReferenceValue;

    public TempTableScanPlan(final Value tempTableReferenceValue) {
        this.tempTableReferenceValue = tempTableReferenceValue;
    }

    @Override
    @SuppressWarnings("NullAway") // NullAway doesn't reliably track @Nullable on byte[] parameters; ListCursor's
                                   // constructor explicitly tolerates (and handles) a null continuation.
    public <M extends Message> RecordCursor<QueryResult> executePlan(final FDBRecordStoreBase<M> store,
                                                                     final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     final ExecuteProperties executeProperties) {
        final var tempTable = Objects.requireNonNull((TempTable)this.tempTableReferenceValue.eval(store, context));
        return new ListCursor<>(tempTable.getList(), continuation);
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RelationalExpression translateCorrelations(final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }
        final var translatedTableReferenceValue =
                tempTableReferenceValue.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedTableReferenceValue != tempTableReferenceValue) {
            return new TempTableScanPlan(translatedTableReferenceValue);
        }
        return this;
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public TempTableScanPlan strictlySorted(FinalMemoizer memoizer) {
        return this;
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
    public boolean hasIndexScan(String indexName) {
        return false;
    }

    @Override
    public Set<String> getUsedIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Override
    public Value getResultValue() {
        return new QueriedValue(Objects.requireNonNull(((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType()));
    }

    public Value getTempTableReferenceValue() {
        return new QueriedValue(Objects.requireNonNull(
                ((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType()));
    }

    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(final RelationalExpression otherExpression,
                                         final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final var otherTempTableScan =  (TempTableScanPlan)otherExpression;
        return tempTableReferenceValue.semanticEquals(otherTempTableScan.tempTableReferenceValue, equivalencesMap);
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(tempTableReferenceValue);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Override
    public int planHash(PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, tempTableReferenceValue);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.TEMP_TABLE_SCAN_OPERATOR,
                        ImmutableList.of(tempTableReferenceValue.toString())),
                childGraphs);
    }

    @Override
    public PTempTableScanPlan toProto(final PlanSerializationContext serializationContext) {
        return PTempTableScanPlan.newBuilder()
                .setTempTableReferenceValue(tempTableReferenceValue.toValueProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setTempTableScanPlan(toProto(serializationContext)).build();
    }

    public static TempTableScanPlan fromProto(final PlanSerializationContext serializationContext,
                                              final PTempTableScanPlan tempTableScanPlanProto) {
        return new TempTableScanPlan(Value.fromValueProto(serializationContext, tempTableScanPlanProto.getTempTableReferenceValue()));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PTempTableScanPlan, TempTableScanPlan> {
        @Override
        public Class<PTempTableScanPlan> getProtoMessageClass() {
            return PTempTableScanPlan.class;
        }

        @Override
        public TempTableScanPlan fromProto(final PlanSerializationContext serializationContext,
                                           final PTempTableScanPlan tempTableScanPlanProto) {
            return TempTableScanPlan.fromProto(serializationContext, tempTableScanPlanProto);
        }
    }
}
