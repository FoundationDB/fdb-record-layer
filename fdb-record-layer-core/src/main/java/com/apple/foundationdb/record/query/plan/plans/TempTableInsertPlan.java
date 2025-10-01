/*
 * TempTableInsertPlan.java
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
import com.apple.foundationdb.record.cursors.TempTableInsertCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PTempTableInsertPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that inserts records into a temporary in-memory buffer {@link TempTable}.
 */
@API(API.Status.INTERNAL)
public class TempTableInsertPlan implements RecordQueryPlanWithChild, PlannerGraphRewritable {

    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Temp-Table-Insert-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(TempTableInsertPlan.class);

    @Nonnull
    private final Quantifier.Physical inner;

    @Nonnull
    private final Value tempTableReferenceValue;

    private final boolean isOwningTempTable;

    protected TempTableInsertPlan(@Nonnull final PlanSerializationContext serializationContext,
                                  @Nonnull final PTempTableInsertPlan tempTableInsertPlanProto) {
        this.inner = Quantifier.Physical.fromProto(serializationContext, tempTableInsertPlanProto.getInner());
        this.tempTableReferenceValue = Value.fromValueProto(serializationContext, tempTableInsertPlanProto.getTempTableReferenceValue());
        this.isOwningTempTable = tempTableInsertPlanProto.getIsOwningTempTable();
    }

    private TempTableInsertPlan(@Nonnull final Quantifier.Physical inner,
                                @Nonnull final Value tempTableReferenceValue,
                                boolean isOwningTempTable) {
        this.inner = inner;
        this.tempTableReferenceValue = tempTableReferenceValue;
        this.isOwningTempTable = isOwningTempTable;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        if (isOwningTempTable) {
            final var typeDescriptor = getInnerTypeDescriptor(context);
            return TempTableInsertCursor.from(continuation,
                    proto -> {
                        final var tempTable = Objects.requireNonNull((TempTable)getTempTableReferenceValue().eval(store, context));
                        if (proto != null) {
                            final var deserialized = TempTable.from(proto, typeDescriptor);
                            deserialized.getIterator().forEachRemaining(tempTable::add);
                        }
                        return tempTable;
                    },
                    childContinuation -> getChild().executePlan(store, context, childContinuation, executeProperties.clearSkipAndLimit()));
        } else {
            return getChild().executePlan(store, context, continuation, executeProperties.clearSkipAndLimit())
                    .map(queryResult ->
                    {
                        final var tempTable = Objects.requireNonNull((TempTable)getTempTableReferenceValue().eval(store, context));
                        tempTable.add(queryResult);
                        return queryResult;
                    });
        }
    }

    @Nullable
    private Descriptors.Descriptor getInnerTypeDescriptor(@Nonnull final EvaluationContext context) {
        final Descriptors.Descriptor typeDescriptor;
        if (tempTableReferenceValue.getResultType().isRelation() && ((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType().isRecord()) {
            final var type = (Type.Record)((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType();
            typeDescriptor = context.getTypeRepository().getMessageDescriptor(type);
        } else {
            typeDescriptor = null;
        }
        return typeDescriptor;
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedTableReferenceValue =
                getTempTableReferenceValue().translateCorrelations(translationMap);
        return new TempTableInsertPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                translatedTableReferenceValue, isOwningTempTable);
    }

    @Override
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public TempTableInsertPlan withChild(@Nonnull final Reference childRef) {
        return new TempTableInsertPlan(Quantifier.physical(childRef),
                getTempTableReferenceValue(),
                isOwningTempTable);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(Objects.requireNonNull(((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType()));
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final var otherTempTableScan =  (TempTableInsertPlan)otherExpression;
        return tempTableReferenceValue.semanticEquals(otherTempTableScan.tempTableReferenceValue, equivalences);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(BASE_HASH.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), getTempTableReferenceValue(), isOwningTempTable);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild(), getTempTableReferenceValue(), isOwningTempTable);
    }

    @Nonnull
    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    /**
     * Rewrite the planner graph for better visualization.
     *
     * @param childGraphs planner graphs of children expression that already have been computed
     *
     * @return the rewritten planner graph that models the filter as a node that uses the expression attribute
     * to depict the record types this operator filters.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {

        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.TemporaryDataNodeWithInfo(getResultType(), ImmutableList.of(getTempTableReferenceValue().toString())),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationOperatorNodeWithInfo(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("TempTableInsert"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    @Nonnull
    @Override
    public PTempTableInsertPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PTempTableInsertPlan.newBuilder()
                .setInner(inner.toProto(serializationContext))
                .setTempTableReferenceValue(getTempTableReferenceValue().toValueProto(serializationContext))
                .setIsOwningTempTable(isOwningTempTable)
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setTempTableInsertPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static TempTableInsertPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PTempTableInsertPlan tempTableInsertPlanProto) {
        return new TempTableInsertPlan(serializationContext, tempTableInsertPlanProto);
    }

    /**
     * Factory method to create a {@link TempTableInsertPlan}.
     *
     * @param inner an input value to transform
     * {@link RecordQueryAbstractDataModificationPlan#currentModifiedRecordAlias()}
     * @param tempTableReferenceValue The table queue identifier to insert into.
     *
     * @return a newly created {@link TempTableInsertPlan}
     */
    @Nonnull
    public static TempTableInsertPlan insertPlan(@Nonnull final Quantifier.Physical inner,
                                                 @Nonnull final Value tempTableReferenceValue,
                                                 boolean isOwningTempTable) {
        return new TempTableInsertPlan(inner, tempTableReferenceValue, isOwningTempTable);
    }

    @Nonnull
    public Value getTempTableReferenceValue() {
        return tempTableReferenceValue;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return tempTableReferenceValue.getCorrelatedToWithoutChildren();
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        // TODO https://github.com/FoundationDB/fdb-record-layer/issues/3020
        getChild().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getChild().getComplexity();
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PTempTableInsertPlan, TempTableInsertPlan> {
        @Nonnull
        @Override
        public Class<PTempTableInsertPlan> getProtoMessageClass() {
            return PTempTableInsertPlan.class;
        }

        @Nonnull
        @Override
        public TempTableInsertPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PTempTableInsertPlan tempTableInsertPlanProto) {
            return TempTableInsertPlan.fromProto(serializationContext, tempTableInsertPlanProto);
        }
    }
}
