/*
 * RecordQueryStoreBindingPlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SchemaIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A plan node that intercepts {@link #executePlan} and substitutes a secondary
 * {@link FDBRecordStoreBase} (looked up by schema name from
 * {@link EvaluationContext#getAuxiliaryStore}) for its inner subtree.
 *
 * <p>This node is emitted by {@code ImplementNestedLoopJoinRule} when the inner side of a
 * {@code RecordQueryFlatMapPlan} belongs to a different schema than the outer side. It is
 * transparent for all planning purposes — it does not reorder, filter, or transform records.</p>
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQueryStoreBindingPlan extends AbstractRelationalExpressionWithChildren implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Store-Binding-Plan");

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final SchemaIdentifier schemaId;

    public RecordQueryStoreBindingPlan(@Nonnull final Quantifier.Physical inner,
                                       @Nonnull final SchemaIdentifier schemaId) {
        this.inner = inner;
        this.schemaId = schemaId;
    }

    @Nonnull
    public SchemaIdentifier getSchemaId() {
        return schemaId;
    }

    @Nonnull
    public RecordQueryPlan getInnerPlan() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    public RecordQueryPlan getChild() {
        return getInnerPlan();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryStoreBindingPlan(
                Quantifier.physical(childRef, inner.getAlias()),
                schemaId);
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final String schemaName = Objects.requireNonNull(schemaId.getSchemaName(),
                "RecordQueryStoreBindingPlan requires a named schema, not current()");
        final FDBRecordStoreBase<?> secondaryStore = context.getAuxiliaryStore(schemaName);
        if (secondaryStore == null) {
            throw new RecordCoreException("No auxiliary store bound for schema: " + schemaName);
        }
        return getInnerPlan().executePlan((FDBRecordStoreBase<M>) secondaryStore, context, continuation, executeProperties);
    }

    @Override
    public boolean isReverse() {
        return getInnerPlan().isReverse();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryStoreBindingPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                             final boolean shouldSimplifyValues,
                                                             @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryStoreBindingPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                schemaId);
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
        return Objects.hash(schemaId);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return schemaId.equals(((RecordQueryStoreBindingPlan) otherExpression).schemaId);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return getInnerPlan().planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getInnerPlan(),
                        schemaId.getSchemaName());
            default:
                throw new RecordCoreException("Unknown PlanHashMode: " + mode.getKind());
        }
    }

    @Override
    public void logPlanStructure(@Nonnull final StoreTimer timer) {
        getInnerPlan().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInnerPlan().getComplexity();
    }

    @Nonnull
    @Override
    public String toString() {
        return "STORE_BIND(" + schemaId + ") | " + getInnerPlan();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.PREDICATE_FILTER_OPERATOR,
                        ImmutableList.of("STORE_BIND {{schema}}"),
                        ImmutableMap.of("schema", Attribute.gml(schemaId.toString()))),
                childGraphs);
    }

    @Nonnull
    @Override
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("RecordQueryStoreBindingPlan proto serialization not yet implemented");
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("RecordQueryStoreBindingPlan proto serialization not yet implemented");
    }

    @Nonnull
    public static RecordQueryStoreBindingPlan of(@Nonnull final RecordQueryPlan innerPlan,
                                                 @Nonnull final SchemaIdentifier schemaId) {
        return new RecordQueryStoreBindingPlan(Quantifier.physical(Reference.plannedOf(innerPlan)), schemaId);
    }
}
