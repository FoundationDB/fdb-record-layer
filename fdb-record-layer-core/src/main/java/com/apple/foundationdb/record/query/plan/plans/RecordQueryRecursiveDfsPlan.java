/*
 * RecordQueryRecursiveDfsPlan.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.RecursiveCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class RecordQueryRecursiveDfsPlan implements RecordQueryPlanWithChildren {

    @Nonnull
    private final Quantifier.Physical root;

    @Nonnull
    private final Quantifier.Physical recursive;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final CorrelationIdentifier priorValueCorrelation;

    public RecordQueryRecursiveDfsPlan(@Nonnull final Quantifier.Physical root,
                                       @Nonnull final Quantifier.Physical recursive,
                                       @Nonnull final CorrelationIdentifier priorValueCorrelation) {
        this.root = root;
        this.recursive = recursive;
        this.priorValueCorrelation = priorValueCorrelation;
        this.resultValue = RecordQuerySetPlan.mergeValues(ImmutableList.of(root, recursive));
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return 0;
    }

    @Override
    public int getRelationalChildCount() {
        return 2;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return resultValue.getCorrelatedToWithoutChildren();
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation, @Nonnull final ExecuteProperties executeProperties) {
        final var nestedExecuteProperties = executeProperties.clearSkipAndLimit();
        return RecursiveCursor.create(
                        rootContinuation ->
                                root.getRangesOverPlan().executePlan(store, context, rootContinuation, nestedExecuteProperties),
                        (parentResult, depth, innerContinuation) -> {
                            final var child2Context = context.withBinding(Bindings.Internal.CORRELATION.bindingName(priorValueCorrelation.getId()), parentResult);
                            return recursive.getRangesOverPlan().executePlan(store, child2Context, innerContinuation, nestedExecuteProperties);
                        },
                        null,
                        continuation
                ).skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit())
                .map(RecursiveCursor.RecursiveValue::getValue);
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return ImmutableList.of(root.getRangesOverPlan(), recursive.getRangesOverPlan());
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return null;
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return null;
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.RECURSIVE_DFS_OPERATOR),
                childGraphs);
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
    }

    @Override
    public int getComplexity() {
        return 0;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(root, recursive);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof RecordQueryRecursiveDfsPlan)) {
            return false;
        }
        final var otherRecursiveDfsPlan = (RecordQueryRecursiveDfsPlan)otherExpression;
        return this.priorValueCorrelation.equals(otherRecursiveDfsPlan.priorValueCorrelation);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(42);
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 2);
        Verify.verify(!translationMap.containsSourceAlias(priorValueCorrelation));
        final var translatedRootQuantifier = translatedQuantifiers.get(0).narrow(Quantifier.Physical.class);
        final var translatedRecursiveQuantifier = translatedQuantifiers.get(1).narrow(Quantifier.Physical.class);
        return new RecordQueryRecursiveDfsPlan(translatedRootQuantifier, translatedRecursiveQuantifier,
                priorValueCorrelation);
    }
}
