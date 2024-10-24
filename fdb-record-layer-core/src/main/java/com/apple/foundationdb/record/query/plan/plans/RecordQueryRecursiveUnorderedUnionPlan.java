/*
 * RecordQueryRecursiveUnionUnorderedPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.cursors.RecursiveUnorderedUnionCursor;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TODO.
 */
public class RecordQueryRecursiveUnorderedUnionPlan implements RecordQueryPlanWithChildren, RecordQuerySetPlan {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Recursive-Unordered-Union-Plan");

    @Nonnull
    private final List<Quantifier.Physical> baseRecursionQuantifiers;

    @Nonnull
    private final Quantifier.Physical recursiveQuantifier;

    public RecordQueryRecursiveUnorderedUnionPlan(@Nonnull final List<Quantifier.Physical> quantifiers) {
        Verify.verify(quantifiers.size() > 1);
        this.baseRecursionQuantifiers = quantifiers.subList(0, quantifiers.size() - 1);
        this.recursiveQuantifier = quantifiers.get(quantifiers.size() - 1);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChildren());
    }

    @Override
    public int getRelationalChildCount() {
        return 1 + baseRecursionQuantifiers.size();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return Set.of();
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final ExecuteProperties childExecuteProperties;
        // Can pass the limit down to all sides, since that is the most we'll take total.
        if (executeProperties.getSkip() > 0) {
            childExecuteProperties = executeProperties.clearSkipAndAdjustLimit();
        } else {
            childExecuteProperties = executeProperties;
        }
        final List<Function<byte[], RecordCursor<QueryResult>>> baseRecursionFunctions = baseRecursionQuantifiers.stream()
                .map(Quantifier.Physical::getRangesOverPlan)
                .map(childPlan -> (Function<byte[], RecordCursor<QueryResult>>)
                        ((byte[] childContinuation) -> childPlan
                                .executePlan(store, context, childContinuation, childExecuteProperties)))
                .collect(Collectors.toList());
        final Function<byte[], RecordCursor<QueryResult>> recursiveFunction =
                ((byte[] childContinuation) -> recursiveQuantifier.getRangesOverPlan().executePlan(store, context, childContinuation, childExecuteProperties));

        return RecursiveUnorderedUnionCursor.create(baseRecursionFunctions, recursiveFunction, continuation, store.getTimer())
                ;
//                .map(result -> {
//                    recursiveQuantifier.getRangesOverPlan()
//                });
    }

    @Nonnull
    @Override
    public List<RecordQueryPlan> getChildren() {
        return Stream.concat(baseRecursionQuantifiers.stream(), Stream.of(recursiveQuantifier))
                .map(Quantifier.Physical::getRangesOverPlan)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return null;
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
        return null;
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
        return null;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return List.of();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        return false;
    }

    @Override
    public int hashCodeWithoutChildren() {
        return 0;
    }

    @Nonnull
    @Override
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return null;
    }

    @Nonnull
    @Override
    public Set<KeyExpression> getRequiredFields() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQuerySetPlan withChildrenReferences(@Nonnull final List<? extends Reference> newChildren) {
        return new RecordQueryRecursiveUnorderedUnionPlan(
                newChildren.stream()
                        .map(Quantifier::physical)
                        .collect(ImmutableList.toImmutableList()));
    }
}
