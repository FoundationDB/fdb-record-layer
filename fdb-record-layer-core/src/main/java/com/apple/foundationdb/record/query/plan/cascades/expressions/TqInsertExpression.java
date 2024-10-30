/*
 * TqInsertExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TableQueue;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementTqInsertRule;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAbstractDataModificationPlan;
import com.apple.foundationdb.record.query.plan.plans.TqInsertPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A logical expression for inserting into a temporary memory buffer {@link TableQueue}.
 * This expression is used to implement a corresponding {@link TqInsertPlan} operator that
 * does exactly that.
 *
 * @see ImplementTqInsertRule for more information.
 */
public class TqInsertExpression implements RelationalExpressionWithChildren, PlannerGraphRewritable {

    @Nonnull
    private final Quantifier.ForEach inner;
    @Nonnull
    private final String targetRecordType;
    @Nonnull
    private final Type.Record targetType;
    @Nonnull
    private final Value resultValue;
    @Nonnull
    private final String tableQueueName;

    public TqInsertExpression(@Nonnull final Quantifier.ForEach inner,
                              @Nonnull final String targetRecordType,
                              @Nonnull final Type.Record targetType,
                              @Nonnull final String tableQueueName) {
        this.inner = inner;
        this.targetRecordType = targetRecordType;
        this.targetType = targetType;
        this.resultValue = new QueriedValue(targetType);
        this.tableQueueName = tableQueueName;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }


    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public InsertExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                  @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new InsertExpression(inner, targetRecordType, targetType);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    public TqInsertPlan toPlan(@Nonnull final Quantifier.Physical physicalInner) {
        Verify.verify(inner.getAlias().equals(physicalInner.getAlias()));
        return TqInsertPlan.insertPlan(physicalInner,
                targetRecordType,
                targetType,
                makeComputationValue(targetType),
                tableQueueName);
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
        final TqInsertExpression otherInsertExpression = (TqInsertExpression)otherExpression;
        return targetRecordType.equals(otherInsertExpression.targetRecordType) &&
                targetType.equals(otherInsertExpression.targetType);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(targetRecordType, targetType);
    }

    @Override
    public String toString() {
        return "InsertTableQueue(" + targetRecordType + ")";
    }

    /**
     * Create a planner graph for better visualization.
     * @return the rewritten planner graph that models the target as a separate node that is connected to the
     *         table-queue insert expression node.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(!childGraphs.isEmpty());

        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.TemporaryDataNodeWithInfo(getResultType(), ImmutableList.of(tableQueueName)),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationLogicalOperatorNode(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("TQINSERT"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    @Nonnull
    private static Value makeComputationValue(@Nonnull final Type targetType) {
        return ObjectValue.of(RecordQueryAbstractDataModificationPlan.currentModifiedRecordAlias(), targetType);
    }
}
