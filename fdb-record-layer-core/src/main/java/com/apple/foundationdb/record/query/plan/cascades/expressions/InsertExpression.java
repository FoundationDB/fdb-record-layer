/*
 * InsertExpression.java
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
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A logical version of {@link RecordQueryInsertPlan}.
 *
 * @see ImplementInsertRule which converts this to a {@link RecordQueryInsertPlan}
 */
public class InsertExpression implements RelationalExpressionWithChildren, PlannerGraphRewritable {

    private static final String OLD_FIELD_NAME = "old";

    private static final String NEW_FIELD_NAME = "new";

    @Nonnull
    private final Quantifier.ForEach inner;
    @Nonnull
    private final String targetRecordType;
    @Nonnull
    private final Type.Record targetType;
    @Nonnull
    private final Descriptors.Descriptor targetDescriptor;

    @Nonnull
    private final Value resultValue;

    public InsertExpression(@Nonnull final Quantifier.ForEach inner,
                            @Nonnull final String targetRecordType,
                            @Nonnull final Type.Record targetType,
                            @Nonnull final Descriptors.Descriptor targetDescriptor) {
        this.inner = inner;
        this.targetRecordType = targetRecordType;
        this.targetType = targetType;
        this.targetDescriptor = targetDescriptor;
        this.resultValue = new QueriedValue(computeResultType(inner.getFlowedObjectType(), targetType));
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
    public InsertExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new InsertExpression(inner, targetRecordType, targetType, targetDescriptor);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    public RecordQueryInsertPlan toPlan(@Nonnull final Quantifier.Physical physicalInner) {
        Verify.verify(inner.getAlias().equals(physicalInner.getAlias()));
        return RecordQueryInsertPlan.insertPlan(physicalInner,
                targetRecordType,
                targetType,
                targetDescriptor,
                makeComputationValue(physicalInner, targetType));
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
        final InsertExpression otherInsertExpression = (InsertExpression)otherExpression;
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
        return "Insert(" + targetRecordType + ")";
    }

    /**
     * Create a planner graph for better visualization.
     * @return the rewritten planner graph that models the target as a separate node that is connected to the
     *         insert expression node.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(!childGraphs.isEmpty());

        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                                getResultType(),
                                ImmutableList.of(targetRecordType)),
                        ImmutableList.of());

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.ModificationLogicalOperatorNode(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("INSERT"),
                        ImmutableMap.of()),
                ImmutableList.<PlannerGraph>builder().addAll(childGraphs).add(graphForTarget).build());
    }

    @Nonnull
    private static Type.Record computeResultType(@Nonnull final Type inType, @Nonnull final Type targetType) {
        return Type.Record.fromFields(false,
                ImmutableList.of(Type.Record.Field.of(inType, Optional.of(OLD_FIELD_NAME)),
                        Type.Record.Field.of(targetType, Optional.of(NEW_FIELD_NAME))));
    }

    @Nonnull
    private static Value makeComputationValue(@Nonnull final Quantifier inner, @Nonnull final Type targetType) {
        final var oldColumn =
                Column.of(Type.Record.Field.of(inner.getFlowedObjectType(), Optional.of(OLD_FIELD_NAME)), new NullValue(inner.getFlowedObjectType()));
        final var newColumn =
                Column.of(Type.Record.Field.of(targetType, Optional.of(NEW_FIELD_NAME)), ObjectValue.of(Quantifier.CURRENT, targetType));
        return RecordConstructorValue.ofColumns(ImmutableList.of(oldColumn, newColumn));
    }
}
