/*
 * UpdateExpression.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAbstractDataModificationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A logical version of {@link RecordQueryUpdatePlan}. This is converted to a {@code RecordQueryUpdatePlan}
 * via the {@link com.apple.foundationdb.record.query.plan.cascades.rules.ImplementUpdateRule}.
 *
 * @see com.apple.foundationdb.record.query.plan.cascades.rules.ImplementUpdateRule
 */
public class UpdateExpression implements RelationalExpressionWithChildren, PlannerGraphRewritable {

    private static final String OLD_FIELD_NAME = "old";
    private static final String NEW_FIELD_NAME = "new";

    @Nonnull
    private final Quantifier.ForEach inner;
    @Nonnull
    private final String targetRecordType;
    @Nonnull
    private final Type.Record targetType;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final Map<FieldValue.FieldPath, Value> transformMap;

    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlatedToWithoutChildrenSupplier;

    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;

    public UpdateExpression(@Nonnull final Quantifier.ForEach inner,
                            @Nonnull final String targetRecordType,
                            @Nonnull final Type.Record targetType,
                            @Nonnull final Map<FieldValue.FieldPath, Value> transformMap) {
        this.inner = inner;
        this.targetRecordType = targetRecordType;
        this.targetType = targetType;
        this.resultValue = new QueriedValue(computeResultType(inner.getFlowedObjectType(), targetType));
        this.transformMap = ImmutableMap.copyOf(transformMap);
        this.correlatedToWithoutChildrenSupplier = Suppliers.memoize(this::computeCorrelatedToWithoutChildren);
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
    }

    @Nonnull
    public Type.Record getTargetType() {
        return targetType;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return correlatedToWithoutChildrenSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return transformMap.values()
                .stream()
                .flatMap(value -> value.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public UpdateExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedTransformMapBuilder = ImmutableMap.<FieldValue.FieldPath, Value>builder();
        for (final var entry : transformMap.entrySet()) {
            translatedTransformMapBuilder.put(entry.getKey(), entry.getValue().translateCorrelations(translationMap));
        }
        return new UpdateExpression(inner, targetRecordType, targetType, translatedTransformMapBuilder.build());
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    public RecordQueryUpdatePlan toPlan(@Nonnull final Quantifier.Physical physicalInner) {
        Verify.verify(inner.getAlias().equals(physicalInner.getAlias()));
        return RecordQueryUpdatePlan.updatePlan(physicalInner,
                targetRecordType,
                targetType,
                transformMap,
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
        final UpdateExpression otherUpdateExpression = (UpdateExpression)otherExpression;
        return targetRecordType.equals(otherUpdateExpression.targetRecordType) &&
               targetType.equals(otherUpdateExpression.targetType) &&
               semanticEqualsForTransformMap(transformMap, otherUpdateExpression.transformMap, equivalencesMap);
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
        return hashCodeWithoutChildrenSupplier.get();
    }

    public int computeHashCodeWithoutChildren() {
        return Objects.hash(targetRecordType, targetType, transformMap);
    }

    @Override
    public String toString() {
        final var str = new StringBuilder("Update(");
        str.append(targetRecordType).append(", ");
        str.append("[").append(transformMap.keySet().stream().map(FieldValue.FieldPath::toString).collect(Collectors.joining(", "))).append("] ");
        return str.toString();
    }

    /**
     * Create a planner graph for better visualization.
     * @return the rewritten planner graph that models the target as a separate node that is connected to the
     *         update expression node.
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

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationLogicalOperatorNode(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("UPDATE"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    @Nonnull
    private static Type.Record computeResultType(@Nonnull final Type inType, @Nonnull final Type targetType) {
        return Type.Record.fromFields(false,
                ImmutableList.of(Type.Record.Field.of(inType, Optional.of(OLD_FIELD_NAME)),
                        Type.Record.Field.of(targetType, Optional.of(NEW_FIELD_NAME))));
    }

    @Nonnull
    public static Value makeComputationValue(@Nonnull final Quantifier inner, @Nonnull final Type targetType) {
        final var oldColumn =
                Column.of(Optional.of(OLD_FIELD_NAME), inner.getFlowedObjectValue());
        final var newColumn =
                Column.of(Optional.of(NEW_FIELD_NAME), ObjectValue.of(RecordQueryAbstractDataModificationPlan.currentModifiedRecordAlias(), targetType));
        return RecordConstructorValue.ofColumns(ImmutableList.of(oldColumn, newColumn));
    }

    private static boolean semanticEqualsForTransformMap(@Nonnull final Map<FieldValue.FieldPath, Value> self,
                                                         @Nonnull final Map<FieldValue.FieldPath, Value> other,
                                                         @Nonnull final AliasMap equivalencesMap) {
        if (self.size() != other.size()) {
            return false;
        }

        for (final var entry : self.entrySet()) {
            final var fieldPath = entry.getKey();
            final var selfValue = entry.getValue();
            final var otherValue = other.get(fieldPath);
            if (!selfValue.semanticEquals(otherValue, equivalencesMap)) {
                return false;
            }
        }
        return true;
    }
}
