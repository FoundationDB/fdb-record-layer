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
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryAbstractDataModificationPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A logical version of {@link RecordQueryUpdatePlan}. This is converted to a {@code RecordQueryUpdatePlan}
 * via the {@link com.apple.foundationdb.record.query.plan.cascades.rules.ImplementUpdateRule}.
 *
 * @see com.apple.foundationdb.record.query.plan.cascades.rules.ImplementUpdateRule
 */
public class UpdateExpression extends AbstractRelationalExpressionWithChildren implements PlannerGraphRewritable {

    private static final String OLD_FIELD_NAME = "old";
    private static final String NEW_FIELD_NAME = "new";

    private final Quantifier.ForEach inner;
    private final String targetRecordType;
    private final Type.Record targetType;

    private final Value resultValue;

    private final Map<FieldValue.FieldPath, Value> transformMap;

    public UpdateExpression(final Quantifier.ForEach inner,
                            final String targetRecordType,
                            final Type.Record targetType,
                            final Map<FieldValue.FieldPath, Value> transformMap) {
        this.inner = inner;
        this.targetRecordType = targetRecordType;
        this.targetType = targetType;
        this.resultValue = new QueriedValue(computeResultType(inner.getFlowedObjectType(), targetType));
        this.transformMap = ImmutableMap.copyOf(transformMap);
    }

    public Type.Record getTargetType() {
        return targetType;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return transformMap.values()
                .stream()
                .flatMap(value -> value.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public UpdateExpression translateCorrelations(final TranslationMap translationMap,
                                                  final boolean shouldSimplifyValues,
                                                  final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedTransformMapBuilder = ImmutableMap.<FieldValue.FieldPath, Value>builder();
        for (final var entry : transformMap.entrySet()) {
            translatedTransformMapBuilder.put(entry.getKey(),
                    entry.getValue().translateCorrelations(translationMap, shouldSimplifyValues));
        }
        return new UpdateExpression(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.ForEach.class),
                targetRecordType, targetType, translatedTransformMapBuilder.build());
    }

    @Override
    public Value getResultValue() {
        return resultValue;
    }

    public RecordQueryUpdatePlan toPlan(final Quantifier.Physical physicalInner) {
        Verify.verify(inner.getAlias().equals(physicalInner.getAlias()));
        return RecordQueryUpdatePlan.updatePlan(physicalInner,
                targetRecordType,
                targetType,
                transformMap,
                makeComputationValue(physicalInner, targetType));
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(RelationalExpression otherExpression,
                                         final AliasMap equivalencesMap) {
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(targetRecordType, targetType, transformMap);
    }

    @Override
    public String toString() {
        final var str = new StringBuilder("Update(");
        str.append(ProtoUtils.toUserIdentifier(targetRecordType)).append(", ");
        str.append("[").append(transformMap.keySet().stream().map(FieldValue.FieldPath::toString).collect(Collectors.joining(", "))).append("] ");
        return str.toString();
    }

    /**
     * Create a planner graph for better visualization.
     * @return the rewritten planner graph that models the target as a separate node that is connected to the
     *         update expression node.
     */
    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(!childGraphs.isEmpty());

        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                                getResultType(),
                                ImmutableList.of(ProtoUtils.toUserIdentifier(targetRecordType))),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationLogicalOperatorNode(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("UPDATE"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    private static Type.Record computeResultType(final Type inType, final Type targetType) {
        return Type.Record.fromFields(false,
                ImmutableList.of(Type.Record.Field.of(inType, Optional.of(OLD_FIELD_NAME)),
                        Type.Record.Field.of(targetType, Optional.of(NEW_FIELD_NAME))));
    }

    public static Value makeComputationValue(final Quantifier inner, final Type targetType) {
        final var oldColumn =
                Column.of(Optional.of(OLD_FIELD_NAME), inner.getFlowedObjectValue());
        final var newColumn =
                Column.of(Optional.of(NEW_FIELD_NAME), ObjectValue.of(RecordQueryAbstractDataModificationPlan.currentModifiedRecordAlias(), targetType));
        return RecordConstructorValue.ofColumns(ImmutableList.of(oldColumn, newColumn));
    }

    private static boolean semanticEqualsForTransformMap(final Map<FieldValue.FieldPath, Value> self,
                                                         final Map<FieldValue.FieldPath, Value> other,
                                                         final AliasMap equivalencesMap) {
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
