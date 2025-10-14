/*
 * TempTableInsertExpression.java
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
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementTempTableInsertRule;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.TempTableInsertPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A logical expression for inserting into a temporary memory buffer {@link TempTable}.
 * This expression is used to implement a corresponding {@link TempTableInsertPlan} operator that
 * does exactly that.
 *
 * @see ImplementTempTableInsertRule for more information.
 */
public class TempTableInsertExpression extends AbstractRelationalExpressionWithChildren implements PlannerGraphRewritable {

    @Nonnull
    private final Quantifier.ForEach inner;

    @Nonnull
    private final Value resultValue;

    @Nonnull
    private final Value tempTableReferenceValue;

    private final boolean isOwningTempTable;

    private TempTableInsertExpression(@Nonnull final Quantifier.ForEach inner,
                                      @Nonnull final Value tempTableReferenceValue,
                                      boolean isOwningTempTable) {
        this.inner = inner;
        this.tempTableReferenceValue = tempTableReferenceValue;
        this.isOwningTempTable = isOwningTempTable;
        Verify.verify(tempTableReferenceValue.getResultType().isRelation());
        final var innerType = ((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType();
        this.resultValue = new QueriedValue(Objects.requireNonNull(innerType));
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return tempTableReferenceValue.getCorrelatedToWithoutChildren();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public TempTableInsertExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                           final boolean shouldSimplifyValues,
                                                           @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedTableReferenceValue =
                tempTableReferenceValue.translateCorrelations(translationMap, shouldSimplifyValues);
        return new TempTableInsertExpression(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.ForEach.class),
                translatedTableReferenceValue, isOwningTempTable);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    public Value getTempTableReferenceValue() {
        return tempTableReferenceValue;
    }

    @Nonnull
    public TempTableInsertPlan toPlan(@Nonnull final Quantifier.Physical physicalInner) {
        Verify.verify(inner.getAlias().equals(physicalInner.getAlias()));
        return TempTableInsertPlan.insertPlan(physicalInner, tempTableReferenceValue, isOwningTempTable);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final TempTableInsertExpression otherInsertExpression = (TempTableInsertExpression)otherExpression;
        return tempTableReferenceValue.semanticEquals(otherInsertExpression.tempTableReferenceValue, equivalencesMap);
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
        return Objects.hash(tempTableReferenceValue);
    }

    @Override
    public String toString() {
        return "TempTableInsert(" + tempTableReferenceValue + ")";
    }

    /**
     * Create a planner graph for better visualization.
     * @return the rewritten planner graph that models the target as a separate node that is connected to the
     *         temporary table insert expression node.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(!childGraphs.isEmpty());

        final var graphForTarget =
                PlannerGraph.fromNodeAndChildGraphs(
                        new PlannerGraph.TemporaryDataNodeWithInfo(getResultType(), ImmutableList.of(tempTableReferenceValue.toString())),
                        ImmutableList.of());

        return PlannerGraph.fromNodeInnerAndTargetForModifications(
                new PlannerGraph.ModificationLogicalOperatorNode(this,
                        NodeInfo.MODIFICATION_OPERATOR,
                        ImmutableList.of("TempTableInsert"),
                        ImmutableMap.of()),
                Iterables.getOnlyElement(childGraphs), graphForTarget);
    }

    /**
     * Creates a new instance of {@link TempTableInsertExpression} that adds records to a constant-bound {@link TempTable},
     * i.e. a temporary table that is not correlated to any other plan operator. Note that this expression generates a
     * physical operator that <li>owns</li> the lifecycle management of the underlying {@link TempTable}.
     *
     * @param inner The source of the inserted records
     * @param constantAlias The alias of the constant-bound temporary table.
     * @param constantId The id of the constant in the constant map.
     * @param type The type of the temporary table records.
     * @return A new {@link TempTableInsertExpression} that adds records to a constant-bound {@link TempTable}.
     */
    @Nonnull
    public static TempTableInsertExpression ofConstant(@Nonnull final Quantifier.ForEach inner,
                                                       @Nonnull final CorrelationIdentifier constantAlias,
                                                       @Nonnull final String constantId,
                                                       @Nonnull final Type type) {
        return ofConstant(inner, constantAlias, constantId, type, true);
    }

    /**
     * Creates a new instance of {@link TempTableInsertExpression} that adds records to a constant-bound {@link TempTable},
     * i.e. a temporary table that is not correlated to any other plan operator.
     *
     * @param inner The source of the inserted records
     * @param constantAlias The alias of the constant-bound temporary table.
     * @param constantId The id of the constant in the constant map.
     * @param type The type of the temporary table records.
     * @param isOwningTempTable if set to {@code True} then the expression will own the lifecycle of the underlying
     *                          {@link TempTable}, otherwise {@code False}.
     * @return A new {@link TempTableInsertExpression} that adds records to a constant-bound {@link TempTable}.
     */
    @Nonnull
    public static TempTableInsertExpression ofConstant(@Nonnull final Quantifier.ForEach inner,
                                                       @Nonnull final CorrelationIdentifier constantAlias,
                                                       @Nonnull final String constantId,
                                                       @Nonnull final Type type,
                                                       boolean isOwningTempTable) {
        return new TempTableInsertExpression(inner, ConstantObjectValue.of(constantAlias, constantId, new Type.Relation(type)), isOwningTempTable);
    }

    /**
     * Creates a new instance of {@link TempTableInsertExpression} that adds records to its own correlated {@link TempTable},
     * i.e. a temporary table that is the result of a table-valued correlation. Note that this expression generates a
     * physical operator that <li>owns</li> the lifecycle management of the underlying {@link TempTable}.
     *
     * @param inner The source of the inserted records
     * @param correlation The table-valued correlation.
     * @param type The type of the temporary table records.
     *
     * @return A new {@link TempTableInsertExpression} that adds records to a correlated {@link TempTable}.
     */
    @Nonnull
    public static TempTableInsertExpression ofCorrelated(@Nonnull final Quantifier.ForEach inner,
                                                         @Nonnull final CorrelationIdentifier correlation,
                                                         @Nonnull final Type type) {
        return ofCorrelated(inner, correlation, type, true);
    }

    /**
     * Creates a new instance of {@link TempTableInsertExpression} that adds records to a correlated {@link TempTable},
     * i.e. a temporary table that is the result of a table-valued correlation.
     *
     * @param inner The source of the inserted records
     * @param correlation The table-valued correlation.
     * @param type The type of the temporary table records.
     * @param isOwningTempTable if set to {@code True} then the expression will own the lifecycle of the underlying
     *                          {@link TempTable}, otherwise {@code False}.
     *
     * @return A new {@link TempTableInsertExpression} that adds records to a correlated {@link TempTable}.
     */
    @Nonnull
    public static TempTableInsertExpression ofCorrelated(@Nonnull final Quantifier.ForEach inner,
                                                         @Nonnull final CorrelationIdentifier correlation,
                                                         @Nonnull final Type type,
                                                         boolean isOwningTempTable) {
        return new TempTableInsertExpression(inner, QuantifiedObjectValue.of(correlation, new Type.Relation(type)), isOwningTempTable);
    }
}
