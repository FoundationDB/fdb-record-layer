/*
 * TempTableScanExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TempTable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.TempTableScanPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A logical expression for scanning from a table-valued {@link Bindings.Internal#CORRELATION}
 * that can correspond to a temporary memory buffer, i.e. a {@link TempTable}.
 * This expression is used to implement a corresponding {@link TempTableScanPlan} operator that
 * does exactly that.
 */
@API(API.Status.EXPERIMENTAL)
public class TempTableScanExpression implements RelationalExpression, PlannerGraphRewritable {
    @Nonnull
    private final Value tempTableReferenceValue;
    @Nonnull
    private final QueriedValue resultValue;

    private TempTableScanExpression(@Nonnull final Value tempTableReferenceValue) {
        this.tempTableReferenceValue = tempTableReferenceValue;
        final var innerType = ((Type.Relation)tempTableReferenceValue.getResultType()).getInnerType();
        this.resultValue = new QueriedValue(Objects.requireNonNull(innerType));
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
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return tempTableReferenceValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public TempTableScanExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                         final boolean shouldSimplifyValues,
                                                         @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new TempTableScanExpression(
                tempTableReferenceValue.translateCorrelations(translationMap, shouldSimplifyValues));
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final var otherTempTableScanExpression = (TempTableScanExpression)otherExpression;
        return getTempTableReferenceValue().semanticEquals(otherTempTableScanExpression.getTempTableReferenceValue(), equivalencesMap);
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
        return Objects.hash(tempTableReferenceValue);
    }

    @Override
    public String toString() {
        return "TempTableScan";
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(childGraphs.isEmpty());

        final PlannerGraph.DataNodeWithInfo dataNodeWithInfo = new PlannerGraph
                .TemporaryDataNodeWithInfo(getResultType(), ImmutableList.of(tempTableReferenceValue.toString()));

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.TEMP_TABLE_SCAN_OPERATOR,
                        ImmutableList.of(),
                        ImmutableMap.of()),
                ImmutableList.of(PlannerGraph.fromNodeAndChildGraphs(
                        dataNodeWithInfo,
                        childGraphs)));
    }

    /**
     * Creates a new instance of {@link TempTableScanExpression} that scans records from a constant-bound {@link TempTable},
     * i.e. a temporary table that is not correlated to any other plan operator.
     *
     * @param constantAlias The alias of the constant-bound temporary table.
     * @param constantId The id of the constant in the constant map.
     * @param type The type of the temporary table records.
     * @return A new {@link TempTableScanExpression} that adds records to a constant-bound {@link TempTable}.
     */
    @Nonnull
    public static TempTableScanExpression ofConstant(@Nonnull final CorrelationIdentifier constantAlias,
                                                     @Nonnull final String constantId,
                                                     @Nonnull final Type type) {
        return new TempTableScanExpression(ConstantObjectValue.of(constantAlias, constantId, new Type.Relation(type)));
    }

    /**
     * Creates a new instance of {@link TempTableScanExpression} that scans records from a correlated {@link TempTable},
     * i.e. a temporary table that is the result of a table-valued correlation.
     *
     * @param correlation The table-valued correlation.
     * @param type The type of the temporary table records.
     * @return A new {@link TempTableScanExpression} that adds records to a correlated {@link TempTable}.
     */
    @Nonnull
    public static TempTableScanExpression ofCorrelated(@Nonnull final CorrelationIdentifier correlation,
                                                       @Nonnull final Type type) {
        return new TempTableScanExpression(QuantifiedObjectValue.of(correlation, new Type.Relation(type)));
    }
}
