/*
 * ViewUpdatabilityAnalyzer.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;

import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Analyzes the compiled logical plan of a view to determine whether it is updatable.
 *
 * <p>A view is updatable when its defining query satisfies all of the following conditions:
 * <ol>
 *   <li>The top-level expression is a {@link SelectExpression} with exactly one child quantifier (no joins).</li>
 *   <li>There are no set operations ({@link LogicalUnionExpression}).</li>
 *   <li>There are no aggregations ({@link GroupByExpression}).</li>
 *   <li>There is no {@code DISTINCT} ({@link LogicalDistinctExpression}).</li>
 *   <li>The single child quantifier ranges over a {@link LogicalTypeFilterExpression} targeting
 *       exactly one base record type.</li>
 *   <li>The projection covers every column of the base table ({@code SELECT *} — no partial
 *       projections, no computed expressions, no {@code DISTINCT} without a GroupBy that would
 *       have been caught earlier).</li>
 * </ol>
 *
 * <p>When all conditions are met, {@link #analyze} returns a populated {@link ViewUpdatabilityInfo}
 * containing the base table, its identifier, any view-level predicates (the view's own WHERE
 * clause), and the quantifier alias that those predicates reference.
 */
@API(API.Status.EXPERIMENTAL)
public final class ViewUpdatabilityAnalyzer {

    private ViewUpdatabilityAnalyzer() {
    }

    /**
     * Returns {@link ViewUpdatabilityInfo} if {@code viewOp} represents an updatable view,
     * or {@link Optional#empty()} if the view is read-only.
     */
    @Nonnull
    public static Optional<ViewUpdatabilityInfo> analyze(@Nonnull final LogicalOperator viewOp,
                                                         @Nonnull final SchemaTemplate catalog) {
        // The compiled view plan is always headed by a forEach quantifier whose target is the
        // SelectExpression produced by the view's query. When the view is loaded from FDB and
        // compiled via RoutineParser (a fresh BaseVisitor with no parent plan fragment), the
        // isTopLevel flag is true, so generateSelect wraps the SelectExpression in a
        // LogicalSortExpression.unsorted(). We look through that transparent wrapper here.
        RelationalExpression topExpr = viewOp.getQuantifier().getRangesOver().get();
        if (topExpr instanceof LogicalSortExpression) {
            final LogicalSortExpression sortExpr = (LogicalSortExpression) topExpr;
            if (sortExpr.getQuantifiers().size() != 1) {
                return Optional.empty();
            }
            topExpr = Iterables.getOnlyElement(sortExpr.getQuantifiers()).getRangesOver().get();
        }

        // Rule 1: must be a simple SELECT (no GROUP BY, DISTINCT, UNION as top-level).
        if (!(topExpr instanceof SelectExpression)) {
            return Optional.empty();
        }
        final SelectExpression selectExpr = (SelectExpression) topExpr;

        // Rule 2: exactly one source quantifier (no joins, no multi-table FROM).
        if (selectExpr.getQuantifiers().size() != 1) {
            return Optional.empty();
        }

        // Rule 3: no disqualifying expressions in the subtree.
        final Quantifier innerQun = Iterables.getOnlyElement(selectExpr.getQuantifiers());
        if (containsDisqualifyingExpression(innerQun.getRangesOver().get())) {
            return Optional.empty();
        }

        // Rule 4: the single inner quantifier must range over a LogicalTypeFilterExpression,
        // which is the signature of a plain base-table scan.
        final RelationalExpression innerExpr = innerQun.getRangesOver().get();
        if (!(innerExpr instanceof LogicalTypeFilterExpression)) {
            return Optional.empty();
        }
        final LogicalTypeFilterExpression typeFilter = (LogicalTypeFilterExpression) innerExpr;

        // Rule 5: the type filter must cover exactly one record type (single base table).
        if (typeFilter.getRecordTypes().size() != 1) {
            return Optional.empty();
        }
        final String storageName = Iterables.getOnlyElement(typeFilter.getRecordTypes());

        // Locate the corresponding RecordLayerTable in the catalog.
        final Optional<RecordLayerTable> baseTableOpt = findTableByStorageName(catalog, storageName);
        if (baseTableOpt.isEmpty()) {
            return Optional.empty();
        }
        final RecordLayerTable baseTable = baseTableOpt.get();

        // Rule 6: the projection must cover all base-table columns — partial projections are
        // non-updatable because the DML rewrite cannot determine default values for
        // omitted columns.  A SELECT * view expands to exactly as many result values as the
        // base table has columns.
        if (selectExpr.getResultValues().size() != baseTable.getColumns().size()) {
            return Optional.empty();
        }

        return Optional.of(new ViewUpdatabilityInfo(
                baseTable,
                Identifier.of(baseTable.getName()),
                selectExpr.getPredicates(),
                innerQun.getAlias()));
    }

    /**
     * Returns {@code true} if {@code expr} or any expression reachable through its quantifiers
     * is a disqualifying expression type (aggregation, DISTINCT, or set operation).
     */
    private static boolean containsDisqualifyingExpression(@Nonnull final RelationalExpression expr) {
        if (expr instanceof GroupByExpression ||
                expr instanceof LogicalDistinctExpression ||
                expr instanceof LogicalUnionExpression) {
            return true;
        }
        for (final Quantifier qun : expr.getQuantifiers()) {
            if (containsDisqualifyingExpression(qun.getRangesOver().get())) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    private static Optional<RecordLayerTable> findTableByStorageName(@Nonnull final SchemaTemplate catalog,
                                                                      @Nonnull final String storageName) {
        try {
            for (final var table : catalog.getTables()) {
                if (table instanceof RecordLayerTable) {
                    final RecordLayerTable rlt = (RecordLayerTable) table;
                    if (storageName.equals(rlt.getType().getStorageName())) {
                        return Optional.of(rlt);
                    }
                }
            }
            return Optional.empty();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }
}
