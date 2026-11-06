/*
 * WindowFunctionLogicalPlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.CompileTimeWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification;
import com.apple.foundationdb.relational.api.ddl.ThrowingQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.ddl.ThrowingMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that a SQL window function makes it through plan generation: the {@code OVER} clause is parsed, the function
 * is resolved through the SQL function catalog, and the resulting logical plan carries a
 * {@link CompileTimeWindowValue} describing the call site.
 *
 * <p>This deliberately stops at the logical plan rather than running the Cascades planner, because the call site is
 * what these tests are about: which columns end up as the function's arguments, its {@code PARTITION BY} and its
 * {@code ORDER BY}, and what frame it carries. Planning only that far needs neither an open record store nor a
 * database. What becomes of the call site afterwards is covered by {@code rank-index.yamsql} end to end.
 *
 * <p>Note the spelling throughout: a ranking window function takes <em>no</em> argument, and the column it ranks by is
 * the {@code ORDER BY} of the {@code OVER} clause, which is how standard SQL spells it. An argument there is a
 * different function -- {@code rank(v) WITHIN GROUP (ORDER BY x)} -- and is rejected, as the last two tests assert.
 */
class WindowFunctionLogicalPlanTest {

    @Nonnull
    private static final String SCHEMA_TEMPLATE_NAME = "WINDOW_FUNCTION_TEST_TEMPLATE";

    @Nonnull
    private static RecordLayerSchemaTemplate schemaTemplate() {
        return RecordLayerSchemaTemplate.newBuilder()
                .setName(SCHEMA_TEMPLATE_NAME)
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("T")
                        .addColumn(RecordLayerColumn.newBuilder().setName("ID")
                                .setDataType(DataType.Primitives.LONG.type()).build())
                        .addColumn(RecordLayerColumn.newBuilder().setName("A")
                                .setDataType(DataType.Primitives.LONG.type()).build())
                        .addColumn(RecordLayerColumn.newBuilder().setName("B")
                                .setDataType(DataType.Primitives.LONG.type()).build())
                        .addPrimaryKeyPart(ImmutableList.of("ID"))
                        .build())
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("U")
                        .addColumn(RecordLayerColumn.newBuilder().setName("ID")
                                .setDataType(DataType.Primitives.LONG.type()).build())
                        .addColumn(RecordLayerColumn.newBuilder().setName("C")
                                .setDataType(DataType.Primitives.LONG.type()).build())
                        .addPrimaryKeyPart(ImmutableList.of("ID"))
                        .build())
                .build();
    }

    /**
     * Generates the logical plan for the given query, stopping short of Cascades optimization.
     */
    @Nonnull
    private static RelationalExpression logicalPlanOf(@Nonnull final String query) throws RelationalException {
        final var schemaTemplate = schemaTemplate();
        final var ast = AstNormalizer.normalizeAst(schemaTemplate, QueryParser.parse(query), PreparedParams.empty(),
                PlannerConfiguration.ofAllAvailableIndexes(), false, PlanHashable.PlanHashMode.VC0, query, QueryEvaluationConstants.empty());
        final var planGenerationContext = new MutablePlanGenerationContext(PreparedParams.empty(),
                PlanHashable.PlanHashMode.VC0, query, ast.getQueryCacheKey().getCanonicalQueryString(),
                ast.getQueryExecutionContext().getParameterHash(), QueryEvaluationConstants.empty());
        final var plan = new BaseVisitor(planGenerationContext, schemaTemplate, ThrowingQueryFactory.INSTANCE,
                ThrowingMetadataOperationsFactory.INSTANCE, URI.create("embed:offline"), false)
                .generateLogicalPlan(ast.getParseTree());
        assertThat(plan).isInstanceOf(QueryPlan.LogicalQueryPlan.class);
        return ((QueryPlan.LogicalQueryPlan)plan).getRelationalExpression();
    }

    /**
     * Collects every {@link Value} appearing in the result value of any expression reachable from the given root.
     */
    @Nonnull
    private static Stream<Value> allValues(@Nonnull final RelationalExpression expression) {
        return Stream.concat(
                expression.getResultValue().preOrderStream(),
                expression.getQuantifiers().stream()
                        .flatMap(quantifier -> allValues(quantifier.getRangesOver().get())));
    }

    @Nonnull
    private static List<CompileTimeWindowValue> windowValuesOf(@Nonnull final String query) throws RelationalException {
        return allValues(logicalPlanOf(query))
                .filter(CompileTimeWindowValue.class::isInstance)
                .map(CompileTimeWindowValue.class::cast)
                .distinct()
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Names the column each of the given values reads, so that assertions do not have to mention the randomly generated
     * correlation alias the values are qualified by.
     */
    @Nonnull
    private static List<String> columnsOf(@Nonnull final List<? extends Value> values) {
        return values.stream()
                .map(value -> Iterables.getLast(((FieldValue)value).getFieldPathNames()))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Names the column each ordering part of the given window value sorts by.
     */
    @Nonnull
    private static List<String> orderingColumnsOf(@Nonnull final CompileTimeWindowValue windowValue) {
        return columnsOf(windowValue.getOrderingParts().stream()
                .map(WindowOrderingPart::getValue)
                .collect(ImmutableList.toImmutableList()));
    }

    public WindowFunctionLogicalPlanTest() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void rankOverPartitionEndsUpAsACompileTimeWindowValue() throws Exception {
        final var windowValues = windowValuesOf("select rank() over (partition by b order by a) from t");

        assertThat(windowValues).hasSize(1);
        final var windowValue = windowValues.get(0);
        assertThat(windowValue.getWindowFunction()).isEqualTo(CompileTimeWindowValue.WindowFunction.RANK);
        assertThat(windowValue.getName()).isEqualTo("rank");
    }

    @Test
    void rankCarriesItsPartitioningAndOrderingColumns() throws Exception {
        final var windowValue = windowValuesOf("select rank() over (partition by b order by a) from t").get(0);

        // the ranked column is the ORDER BY and the PARTITION BY column is the partitioning value; neither is an argument
        assertThat(windowValue.getArgumentValues()).isEmpty();
        assertThat(columnsOf(windowValue.getPartitioningValues())).containsExactly("B");
        assertThat(orderingColumnsOf(windowValue)).containsExactly("A");
        assertThat(windowValue.getWindowFrameSpecification().isDefault()).isTrue();
    }

    @Test
    void rankAcceptsSeveralOrderingColumns() throws Exception {
        final var windowValue = windowValuesOf("select rank() over (partition by b order by a, id) from t").get(0);

        assertThat(orderingColumnsOf(windowValue)).containsExactly("A", "ID");
        assertThat(windowValue.getWindowFunction()).isEqualTo(CompileTimeWindowValue.WindowFunction.RANK);
    }

    @Test
    void rankOverOrderByCarriesTheOrderingColumn() throws Exception {
        final var windowValue = windowValuesOf("select rank() over (partition by a order by b) from t").get(0);

        assertThat(columnsOf(windowValue.getPartitioningValues())).containsExactly("A");
        assertThat(orderingColumnsOf(windowValue)).containsExactly("B");
    }

    @Test
    void rankWithoutPartitioningIsStillAWindowValue() throws Exception {
        final var windowValue = windowValuesOf("select rank() over (order by a) from t").get(0);

        assertThat(windowValue.getWindowFunction()).isEqualTo(CompileTimeWindowValue.WindowFunction.RANK);
        assertThat(windowValue.getPartitioningValues()).isEmpty();
        assertThat(orderingColumnsOf(windowValue)).containsExactly("A");
    }

    @Test
    void rankWithAnExplicitFrameCarriesThatFrameRatherThanTheDefault() throws Exception {
        final var windowValue = windowValuesOf("select rank() over "
                + "(partition by b order by a rows between 1 preceding and current row) from t").get(0);

        final var frameSpecification = windowValue.getWindowFrameSpecification();
        assertThat(frameSpecification.isDefault()).isFalse();
        assertThat(frameSpecification.frameType()).isEqualTo(WindowFrameSpecification.FrameType.ROW);
        assertThat(frameSpecification.left()).isInstanceOf(WindowFrameSpecification.Bounded.class);
        assertThat(((WindowFrameSpecification.Bounded)frameSpecification.left()).limit().isConstant()).isTrue();
        assertThat(frameSpecification.right()).isInstanceOf(WindowFrameSpecification.CurrentRow.class);
        assertThat(frameSpecification.exclusion()).isEqualTo(WindowFrameSpecification.Exclusion.NO_OTHER);

        // the frame travels alongside the rest of the call site, it does not replace any of it
        assertThat(columnsOf(windowValue.getPartitioningValues())).containsExactly("B");
        assertThat(orderingColumnsOf(windowValue)).containsExactly("A");
    }

    @Test
    void rankWithARangeFrameCarriesTheFrameTypeAndExclusion() throws Exception {
        final var windowValue = windowValuesOf("select rank() over (partition by b order by a "
                + "range between unbounded preceding and current row exclude ties) from t").get(0);

        final var frameSpecification = windowValue.getWindowFrameSpecification();
        assertThat(frameSpecification.frameType()).isEqualTo(WindowFrameSpecification.FrameType.RANGE);
        assertThat(frameSpecification.left()).isEqualTo(WindowFrameSpecification.Unbounded.INSTANCE);
        assertThat(frameSpecification.right()).isInstanceOf(WindowFrameSpecification.CurrentRow.class);
        assertThat(frameSpecification.exclusion()).isEqualTo(WindowFrameSpecification.Exclusion.TIES);
    }

    @Test
    void rankWithAFrameStartOnlyEndsTheFrameAtTheCurrentRow() throws Exception {
        final var windowValue = windowValuesOf(
                "select rank() over (partition by b order by a rows unbounded preceding) from t").get(0);

        final var frameSpecification = windowValue.getWindowFrameSpecification();
        assertThat(frameSpecification.left()).isEqualTo(WindowFrameSpecification.Unbounded.INSTANCE);
        assertThat(frameSpecification.right()).isInstanceOf(WindowFrameSpecification.CurrentRow.class);
    }

    @Test
    void rankOverAJoinEndsUpAsACompileTimeWindowValue() throws Exception {
        final var windowValues = windowValuesOf(
                "select rank() over (partition by t.b order by t.a) from t, u where t.id = u.id");

        assertThat(windowValues).hasSize(1);
        final var windowValue = windowValues.get(0);
        assertThat(windowValue.getWindowFunction()).isEqualTo(CompileTimeWindowValue.WindowFunction.RANK);
        assertThat(columnsOf(windowValue.getPartitioningValues())).containsExactly("B");
        assertThat(orderingColumnsOf(windowValue)).containsExactly("A");
    }

    @Test
    void rankOverAJoinCanPartitionByTheOtherTable() throws Exception {
        final var windowValue = windowValuesOf(
                "select rank() over (partition by u.c order by t.a) from t, u where t.id = u.id").get(0);

        assertThat(columnsOf(windowValue.getPartitioningValues())).containsExactly("C");
        assertThat(orderingColumnsOf(windowValue)).containsExactly("A");
    }

    /**
     * An argument is what tells {@code rank(v) WITHIN GROUP (ORDER BY x)} -- an aggregate asking what rank {@code v}
     * would get -- apart from the window function, so it has no meaning in an {@code OVER} clause and is refused
     * rather than silently taken as the ranked column.
     */
    @Test
    void rankWithAnArgumentInTheOverClauseIsRejected() {
        assertThatThrownBy(() -> windowValuesOf("select rank(a) over (partition by b order by a) from t"))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("ranks by the ORDER BY of its OVER clause");
    }

    /**
     * Without an {@code ORDER BY} there is nothing to rank by, and the standard requires one for every ranking
     * function.
     */
    @Test
    void rankWithoutAnOrderByIsRejected() {
        assertThatThrownBy(() -> windowValuesOf("select rank() over (partition by b) from t"))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("needs an ORDER BY in its OVER clause");
    }
}
