/*
 * PlannerGraphTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fullTypeScan;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for transforming {@link com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression}s
 * into {@link com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph}s.
 */
class PlannerGraphTest {
    @BeforeEach
    void setUpDebugger() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    @Nonnull
    private RecordMetaData sampleMetaData() {
        return RecordMetaData.build(TestRecords1Proto.getDescriptor());
    }

    @Test
    void simpleSelect() {
        final Quantifier base = fullTypeScan(sampleMetaData(), "MySimpleRecord");
        final SelectExpression select = selectWithPredicates(base,
                fieldPredicate(base, "num_value_2", new Comparisons.NullComparison(Comparisons.Type.IS_NULL)));
        final PlannerGraph graph = PlannerGraphVisitor.forInternalShow(false).visit(select);
        final PlannerGraph.Node root = graph.getRoot();
        assertThat(root)
                .isInstanceOf(PlannerGraph.LogicalOperatorNode.class);
        assertThat(root.getName())
                .isEqualTo("SELECT " + select.getResultValue());
        assertThat(root.getDetails())
                .containsExactly("WHERE q2.num_value_2 [IS_NULL]");
        assertThat(graph.getNetwork().adjacentNodes(root))
                .hasSize(1);
    }

    @Test
    void selectWithLotsOfPredicates() {
        final Quantifier base = fullTypeScan(sampleMetaData(), "MySimpleRecord");
        final ConstantObjectValue strConstant = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.STRING));
        final ConstantObjectValue longConstant = ConstantObjectValue.of(Quantifier.constant(), "2", Type.primitiveType(Type.TypeCode.LONG));
        final SelectExpression select = selectWithPredicates(base,
                fieldPredicate(base, "num_value_2", new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                fieldPredicate(base, "str_value_indexed", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, strConstant)),
                fieldPredicate(base, "num_value_3_indexed", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 400)),
                fieldPredicate(base, "rec_no", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, longConstant)));
        final PlannerGraph graph = PlannerGraphVisitor.forInternalShow(false).visit(select);
        final PlannerGraph.Node root = graph.getRoot();
        assertThat(root)
                .isInstanceOf(PlannerGraph.LogicalOperatorNode.class);
        assertThat(root.getName())
                .isEqualTo("SELECT " + select.getResultValue());
        assertThat(root.getDetails())
                .hasSize(1);
        final String predicateDetail = root.getDetails().get(0);
        assertThat(predicateDetail)
                .as("large where clauses should be converted into a hex representation of their hash")
                .matches("[a-f0-9]{8}");
        assertThat(graph.getNetwork().adjacentNodes(root))
                .hasSize(1);
    }
}
