/*
 * ValueToKeyExpressionVisitorTest.java
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.CallSiteArguments;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexOnlyAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Input projection list -> expected {@link KeyExpression}, for the shapes {@code IndexTest} produces.
 */
class ValueToKeyExpressionVisitorTest {

    @Nonnull
    private static final Type.Record T2_TYPE = t2Type();

    @Nonnull
    private static Type.Record t2Type() {
        final var template = RecordLayerSchemaTemplate.newBuilder()
                .setName("TEST_TEMPLATE")
                .setVersion(1)
                .addTable(RecordLayerTable.newBuilder(false)
                        .setName("T2")
                        .addColumn(RecordLayerColumn.newBuilder().setName("COL1")
                                .setDataType(DataType.Primitives.LONG.type()).build())
                        .addColumn(RecordLayerColumn.newBuilder().setName("COL2")
                                .setDataType(DataType.Primitives.LONG.type()).build())
                        .addColumn(RecordLayerColumn.newBuilder().setName("COL3")
                                .setDataType(DataType.Primitives.LONG.type()).build())
                        .addPrimaryKeyPart(ImmutableList.of("COL1"))
                        .build())
                .build();
        return Type.Record.fromDescriptor(template.getDescriptor("T2"));
    }

    @Nonnull
    private static Value col(@Nonnull final String name) {
        return FieldValue.ofFieldName(QuantifiedObjectValue.of(Quantifier.current(), T2_TYPE), name);
    }

    @Nonnull
    private static KeyExpression translate(@Nonnull final Value... projectedValues) {
        return translate(RecordConstructorValue.ofUnnamed(List.of(projectedValues)));
    }

    @Nonnull
    private static KeyExpression translate(@Nonnull final Value value) {
        return ValueToKeyExpressionVisitor.translate(value, Map.of(), ExtremumEverStorage.TUPLE).keyExpression();
    }

    @Nonnull
    private static String indexType(@Nonnull final ExtremumEverStorage extremumEverStorage,
                                    @Nonnull final Value... projectedValues) {
        return ValueToKeyExpressionVisitor
                .translate(RecordConstructorValue.ofUnnamed(List.of(projectedValues)), Map.of(), extremumEverStorage)
                .indexType();
    }

    @Test
    void singleColumn() {
        assertThat(translate(col("COL1"))).isEqualTo(field("COL1"));
    }

    @Test
    void twoColumns() {
        assertThat(translate(col("COL1"), col("COL2")))
                .isEqualTo(concat(field("COL1"), field("COL2")));
    }

    @Test
    void threeColumns() {
        assertThat(translate(col("COL1"), col("COL2"), col("COL3")))
                .isEqualTo(concat(field("COL1"), field("COL2"), field("COL3")));
    }

    @Test
    void literal() {
        assertThat(translate(LiteralValue.ofScalar(5L))).isEqualTo(Key.Expressions.value(5L));
    }

    @Test
    void arithmeticOverColumns() {
        final var sum = new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_LL, col("COL1"), col("COL2"));
        assertThat(translate(sum))
                .isEqualTo(Key.Expressions.function("add", concat(field("COL1"), field("COL2"))));
    }

    @Test
    void minEverGroupedByOneColumn() {
        assertThat(translate(minEver(col("COL3")), col("COL1")))
                .isEqualTo(field("COL3").groupBy(field("COL1")));
    }

    @Test
    void minEverGroupedByTwoColumns() {
        assertThat(translate(minEver(col("COL3")), col("COL1"), col("COL2")))
                .isEqualTo(field("COL3").groupBy(field("COL1"), field("COL2")));
    }

    @Test
    void maxEverGroupedByOneColumn() {
        assertThat(translate(maxEver(col("COL1")), col("COL2")))
                .isEqualTo(field("COL1").groupBy(field("COL2")));
    }

    @Test
    void minEverUngrouped() {
        assertThat(translate(minEver(col("COL3"))))
                .isEqualTo(field("COL3").ungrouped());
    }

    @Test
    void sumGroupedByTwoColumns() {
        assertThat(translate(sum(col("COL2")), col("COL1"), col("COL3")))
                .isEqualTo(field("COL2").groupBy(field("COL1"), field("COL3")));
    }

    @Test
    void maxGroupedByOneColumn() {
        assertThat(translate(max(col("COL2")), col("COL1")))
                .isEqualTo(field("COL2").groupBy(field("COL1")));
    }

    @Test
    void countStarGroupedByOneColumn() {
        assertThat(translate(new CountValue(true, col("COL1")), col("COL1")))
                .isEqualTo(new GroupingKeyExpression(field("COL1"), 0));
    }

    @Test
    void countStarUngrouped() {
        assertThat(translate(new CountValue(true, col("COL1"))))
                .isEqualTo(new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0));
    }

    @Test
    void countNotNullGroupedByOneColumn() {
        assertThat(translate(new CountValue(false, col("COL3")), col("COL1")))
                .isEqualTo(field("COL3").groupBy(field("COL1")));
    }

    @Test
    void bareAggregateWithoutARecord() {
        assertThat(translate(max(col("COL2")))).isEqualTo(field("COL2").ungrouped());
    }

    @Test
    void recordHoldingOnlyTheAggregateMatchesTheBareAggregate() {
        assertThat(translate(RecordConstructorValue.ofUnnamed(List.of(max(col("COL2"))))))
                .isEqualTo(translate(max(col("COL2"))));
    }

    @Test
    void twoAggregatesAreRejected() {
        final var thrown = Assertions.assertThrows(UncheckedRelationalException.class,
                () -> translate(sum(col("COL2")), max(col("COL3")), col("COL1")));
        assertThat(thrown.unwrap().getErrorCode()).isEqualTo(ErrorCode.UNSUPPORTED_OPERATION);
    }

    @Test
    void unsupportedValueIsRejected() {
        final var thrown = Assertions.assertThrows(UncheckedRelationalException.class,
                () -> translate(new NullValue(Type.primitiveType(Type.TypeCode.LONG))));
        assertThat(thrown.unwrap().getErrorCode()).isEqualTo(ErrorCode.UNSUPPORTED_OPERATION);
        // the message evaluateAtValue raises, which is where a value with no visitation method of its own lands
        assertThat(thrown.getMessage()).contains("unable to construct expression");
    }

    @Test
    void unsupportedValueAmongTheProjectedColumnsIsRejected() {
        final var thrown = Assertions.assertThrows(UncheckedRelationalException.class,
                () -> translate(col("COL1"), new NullValue(Type.primitiveType(Type.TypeCode.LONG))));
        assertThat(thrown.unwrap().getErrorCode()).isEqualTo(ErrorCode.UNSUPPORTED_OPERATION);
        assertThat(thrown.getMessage()).contains("unable to construct expression");
    }

    @Test
    void noAggregateImpliesValueIndex() {
        assertThat(indexType(ExtremumEverStorage.TUPLE, col("COL1"), col("COL2"))).isEqualTo(IndexTypes.VALUE);
    }

    @Test
    void sumImpliesSumIndex() {
        assertThat(indexType(ExtremumEverStorage.TUPLE, sum(col("COL2")), col("COL1"))).isEqualTo(IndexTypes.SUM);
    }

    @Test
    void countStarImpliesCountIndex() {
        assertThat(indexType(ExtremumEverStorage.TUPLE, new CountValue(true, col("COL1")), col("COL1"))).isEqualTo(IndexTypes.COUNT);
    }

    @Test
    void minEverImpliesTupleBasedIndex() {
        assertThat(indexType(ExtremumEverStorage.TUPLE, minEver(col("COL3")), col("COL1"))).isEqualTo(IndexTypes.MIN_EVER_TUPLE);
    }

    @Test
    void minEverImpliesLongBasedIndexWithLegacyExtremumEver() {
        assertThat(indexType(ExtremumEverStorage.LONG, minEver(col("COL3")), col("COL1"))).isEqualTo(IndexTypes.MIN_EVER_LONG);
    }

    @Nonnull
    private static Value minEver(@Nonnull final Value child) {
        return (Value)new IndexOnlyAggregateValue.MinEverFn().encapsulate(CallSiteArguments.ofPositional(child));
    }

    @Nonnull
    private static Value maxEver(@Nonnull final Value child) {
        return (Value)new IndexOnlyAggregateValue.MaxEverFn().encapsulate(CallSiteArguments.ofPositional(child));
    }

    @Nonnull
    private static Value sum(@Nonnull final Value child) {
        return new NumericAggregationValue.Sum(NumericAggregationValue.PhysicalOperator.SUM_L, child);
    }

    @Nonnull
    private static Value max(@Nonnull final Value child) {
        return new NumericAggregationValue.Max(NumericAggregationValue.PhysicalOperator.MAX_L, child);
    }
}
