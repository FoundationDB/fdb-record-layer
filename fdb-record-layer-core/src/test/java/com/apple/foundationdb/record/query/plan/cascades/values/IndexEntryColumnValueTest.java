/*
 * IndexEntryColumnValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.TupleSource;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.VerifyException;
import com.google.common.primitives.ImmutableIntArray;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link IndexEntryColumnValue}.
 */
class IndexEntryColumnValueTest {

    @Nonnull
    private static final CorrelationIdentifier ALIAS = CorrelationIdentifier.of("q");
    @Nonnull
    private static final CorrelationIdentifier OTHER_ALIAS = CorrelationIdentifier.of("p");
    @Nonnull
    private static final Type INT_TYPE = Type.primitiveType(Type.TypeCode.INT);
    @Nonnull
    private static final Index INDEX = new Index("test_index", Key.Expressions.field("num_value_unique"));

    /**
     * Tests that the value reads an element from the key or the value tuple of the index entry at its ordinal path.
     */
    @Test
    void testIndexEntryColumnValue1() {
        final var context = bind(new IndexEntry(INDEX, Tuple.from(3L, 42L), Tuple.from(7L)));

        assertThat(columnValue(TupleSource.KEY, 0).eval(null, context)).isEqualTo(3);
        assertThat(columnValue(TupleSource.KEY, 1).eval(null, context)).isEqualTo(42);
        assertThat(columnValue(TupleSource.VALUE, 0).eval(null, context)).isEqualTo(7);
    }

    /**
     * Tests that the value returns null for an element that the index entry stores as null, such as the cardinality
     * of a null array.
     */
    @Test
    void testIndexEntryColumnValue2() {
        final var context = bind(new IndexEntry(INDEX, Tuple.from((Object)null), Tuple.from()));

        assertThat(columnValue(TupleSource.KEY, 0).eval(null, context)).isNull();
    }

    /**
     * Tests that evaluating the value against a record that does not carry its index entry fails instead of returning
     * null.
     */
    @Test
    void testIndexEntryColumnValue3() {
        final var context = EvaluationContext.empty()
                .withBinding(Bindings.Internal.CORRELATION, ALIAS, QueryResult.ofComputed(42));

        assertThatThrownBy(() -> columnValue(TupleSource.KEY, 0).eval(null, context))
                .isInstanceOf(VerifyException.class);
    }

    /**
     * Tests that two values are equal only if they read the same position of the same tuple through the same
     * quantifier.
     */
    @Test
    void testIndexEntryColumnValue4() {
        final var keyColumn0 = columnValue(TupleSource.KEY, 0);

        assertThat(keyColumn0).isEqualTo(columnValue(TupleSource.KEY, 0));
        assertThat(keyColumn0).hasSameHashCodeAs(columnValue(TupleSource.KEY, 0));
        assertThat(keyColumn0).isNotEqualTo(columnValue(TupleSource.KEY, 1));
        assertThat(keyColumn0).isNotEqualTo(columnValue(TupleSource.VALUE, 0));
        assertThat(keyColumn0).isNotEqualTo(
                new IndexEntryColumnValue(OTHER_ALIAS, TupleSource.KEY, ImmutableIntArray.of(0), INT_TYPE));
        assertThat(keyColumn0).isNotEqualTo(QuantifiedObjectValue.of(ALIAS, INT_TYPE));
    }

    /**
     * Tests that rebasing moves the value onto another quantifier without changing what it reads, which pushing a
     * projection underneath a fetch relies on.
     */
    @Test
    void testIndexEntryColumnValue5() {
        final var rebased = columnValue(TupleSource.VALUE, 2).rebaseLeaf(OTHER_ALIAS);

        assertThat(rebased.getCorrelatedTo()).containsExactly(OTHER_ALIAS);
        assertThat(rebased).isEqualTo(
                new IndexEntryColumnValue(OTHER_ALIAS, TupleSource.VALUE, ImmutableIntArray.of(2), INT_TYPE));
    }

    /**
     * Tests that the value survives a round trip through its serialized form, which a cached plan continuation
     * depends on.
     */
    @Test
    void testIndexEntryColumnValue6() {
        final var columnValue = columnValue(TupleSource.VALUE, 1);
        final var proto = columnValue.toValueProto(PlanSerializationContext.newForCurrentMode());

        assertThat(Value.fromValueProto(PlanSerializationContext.newForCurrentMode(), proto)).isEqualTo(columnValue);
    }

    /**
     * Tests that the explain output names the quantifier, the tuple, and the ordinal path that the value reads.
     */
    @Test
    void testIndexEntryColumnValue7() {
        assertThat(explain(columnValue(TupleSource.KEY, 0))).isEqualTo("q.KEY:[0]");
        assertThat(explain(columnValue(TupleSource.VALUE, 1))).isEqualTo("q.VALUE:[1]");
        assertThat(explain(new IndexEntryColumnValue(Quantifier.current(), TupleSource.KEY, ImmutableIntArray.of(0, 2), INT_TYPE)))
                .isEqualTo("_.KEY:[0, 2]");
    }

    @Nonnull
    private static IndexEntryColumnValue columnValue(@Nonnull final TupleSource source, final int ordinal) {
        return new IndexEntryColumnValue(ALIAS, source, ImmutableIntArray.of(ordinal), INT_TYPE);
    }

    @Nonnull
    private static String explain(@Nonnull final Value value) {
        return value.explain().getExplainTokens().render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)).toString();
    }

    @Nonnull
    private static EvaluationContext bind(@Nonnull final IndexEntry indexEntry) {
        final var recordType = RecordMetaData.build(TestRecords1Proto.getDescriptor()).getRecordType("MySimpleRecord");
        final var queriedRecord =
                FDBQueriedRecord.covered(INDEX, indexEntry, Tuple.from(1L), recordType,
                        TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1L).build());
        return EvaluationContext.empty()
                .withBinding(Bindings.Internal.CORRELATION, ALIAS,
                        QueryResult.fromQueriedRecord(Type.any(), EvaluationContext.empty(), queriedRecord));
    }
}
