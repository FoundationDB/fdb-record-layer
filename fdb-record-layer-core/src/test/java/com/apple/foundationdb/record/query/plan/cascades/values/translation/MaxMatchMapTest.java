/*
 * MaxMatchMapTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.translation;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Tests for {@link MaxMatchMap#compute} with wide records. A field of a record can have more than one variant, such
 * as a field value with a fused and with an unfused field path, so the number of combinations of the variants of the
 * fields of a record is exponential in the number of its fields. The matching has to find the matches without
 * enumerating these combinations. The query values are over {@code q}, the candidate values are over {@code base}, and
 * the alias map relates the two, as in the matching of a candidate.
 */
class MaxMatchMapTest {
    private static final CorrelationIdentifier BASE = CorrelationIdentifier.of("base");
    private static final CorrelationIdentifier QUERY = CorrelationIdentifier.of("q");
    private static final CorrelationIdentifier TARGET = CorrelationIdentifier.of("v");
    private static final Type LONG = Type.primitiveType(Type.TypeCode.LONG);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    /**
     * The query reads the field {@code a}, which the candidate does not provide. The candidate provides two of the
     * fields of the wide record {@code header}, and {@code null} for all other fields (issue #4923).
     */
    @ParameterizedTest
    @ValueSource(ints = {4, 57, 200})
    void computeFieldNotProvidedNextToWideRecordFindsNoMatch(final int width) {
        final var headerType = headerType(width);
        final var recordType = recordType(headerType);
        final var base = QuantifiedObjectValue.of(BASE, recordType);
        final var headerFieldsBuilder = ImmutableList.<Value>builder();
        for (int i = 0; i < headerType.getFields().size(); i++) {
            final var field = headerType.getFields().get(i);
            headerFieldsBuilder.add(i < 2
                                    ? FieldValue.ofFieldNames(base, ImmutableList.of("header", field.getFieldName()))
                                    : new NullValue(field.getFieldType()));
        }
        final var candidateValue = candidateWithHeader(recordType, record(headerType, headerFieldsBuilder.build()));
        final var queryValue = query(recordType, "a");

        final var maxMatchMap = assertTimeoutPreemptively(TIMEOUT, () -> compute(queryValue, candidateValue));
        assertThat(maxMatchMap.getMap()).isEmpty();
        assertThat(maxMatchMap.translateQueryValueMaybe(TARGET)).isEmpty();
    }

    /**
     * The candidate provides the whole record {@code header}, but each of its fields as a field of the field value
     * {@code base.header}, so the query matches the record only with the unfused variant of each of its fields.
     */
    @Test
    void computeWideRecordOfUnfusedFieldsMatchesRecord() {
        final var headerType = headerType(30);
        final var recordType = recordType(headerType);
        final var header = FieldValue.ofFieldName(QuantifiedObjectValue.of(BASE, recordType), "header");
        final var candidateValue =
                candidateWithHeader(recordType, record(headerType, unfusedFields(header, headerType)));
        final var queryValue = query(recordType, "header");

        final var maxMatchMap = assertTimeoutPreemptively(TIMEOUT, () -> compute(queryValue, candidateValue));
        assertThat(maxMatchMap.translateQueryValueMaybe(TARGET).map(Object::toString)).contains("(v.header AS x)");
    }

    /**
     * The query reads the same wide record twice. The candidate provides it once with fused and once with unfused
     * field values. So the query matches the candidate as a whole only with the fused variants of the fields in the
     * first column and with the unfused variants in the second column. The best variants of the second column are the
     * fused ones, which match the fields of the first column of the candidate.
     */
    @Test
    void computeRecordReadTwiceWithDifferentFieldValuesMatchesWholeCandidate() {
        final var headerType = headerType(20);
        final var recordType = Type.Record.fromFields(false, ImmutableList.of(field("header", headerType)));
        final var pairType =
                Type.Record.fromFields(false, ImmutableList.of(field("a", headerType), field("b", headerType)));
        final var base = QuantifiedObjectValue.of(BASE, recordType);
        final var candidateValue = record(pairType, ImmutableList.of(
                record(headerType, fusedFields(base, headerType)),
                record(headerType, unfusedFields(FieldValue.ofFieldName(base, "header"), headerType))));
        final var queryHeader = FieldValue.ofFieldName(QuantifiedObjectValue.of(QUERY, recordType), "header");
        final var queryValue = record(pairType, ImmutableList.of(queryHeader, queryHeader));

        final var maxMatchMap = assertTimeoutPreemptively(TIMEOUT, () -> compute(queryValue, candidateValue));
        assertThat(Iterables.getOnlyElement(maxMatchMap.getMap().values())).isSameAs(candidateValue);
    }

    /**
     * The candidate adds a constant to an unfused field value. The children of an arithmetic value are not reachable
     * from the root of the candidate, so the field value of the query matches nothing on its own. The query matches
     * the candidate as a whole only with the unfused variant of that field value.
     */
    @Test
    void computeArithmeticValueOverUnfusedFieldMatchesWholeCandidate() {
        final var headerType = headerType(4);
        final var recordType = recordType(headerType);
        final var sumType = Type.Record.fromFields(false, ImmutableList.of(field("s", LONG)));
        final var header = FieldValue.ofFieldName(QuantifiedObjectValue.of(BASE, recordType), "header");
        final var candidateValue = record(sumType, ImmutableList.of(new ArithmeticValue(
                ArithmeticValue.PhysicalOperator.ADD_LL, FieldValue.ofFieldName(header, "h0"),
                LiteralValue.ofScalar(1L))));
        final var queryValue = record(sumType, ImmutableList.of(new ArithmeticValue(
                ArithmeticValue.PhysicalOperator.ADD_LL,
                FieldValue.ofFieldNames(QuantifiedObjectValue.of(QUERY, recordType), ImmutableList.of("header", "h0")),
                LiteralValue.ofScalar(1L))));

        final var maxMatchMap = compute(queryValue, candidateValue);
        assertThat(Iterables.getOnlyElement(maxMatchMap.getMap().values())).isSameAs(candidateValue);
    }

    /**
     * Returns {@code (h0, ..., h{width-2}, nested: (n0, n1))}, the record type {@code header} of issue #4923.
     */
    @Nonnull
    private static Type.Record headerType(final int width) {
        final var fieldsBuilder = ImmutableList.<Type.Record.Field>builder();
        for (int i = 0; i < width - 1; i++) {
            fieldsBuilder.add(field("h" + i, LONG));
        }
        fieldsBuilder.add(field("nested",
                Type.Record.fromFields(false, ImmutableList.of(field("n0", LONG), field("n1", LONG)))));
        return Type.Record.fromFields(false, fieldsBuilder.build());
    }

    /**
     * Returns {@code (header, a, b, c)}.
     */
    @Nonnull
    private static Type.Record recordType(@Nonnull final Type.Record headerType) {
        return Type.Record.fromFields(false, ImmutableList.of(field("header", headerType), field("a", LONG),
                field("b", LONG), field("c", LONG)));
    }

    /**
     * Returns a record of {@code recordType} with the given value for {@code header} and {@code null} for its other
     * fields.
     */
    @Nonnull
    private static Value candidateWithHeader(@Nonnull final Type.Record recordType, @Nonnull final Value header) {
        final var columnsBuilder = ImmutableList.<Value>builder().add(header);
        final var fields = recordType.getFields();
        for (final var field : fields.subList(1, fields.size())) {
            columnsBuilder.add(new NullValue(field.getFieldType()));
        }
        return record(recordType, columnsBuilder.build());
    }

    /**
     * Returns {@code base.header.f}, with a fused field path, for each field {@code f} of {@code headerType}.
     */
    @Nonnull
    private static List<Value> fusedFields(@Nonnull final Value base, @Nonnull final Type.Record headerType) {
        return headerType.getFields().stream()
                .map(field -> (Value)FieldValue.ofFieldNames(base, ImmutableList.of("header", field.getFieldName())))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns the field value of {@code header} for each field {@code f} of {@code headerType}, a field value of a
     * field value if {@code header} is one.
     */
    @Nonnull
    private static List<Value> unfusedFields(@Nonnull final Value header, @Nonnull final Type.Record headerType) {
        return headerType.getFields().stream()
                .map(field -> (Value)FieldValue.ofFieldName(header, field.getFieldName()))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Returns {@code (q.fieldName AS x)}.
     */
    @Nonnull
    private static Value query(@Nonnull final Type.Record recordType, @Nonnull final String fieldName) {
        return RecordConstructorValue.ofColumns(ImmutableList.of(Column.of(Optional.of("x"),
                FieldValue.ofFieldName(QuantifiedObjectValue.of(QUERY, recordType), fieldName))));
    }

    @Nonnull
    private static Value record(@Nonnull final Type.Record type, @Nonnull final List<Value> columns) {
        final var columnsBuilder = ImmutableList.<Column<? extends Value>>builder();
        for (int i = 0; i < columns.size(); i++) {
            columnsBuilder.add(Column.of(type.getFields().get(i), columns.get(i)));
        }
        return RecordConstructorValue.ofColumns(columnsBuilder.build());
    }

    @Nonnull
    private static MaxMatchMap compute(@Nonnull final Value queryValue, @Nonnull final Value candidateValue) {
        return MaxMatchMap.compute(queryValue, candidateValue, ImmutableSet.of(BASE, QUERY),
                ValueEquivalence.fromAliasMap(AliasMap.ofAliases(QUERY, BASE)));
    }

    @Nonnull
    private static Type.Record.Field field(@Nonnull final String name, @Nonnull final Type type) {
        return Type.Record.Field.of(type, Optional.of(name));
    }
}
