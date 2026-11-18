/*
 * MaxMatchMapNotProvidedTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NotProvidedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Tests for {@link MaxMatchMap} against a candidate in which the fields a scan does not provide are
 * {@link NotProvidedValue}s, such as the record a covering index scan provides.
 */
class MaxMatchMapNotProvidedTest {
    private static final CorrelationIdentifier BASE = CorrelationIdentifier.of("base");
    private static final CorrelationIdentifier QUERY = CorrelationIdentifier.of("q");
    private static final CorrelationIdentifier TARGET = CorrelationIdentifier.of("v");
    private static final Type LONG = Type.primitiveType(Type.TypeCode.LONG);
    private static final NestedTypes NESTED = new NestedTypes();

    /**
     * The query reads a field the scan does not provide, next to a wide non-nullable record that the scan provides in
     * part. With {@link NullValue}s in place of the fields not provided, the search is exponential in the width of that
     * record; with {@link NotProvidedValue}s it is not.
     */
    @Test
    void computeWideRecordWithNotProvidedFieldsIsNotExponential() {
        for (final int width : new int[] {57, 200}) {
            final var maxMatchMap = assertTimeoutPreemptively(Duration.ofSeconds(10),
                    () -> computeReadingNotProvidedField(width, NotProvidedValue::new), "width " + width);
            assertThat(maxMatchMap.getMap()).isEmpty();
            assertThat(maxMatchMap.translateQueryValueMaybe(TARGET)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 5, 8})
    void computeWithNotProvidedFieldsMatchesLikeNullFields(final int width) {
        final var withNull = computeReadingNotProvidedField(width, NullValue::new);
        final var withNotProvided = computeReadingNotProvidedField(width, NotProvidedValue::new);
        assertThat(withNotProvided.getMap()).isEqualTo(withNull.getMap());
        assertThat(withNotProvided.getQueryValue()).isEqualTo(withNull.getQueryValue());
    }

    /**
     * Fields that are provided are matched at any depth, also below records that are only provided in part, and a
     * record that is only provided in part is never matched as a whole.
     */
    @Test
    void computeMatchesProvidedFieldsBelowPartiallyProvidedRecords() {
        // the scan provides header.h0, header.h1, header.nested.n0 and b, but not header.nested.n1 and not a
        final Value candidate = record(NESTED.record,
                record(NESTED.header,
                        read("header", "h0"),
                        read("header", "h1"),
                        record(NESTED.nested, read("header", "nested", "n0"), new NotProvidedValue(LONG))),
                new NotProvidedValue(LONG),
                read("b"));

        assertThat(translate(query("header", "nested", "n0"), candidate)).contains("(v.header.nested.n0 AS x)");
        assertThat(translate(query("header", "h1"), candidate)).contains("(v.header.h1 AS x)");
        assertThat(translate(query("b"), candidate)).contains("(v.b AS x)");
        assertThat(translate(query("a"), candidate)).isEmpty();
        assertThat(translate(query("header", "nested", "n1"), candidate)).isEmpty();
        assertThat(translate(query("header", "nested"), candidate)).isEmpty();
        assertThat(translate(query("header"), candidate)).isEmpty();
    }

    /**
     * A record that is provided as a whole is matched as a whole, and its fields are read from it.
     */
    @Test
    void computeMatchesRecordProvidedAsWhole() {
        // the scan provides header.nested as a whole and b, nothing else
        final Value candidate = record(NESTED.record,
                record(NESTED.header, new NotProvidedValue(LONG), new NotProvidedValue(LONG), read("header", "nested")),
                new NotProvidedValue(LONG),
                read("b"));

        assertThat(translate(query("header", "nested"), candidate)).contains("(v.header.nested AS x)");
        assertThat(translate(query("header", "nested", "n1"), candidate)).contains("(v.header.nested.n1 AS x)");
        assertThat(translate(query("header", "h0"), candidate)).isEmpty();
    }

    /** {@code (header: (h0, h1, nested: (n0, n1)), a, b)}, none of the records nullable. */
    private static final class NestedTypes {
        private final Type.Record nested = Type.Record.fromFields(false, ImmutableList.of(field("n0", LONG), field("n1", LONG)));
        private final Type.Record header = Type.Record.fromFields(false,
                ImmutableList.of(field("h0", LONG), field("h1", LONG), field("nested", nested)));
        private final Type.Record record = Type.Record.fromFields(false,
                ImmutableList.of(field("header", header), field("a", LONG), field("b", LONG)));
        private final Value base = QuantifiedObjectValue.of(BASE, record);
        private final Value query = QuantifiedObjectValue.of(QUERY, record);
    }

    @Nonnull
    private static Value read(@Nonnull final String... path) {
        return FieldValue.ofFieldNames(NESTED.base, ImmutableList.copyOf(path));
    }

    @Nonnull
    private static Value query(@Nonnull final String... path) {
        return RecordConstructorValue.ofColumns(ImmutableList.of(
                Column.of(Optional.of("x"), FieldValue.ofFieldNames(NESTED.query, ImmutableList.copyOf(path)))));
    }

    @Nonnull
    private static Value record(@Nonnull final Type.Record type, @Nonnull final Value... columns) {
        final var builder = ImmutableList.<Column<? extends Value>>builder();
        for (int i = 0; i < columns.length; i++) {
            builder.add(Column.of(type.getFields().get(i), columns[i]));
        }
        return RecordConstructorValue.ofColumns(builder.build());
    }

    @Nonnull
    private static Optional<String> translate(@Nonnull final Value queryValue, @Nonnull final Value candidateValue) {
        return compute(queryValue, candidateValue).translateQueryValueMaybe(TARGET).map(Object::toString);
    }

    @Nonnull
    private static MaxMatchMap compute(@Nonnull final Value queryValue, @Nonnull final Value candidateValue) {
        return MaxMatchMap.compute(queryValue, candidateValue, ImmutableSet.of(BASE, QUERY),
                ValueEquivalence.fromAliasMap(AliasMap.ofAliases(QUERY, BASE)));
    }

    /**
     * {@code (q.a AS a)} against what a covering scan over {@code header.h0} and {@code header.h1} provides for
     * {@code (header: (h0, ..., h{width-2}, nested: (n0, n1)), a, b, c)}, with {@code absent} standing for every field
     * the scan does not provide.
     */
    @Nonnull
    private static MaxMatchMap computeReadingNotProvidedField(final int width, @Nonnull final Function<Type, Value> absent) {
        final List<Type.Record.Field> headerFields = new ArrayList<>();
        for (int i = 0; i < width - 1; i++) {
            headerFields.add(field("h" + i, LONG));
        }
        headerFields.add(field("nested", Type.Record.fromFields(false, ImmutableList.of(field("n0", LONG), field("n1", LONG)))));
        final Type.Record headerType = Type.Record.fromFields(false, headerFields);
        final Type.Record recordType = Type.Record.fromFields(false,
                ImmutableList.of(field("header", headerType), field("a", LONG), field("b", LONG), field("c", LONG)));
        final Value base = QuantifiedObjectValue.of(BASE, recordType);

        final var header = ImmutableList.<Column<? extends Value>>builder();
        for (int i = 0; i < headerFields.size(); i++) {
            final var headerField = headerFields.get(i);
            header.add(Column.of(headerField, i < 2
                                              ? FieldValue.ofFieldNames(base, ImmutableList.of("header", headerField.getFieldName()))
                                              : absent.apply(headerField.getFieldType())));
        }
        final var top = ImmutableList.<Column<? extends Value>>builder();
        top.add(Column.of(recordType.getFields().get(0), RecordConstructorValue.ofColumns(header.build())));
        for (int i = 1; i < recordType.getFields().size(); i++) {
            top.add(Column.of(recordType.getFields().get(i), absent.apply(recordType.getFields().get(i).getFieldType())));
        }
        final Value queryValue = RecordConstructorValue.ofColumns(ImmutableList.of(
                Column.of(Optional.of("a"), FieldValue.ofFieldName(QuantifiedObjectValue.of(QUERY, recordType), "a"))));
        return compute(queryValue, RecordConstructorValue.ofColumns(top.build()));
    }

    @Nonnull
    private static Type.Record.Field field(@Nonnull final String name, @Nonnull final Type type) {
        return Type.Record.Field.of(type, Optional.of(name));
    }
}
