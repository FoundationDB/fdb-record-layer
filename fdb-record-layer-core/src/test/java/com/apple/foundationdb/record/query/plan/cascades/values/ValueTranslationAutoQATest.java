/*
 * ValueTranslationAutoQATest.java
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

import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for value translation and max match maps.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class ValueTranslationAutoQATest {
    @Nonnull
    private final CorrelationIdentifier qtAlias = CorrelationIdentifier.of("T");
    @Nonnull
    private final CorrelationIdentifier ctAlias = CorrelationIdentifier.of("T'");
    @Nonnull
    private final CorrelationIdentifier qmAlias = CorrelationIdentifier.of("M");
    @Nonnull
    private final CorrelationIdentifier cmAlias = CorrelationIdentifier.of("M'");
    @Nonnull
    private final CorrelationIdentifier candidateAlias = CorrelationIdentifier.of("C");

    @Nonnull
    private final QuantifiedObjectValue qt = QuantifiedObjectValue.of(qtAlias, recordTType());
    @Nonnull
    private final QuantifiedObjectValue ct = QuantifiedObjectValue.of(ctAlias, recordTType());
    @Nonnull
    private final QuantifiedObjectValue qm = QuantifiedObjectValue.of(qmAlias, recordMType());
    @Nonnull
    private final QuantifiedObjectValue cm = QuantifiedObjectValue.of(cmAlias, recordMType());

    @Test
    void translateQueryValueMaybeCandidateWithDuplicateColumnsTranslates() {
        // BUG AQA-17: translateQueryValueMaybe calls Iterables.getOnlyElement on Value.pullUp, which can return several
        // results when the candidate repeats a column, so it throws IllegalArgumentException. Spec: MaxMatchMap
        // translateQueryValueMaybe javadoc, returns "the translated query value or Optional.empty()".
        // a query column that the candidate exposes twice can be pulled up through either of them
        final var queryValue = rcv(fv(ct, "a"));
        final var candidateValue = rcv(fv(ct, "a"), fv(ct, "a"));

        final var maxMatchMap = calculate(queryValue, candidateValue);
        assertThat(maxMatchMap.getMap()).isEqualTo(ImmutableMap.of(fv(ct, "a"), fv(ct, "a")));

        final var translated = maxMatchMap.translateQueryValueMaybe(candidateAlias);
        assertThat(translated).isPresent();
        final var c = QuantifiedObjectValue.of(candidateAlias, candidateValue.getResultType());
        assertThat(translated.get()).isIn(rcv(FieldValue.ofOrdinalNumber(c, 0)), rcv(FieldValue.ofOrdinalNumber(c, 1)));
    }

    @Nonnull
    private static MaxMatchMap calculate(@Nonnull final Value queryValue, @Nonnull final Value candidateValue) {
        return MaxMatchMap.compute(queryValue, candidateValue, candidateValue.getCorrelatedTo());
    }

    @Nonnull
    private static Type.Record recordTType() {
        return record(
                field("a", record(intField("q"), intField("r"))),
                field("b", record(intField("t"), intField("m"))),
                field("j", record(intField("s"), intField("q"))));
    }

    @Nonnull
    private static Type.Record recordMType() {
        return record(intField("m1"), intField("m2"), intField("m3"));
    }

    @Nonnull
    private static Type.Record record(@Nonnull final Type.Record.Field... fields) {
        return Type.Record.fromFields(false, ImmutableList.copyOf(fields));
    }

    @Nonnull
    private static Type.Record.Field intField(@Nonnull final String name) {
        return field(name, Type.primitiveType(Type.TypeCode.INT));
    }

    @Nonnull
    private static Type.Record.Field field(@Nonnull final String name, @Nonnull final Type type) {
        return Type.Record.Field.of(type, Optional.of(name));
    }

    @Nonnull
    private static Value rcv(@Nonnull final Value... values) {
        return RecordConstructorValue.ofUnnamed(Arrays.asList(values));
    }

    @Nonnull
    private static Value rcv(final boolean isNullable, @Nonnull final Object... valuesAndNames) {
        final var columnsBuilder = ImmutableList.<Column<? extends Value>>builder();
        for (int i = 0; i < valuesAndNames.length - 1; i += 2) {
            final var value = (Value)Objects.requireNonNull(valuesAndNames[i]);
            final var fieldName = (String)Objects.requireNonNull(valuesAndNames[i + 1]);
            columnsBuilder.add(Column.of(Optional.of(fieldName), value));
        }
        return RecordConstructorValue.ofColumns(columnsBuilder.build(), isNullable);
    }

    @Nonnull
    private static FieldValue fv(@Nonnull final Value base, @Nonnull final String... names) {
        FieldValue result = FieldValue.ofFieldNameAndFuseIfPossible(base, names[0]);
        for (int i = 1; i < names.length; i++) {
            result = FieldValue.ofFieldNameAndFuseIfPossible(result, names[i]);
        }
        return result;
    }
}
