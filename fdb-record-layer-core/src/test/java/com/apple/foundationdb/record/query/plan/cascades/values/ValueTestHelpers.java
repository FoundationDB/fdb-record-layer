/*
 * ValueTestHelpers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

/**
 * Test helpers for creating various kinds of {@link Value}s.
 */
public class ValueTestHelpers {

    public static RecordConstructorValue rcv() {
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")),
                                LiteralValue.ofScalar("fieldValueA")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("b")),
                                LiteralValue.ofScalar("fieldValueB")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("c")),
                                LiteralValue.ofScalar("fieldValueC")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("d")),
                                LiteralValue.ofScalar("fieldValueD")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("e")),
                                LiteralValue.ofScalar("fieldValueE")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("x")),
                                LiteralValue.ofScalar("fieldValueX")));
        return RecordConstructorValue.ofColumns(columns);
    }

    public static RecordConstructorValue rcv2() {
        final ImmutableList<Column<? extends Value>> columns =
                ImmutableList.of(
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")),
                                LiteralValue.ofScalar("fieldValueA")),
                        Column.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("rest_no")),
                                LiteralValue.ofScalar(42)));
        return RecordConstructorValue.ofColumns(columns);
    }

    public static QuantifiedObjectValue qov() {
        final var resultType = rcv().getResultType();
        return QuantifiedObjectValue.of(Quantifier.current(), resultType);
    }

    public static Value field(final Value value, final String fieldName) {
        return FieldValue.ofFieldName(value, fieldName);
    }
}
