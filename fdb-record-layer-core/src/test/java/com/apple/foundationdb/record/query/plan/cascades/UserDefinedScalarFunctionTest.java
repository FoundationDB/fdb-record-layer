/*
 * MacroFunctionTest.java
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


import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Tests of {@link UserDefinedScalarFunction}.
 */
public class UserDefinedScalarFunctionTest {
    @Test
    void testColumnProjection() {
        ImmutableList<Type.Record.Field> fields = ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("id")));
        Type record = Type.Record.fromFields(false, fields);
        QuantifiedObjectValue param = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueID(), record);
        FieldValue bodyValue = FieldValue.ofFieldName(param, "name");
        UserDefinedScalarFunction macroFunction = new UserDefinedScalarFunction("getName", ImmutableList.of(param), bodyValue);

        RecordConstructorValue testValue1 = RecordConstructorValue.ofColumns(ImmutableList.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), "Rose")),
                Column.of(fields.get(1), new LiteralValue<>(fields.get(1).getFieldType(), 1L))
        ));

        Assertions.assertEquals(FieldValue.ofFieldName(testValue1, "name"), macroFunction.encapsulate(ImmutableList.of(testValue1)));
    }

    @Test
    void testAdd() {
        ImmutableList<Type.Record.Field> fields = ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("id")));
        Type record = Type.Record.fromFields(false, fields);
        QuantifiedObjectValue param1 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueID(), record);
        QuantifiedObjectValue param2 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueID(), record);

        ArithmeticValue bodyValue = new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_LL, param1, param2);
        UserDefinedScalarFunction macroFunction = new UserDefinedScalarFunction("add", ImmutableList.of(param1, param2), bodyValue);

        RecordConstructorValue testValue1 = RecordConstructorValue.ofColumns(ImmutableList.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), 1L))
        ));
        RecordConstructorValue testValue2 = RecordConstructorValue.ofColumns(ImmutableList.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), 2L))
        ));

        Assertions.assertEquals(new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_LL, testValue1, testValue2), macroFunction.encapsulate(ImmutableList.of(testValue1, testValue2)));
    }
}
