/*
 * UserDefinedMacroFunctionTest.java
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
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Tests of {@link UserDefinedMacroFunction}.
 */
public class UserDefinedMacroFunctionTest {
    @Test
    void testColumnProjection() {
        ImmutableList<Type.Record.Field> fields = ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("name")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("id")));
        Type record = Type.Record.fromFields(false, fields);
        QuantifiedObjectValue param = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), record);
        FieldValue bodyValue = FieldValue.ofFieldName(param, "name");
        UserDefinedMacroFunction macroFunction = new UserDefinedMacroFunction("getName", ImmutableList.of(param), bodyValue);

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
        QuantifiedObjectValue param1 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), record);
        QuantifiedObjectValue param2 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), record);

        ArithmeticValue bodyValue = new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_LL, param1, param2);
        UserDefinedMacroFunction macroFunction = new UserDefinedMacroFunction("add", ImmutableList.of(param1, param2), bodyValue);

        RecordConstructorValue testValue1 = RecordConstructorValue.ofColumns(ImmutableList.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), 1L))
        ));
        RecordConstructorValue testValue2 = RecordConstructorValue.ofColumns(ImmutableList.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), 2L))
        ));

        Assertions.assertEquals(new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_LL, testValue1, testValue2), macroFunction.encapsulate(ImmutableList.of(testValue1, testValue2)));
    }

    @Test
    void testEncapsulateWithLiteralValue() {
        // Test UDF that simply returns a literal value
        Type longType = Type.primitiveType(Type.TypeCode.LONG);
        QuantifiedObjectValue param = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), longType);

        LiteralValue<Long> literalBody = new LiteralValue<>(longType, 42L);
        UserDefinedMacroFunction constantFunction = new UserDefinedMacroFunction("constant", ImmutableList.of(param), literalBody);

        LiteralValue<Long> inputValue = new LiteralValue<>(longType, 123L);

        // The function should return the literal value regardless of input
        Assertions.assertEquals(literalBody, constantFunction.encapsulate(ImmutableList.of(inputValue)));
    }

    @Test
    void testEncapsulateWithMultipleFieldAccess() {
        // Test UDF that accesses multiple fields from a record
        ImmutableList<Type.Record.Field> fields = ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("firstName")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("lastName")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("age")));
        Type record = Type.Record.fromFields(false, fields);
        QuantifiedObjectValue param = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), record);

        // Function returns firstName field
        FieldValue bodyValue = FieldValue.ofFieldName(param, "firstName");
        UserDefinedMacroFunction getFirstNameFunction = new UserDefinedMacroFunction("getFirstName", ImmutableList.of(param), bodyValue);

        RecordConstructorValue testValue = RecordConstructorValue.ofColumns(ImmutableList.of(
                Column.of(fields.get(0), new LiteralValue<>(fields.get(0).getFieldType(), "John")),
                Column.of(fields.get(1), new LiteralValue<>(fields.get(1).getFieldType(), "Doe")),
                Column.of(fields.get(2), new LiteralValue<>(fields.get(2).getFieldType(), 30L))
        ));

        Assertions.assertEquals(FieldValue.ofFieldName(testValue, "firstName"), getFirstNameFunction.encapsulate(ImmutableList.of(testValue)));
    }

    @Test
    void testEncapsulateArgumentCountMismatch() {
        // Test that encapsulate throws exception when argument count doesn't match
        Type longType = Type.primitiveType(Type.TypeCode.LONG);
        QuantifiedObjectValue param1 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), longType);
        QuantifiedObjectValue param2 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), longType);

        ArithmeticValue bodyValue = new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_LL, param1, param2);
        UserDefinedMacroFunction addFunction = new UserDefinedMacroFunction("add", ImmutableList.of(param1, param2), bodyValue);

        LiteralValue<Long> singleArg = new LiteralValue<>(longType, 42L);

        // Should throw exception because function expects 2 arguments but only 1 provided
        Assertions.assertThrows(Exception.class, () -> {
            addFunction.encapsulate(ImmutableList.of(singleArg));
        });
    }

    @Test
    void testEncapsulateWithComplexArithmetic() {
        // Test UDF with complex arithmetic operations
        Type longType = Type.primitiveType(Type.TypeCode.LONG);
        QuantifiedObjectValue param1 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), longType);
        QuantifiedObjectValue param2 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), longType);
        QuantifiedObjectValue param3 = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), longType);

        // Function computes: (param1 + param2) * param3
        ArithmeticValue addValue = new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_LL, param1, param2);
        ArithmeticValue bodyValue = new ArithmeticValue(ArithmeticValue.PhysicalOperator.MUL_LL, addValue, param3);
        UserDefinedMacroFunction complexFunction = new UserDefinedMacroFunction("complexCalc", ImmutableList.of(param1, param2, param3), bodyValue);

        LiteralValue<Long> arg1 = new LiteralValue<>(longType, 10L);
        LiteralValue<Long> arg2 = new LiteralValue<>(longType, 20L);
        LiteralValue<Long> arg3 = new LiteralValue<>(longType, 3L);

        ArithmeticValue expectedAdd = new ArithmeticValue(ArithmeticValue.PhysicalOperator.ADD_LL, arg1, arg2);
        ArithmeticValue expectedResult = new ArithmeticValue(ArithmeticValue.PhysicalOperator.MUL_LL, expectedAdd, arg3);

        Assertions.assertEquals(expectedResult, complexFunction.encapsulate(ImmutableList.of(arg1, arg2, arg3)));
    }

    @Test
    void testEncapsulateNamedArgumentsNotSupported() {
        // Test that named arguments are not supported
        Type longType = Type.primitiveType(Type.TypeCode.LONG);
        QuantifiedObjectValue param = QuantifiedObjectValue.of(CorrelationIdentifier.uniqueId(), longType);

        UserDefinedMacroFunction identityFunction = new UserDefinedMacroFunction("identity", ImmutableList.of(param), param);

        LiteralValue<Long> argValue = new LiteralValue<>(longType, 42L);

        // Should throw exception because named arguments are not supported
        Assertions.assertThrows(Exception.class, () -> {
            identityFunction.encapsulate(ImmutableMap.of("param", argValue));
        });
    }
}
