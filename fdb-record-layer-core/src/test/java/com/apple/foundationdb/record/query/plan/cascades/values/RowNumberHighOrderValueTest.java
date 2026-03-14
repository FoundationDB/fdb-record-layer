/*
 * RowNumberHighOrderValueTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.planprotos.PRowNumberHighOrderValue;
import com.apple.foundationdb.record.query.plan.cascades.BuiltInFunction;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link RowNumberHighOrderValue}.
 */
class RowNumberHighOrderValueTest {

    @Test
    void testConstructorWithParameters() {
        final var value = new RowNumberHighOrderValue(100, true);
        Assertions.assertNotNull(value, "RowNumberHighOrderValue should be created successfully");
    }

    @Test
    void testConstructorWithNullParameters() {
        final var value = new RowNumberHighOrderValue(null, null);
        Assertions.assertNotNull(value, "RowNumberHighOrderValue should be created with null parameters");
    }

    @Test
    void testConstructorFromProto() {
        final var proto = PRowNumberHighOrderValue.newBuilder()
                .setEfSearch(100)
                .setIsReturningVectors(true)
                .build();

        final var value = new RowNumberHighOrderValue(proto);
        Assertions.assertNotNull(value, "RowNumberHighOrderValue should be created from proto");
    }

    @Test
    void testConstructorFromProtoWithoutOptionalFields() {
        final var proto = PRowNumberHighOrderValue.newBuilder().build();

        final var value = new RowNumberHighOrderValue(proto);
        Assertions.assertNotNull(value, "RowNumberHighOrderValue should be created from proto without optional fields");
    }

    @Test
    void testComputeChildren() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var children = value.getChildren();

        Assertions.assertNotNull(children, "Children should not be null");
        Assertions.assertEquals(0, ImmutableList.copyOf(children).size(),
                "RowNumberHighOrderValue should have no children as it's a LeafValue");
    }

    @Test
    void testToProtoWithAllParameters() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = value.toProto(serializationContext);

        Assertions.assertNotNull(proto, "Proto should not be null");
        Assertions.assertTrue(proto.hasEfSearch(), "Proto should have efSearch");
        Assertions.assertEquals(100, proto.getEfSearch(), "efSearch should be 100");
        Assertions.assertTrue(proto.hasIsReturningVectors(), "Proto should have isReturningVectors");
        Assertions.assertTrue(proto.getIsReturningVectors(), "isReturningVectors should be true");
    }

    @Test
    void testToProtoWithNullParameters() {
        final var value = new RowNumberHighOrderValue(null, null);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = value.toProto(serializationContext);

        Assertions.assertNotNull(proto, "Proto should not be null");
        Assertions.assertFalse(proto.hasEfSearch(), "Proto should not have efSearch when null");
        Assertions.assertFalse(proto.hasIsReturningVectors(), "Proto should not have isReturningVectors when null");
    }

    @Test
    void testToValueProto() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var valueProto = value.toValueProto(serializationContext);

        Assertions.assertNotNull(valueProto, "Value proto should not be null");
        Assertions.assertTrue(valueProto.hasRowNumberHighOrderValue(),
                "Value proto should have RowNumberHighOrderValue");
        Assertions.assertEquals(100, valueProto.getRowNumberHighOrderValue().getEfSearch(),
                "efSearch should be preserved in value proto");
    }

    @Test
    void testFromProtoStatic() {
        final var proto = PRowNumberHighOrderValue.newBuilder()
                .setEfSearch(200)
                .setIsReturningVectors(false)
                .build();

        final var value = RowNumberHighOrderValue.fromProto(proto);

        Assertions.assertNotNull(value, "Value should not be null");
        // Verify by serializing back
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var roundTripProto = value.toProto(serializationContext);
        Assertions.assertEquals(200, roundTripProto.getEfSearch(), "efSearch should match");
        Assertions.assertFalse(roundTripProto.getIsReturningVectors(), "isReturningVectors should match");
    }

    @Test
    void testFromProtoDeserializer() {
        final var proto = PRowNumberHighOrderValue.newBuilder()
                .setEfSearch(150)
                .setIsReturningVectors(true)
                .build();

        final var deserializer = new RowNumberHighOrderValue.Deserializer();
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var value = deserializer.fromProto(serializationContext, proto);

        Assertions.assertNotNull(value, "Deserialized value should not be null");
        // Verify by serializing back
        final var roundTripProto = value.toProto(serializationContext);
        Assertions.assertEquals(150, roundTripProto.getEfSearch(), "efSearch should match");
        Assertions.assertTrue(roundTripProto.getIsReturningVectors(), "isReturningVectors should match");
    }

    @Test
    void testPlanHash() {
        final var value1 = new RowNumberHighOrderValue(100, true);
        final var value2 = new RowNumberHighOrderValue(100, true);
        final var value3 = new RowNumberHighOrderValue(200, false);

        final int hash1 = value1.planHash(PlanHashable.PlanHashMode.VC0);
        final int hash2 = value2.planHash(PlanHashable.PlanHashMode.VC0);
        final int hash3 = value3.planHash(PlanHashable.PlanHashMode.VC0);

        Assertions.assertEquals(hash1, hash2,
                "Same parameters should produce same plan hash");
        Assertions.assertNotEquals(hash1, hash3,
                "Different parameters should produce different plan hash");
    }

    @Test
    void testHashCodeWithoutChildren() {
        final var value1 = new RowNumberHighOrderValue(100, true);
        final var value2 = new RowNumberHighOrderValue(100, true);
        final var value3 = new RowNumberHighOrderValue(200, false);

        final int hash1 = value1.hashCodeWithoutChildren();
        final int hash2 = value2.hashCodeWithoutChildren();
        final int hash3 = value3.hashCodeWithoutChildren();

        Assertions.assertEquals(hash1, hash2,
                "Same parameters should produce same hash code");
        Assertions.assertNotEquals(hash1, hash3,
                "Different parameters should produce different hash code");
    }

    @Test
    void testExplain() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var literal = LiteralValue.ofScalar(42);

        final var explainTokens = value.explain(ImmutableList.of(literal::explain));

        Assertions.assertNotNull(explainTokens, "Explain tokens should not be null");
        Assertions.assertNotNull(explainTokens.getExplainTokens(), "Explain tokens should contain tokens");
    }

    @Test
    void testEvalWithoutStoreReturnsBuiltInFunction() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var evaluationContext = EvaluationContext.empty();

        final var result = value.evalWithoutStore(evaluationContext);

        Assertions.assertNotNull(result, "Eval should return a non-null result");
        Assertions.assertInstanceOf(BuiltInFunction.class, result,
                "Eval should return a BuiltInFunction");
        Assertions.assertEquals("row_number", result.getFunctionName(),
                "Function name should be 'row_number'");
    }

    @Test
    void testEvalWithoutStoreMemoization() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var evaluationContext = EvaluationContext.empty();

        final var result1 = value.evalWithoutStore(evaluationContext);
        final var result2 = value.evalWithoutStore(evaluationContext);

        Assertions.assertSame(result1, result2,
                "Multiple calls to evalWithoutStore should return the same memoized instance");
    }

    @Test
    void testEvalThrowsException() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var evaluationContext = EvaluationContext.empty();

        RecordCoreException exception = Assertions.assertThrows(RecordCoreException.class,
                () -> value.eval(null, evaluationContext),
                "HighOrderValue#eval() should throw RecordCoreException because it's compile-time only");
        Assertions.assertEquals("compile-time value can not be evaluated at runtime", exception.getMessage(),
                "Exception should have the correct error message");
    }

    @Test
    void testGetResultType() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var resultType = value.getResultType();

        Assertions.assertEquals(Type.FUNCTION, resultType,
                "HighOrderValue should have FUNCTION result type");
    }

    @Test
    void testCurriedFunctionEncapsulation() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var curriedFn = value.evalWithoutStore(EvaluationContext.empty());

        Assertions.assertNotNull(curriedFn, "Curried function should not be null");
        Assertions.assertEquals("row_number", curriedFn.getFunctionName(),
                "Curried function name should be 'row_number'");

        // Test encapsulation with valid arguments
        final var partitioningValues = AbstractArrayConstructorValue.LightArrayConstructorValue.of(
                ImmutableList.of(LiteralValue.ofScalar(1)));
        final var argumentValues = AbstractArrayConstructorValue.LightArrayConstructorValue.of(
                ImmutableList.of(LiteralValue.ofScalar(2)));
        final List<Value> arguments = ImmutableList.of(partitioningValues, argumentValues);

        final var rowNumberValue = curriedFn.encapsulate(arguments);

        Assertions.assertNotNull(rowNumberValue, "Encapsulated value should not be null");
        Assertions.assertInstanceOf(RowNumberValue.class, rowNumberValue,
                "Encapsulated value should be a RowNumberValue");
    }

    @Test
    void testCurriedFunctionRejectsInvalidArgumentCount() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var curriedFn = value.evalWithoutStore(EvaluationContext.empty());

        // Test with wrong number of arguments
        final List<Value> invalidArguments = ImmutableList.of(LiteralValue.ofScalar(1));

        Assertions.assertThrows(SemanticException.class,
                () -> curriedFn.encapsulate(invalidArguments),
                "Should throw SemanticException for invalid argument count");
    }

    @Test
    void testCurriedFunctionRejectsInvalidArgumentTypes() {
        final var value = new RowNumberHighOrderValue(100, true);
        final var curriedFn = value.evalWithoutStore(EvaluationContext.empty());

        // Test with wrong argument types (not array constructors)
        final List<Value> invalidArguments = ImmutableList.of(
                LiteralValue.ofScalar(1),
                LiteralValue.ofScalar(2)
        );

        Assertions.assertThrows(SemanticException.class,
                () -> curriedFn.encapsulate(invalidArguments),
                "Should throw SemanticException for invalid argument types");
    }

    @Test
    void testSerializationRoundTrip() {
        final var original = new RowNumberHighOrderValue(100, true);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);

        // Serialize
        final var proto = original.toProto(serializationContext);

        // Deserialize
        final var deserialized = RowNumberHighOrderValue.fromProto(proto);

        // Verify equality through plan hash
        Assertions.assertEquals(
                original.planHash(PlanHashable.PlanHashMode.VC0),
                deserialized.planHash(PlanHashable.PlanHashMode.VC0),
                "Serialization round-trip should preserve plan hash"
        );

        Assertions.assertEquals(
                original.hashCodeWithoutChildren(),
                deserialized.hashCodeWithoutChildren(),
                "Serialization round-trip should preserve hash code"
        );
    }
}
