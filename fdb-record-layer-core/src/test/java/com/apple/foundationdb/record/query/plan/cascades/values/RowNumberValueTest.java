/*
 * RowNumberValueTest.java
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Tests for {@link RowNumberValue}.
 */
class RowNumberValueTest {

    @Test
    void testConstructorWithParameters() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new RowNumberValue(partitioningValues, argumentValues, 100, true);
        Assertions.assertNotNull(value, "RowNumberValue should be created successfully");
    }

    @Test
    void testConstructorWithNullParameters() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new RowNumberValue(partitioningValues, argumentValues, null, null);
        Assertions.assertNotNull(value, "RowNumberValue should be created with null parameters");
    }

    @Test
    void testConstructorFromProto() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var originalValue = new RowNumberValue(partitioningValues, argumentValues, 100, true);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = originalValue.toProto(serializationContext);

        final var value = new RowNumberValue(serializationContext, proto);
        Assertions.assertNotNull(value, "RowNumberValue should be created from proto");
    }

    @Test
    void testConstructorFromProtoWithoutOptionalFields() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var originalValue = new RowNumberValue(partitioningValues, argumentValues, null, null);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = originalValue.toProto(serializationContext);

        final var value = new RowNumberValue(serializationContext, proto);
        Assertions.assertNotNull(value, "RowNumberValue should be created from proto without optional fields");
    }

    @Test
    void testGetName() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new RowNumberValue(partitioningValues, argumentValues, 100, true);

        Assertions.assertEquals("ROW_NUMBER", value.getName(), "Name should be ROW_NUMBER");
    }

    @Test
    void testPlanHash() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value1 = new RowNumberValue(partitioningValues, argumentValues, 100, true);
        final var value2 = new RowNumberValue(partitioningValues, argumentValues, 100, true);
        final var value3 = new RowNumberValue(partitioningValues, argumentValues, 200, false);

        final int hash1 = value1.planHash(PlanHashable.PlanHashMode.VC0);
        final int hash2 = value2.planHash(PlanHashable.PlanHashMode.VC0);
        final int hash3 = value3.planHash(PlanHashable.PlanHashMode.VC0);

        Assertions.assertEquals(hash1, hash2,
                "Same parameters should produce same plan hash");
        Assertions.assertNotEquals(hash1, hash3,
                "Different parameters should produce different plan hash");
    }

    @Test
    void testGetResultType() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new RowNumberValue(partitioningValues, argumentValues, 100, true);
        final var resultType = value.getResultType();

        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.LONG), resultType,
                "RowNumberValue should have LONG result type");
    }

    @Test
    void testWithChildren() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new RowNumberValue(partitioningValues, argumentValues, 100, true);

        final var newPartitioningValues = ImmutableList.of(LiteralValue.ofScalar(3));
        final var newArgumentValues = ImmutableList.of(LiteralValue.ofScalar(4));
        final var newChildren = ImmutableList.<Value>builder()
                .addAll(newPartitioningValues)
                .addAll(newArgumentValues)
                .build();

        final var newValue = value.withChildren(newChildren);

        Assertions.assertNotNull(newValue, "New value should not be null");
        Assertions.assertInstanceOf(RowNumberValue.class, newValue,
                "New value should be a RowNumberValue");
        Assertions.assertNotSame(value, newValue,
                "New value should be a different instance");
    }

    @Test
    void testToProtoWithAllParameters() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new RowNumberValue(partitioningValues, argumentValues, 100, true);
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
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new RowNumberValue(partitioningValues, argumentValues, null, null);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = value.toProto(serializationContext);

        Assertions.assertNotNull(proto, "Proto should not be null");
        Assertions.assertFalse(proto.hasEfSearch(), "Proto should not have efSearch when null");
        Assertions.assertFalse(proto.hasIsReturningVectors(), "Proto should not have isReturningVectors when null");
    }

    @Test
    void testToValueProto() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new RowNumberValue(partitioningValues, argumentValues, 100, true);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var valueProto = value.toValueProto(serializationContext);

        Assertions.assertNotNull(valueProto, "Value proto should not be null");
        Assertions.assertTrue(valueProto.hasRowNumberValue(),
                "Value proto should have RowNumberValue");
        Assertions.assertEquals(100, valueProto.getRowNumberValue().getEfSearch(),
                "efSearch should be preserved in value proto");
    }

    @Test
    void testFromProtoStatic() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var originalValue = new RowNumberValue(partitioningValues, argumentValues, 200, false);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = originalValue.toProto(serializationContext);

        final var value = RowNumberValue.fromProto(serializationContext, proto);

        Assertions.assertNotNull(value, "Value should not be null");
        final var roundTripProto = value.toProto(serializationContext);
        Assertions.assertEquals(200, roundTripProto.getEfSearch(), "efSearch should match");
        Assertions.assertFalse(roundTripProto.getIsReturningVectors(), "isReturningVectors should match");
    }

    @Test
    void testFromProtoDeserializer() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var originalValue = new RowNumberValue(partitioningValues, argumentValues, 150, true);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = originalValue.toProto(serializationContext);

        final var deserializer = new RowNumberValue.Deserializer();
        final var value = deserializer.fromProto(serializationContext, proto);

        Assertions.assertNotNull(value, "Deserialized value should not be null");
        final var roundTripProto = value.toProto(serializationContext);
        Assertions.assertEquals(150, roundTripProto.getEfSearch(), "efSearch should match");
        Assertions.assertTrue(roundTripProto.getIsReturningVectors(), "isReturningVectors should match");
    }

    @Test
    void testSerializationRoundTrip() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var original = new RowNumberValue(partitioningValues, argumentValues, 100, true);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);

        final var proto = original.toProto(serializationContext);
        final var deserialized = RowNumberValue.fromProto(serializationContext, proto);

        Assertions.assertEquals(
                original.planHash(PlanHashable.PlanHashMode.VC0),
                deserialized.planHash(PlanHashable.PlanHashMode.VC0),
                "Serialization round-trip should preserve plan hash"
        );
    }

    @Test
    void testRowNumberHighOrderFnEncapsulateWithNamedArguments() {
        final var fn = new RowNumberValue.RowNumberHighOrderFn();

        final var efSearchValue = LiteralValue.ofScalar(100);
        final var returnsVectorsValue = LiteralValue.ofScalar(true);
        final var namedArguments = Map.of(
                RowNumberValue.RowNumberHighOrderFn.EF_SEARCH_ARGUMENT, efSearchValue,
                RowNumberValue.RowNumberHighOrderFn.INDEX_RETURNS_VECTORS_ARGUMENT, returnsVectorsValue
        );

        final var result = fn.encapsulate(namedArguments);

        Assertions.assertNotNull(result, "Encapsulated value should not be null");
        Assertions.assertInstanceOf(RowNumberHighOrderValue.class, result,
                "Result should be RowNumberHighOrderValue");
    }

    @Test
    void testRowNumberHighOrderFnEncapsulateWithNoArguments() {
        final var fn = new RowNumberValue.RowNumberHighOrderFn();
        final var namedArguments = Map.<String, LiteralValue<?>>of();

        final var result = fn.encapsulate(namedArguments);

        Assertions.assertNotNull(result, "Encapsulated value should not be null");
        Assertions.assertInstanceOf(RowNumberHighOrderValue.class, result,
                "Result should be RowNumberHighOrderValue");
    }

    @Test
    void testRowNumberHighOrderFnEncapsulateRejectsInvalidNamedArgument() {
        final var fn = new RowNumberValue.RowNumberHighOrderFn();

        final var invalidValue = LiteralValue.ofScalar(100);
        final var namedArguments = Map.of("invalid_argument", invalidValue);

        Assertions.assertThrows(SemanticException.class,
                () -> fn.encapsulate(namedArguments),
                "Should reject invalid named arguments");
    }

    @Test
    void testRowNumberHighOrderFnEncapsulateRejectsTooManyNamedArguments() {
        final var fn = new RowNumberValue.RowNumberHighOrderFn();

        final var efSearchValue = LiteralValue.ofScalar(100);
        final var returnsVectorsValue = LiteralValue.ofScalar(true);
        final var extraValue = LiteralValue.ofScalar(42);
        final var namedArguments = Map.of(
                RowNumberValue.RowNumberHighOrderFn.EF_SEARCH_ARGUMENT, efSearchValue,
                RowNumberValue.RowNumberHighOrderFn.INDEX_RETURNS_VECTORS_ARGUMENT, returnsVectorsValue,
                "extra", extraValue
        );

        Assertions.assertThrows(SemanticException.class,
                () -> fn.encapsulate(namedArguments),
                "Should reject too many named arguments");
    }
}
