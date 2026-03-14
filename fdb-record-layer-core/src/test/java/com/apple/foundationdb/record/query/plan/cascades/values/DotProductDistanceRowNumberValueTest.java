/*
 * DotProductDistanceRowNumberValueTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DotProductDistanceRowNumberValue}.
 */
class DotProductDistanceRowNumberValueTest {

    @Test
    void testConstructorWithParameters() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        Assertions.assertNotNull(value, "DotProductDistanceRowNumberValue should be created successfully");
    }

    @Test
    void testConstructorFromProto() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var originalValue = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = originalValue.toProto(serializationContext);

        final var value = new DotProductDistanceRowNumberValue(serializationContext, proto);
        Assertions.assertNotNull(value, "DotProductDistanceRowNumberValue should be created from proto");
    }

    @Test
    void testGetName() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);

        Assertions.assertEquals("DotProductDistanceRowNumber", value.getName(),
                "Name should be DotProductDistanceRowNumber");
    }

    @Test
    void testPlanHash() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value1 = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        final var value2 = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);

        final var partitioningValues3 = ImmutableList.of(LiteralValue.ofScalar(3));
        final var argumentValues3 = ImmutableList.of(LiteralValue.ofScalar(4));
        final var value3 = new DotProductDistanceRowNumberValue(partitioningValues3, argumentValues3);

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
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        final var resultType = value.getResultType();

        Assertions.assertEquals(Type.primitiveType(Type.TypeCode.LONG), resultType,
                "DotProductDistanceRowNumberValue should have LONG result type");
    }

    @Test
    void testWithChildren() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);

        final var newPartitioningValues = ImmutableList.of(LiteralValue.ofScalar(3));
        final var newArgumentValues = ImmutableList.of(LiteralValue.ofScalar(4));
        final var newChildren = ImmutableList.<Value>builder()
                .addAll(newPartitioningValues)
                .addAll(newArgumentValues)
                .build();

        final var newValue = value.withChildren(newChildren);

        Assertions.assertNotNull(newValue, "New value should not be null");
        Assertions.assertInstanceOf(DotProductDistanceRowNumberValue.class, newValue,
                "New value should be a DotProductDistanceRowNumberValue");
        Assertions.assertNotSame(value, newValue,
                "New value should be a different instance");
    }

    @Test
    void testToProto() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = value.toProto(serializationContext);

        Assertions.assertNotNull(proto, "Proto should not be null");
        Assertions.assertTrue(proto.hasSuper(), "Proto should have super (WindowedValue) field");
    }

    @Test
    void testToValueProto() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var valueProto = value.toValueProto(serializationContext);

        Assertions.assertNotNull(valueProto, "Value proto should not be null");
        Assertions.assertTrue(valueProto.hasDotProductDistanceRowNumberValue(),
                "Value proto should have DotProductDistanceRowNumberValue");
    }

    @Test
    void testFromProtoStatic() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var originalValue = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = originalValue.toProto(serializationContext);

        final var value = DotProductDistanceRowNumberValue.fromProto(serializationContext, proto);

        Assertions.assertNotNull(value, "Value should not be null");
        Assertions.assertEquals(
                originalValue.planHash(PlanHashable.PlanHashMode.VC0),
                value.planHash(PlanHashable.PlanHashMode.VC0),
                "Deserialized value should have same plan hash as original"
        );
    }

    @Test
    void testFromProtoDeserializer() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var originalValue = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = originalValue.toProto(serializationContext);

        final var deserializer = new DotProductDistanceRowNumberValue.Deserializer();
        final var value = deserializer.fromProto(serializationContext, proto);

        Assertions.assertNotNull(value, "Deserialized value should not be null");
        Assertions.assertEquals(
                originalValue.planHash(PlanHashable.PlanHashMode.VC0),
                value.planHash(PlanHashable.PlanHashMode.VC0),
                "Deserialized value should have same plan hash as original"
        );
    }

    @Test
    void testSerializationRoundTrip() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var original = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);
        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);

        final var proto = original.toProto(serializationContext);
        final var deserialized = DotProductDistanceRowNumberValue.fromProto(serializationContext, proto);

        Assertions.assertEquals(
                original.planHash(PlanHashable.PlanHashMode.VC0),
                deserialized.planHash(PlanHashable.PlanHashMode.VC0),
                "Serialization round-trip should preserve plan hash"
        );
    }

    @Test
    void testWithEmptyPartitioningValues() {
        final var partitioningValues = ImmutableList.<Value>of();
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(2));
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);

        Assertions.assertNotNull(value, "Value should be created with empty partitioning values");
        Assertions.assertEquals(0, ImmutableList.copyOf(value.getPartitioningValues()).size(),
                "Partitioning values should be empty");
    }

    @Test
    void testWithMultiplePartitioningValues() {
        final var partitioningValues = ImmutableList.of(
                LiteralValue.ofScalar(1),
                LiteralValue.ofScalar(2),
                LiteralValue.ofScalar(3)
        );
        final var argumentValues = ImmutableList.of(LiteralValue.ofScalar(4));
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);

        Assertions.assertNotNull(value, "Value should be created with multiple partitioning values");
        Assertions.assertEquals(3, ImmutableList.copyOf(value.getPartitioningValues()).size(),
                "Should have 3 partitioning values");
    }

    @Test
    void testWithMultipleArgumentValues() {
        final var partitioningValues = ImmutableList.of(LiteralValue.ofScalar(1));
        final var argumentValues = ImmutableList.of(
                LiteralValue.ofScalar(2),
                LiteralValue.ofScalar(3)
        );
        final var value = new DotProductDistanceRowNumberValue(partitioningValues, argumentValues);

        Assertions.assertNotNull(value, "Value should be created with multiple argument values");
        Assertions.assertEquals(2, ImmutableList.copyOf(value.getArgumentValues()).size(),
                "Should have 2 argument values");
    }

    @Test
    void testDeserializerGetProtoMessageClass() {
        final var deserializer = new DotProductDistanceRowNumberValue.Deserializer();
        final var protoClass = deserializer.getProtoMessageClass();

        Assertions.assertNotNull(protoClass, "Proto message class should not be null");
        Assertions.assertEquals("PDotProductDistanceRowNumberValue", protoClass.getSimpleName(),
                "Proto class should be PDotProductDistanceRowNumberValue");
    }
}
