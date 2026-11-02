/*
 * MessageHelperTests.java
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

import com.apple.foundationdb.record.TestRecordsTransformProto;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Tests for {@link MessageHelpers.TransformationTrieNode} and {@link MessageHelpers.CoercionTrieNode} hashing
 * and equality semantics, and for the coercion of messages between descriptors.
 */
public class MessageHelperTests {

    private static Type.Record someRecordType() {
        final var aaType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac"))));

        final var aType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aaType, Optional.of("aa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac"))));

        final var xType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc"))));

        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aType, Optional.of("a")),
                Type.Record.Field.of(xType, Optional.of("x")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z"))));
    }

    private static Type.Record someOtherRecordType() {
        final var bbType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("bba")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("bbb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("bbc"))));

        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z")),
                Type.Record.Field.of(bbType, Optional.of("a"))));
    }

    private static MessageHelpers.CoercionTrieNode someCoercionTrieNode() {
        final var recordType = someRecordType();
        final var inValue = QuantifiedObjectValue.of(Quantifier.current(), recordType);
        final var a_aa_aaa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aaa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));

        final var transformMap =
                ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa_aaa.getFieldPath(), new LiteralValue<>("1"),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2),
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        final var coercedType = Type.Record.fromDescriptor(TestRecordsTransformProto.TransformMessageMaxTypes.getDescriptor());
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);

        return PromoteValue.computePromotionsTrie(coercedType, inValue.getResultType(), transformationsTrie);
    }

    private static MessageHelpers.CoercionTrieNode someOtherCoercionTrieNode() {
        final var recordType = someOtherRecordType();
        final var inValue = QuantifiedObjectValue.of(Quantifier.current(), recordType);
        final var a_bbb = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "bbb"));

        final var transformMap = ImmutableMap.<FieldValue.FieldPath, Value>of(a_bbb.getFieldPath(), new LiteralValue<>(1.0d));

        final var coercedType = Type.Record.fromDescriptor(TestRecordsTransformProto.OtherTransformMessageMaxTypes.getDescriptor());
        final var transformationsTrie =
                RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);

        return PromoteValue.computePromotionsTrie(coercedType, inValue.getResultType(), transformationsTrie);
    }

    private static MessageHelpers.TransformationTrieNode someTransformationTrieNode() {
        final var recordType = someRecordType();
        final var inValue = QuantifiedObjectValue.of(Quantifier.current(), recordType);
        final var a_aa_aaa = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aaa"));
        final var a_aa_aab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "aa", "aab"));
        final var a_ab = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "ab"));

        final var transformMap = ImmutableMap.<FieldValue.FieldPath, Value>of(a_aa_aaa.getFieldPath(), new LiteralValue<>("1"),
                        a_aa_aab.getFieldPath(), new LiteralValue<>(2),
                        a_ab.getFieldPath(), new LiteralValue<>(3));

        return RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                        transformMap);
    }

    private static MessageHelpers.TransformationTrieNode someOtherTransformationTrieNode() {
        final var recordType = someOtherRecordType();
        final var inValue = QuantifiedObjectValue.of(Quantifier.current(), recordType);
        final var a_bba = FieldValue.ofFieldNames(inValue, ImmutableList.of("a", "bba"));

        final var transformMap = ImmutableMap.<FieldValue.FieldPath, Value>of(a_bba.getFieldPath(), new LiteralValue<>("1"));

        return RecordQueryUpdatePlan.computeTrieForFieldPaths(RecordQueryUpdatePlan.checkAndPrepareOrderedFieldPaths(transformMap),
                transformMap);
    }

    @Test
    public void testSelfEqualityAndHashCodeTransformationTrieNode() {
        final var transformTrie = someTransformationTrieNode();
        Assertions.assertEquals(transformTrie.hashCode(), transformTrie.hashCode());
        Assertions.assertEquals(transformTrie, transformTrie);
    }

    @Test
    public void testSelfEqualityAndHashCodeCoercionTrieNode() {
        final var coercionNode = someCoercionTrieNode();
        Assertions.assertEquals(coercionNode.hashCode(), coercionNode.hashCode());
        Assertions.assertEquals(coercionNode, coercionNode);
    }

    @Test
    public void testEqualityWithOtherTransformationTrieNode() {
        final var transformTrie = someTransformationTrieNode();
        final var sameTransformTrie = someTransformationTrieNode();
        final var differentTransformTie = someOtherTransformationTrieNode();

        Assertions.assertEquals(transformTrie.hashCode(), sameTransformTrie.hashCode());
        Assertions.assertEquals(transformTrie, sameTransformTrie);

        Assertions.assertNotEquals(differentTransformTie, transformTrie);
    }

    @Test
    public void testEqualityWithOtherCoercionTrieNode() {
        final var coercionTrie = someCoercionTrieNode();
        final var sameCoercionTrie = someCoercionTrieNode();
        final var differentCoercionTie = someOtherCoercionTrieNode();

        Assertions.assertEquals(coercionTrie.hashCode(), sameCoercionTrie.hashCode());
        Assertions.assertEquals(coercionTrie, sameCoercionTrie);

        Assertions.assertNotEquals(differentCoercionTie, coercionTrie);
    }

    @Nonnull
    private static Type.Record personType(@Nonnull final List<Integer> fieldIndexes,
                                          @Nonnull final List<Integer> addressFieldIndexes) {
        final var addressType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("street"), Optional.of(addressFieldIndexes.get(0))),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("city"), Optional.of(addressFieldIndexes.get(1)))));
        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("first_name"), Optional.of(fieldIndexes.get(0))),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("last_name"), Optional.of(fieldIndexes.get(1))),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("id"), Optional.of(fieldIndexes.get(2))),
                Type.Record.Field.of(addressType, Optional.of("address"), Optional.of(fieldIndexes.get(3)))));
    }

    @Nonnull
    private static Descriptors.Descriptor descriptorOf(@Nonnull final Type.Record type) {
        // a repository of its own, so that types that only differ in their field indexes do not share a descriptor
        return Objects.requireNonNull(TypeRepository.newBuilder().addTypeIfNeeded(type).build().getMessageDescriptor(type));
    }

    @Nonnull
    private static Object fieldOf(@Nonnull final Message message, @Nonnull final String fieldName) {
        return message.getField(Objects.requireNonNull(message.getDescriptorForType().findFieldByName(fieldName)));
    }

    /**
     * Two descriptors of the same structure need not number its fields the same way, e.g. if one of them keeps the
     * numbers of a proto that skips some. Copying between them has to relate the fields by position, as relating them
     * by number would put a value into whichever field happens to carry its number on the other side.
     */
    @Test
    public void testCoerceMessageByIndexRelatesFieldsByPosition() {
        final var skippingType = personType(ImmutableList.of(1, 3, 4, 5), ImmutableList.of(1, 3));
        final var sequentialType = personType(ImmutableList.of(1, 2, 3, 4), ImmutableList.of(1, 2));
        final var skippingDescriptor = descriptorOf(skippingType);
        final var sequentialDescriptor = descriptorOf(sequentialType);
        Assertions.assertEquals(3, skippingDescriptor.findFieldByName("last_name").getNumber());
        Assertions.assertEquals(3, sequentialDescriptor.findFieldByName("id").getNumber());

        final var addressDescriptor = skippingDescriptor.findFieldByName("address").getMessageType();
        final var address = DynamicMessage.newBuilder(addressDescriptor)
                .setField(addressDescriptor.findFieldByName("street"), "123 Main St")
                .setField(addressDescriptor.findFieldByName("city"), "Springfield")
                .build();
        final var person = DynamicMessage.newBuilder(skippingDescriptor)
                .setField(skippingDescriptor.findFieldByName("first_name"), "Alice")
                .setField(skippingDescriptor.findFieldByName("last_name"), "Smith")
                .setField(skippingDescriptor.findFieldByName("id"), 1L)
                .setField(skippingDescriptor.findFieldByName("address"), address)
                .build();

        final var coerced = MessageHelpers.coerceMessageByIndexIfNeeded(sequentialDescriptor, person);
        Assertions.assertSame(sequentialDescriptor, coerced.getDescriptorForType());
        Assertions.assertEquals("Alice", fieldOf(coerced, "first_name"));
        Assertions.assertEquals("Smith", fieldOf(coerced, "last_name"));
        Assertions.assertEquals(1L, fieldOf(coerced, "id"));
        final var coercedAddress = (Message)fieldOf(coerced, "address");
        Assertions.assertEquals("123 Main St", fieldOf(coercedAddress, "street"));
        Assertions.assertEquals("Springfield", fieldOf(coercedAddress, "city"));

        // there and back again
        Assertions.assertEquals(person, MessageHelpers.coerceMessageByIndexIfNeeded(skippingDescriptor, coerced));
        // a message already of the descriptor is returned as is
        Assertions.assertSame(person, MessageHelpers.coerceMessageByIndexIfNeeded(skippingDescriptor, person));
        // a coercion without any promotions relates the fields by position as well
        Assertions.assertEquals(coerced,
                MessageHelpers.coerceObject(null, sequentialType, sequentialDescriptor, skippingType, person));
    }
}
