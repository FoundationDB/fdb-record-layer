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
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
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
 * and equality semantics, and for the deep copy of messages between descriptors.
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
    private static Type.Record.Field field(@Nonnull final Type type, @Nonnull final String name, final int index) {
        return Type.Record.Field.of(type, Optional.of(name), Optional.of(index));
    }

    @Nonnull
    private static Type.Record personType(@Nonnull final List<Integer> fieldIndexes,
                                          @Nonnull final List<Integer> addressFieldIndexes) {
        final var addressType = Type.Record.fromFields(false, ImmutableList.of(
                field(Type.primitiveType(Type.TypeCode.STRING), "street", addressFieldIndexes.get(0)),
                field(Type.primitiveType(Type.TypeCode.STRING), "city", addressFieldIndexes.get(1))));
        final var statusType = Type.Enum.fromValues(true,
                ImmutableList.of(Type.Enum.EnumValue.from("ACTIVE", 0), Type.Enum.EnumValue.from("RETIRED", 1)));
        return Type.Record.fromFields(ImmutableList.of(
                field(Type.primitiveType(Type.TypeCode.STRING), "first_name", fieldIndexes.get(0)),
                field(Type.primitiveType(Type.TypeCode.STRING), "last_name", fieldIndexes.get(1)),
                field(Type.primitiveType(Type.TypeCode.LONG), "id", fieldIndexes.get(2)),
                field(addressType.withNullability(true), "address", fieldIndexes.get(3)),
                field(statusType, "status", fieldIndexes.get(4)),
                field(new Type.Array(false, addressType), "addresses", fieldIndexes.get(5)),
                field(new Type.Array(true, addressType), "past_addresses", fieldIndexes.get(6))));
    }

    @Nonnull
    private static Type.Record skippingPersonType() {
        return personType(ImmutableList.of(1, 3, 4, 5, 6, 8, 9), ImmutableList.of(1, 3));
    }

    @Nonnull
    private static Descriptors.Descriptor descriptorOf(@Nonnull final Type.Record type) {
        // a repository of its own, so that types that only differ in their field indexes do not share a descriptor
        return Objects.requireNonNull(TypeRepository.newBuilder().addTypeIfNeeded(type).build().getMessageDescriptor(type));
    }

    @Nonnull
    private static Message address(@Nonnull final Descriptors.Descriptor descriptor, @Nonnull final String street,
                                   @Nonnull final String city) {
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("street"), street)
                .setField(descriptor.findFieldByName("city"), city)
                .build();
    }

    @Nonnull
    private static Message person(@Nonnull final Descriptors.Descriptor descriptor) {
        final var addressDescriptor = descriptor.findFieldByName("address").getMessageType();
        final var pastAddressesDescriptor = descriptor.findFieldByName("past_addresses").getMessageType();
        final var pastAddresses = DynamicMessage.newBuilder(pastAddressesDescriptor)
                .addRepeatedField(pastAddressesDescriptor.findFieldByName("values"), address(addressDescriptor, "1 Old Rd", "Ogdenville"))
                .build();
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName("first_name"), "Alice")
                .setField(descriptor.findFieldByName("last_name"), "Smith")
                .setField(descriptor.findFieldByName("id"), 1L)
                .setField(descriptor.findFieldByName("address"), address(addressDescriptor, "123 Main St", "Springfield"))
                .setField(descriptor.findFieldByName("status"),
                        descriptor.findFieldByName("status").getEnumType().findValueByName("RETIRED"))
                .addRepeatedField(descriptor.findFieldByName("addresses"), address(addressDescriptor, "456 Oak Ave", "Shelbyville"))
                .addRepeatedField(descriptor.findFieldByName("addresses"), address(addressDescriptor, "789 Pine Rd", "Capital City"))
                .setField(descriptor.findFieldByName("past_addresses"), pastAddresses)
                .build();
    }

    @Nonnull
    private static Object fieldOf(@Nonnull final Message message, @Nonnull final String fieldName) {
        return message.getField(Objects.requireNonNull(message.getDescriptorForType().findFieldByName(fieldName)));
    }

    private static void assertAddress(@Nonnull final Descriptors.Descriptor expectedDescriptor,
                                      @Nonnull final String expectedStreet, @Nonnull final String expectedCity,
                                      @Nonnull final Object actual) {
        final var actualMessage = (Message)actual;
        Assertions.assertSame(expectedDescriptor, actualMessage.getDescriptorForType());
        Assertions.assertEquals(expectedStreet, fieldOf(actualMessage, "street"));
        Assertions.assertEquals(expectedCity, fieldOf(actualMessage, "city"));
    }

    /**
     * Asserts that {@code actual}, a message of {@code descriptor}, carries the values {@link #person} puts into a
     * message, each of them in the field of the same name.
     */
    private static void assertPerson(@Nonnull final Descriptors.Descriptor descriptor, @Nonnull final Message actual) {
        Assertions.assertSame(descriptor, actual.getDescriptorForType());
        Assertions.assertEquals("Alice", fieldOf(actual, "first_name"));
        Assertions.assertEquals("Smith", fieldOf(actual, "last_name"));
        Assertions.assertEquals(1L, fieldOf(actual, "id"));

        final var addressDescriptor = descriptor.findFieldByName("address").getMessageType();
        assertAddress(addressDescriptor, "123 Main St", "Springfield", fieldOf(actual, "address"));

        final var status = (Descriptors.EnumValueDescriptor)fieldOf(actual, "status");
        Assertions.assertSame(descriptor.findFieldByName("status").getEnumType(), status.getType());
        Assertions.assertEquals("RETIRED", status.getName());

        final var addresses = (List<?>)fieldOf(actual, "addresses");
        Assertions.assertEquals(2, addresses.size());
        assertAddress(addressDescriptor, "456 Oak Ave", "Shelbyville", addresses.get(0));
        assertAddress(addressDescriptor, "789 Pine Rd", "Capital City", addresses.get(1));

        final var pastAddresses = (Message)fieldOf(actual, "past_addresses");
        Assertions.assertSame(descriptor.findFieldByName("past_addresses").getMessageType(),
                pastAddresses.getDescriptorForType());
        final var pastAddressesValues = (List<?>)fieldOf(pastAddresses, "values");
        Assertions.assertEquals(1, pastAddressesValues.size());
        assertAddress(addressDescriptor, "1 Old Rd", "Ogdenville", pastAddressesValues.get(0));
    }

    /**
     * Two descriptors of the same structure need not number its fields the same way, e.g. if one of them keeps the
     * numbers of a proto that skips some. A deep copy between them has to relate the fields by position, as relating
     * them by number would put a value into whichever field happens to carry its number on the other side. This holds
     * for nested records, records in arrays, and enum values, which are related by name, alike.
     */
    @Test
    public void testDeepCopyMessageRelatesFieldsByPosition() {
        final var skippingDescriptor = descriptorOf(skippingPersonType());
        final var sequentialDescriptor = descriptorOf(personType(ImmutableList.of(1, 2, 3, 4, 5, 6, 7), ImmutableList.of(1, 2)));
        Assertions.assertEquals(3, skippingDescriptor.findFieldByName("last_name").getNumber());
        Assertions.assertEquals(3, sequentialDescriptor.findFieldByName("id").getNumber());

        final var person = person(skippingDescriptor);
        final var copied = MessageHelpers.deepCopyMessageIfNeeded(sequentialDescriptor, person);
        assertPerson(sequentialDescriptor, copied);

        // there and back again
        Assertions.assertEquals(person, MessageHelpers.deepCopyMessageIfNeeded(skippingDescriptor, copied));
        // a message that already is of the target descriptor is returned as is
        Assertions.assertSame(person, MessageHelpers.deepCopyMessageIfNeeded(skippingDescriptor, person));
    }

    /**
     * The target descriptor may have fields after the ones of the message, such as the pseudo-fields of a record type.
     */
    @Test
    public void testDeepCopyMessageIntoDescriptorWithPseudoFields() {
        final var skippingType = skippingPersonType();
        final var skippingDescriptor = descriptorOf(skippingType);
        final var withPseudoFieldsType = skippingType.addPseudoFields();
        final var withPseudoFieldsDescriptor = descriptorOf(withPseudoFieldsType);
        Assertions.assertEquals(skippingDescriptor.getFields().size() + PseudoField.values().length,
                withPseudoFieldsDescriptor.getFields().size());

        final var copied = MessageHelpers.deepCopyMessageIfNeeded(withPseudoFieldsDescriptor, person(skippingDescriptor));
        assertPerson(withPseudoFieldsDescriptor, copied);
        for (final var pseudoField : PseudoField.values()) {
            Assertions.assertFalse(copied.hasField(withPseudoFieldsDescriptor.findFieldByName(pseudoField.getFieldName())));
        }
    }
}
