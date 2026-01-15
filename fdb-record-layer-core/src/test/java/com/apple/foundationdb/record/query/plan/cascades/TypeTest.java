/*
 * TypeTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.TestRecordsUuidProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.TypeTestProto;
import com.apple.foundationdb.record.evolution.TestHeaderAsGroupProto;
import com.apple.foundationdb.record.planprotos.PType;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.record.util.RandomUtil;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.test.BooleanSource;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for synthesizing a protobuf descriptor from a {@link Type} object and lifting a Java object type into an
 * equivalent {@link Type}.
 */
class TypeTest {

    static class ProtobufRandomMessageProvider implements ArgumentsProvider {

        private static final int seed = 42;
        private static final Random random = new Random(seed);

        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) {
            return Stream.of(
                    Arguments.of(
                            "TestRecords4WrapperProto.RestaurantRecord", TestRecords4WrapperProto.RestaurantRecord.newBuilder()
                                    .setRestNo(random.nextLong())
                                    .setName("randomString" + random.nextInt())
                                    .setCustomer(TestRecords4WrapperProto.StringList.newBuilder().addAllValues(List.of("randomString" + random.nextInt(), "randomString" + random.nextInt(), "randomString" + random.nextInt())).build())
                                    .setReviews(TestRecords4WrapperProto.RestaurantReviewList.newBuilder().addValues(TestRecords4WrapperProto.RestaurantReview.newBuilder().setReviewer(random.nextInt()).setRating(random.nextInt()).build()).build())
                                    .build()),
                    Arguments.of(
                            "TestRecords4WrapperProto.RestaurantComplexRecord", TestRecords4WrapperProto.RestaurantComplexRecord.newBuilder()
                                    .setRestNo(random.nextLong())
                                    .setName("randomString" + random.nextInt())
                                    .setCustomer(TestRecords4WrapperProto.StringList.newBuilder().addAllValues(List.of("randomString" + random.nextInt(), "randomString" + random.nextInt(), "randomString" + random.nextInt())).build())
                                    .setReviews(TestRecords4WrapperProto.RestaurantComplexReviewList.newBuilder().addValues(TestRecords4WrapperProto.RestaurantComplexReview.newBuilder().setReviewer(random.nextInt()).setRating(random.nextInt())
                                            .setEndorsements(TestRecords4WrapperProto.ReviewerEndorsementsList.newBuilder()
                                                    .addValues(TestRecords4WrapperProto.ReviewerEndorsements.newBuilder().setEndorsementId(random.nextInt()).setEndorsementText(TestRecords4WrapperProto.StringList.newBuilder().addAllValues(List.of("randomString" + random.nextInt(), "randomString" + random.nextInt())))))))
                                    .build()),
                    Arguments.of(
                            "TestRecords1Proto.MySimpleRecord", TestRecords1Proto.MySimpleRecord.newBuilder()
                                    .setRecNo(random.nextLong())
                                    .setStrValueIndexed("randomString" + random.nextInt())
                                    .setNumValueUnique(random.nextInt())
                                    .setNumValue2(random.nextInt())
                                    .setNumValue3Indexed(random.nextInt())
                                    .addAllRepeater(List.of(random.nextInt(), random.nextInt(), random.nextInt())).build()),
                    Arguments.of(
                            "TestRecords1Proto.MyOtherRecord", TestRecords1Proto.MyOtherRecord.newBuilder()
                                    .setRecNo(random.nextInt())
                                    .setNumValue2(random.nextInt())
                                    .setNumValue3Indexed(random.nextInt()).build()
                    ),
                    Arguments.of(
                            "TestRecords2Proto.MyLongRecord", TestRecords2Proto.MyLongRecord.newBuilder()
                                    .setRecNo(random.nextInt())
                                    .setBytesValue(RandomUtil.randomByteString(random, 20)).build()
                    ),
                    Arguments.of(
                            "TestRecords3Proto.MyHierarchicalRecord", TestRecords3Proto.MyHierarchicalRecord.newBuilder()
                                    .setChildName("randomString" + random.nextInt())
                                    .setParentPath("randomString" + random.nextInt())
                                    .setNumValueIndexed(random.nextInt()).build()
                    ),
                    Arguments.of(
                            "TestRecords4Proto.RestaurantReviewer", TestRecords4Proto.RestaurantReviewer.newBuilder()
                                    .setName("randomString" + random.nextInt())
                                    .setEmail("randomString" + random.nextInt())
                                    .setId(random.nextLong())
                                    .setStats(TestRecords4Proto.ReviewerStats.newBuilder()
                                            .setHometown("randomString" + random.nextInt())
                                            .setStartDate(random.nextLong())
                                            .setSchoolName("randomString" + random.nextInt()).build()
                                    ).build()
                    ),
                    Arguments.of(
                          "TestRecordsUuidProto.UuidRecord", TestRecordsUuidProto.UuidRecord.newBuilder()
                                  .setPkey(TupleFieldsProto.UUID.newBuilder()
                                          .setMostSignificantBits(98452560)
                                          .setLeastSignificantBits(30900234)
                                          .build())
                                  .build()
                    )
            );
        }
    }

    private static void areEqual(final List<Descriptors.FieldDescriptor> expectedFields,
                                 final List<Descriptors.FieldDescriptor> actualFields) {
        Assertions.assertEquals(expectedFields.size(), actualFields.size());
        for (Descriptors.FieldDescriptor expectedField : expectedFields) {
            Descriptors.FieldDescriptor actualField = actualFields.get(expectedField.getIndex());
            Assertions.assertEquals(expectedField.getName(), actualField.getName());
            Assertions.assertEquals(expectedField.getType(), actualField.getType());
            // Assertions.assertEquals(expectedField.toProto().getLabel(), actualField.toProto().getLabel()); fails if field is REQUIRED.
            if (expectedField.toProto().getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED) {
                Assertions.assertEquals(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL, actualField.toProto().getLabel());
            } else {
                Assertions.assertEquals(expectedField.toProto().getLabel(), actualField.toProto().getLabel());
            }
        }
    }

    private static void areEqual(final Map<Descriptors.FieldDescriptor, Object> expectedFieldMap,
                                 final Descriptors.Descriptor actualDescriptor,
                                 final Message actual) {
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : expectedFieldMap.entrySet()) {
            final Object expectedValue = entry.getValue();
            final Object actualValue = actual.getField(actualDescriptor.findFieldByName(entry.getKey().getName()));
            if (entry.getKey().isRepeated()) {
                List<Object> expectedValueList = new ArrayList<>((Collection<?>)expectedValue);
                List<Object> actualValueList = new ArrayList<>((Collection<?>)actualValue);
                Assertions.assertEquals(expectedValueList.size(), actualValueList.size());
                for (int i = 0; i < expectedValueList.size(); i++) {
                    if (entry.getKey().getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                        areEqual((Message)expectedValueList.get(i), (Message)actualValueList.get(i), actualDescriptor.findFieldByName(entry.getKey().getName()).getMessageType());
                    } else {
                        Assertions.assertEquals(expectedValueList.get(i), actualValueList.get(i));
                    }
                }
            } else if (actualValue instanceof DynamicMessage) {
                areEqual((Message)expectedValue, (Message)actualValue, actualDescriptor.findFieldByName(entry.getKey().getName()).getMessageType());
            } else {
                Assertions.assertEquals(expectedValue, actualValue);
            }
        }
    }

    private static void areEqual(final Message expected,
                                 final Message actual,
                                 final Descriptors.Descriptor actualDescriptor) {
        Assertions.assertEquals(expected.getAllFields().size(), actual.getAllFields().size());
        // assert metadata equality
        areEqual(new ArrayList<>(expected.getDescriptorForType().getFields()), actualDescriptor.getFields());
        // assert data equality
        areEqual(expected.getAllFields(), actualDescriptor, actual);
    }

    @ParameterizedTest(name = "[{index}] test synthesize {0}")
    @ArgumentsSource(ProtobufRandomMessageProvider.class)
    void recordTypeIsParsable(final String paramTestTitleIgnored, final Message message) throws Exception {
        TypeRepository.Builder builder = TypeRepository.newBuilder();
        final Type.Record recordType = Type.Record.fromDescriptor(message.getDescriptorForType());
        final Optional<String> typeName = builder.defineAndResolveType(recordType);
        Assertions.assertTrue(typeName.isPresent());
        final TypeRepository typeRepository = builder.build();
        final Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .addAllMessageType(typeRepository.getMessageTypes().stream().map(typeRepository::getMessageDescriptor).filter(Objects::nonNull).map(Descriptors.Descriptor::toProto).collect(Collectors.toUnmodifiableList()))
                        .build(),
                TypeRepository.DEPENDENCIES.toArray(new Descriptors.FileDescriptor[0]));
        final Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName(typeName.get());
        final Message actual = DynamicMessage.parseFrom(messageDescriptor, message.toByteArray());
        areEqual(message, actual, messageDescriptor);
    }

    static class TypesProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                            final ExtensionContext context) throws Exception {
            final var listOfNulls = new LinkedList<Integer>();
            listOfNulls.add(null);
            final var listOfNullsAndNonNulls = new LinkedList<Integer>();
            listOfNullsAndNonNulls.add(null);
            listOfNullsAndNonNulls.add(42);
            listOfNullsAndNonNulls.add(43);

            final var descriptor = TestRecords4Proto.ReviewerStats.getDescriptor();
            final var actualMessage = DynamicMessage.newBuilder(descriptor)
                    .setField(descriptor.findFieldByName("school_name"), "blah")
                    .setField(descriptor.findFieldByName("start_date"), 34343L)
                    .setField(descriptor.findFieldByName("hometown"), "blah2")
                    .build();

            return Stream.of(

                    // Typed objects
                    Arguments.of(LiteralValue.ofScalar(false), Type.primitiveType(Type.TypeCode.BOOLEAN, false)),
                    Arguments.of(LiteralValue.ofScalar(42), Type.primitiveType(Type.TypeCode.INT, false)),
                    Arguments.of(LiteralValue.ofScalar(42.1d), Type.primitiveType(Type.TypeCode.DOUBLE, false)),
                    Arguments.of(LiteralValue.ofScalar(42.2f), Type.primitiveType(Type.TypeCode.FLOAT, false)),
                    Arguments.of(LiteralValue.ofScalar(43L), Type.primitiveType(Type.TypeCode.LONG, false)),
                    Arguments.of(LiteralValue.ofScalar("foo"), Type.primitiveType(Type.TypeCode.STRING, false)),
                    Arguments.of(LiteralValue.ofScalar(ByteString.copyFrom("bar", Charset.defaultCharset().name())), Type.primitiveType(Type.TypeCode.BYTES, false)),


                    // Primitives
                    Arguments.of(null, Type.nullType()),
                    Arguments.of(false, Type.primitiveType(Type.TypeCode.BOOLEAN, false)),
                    Arguments.of(42, Type.primitiveType(Type.TypeCode.INT, false)),
                    Arguments.of(42.1d, Type.primitiveType(Type.TypeCode.DOUBLE, false)),
                    Arguments.of(42.2f, Type.primitiveType(Type.TypeCode.FLOAT, false)),
                    Arguments.of(43L, Type.primitiveType(Type.TypeCode.LONG, false)),
                    Arguments.of("foo", Type.primitiveType(Type.TypeCode.STRING, false)),
                    Arguments.of(ByteString.copyFrom("bar", Charset.defaultCharset().name()), Type.primitiveType(Type.TypeCode.BYTES, false)),

                    // UUID
                    Arguments.of(UUID.fromString("eebee473-690b-48c1-beb0-07c3aca77768"), Type.uuidType(false)),

                    // Arrays
                    Arguments.of(listOfNulls, new Type.Array(Type.any())),
                    Arguments.of(listOfNullsAndNonNulls, new Type.Array(Type.primitiveType(Type.TypeCode.INT, true)), false),
                    Arguments.of(List.of(false), new Type.Array(Type.primitiveType(Type.TypeCode.BOOLEAN, false))),
                    Arguments.of(List.of(42), new Type.Array(Type.primitiveType(Type.TypeCode.INT, false))),
                    Arguments.of(List.of(42.1d), new Type.Array(Type.primitiveType(Type.TypeCode.DOUBLE, false))),
                    Arguments.of(List.of(42.2f), new Type.Array(Type.primitiveType(Type.TypeCode.FLOAT, false))),
                    Arguments.of(List.of(43L), new Type.Array(Type.primitiveType(Type.TypeCode.LONG, false))),
                    Arguments.of(List.of("foo"), new Type.Array(Type.primitiveType(Type.TypeCode.STRING, false))),
                    Arguments.of(List.of(ByteString.copyFrom("bar", Charset.defaultCharset().name())), new Type.Array(Type.primitiveType(Type.TypeCode.BYTES, false))),


                    // Arrays of arrays
                    Arguments.of(List.of(listOfNulls), new Type.Array(new Type.Array(Type.any())), false),
                    Arguments.of(List.of(listOfNullsAndNonNulls), new Type.Array(new Type.Array(Type.primitiveType(Type.TypeCode.INT, true)))),
                    Arguments.of(List.of(List.of(false), List.of(false)), new Type.Array(new Type.Array(Type.primitiveType(Type.TypeCode.BOOLEAN, false)))),
                    Arguments.of(List.of(List.of(42), List.of(42)), new Type.Array(new Type.Array(Type.primitiveType(Type.TypeCode.INT, false)))),
                    Arguments.of(List.of(List.of(42.1d), List.of(42.1d)), new Type.Array(new Type.Array(Type.primitiveType(Type.TypeCode.DOUBLE, false)))),
                    Arguments.of(List.of(List.of(42.2f), List.of(42.2f)), new Type.Array(new Type.Array(Type.primitiveType(Type.TypeCode.FLOAT, false)))),
                    Arguments.of(List.of(List.of(43L), List.of(43L)), new Type.Array(new Type.Array(Type.primitiveType(Type.TypeCode.LONG, false)))),
                    Arguments.of(List.of(List.of("foo"), List.of("foo")), new Type.Array(new Type.Array(Type.primitiveType(Type.TypeCode.STRING, false)))),
                    Arguments.of(List.of(List.of(ByteString.copyFrom("bar", Charset.defaultCharset().name())),
                            List.of(ByteString.copyFrom("bar", Charset.defaultCharset().name()))), new Type.Array(new Type.Array(Type.primitiveType(Type.TypeCode.BYTES, false))), false),

                    // DynamicMessage
                    Arguments.of(actualMessage, Type.Record.fromFields(false, List.of(
                            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("start_date")),
                            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("school_name")),
                            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("hometown"))
                    ))),

                    // Unsupported cases
                    Arguments.of(new int[] {1, 2, 3}, Type.any()), // primitive Arrays are not supported
                    Arguments.of(new Integer[] {1, 2, 3}, Type.any()), // object Arrays are not supported
                    Arguments.of(TestRecords2Proto.MyLongRecord.newBuilder().setRecNo(42).setBytesValue(RandomUtil.randomByteString(ThreadLocalRandom.current(), 20)).build(), Type.any()), // messages are not supported
                    Arguments.of(Type.TypeCode.ANY, Type.any()) // enums are not supported
            );
        }
    }

    @ParameterizedTest(name = "[{index} Java object {0}, Expected type {1}]")
    @ArgumentsSource(TypesProvider.class)
    void testTypeLifting(@Nullable final Object object, @Nonnull final Type expectedType) {
        final Type typeFromObject = Type.fromObject(object);
        Assertions.assertEquals(expectedType, typeFromObject);
    }

    @Test
    void normalizeFieldRetainsNumbersIfSet() {
        final List<Type.Record.Field> originalFields = List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a")), // 1
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("b")), // 2
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("c"), Optional.of(100)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("d")), // 4
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("e"), Optional.of(6)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f")), // would be 6, but taken, so given 7
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("g"), Optional.of(8)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("h")), // 9
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("i")), // would be, but 10 and 11 are both taken (ahead), so given 12
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("j"), Optional.of(10)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("k"), Optional.of(11))
        );

        final List<Type.Record.Field> numberedFields = List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"), Optional.of(1)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("b"), Optional.of(2)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("c"), Optional.of(100)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("d"), Optional.of(4)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("e"), Optional.of(6)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f"), Optional.of(7)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("g"), Optional.of(8)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("h"), Optional.of(9)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("i"), Optional.of(12)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("j"), Optional.of(10)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("k"), Optional.of(11))
        );

        // Double check that the numbered fields all have the same number as the original (if set)
        for (int i = 0; i < originalFields.size(); i++) {
            final Optional<Integer> originalIndex = originalFields.get(i).getFieldIndexOptional();
            if (originalIndex.isPresent()) {
                assertThat(numberedFields.get(i).getFieldIndexOptional())
                        .isEqualTo(originalIndex);
            }
        }

        assertNormalizedFieldsHaveNumbers(originalFields, numberedFields);
    }

    @Test
    void normalizeRemovesDuplicateFieldIndexes() {
        final List<Type.Record.Field> originalFields = List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a")), // 1
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("b")), // 2
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("c"), Optional.of(10)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("d")), // 4
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("e"), Optional.of(6)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f")), // would be 6, but taken, so would be given 7
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("g"), Optional.of(6)) // dupe!
        );
        final List<Type.Record.Field> numberedFields = List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"), Optional.of(1)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("b"), Optional.of(2)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("c"), Optional.of(3)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("d"), Optional.of(4)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("e"), Optional.of(5)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("f"), Optional.of(6)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("g"), Optional.of(7))
        );

        assertNormalizedFieldsHaveNumbers(originalFields, numberedFields);
    }

    private void assertNormalizedFieldsHaveNumbers(@Nonnull List<Type.Record.Field> originalFields, @Nonnull List<Type.Record.Field> numberedFields) {
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            final Type.Record fromOriginal = Type.Record.fromFields(originalFields);
            final Type.Record fromNumbered = Type.Record.fromFields(numberedFields);
            softly.assertThat(fromOriginal)
                    .isEqualTo(fromNumbered)
                    .hasSameHashCodeAs(fromNumbered);

            final Type.Record withPseudoFields = fromOriginal.addPseudoFields();
            softly.assertThat(fromOriginal)
                    .isNotEqualTo(withPseudoFields)
                    .doesNotHaveSameHashCodeAs(withPseudoFields);

            // All the field indexes should match the indexes in the numbered list.
            for (int i = 0; i < fromOriginal.getFields().size(); i++) {
                final Type.Record.Field fromOriginalField = fromOriginal.getField(i);
                final Optional<Integer> expectedIndex = numberedFields.get(i).getFieldIndexOptional();
                softly.assertThat(fromOriginalField.getFieldIndexOptional())
                        .isEqualTo(expectedIndex)
                        .isEqualTo(fromNumbered.getField(i).getFieldIndexOptional())
                        .isEqualTo(withPseudoFields.getField(i).getFieldIndexOptional());
            }

            // Double check that there are not duplicate indexes assigned to different fields
            softly.assertThat(fromOriginal.getFields().stream().map(Type.Record.Field::getFieldIndex).collect(Collectors.toList()))
                    .doesNotHaveDuplicates();
            softly.assertThat(fromNumbered.getFields().stream().map(Type.Record.Field::getFieldIndex).collect(Collectors.toList()))
                    .doesNotHaveDuplicates();
            softly.assertThat(withPseudoFields.getFields().stream().map(Type.Record.Field::getFieldIndex).collect(Collectors.toList()))
                    .doesNotHaveDuplicates();
        }
    }

    @Nonnull
    static Stream<Type.Enum> enumTypes() {
        return Stream.of(
                Type.Enum.fromValues(false, List.of(Type.Enum.EnumValue.from("A", 0), Type.Enum.EnumValue.from("B", 1), Type.Enum.EnumValue.from("C", 2))),
                Type.Enum.fromValues(false, List.of(Type.Enum.EnumValue.from("A", 0), Type.Enum.EnumValue.from("B", 1), Type.Enum.EnumValue.from("C", 3))),
                Type.Enum.fromValues(false, List.of(Type.Enum.EnumValue.from("A", 0), Type.Enum.EnumValue.from("B", 1), Type.Enum.EnumValue.from("D", 2))),
                Type.Enum.fromValues(false, List.of(Type.Enum.EnumValue.from("A..", 0), Type.Enum.EnumValue.from("B__", 1), Type.Enum.EnumValue.from("__C$", 2)))
        );
    }

    @Nonnull
    static Stream<Type.Record> recordTypes() {
        return Stream.of(
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.empty()))),
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a")))),
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("b")))),
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a")))),
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("b")))),
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.empty(), Optional.of(1)))),
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a"), Optional.of(2)))),
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("b"), Optional.of(2)))),
                Type.Record.fromFields(List.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("a.b"), Optional.of(2)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.Record.fromFields(List.of(
                                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("b"), Optional.of(2)))
                        ), Optional.of("a"), Optional.of(1)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.Record.fromFields(List.of(
                                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("b"), Optional.of(2)))
                        ), Optional.of("a"), Optional.of(1)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("b"), Optional.of(3)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("b"), Optional.of(3)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("b"), Optional.of(3)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(new Type.Array(false, Type.primitiveType(Type.TypeCode.LONG, false)), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(new Type.Array(false, Type.primitiveType(Type.TypeCode.STRING, false)), Optional.of("b"), Optional.of(3)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(new Type.Array(true, Type.primitiveType(Type.TypeCode.LONG, false)), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(new Type.Array(false, Type.primitiveType(Type.TypeCode.STRING, false)), Optional.of("b"), Optional.of(3)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(new Type.Array(true, Type.primitiveType(Type.TypeCode.LONG, false)), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(new Type.Array(true, Type.primitiveType(Type.TypeCode.STRING, false)), Optional.of("b"), Optional.of(3)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(new Type.Array(false, Type.primitiveType(Type.TypeCode.LONG, true)), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(new Type.Array(false, Type.primitiveType(Type.TypeCode.STRING, false)), Optional.of("b"), Optional.of(3)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(new Type.Array(false, Type.primitiveType(Type.TypeCode.LONG, true)), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(new Type.Array(false, Type.primitiveType(Type.TypeCode.STRING, true)), Optional.of("b"), Optional.of(3)))),
                Type.Record.fromFields(List.of(
                        Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"), Optional.of(1)),
                        Type.Record.Field.of(Type.Enum.fromValues(false, List.of(Type.Enum.EnumValue.from("A", 0), Type.Enum.EnumValue.from("B", 1))), Optional.of("b"), Optional.of(3))))
        );
    }

    @Nonnull
    static Stream<Type> types() {
        Stream<Type> primitiveTypes = Stream.of(Type.TypeCode.values())
                .filter(Type.TypeCode::isPrimitive)
                .filter(code -> code != Type.TypeCode.VECTOR)
                .map(Type::primitiveType);

        Stream<Type> otherTypes = Stream.of(
                new Type.AnyRecord(false),
                Type.uuidType(false),
                Type.Vector.of(false, 16, 500),
                Type.Vector.of(false, 32, 500),
                Type.Vector.of(false, 16, 1000),
                Type.Vector.of(false, 32, 1000)
        );

        Stream<Type> nullableAndNonNullableTypes = Stream.concat(
                Stream.of(Type.any(), Type.nullType(), Type.noneType()),
                Streams.concat(primitiveTypes, otherTypes, enumTypes(), recordTypes()).flatMap(t -> Stream.of(t.withNullability(false), t.withNullability(true)))
        );

        return nullableAndNonNullableTypes
                .flatMap(t -> Stream.of(t, new Type.Array(false, t), new Type.Array(true, t), new Type.Relation(t)));
    }

    @Nonnull
    static Stream<Arguments> typesWithIndex() {
        final List<Type> typeList = types().collect(Collectors.toList());
        return IntStream.range(0, typeList.size())
                .mapToObj(index -> Arguments.of(index, typeList.get(index)));
    }

    @ParameterizedTest(name = "[{index} serialization of {0}]")
    @MethodSource("types")
    void testSerialization(@Nonnull final Type type) {
        PlanSerializationContext serializationContext = PlanSerializationContext.newForCurrentMode();
        final PType typeProto = type.toTypeProto(serializationContext);

        PlanSerializationContext deserializationContext = PlanSerializationContext.newForCurrentMode();
        final Type recreatedType = Type.fromTypeProto(deserializationContext, typeProto);
        Assertions.assertEquals(type, recreatedType);
    }

    @ParameterizedTest(name = "[{index} nullability of {0}]")
    @MethodSource("types")
    void testNullability(@Nonnull final Type type) {
        if (type instanceof Type.None || type instanceof Type.Relation) {
            // None and Relational are special and are always not nullable
            Assertions.assertSame(type, type.notNullable());
            Assertions.assertThrows(VerifyException.class, type::nullable);
            return;
        }

        final Type nullableType = type.nullable();
        if (type.isNullable()) {
            Assertions.assertSame(nullableType, type);
        } else {
            Assertions.assertNotEquals(nullableType, type);
        }

        if (type instanceof Type.Any || type instanceof Type.Null) {
            // These types do not have a not-nullable variation
            Assertions.assertThrows(Throwable.class, type::notNullable);
            return;
        }

        final Type notNullableType = type.notNullable();
        if (type.isNullable()) {
            Assertions.assertNotEquals(notNullableType, type);
        } else {
            Assertions.assertSame(notNullableType, type);
        }

        // Converting the type back and forth from nullable and not nullable should
        // produce the original type
        Assertions.assertTrue(nullableType.isNullable());
        Assertions.assertFalse(notNullableType.isNullable());
        Assertions.assertEquals(nullableType, notNullableType.nullable());
        Assertions.assertEquals(notNullableType, nullableType.notNullable());
    }

    @ParameterizedTest(name = "pairwiseEquality[{index} {1}]")
    @MethodSource("typesWithIndex")
    void pairwiseEquality(int index, @Nonnull Type type) {
        final List<Type> typeList = types().collect(Collectors.toList());
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            // Check this type for equality/inequality against items in the list. It should only be equal to the
            // item at the same position.
            // Note that we could have one test case for each pair of types, bun then we'd have thousands and
            // thousands of tests added to the report, which clogs things up.
            final PType typeProto = type.toTypeProto(PlanSerializationContext.newForCurrentMode());
            for (int i = 0; i < typeList.size(); i++) {
                final Type otherType = typeList.get(i);
                final PType otherTypeProto = otherType.toTypeProto(PlanSerializationContext.newForCurrentMode());

                if (i == index) {
                    softly.assertThat(type)
                            .isEqualTo(otherType)
                            .hasSameHashCodeAs(otherType);
                    softly.assertThat(typeProto)
                            .isEqualTo(otherTypeProto);
                } else {
                    softly.assertThat(type)
                            .isNotEqualTo(otherType);
                    softly.assertThat(typeProto)
                            .isNotEqualTo(otherTypeProto);
                }
            }
        }
    }

    enum EnumForTesting {
        ALPHA,
        BRAVO,
        __CHARLIE,
        DELTA__1,
    }

    @Test
    void createEnumFromJavaEnum() {
        final Type.Enum fromJava = Type.Enum.forJavaEnum(EnumForTesting.class);
        assertThat(fromJava)
                .isEqualTo(Type.Enum.fromValues(false, List.of(
                        // Java enum values assigned field positions starting with zero
                        Type.Enum.EnumValue.from("ALPHA", 0),
                        Type.Enum.EnumValue.from("BRAVO", 1),
                        Type.Enum.EnumValue.from("__CHARLIE", 2),
                        Type.Enum.EnumValue.from("DELTA__1", 3))));
        assertThat(fromJava.getName())
                .isNull();
        assertThat(fromJava.getStorageName())
                .isNull();
        assertThat(fromJava.getEnumValues().stream().map(Type.Enum.EnumValue::getStorageName).collect(Collectors.toList()))
                .containsExactly("ALPHA", "BRAVO", "__CHARLIE", "DELTA__01");
    }

    @Nonnull
    static Stream<Arguments> enumTypesWithNames() {
        return Stream.of(Pair.<String, String>of(null, null),
                        Pair.of("myEnumType", "myEnumType"),
                        Pair.of("__myEnumType", "__myEnumType"),
                        Pair.of("__myEnum.Type", "__myEnum__2Type"),
                        Pair.of("__myEnum$Type", "__myEnum__1Type"),
                        Pair.of("__myEnum__Type", "__myEnum__0Type")
                ).flatMap(namePair -> enumTypes().map(enumType -> {
                    if (namePair.getLeft() == null) {
                        return Arguments.of(enumType, namePair.getRight());
                    } else {
                        return Arguments.of(Type.Enum.fromValuesWithName(namePair.getLeft(), enumType.isNullable(), enumType.getEnumValues()), namePair.getRight());
                    }
                }));
    }

    @ParameterizedTest(name = "createEnumProtobuf[{0} storageName={1}]")
    @MethodSource("enumTypesWithNames")
    void createEnumProtobuf(@Nonnull Type.Enum enumType, @Nullable String expectedStorageName) {
        final TypeRepository.Builder typeBuilder = TypeRepository.newBuilder();
        enumType.defineProtoType(typeBuilder);
        typeBuilder.build();
        final TypeRepository repository = typeBuilder.build();

        final String enumTypeName = repository.getProtoTypeName(enumType);
        assertThat(repository.getEnumTypes())
                .containsExactly(enumTypeName);
        if (expectedStorageName == null) {
            assertThat(enumType.getName())
                    .isNull();
            assertThat(enumType.getStorageName())
                    .isNull();
            assertThat(enumTypeName)
                    .isNotNull();
        } else {
            assertThat(expectedStorageName)
                    .isEqualTo(enumTypeName)
                    .isEqualTo(enumType.getStorageName());
        }
        final Descriptors.EnumDescriptor enumDescriptor = repository.getEnumDescriptor(enumType);
        assertThat(enumDescriptor)
                .isNotNull()
                .isSameAs(repository.getEnumDescriptor(enumTypeName));
        assertThat(enumDescriptor.getName())
                .isEqualTo(enumTypeName);

        final Type.Enum fromProto = Type.Enum.fromDescriptor(enumType.isNullable(), enumDescriptor);
        assertThat(fromProto)
                .isEqualTo(enumType);
    }

    @Test
    void translatesGroupTypeAsRecord() {
        final Type.Record fromGroup = Type.Record.fromDescriptor(TestHeaderAsGroupProto.MyRecord.getDescriptor());
        final Type.Record fromNested = Type.Record.fromDescriptor(TestRecordsWithHeaderProto.MyRecord.getDescriptor());

        assertThat(fromGroup)
                .isEqualTo(fromNested)
                .hasSameHashCodeAs(fromNested);

        final Type.Record headerType = Type.Record.fromFields(false, ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("rec_no")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, false), Optional.of("path")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, true), Optional.of("num"))
        ));

        assertThat(fromGroup.getFieldNameFieldMap())
                .hasEntrySatisfying("header", field ->
                        assertThat(field.getFieldType())
                                .isEqualTo(headerType)
                                .hasSameHashCodeAs(headerType));
    }

    @ParameterizedTest(name = "enumEqualsIgnoresName[{0}]")
    @MethodSource("enumTypes")
    void enumEqualsIgnoresName(@Nonnull Type.Enum enumType) {
        final Type.Enum typeWithName1 = Type.Enum.fromValuesWithName("name_one", enumType.isNullable(), enumType.getEnumValues());
        final Type.Enum typeWithName2 = Type.Enum.fromValuesWithName("name_two", enumType.isNullable(), enumType.getEnumValues());

        assertThat(typeWithName1.getName())
                .isNotEqualTo(typeWithName2.getName());
        assertThat(typeWithName1)
                .isEqualTo(typeWithName2)
                .hasSameHashCodeAs(typeWithName2);
    }

    @Nonnull
    static Stream<Arguments> recordTypesWithNames() {
        return Stream.of(Pair.<String, String>of(null, null),
                Pair.of("myEnumType", "myEnumType"),
                Pair.of("__myEnumType", "__myEnumType"),
                Pair.of("__myEnum.Type", "__myEnum__2Type"),
                Pair.of("__myEnum$Type", "__myEnum__1Type"),
                Pair.of("__myEnum__Type", "__myEnum__0Type")
        ).flatMap(namePair -> recordTypes().map(recordType -> {
            if (namePair.getLeft() == null) {
                return Arguments.of(recordType, namePair.getRight());
            } else {
                return Arguments.of(recordType.withName(namePair.getLeft()), namePair.getRight());
            }
        }));
    }

    @ParameterizedTest(name = "createRecordProtobuf[{0} storageName={1}]")
    @MethodSource("recordTypesWithNames")
    void createRecordProtobuf(@Nonnull Type.Record recordType, @Nullable String expectedStorageName) {
        final TypeRepository.Builder typeBuilder = TypeRepository.newBuilder();
        recordType.defineProtoType(typeBuilder);
        typeBuilder.build();
        final TypeRepository repository = typeBuilder.build();

        final String recordTypeName = repository.getProtoTypeName(recordType);
        assertThat(repository.getMessageTypes())
                .contains(recordTypeName);
        if (expectedStorageName == null) {
            assertThat(recordType.getName())
                    .isNull();
            assertThat(recordType.getStorageName())
                    .isNull();
            assertThat(recordTypeName)
                    .isNotNull();
        } else {
            assertThat(recordTypeName)
                    .isEqualTo(expectedStorageName)
                    .isEqualTo(recordType.getStorageName());
        }
        final Descriptors.Descriptor messageDescriptor = repository.getMessageDescriptor(recordType);
        assertThat(messageDescriptor)
                .isNotNull()
                .isSameAs(repository.getMessageDescriptor(recordTypeName));
        assertThat(messageDescriptor.getName())
                .isEqualTo(recordTypeName);

        final Type.Record fromDescriptor = Type.Record.fromDescriptor(messageDescriptor).withNullability(recordType.isNullable());
        assertThat(fromDescriptor.getName())
                .as("storage name of record not included in type from message descriptor")
                .isNull();
        assertThat(fromDescriptor)
                .isEqualTo(adjustFieldsForDescriptorParsing(recordType));
    }

    @Nonnull
    private Type.Record adjustFieldsForDescriptorParsing(@Nonnull Type.Record recordType) {
        // There are a number of changes that happen to a type when we create a protobuf descriptor for it that
        // fail to round trip. These may be bugs, but we can at least assert that everything except for these
        // components are preserved
        final ImmutableList.Builder<Type.Record.Field> newFields = ImmutableList.builderWithExpectedSize(recordType.getFields().size());
        for (Type.Record.Field field : recordType.getFields()) {
            Type fieldType = field.getFieldType();
            if (fieldType instanceof Type.Array) {
                // Array types retain their nullability as there are separate. However, they make their own element types not nullable
                Type elementType = ((Type.Array)fieldType).getElementType();
                if (elementType instanceof Type.Record) {
                    elementType = adjustFieldsForDescriptorParsing((Type.Record) elementType);
                }
                elementType = elementType.withNullability(false);
                fieldType = new Type.Array(fieldType.isNullable(), elementType);
            } else if (fieldType instanceof Type.Record) {
                // We need to recursively apply operations to record field types
                fieldType = adjustFieldsForDescriptorParsing((Type.Record) fieldType).withNullability(true);
            } else {
                // By default, the field is nullable. This is because the generated descriptor uses
                // LABEL_OPTIONAL on each type, so we make the field nullable
                fieldType = fieldType.withNullability(true);
            }
            newFields.add(Type.Record.Field.of(fieldType, field.getFieldNameOptional(), field.getFieldIndexOptional()));
        }
        return Type.Record.fromFields(recordType.isNullable(), newFields.build());
    }

    @ParameterizedTest(name = "recordEqualsIgnoresName[{0}]")
    @MethodSource("recordTypes")
    void recordEqualsIgnoresName(@Nonnull Type.Record recordType) {
        final Type.Record typeWithName1 = recordType.withName("name_one");
        final Type.Record typeWithName2 = recordType.withName("name_two");

        assertThat(typeWithName1.getName())
                .isNotEqualTo(typeWithName2.getName());
        assertThat(typeWithName1)
                .isEqualTo(typeWithName2)
                .hasSameHashCodeAs(typeWithName2);
    }

    @ParameterizedTest(name = "updateFieldNullability[{0}]")
    @MethodSource("recordTypes")
    void updateFieldNullability(@Nonnull Type.Record recordType) {
        for (Type.Record.Field field : recordType.getFields()) {
            final Type fieldType = field.getFieldType();

            // Validate that the non-nullable field type matches the not-nullable version of the field
            final Type.Record.Field nonNullableField = field.withNullability(false);
            assertThat(nonNullableField.getFieldType())
                    .isEqualTo(fieldType.notNullable())
                    .isNotEqualTo(fieldType.nullable())
                    .matches(Type::isNotNullable);

            // Validate that the nullable field type matches the nullable version of the field
            final Type.Record.Field nullableField = field.withNullability(true);
            assertThat(nullableField.getFieldType())
                    .isEqualTo(fieldType.nullable())
                    .isNotEqualTo(fieldType.notNullable())
                    .matches(Type::isNullable);

            // All other components of the field should remain the same
            assertThat(field.getFieldIndexOptional())
                    .isEqualTo(nullableField.getFieldIndexOptional())
                    .isEqualTo(nonNullableField.getFieldIndexOptional());
            assertThat(field.getFieldNameOptional())
                    .isEqualTo(nullableField.getFieldNameOptional())
                    .isEqualTo(nonNullableField.getFieldNameOptional());
            assertThat(field.getFieldStorageNameOptional())
                    .isEqualTo(nullableField.getFieldStorageNameOptional())
                    .isEqualTo(nonNullableField.getFieldStorageNameOptional());
        }
    }

    /**
     * Validate all primitive types can be parsed as field elements of a type. If this test fails after introducing
     * a new primitive type, update the Protobuf definition in {@code type_test.proto} or exclude it from consideration
     * if its type should not automatically be inferred for some reason.
     */
    @Test
    void testPrimitivesFromDescriptor() {
        final Descriptors.Descriptor descriptor = TypeTestProto.PrimitiveFields.getDescriptor();
        final Type.Record type = Type.Record.fromDescriptor(descriptor);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            for (Type.TypeCode typeCode : Type.TypeCode.values()) {
                if (!typeCode.isPrimitive() || typeCode == Type.TypeCode.NULL || typeCode == Type.TypeCode.UNKNOWN || typeCode == Type.TypeCode.VERSION || typeCode == Type.TypeCode.VECTOR) {
                    continue;
                }
                Type primitiveType = Type.primitiveType(typeCode);
                final String nameBase = typeCode.name().toLowerCase(Locale.ROOT) + "_field";

                // Should contain nullable, not-nullable, nullable array, and not-nullable array fields
                assertContainsField(softly, type, nameBase, primitiveType.nullable(), descriptor);
                assertContainsField(softly, type, "req_" + nameBase, primitiveType.notNullable(), descriptor);
                assertContainsField(softly, type, "arr_" + nameBase, new Type.Array(true, primitiveType.notNullable()), descriptor);
                assertContainsField(softly, type, "req_arr_" + nameBase, new Type.Array(false, primitiveType.notNullable()), descriptor);
            }

            // Ensure all fields have been picked up
            final List<Descriptors.FieldDescriptor> fieldDescriptors = descriptor.getFields();
            for (Descriptors.FieldDescriptor fieldDescriptor : fieldDescriptors) {
                softly.assertThat(type.getFieldNameFieldMap())
                        .containsKey(fieldDescriptor.getName());
            }
        }
    }

    /**
     * Simple case of resolving fields from a protobuf descriptor. None of the field names require any messaging for escape sequences.
     */
    @ParameterizedTest(name = "testSimpleFromDescriptor[preserveNames={0}]")
    @BooleanSource
    void testSimpleFromDescriptor(boolean preserveNames) {
        final Descriptors.Descriptor descriptor = TypeTestProto.Type1.getDescriptor();
        final Type.Record type = preserveNames ? Type.Record.fromDescriptorPreservingName(descriptor) : Type.Record.fromDescriptor(descriptor);

        final Descriptors.Descriptor outerDescriptor = TypeTestProto.Type2.getDescriptor();
        final Type.Record outerType = preserveNames ? Type.Record.fromDescriptorPreservingName(outerDescriptor) : Type.Record.fromDescriptor(outerDescriptor);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            assertContainsField(softly, type, "alpha", Type.primitiveType(Type.TypeCode.STRING, true), descriptor);
            assertContainsField(softly, type, "beta", Type.primitiveType(Type.TypeCode.LONG, false), descriptor);
            assertContainsField(softly, type, "uuid_field", Type.UUID_NULL_INSTANCE, descriptor);
            assertTypeNames(softly, type, descriptor, preserveNames);
            assertTypeNames(softly, roundTrip(type), descriptor, preserveNames);

            assertContainsField(softly, outerType, "alpha", type.nullable(), outerDescriptor);
            assertContainsField(softly, outerType, "beta", type.notNullable(), outerDescriptor);
            final Type.Enum enumType = Type.Enum.fromValues(true,
                    List.of(Type.Enum.EnumValue.from("ALPHA", 0), Type.Enum.EnumValue.from("BRAVO", 1), Type.Enum.EnumValue.from("CHARLIE", 3), Type.Enum.EnumValue.from("DELTA", 4)));
            assertContainsField(softly, outerType, "gamma", enumType, outerDescriptor);
            assertContainsField(softly, outerType, "delta", new Type.Array(true, type.notNullable()), outerDescriptor);
            assertContainsField(softly, outerType, "epsilon", new Type.Array(false, type.notNullable()), outerDescriptor);
            assertContainsField(softly, outerType, "zeta", new Type.Array(true, enumType.notNullable()), outerDescriptor);
            assertContainsField(softly, outerType, "eta", new Type.Array(false, enumType.notNullable()), outerDescriptor);
            assertTypeNames(softly, outerType, outerDescriptor, preserveNames);
            assertTypeNames(softly, roundTrip(outerType), outerDescriptor, preserveNames);
        }
    }

    /**
     * Test where a type descriptor has some field names that require escaping. In this case, all the field translations
     * are reversible, so it doesn't matter if the name is preserved from the protobuf file or re-calculated using
     * {@link com.apple.foundationdb.record.util.ProtoUtils#toProtoBufCompliantName(String)}.
     */
    @ParameterizedTest(name = "testEscapesFromDescriptor[preserveNames={0}]")
    @BooleanSource
    void testEscapesFromDescriptor(boolean preserveNames) {
        final Descriptors.Descriptor descriptor = TypeTestProto.Type1__0Escaped.getDescriptor();
        final Type.Record type = preserveNames ? Type.Record.fromDescriptorPreservingName(descriptor) : Type.Record.fromDescriptor(descriptor);

        final Descriptors.Descriptor outerDescriptor = TypeTestProto.Type2__0Escaped.getDescriptor();
        final Type.Record outerType = preserveNames ? Type.Record.fromDescriptorPreservingName(outerDescriptor) : Type.Record.fromDescriptor(outerDescriptor);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            assertContainsField(softly, type, "alpha$", Type.primitiveType(Type.TypeCode.STRING, true), descriptor.findFieldByName("alpha__1"));
            assertContainsField(softly, type, "beta.", Type.primitiveType(Type.TypeCode.LONG, false), descriptor.findFieldByName("beta__2"));
            assertContainsField(softly, type, "uuid__field", Type.UUID_NULL_INSTANCE, descriptor.findFieldByName("uuid__0field"));
            assertTypeNames(softly, type, descriptor, preserveNames);
            assertTypeNames(softly, roundTrip(type), descriptor, preserveNames);

            assertContainsField(softly, outerType, "alpha$", type.nullable(), outerDescriptor.findFieldByName("alpha__1"));
            assertContainsField(softly, outerType, "beta.", type.notNullable(), outerDescriptor.findFieldByName("beta__2"));
            final Type.Enum enumType = Type.Enum.fromValues(true,
                    List.of(Type.Enum.EnumValue.from("ALPHA__", 0), Type.Enum.EnumValue.from("BRAVO$", 1), Type.Enum.EnumValue.from("CHARLIE.", 4), Type.Enum.EnumValue.from("DELTA__3", 3)));
            assertContainsField(softly, outerType, "gamma__", enumType, outerDescriptor.findFieldByName("gamma__0"));
            assertContainsField(softly, outerType, "delta.array", new Type.Array(true, type.notNullable()), outerDescriptor.findFieldByName("delta__2array"));
            assertContainsField(softly, outerType, "epsilon.array", new Type.Array(false, type.notNullable()), outerDescriptor.findFieldByName("epsilon__2array"));
            assertContainsField(softly, outerType, "zeta.array", new Type.Array(true, enumType.notNullable()), outerDescriptor.findFieldByName("zeta__2array"));
            assertContainsField(softly, outerType, "eta.array", new Type.Array(false, enumType.notNullable()), outerDescriptor.findFieldByName("eta__2array"));
            assertTypeNames(softly, outerType, outerDescriptor, preserveNames);
            assertTypeNames(softly, roundTrip(outerType), outerDescriptor, preserveNames);
        }
    }

    /**
     * Test where a type descriptor has some field names that require escaping, but the original fields are imperfect.
     * The escaped names which are stored are <em>not</em> reversible back to the original field names. This can happen
     * if we have an unescaped double-underscore in the field. To make sure the storage names that come back from, say,
     * placing the field in a type repository, we need to preserve the original field names.
     */
    @ParameterizedTest(name = "testEscapesMalformedFromDescriptor[preserveNames={0}]")
    @BooleanSource
    void testEscapesMalformedFromDescriptor(boolean preserveNames) {
        final Descriptors.Descriptor descriptor = TypeTestProto.Type1__EscapingNotReversible.getDescriptor();
        final Type.Record type = preserveNames ? Type.Record.fromDescriptorPreservingName(descriptor) : Type.Record.fromDescriptor(descriptor);

        final Descriptors.Descriptor outerDescriptor = TypeTestProto.Type2__EscapedNotReversible.getDescriptor();
        final Type.Record outerType = preserveNames ? Type.Record.fromDescriptorPreservingName(outerDescriptor) : Type.Record.fromDescriptor(outerDescriptor);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            assertContainsField(softly, type, "alpha$__blah", Type.primitiveType(Type.TypeCode.STRING, true), descriptor.findFieldByName("alpha__1__blah"));
            assertContainsField(softly, type, "beta.__", Type.primitiveType(Type.TypeCode.LONG, false), descriptor.findFieldByName("beta__2__"));
            assertContainsField(softly, type, "uuid__$field", Type.UUID_NULL_INSTANCE, descriptor.findFieldByName("uuid____1field"));
            assertTypeNames(softly, type, descriptor, preserveNames);
            assertTypeNames(softly, roundTrip(type), descriptor, preserveNames);

            assertContainsField(softly, outerType, "alpha__blah", type.nullable(), outerDescriptor.findFieldByName("alpha__blah"));
            assertContainsField(softly, outerType, "beta.__", type.notNullable(), outerDescriptor.findFieldByName("beta__2__"));
            final Type.Enum enumType = Type.Enum.fromValues(true,
                    List.of(Type.Enum.EnumValue.from("ALPHA__", 0), Type.Enum.EnumValue.from("BRAVO_$", 1), Type.Enum.EnumValue.from("CHARLIE__.", 7), Type.Enum.EnumValue.from("DELTA__3", 4)));
            assertContainsField(softly, outerType, "gamma__", enumType, outerDescriptor.findFieldByName("gamma__"));
            assertContainsField(softly, outerType, "delta__array", new Type.Array(true, type.notNullable()), outerDescriptor.findFieldByName("delta__array"));
            assertContainsField(softly, outerType, "epsilon__array", new Type.Array(false, type.notNullable()), outerDescriptor.findFieldByName("epsilon__array"));
            assertTypeNames(softly, outerType, outerDescriptor, preserveNames);
            assertTypeNames(softly, roundTrip(outerType), outerDescriptor, preserveNames);
        }
    }

    private static void assertContainsField(@Nonnull SoftAssertions softly, @Nonnull Type.Record recordType, @Nonnull String fieldName, @Nonnull Type fieldType, @Nonnull Descriptors.Descriptor messageDescriptor) {
        final Descriptors.FieldDescriptor fieldDescriptor = messageDescriptor.findFieldByName(fieldName);
        softly.assertThat(fieldDescriptor)
                .as("field %s not found in descriptor", fieldDescriptor)
                .isNotNull();
        assertContainsField(softly, recordType, fieldName, fieldType, fieldDescriptor);
    }

    private static void assertContainsField(@Nonnull SoftAssertions softly, @Nonnull Type.Record recordType, @Nonnull String fieldName, @Nonnull Type fieldType, @Nullable Descriptors.FieldDescriptor fieldDescriptor) {
        final Map<String, Type.Record.Field> fieldMap = recordType.getFieldNameFieldMap();
        softly.assertThat(fieldMap)
                .as("should have had field %s", fieldName)
                .containsKey(fieldName);
        final Type.Record.Field field = fieldMap.get(fieldName);
        if (field == null) {
            return;
        }
        softly.assertThat(field.getFieldType())
                .as("field %s had unexpected type", fieldName)
                .isEqualTo(fieldType);
        softly.assertThat(field.getFieldName())
                .as("field %s had unexpected name", fieldName)
                .isEqualTo(fieldName);
        if (fieldDescriptor == null) {
            softly.fail("cannot compare with null field descriptor for field %s", fieldName);
        } else {
            softly.assertThat(field.getFieldStorageName())
                    .as("field %s had unexpected storage name", fieldName)
                    .isEqualTo(fieldDescriptor.getName());
            if (field.getFieldType() instanceof Type.Enum) {
                Type.Enum fieldEnumType = (Type.Enum) field.getFieldType();
                final List<String> storageNames = fieldEnumType.getEnumValues().stream().map(Type.Enum.EnumValue::getStorageName).collect(Collectors.toList());
                final List<String> expectedStorageNames = fieldDescriptor.getEnumType().getValues().stream().map(Descriptors.EnumValueDescriptor::getName).collect(Collectors.toList());
                softly.assertThat(storageNames)
                        .as("field %s enum type should have same names as stored enum", fieldName)
                        .isEqualTo(expectedStorageNames);

                final List<Integer> numbers = fieldEnumType.getEnumValues().stream().map(Type.Enum.EnumValue::getNumber).collect(Collectors.toList());
                final List<Integer> expectedNumbers = fieldDescriptor.getEnumType().getValues().stream().map(Descriptors.EnumValueDescriptor::getNumber).collect(Collectors.toList());
                softly.assertThat(numbers)
                        .as("field %s enum type should have same numbers as stored enum", fieldName)
                        .isEqualTo(expectedNumbers);
            }
        }
    }

    private static void assertTypeNames(@Nonnull SoftAssertions softly, @Nonnull Type.Record recordType, @Nonnull Descriptors.Descriptor descriptor, boolean preserveNames) {
        if (preserveNames) {
            softly.assertThat(recordType.getName())
                    .isEqualTo(ProtoUtils.toUserIdentifier(descriptor.getName()));
            softly.assertThat(recordType.getStorageName())
                    .isEqualTo(descriptor.getName());
        } else {
            softly.assertThat(recordType.getName())
                    .isNull();
            softly.assertThat(recordType.getStorageName())
                    .isNull();

        }
        for (Type.Record.Field field : recordType.getFields()) {
            Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(field.getFieldStorageName());
            softly.assertThat(fieldDescriptor)
                    .as("field descriptor not found for field %s", field)
                    .isNotNull();
            if (fieldDescriptor == null) {
                // Can't proceed further
                return;
            }

            // Extract the element from arrays. If the type is not nullable, we just replace the field
            // type with its element type. If the field is nullable, we need to also replace the
            // field descriptor so it points to the underlying values.
            Type fieldType = field.getFieldType();
            if (fieldType instanceof Type.Array) {
                Type.Array fieldArrayType = (Type.Array) fieldType;
                fieldType = fieldArrayType.getElementType();
                if (fieldArrayType.isNullable()) {
                    fieldDescriptor = fieldDescriptor.getMessageType().findFieldByName("values");
                }
                softly.assertThat(fieldDescriptor.isRepeated())
                        .as("array field %s should be over repeated field", field)
                        .isTrue();
            }

            // Look to make sure enum and record names are preserved (or not)
            if (fieldType instanceof Type.Enum) {
                final Type.Enum fieldEnumType = (Type.Enum) fieldType;
                if (preserveNames) {
                    final Descriptors.EnumDescriptor fieldEnumDescriptor = fieldDescriptor.getEnumType();
                    softly.assertThat(fieldEnumType.getName())
                            .isEqualTo(ProtoUtils.toUserIdentifier(fieldEnumDescriptor.getName()));
                    softly.assertThat(fieldEnumType.getStorageName())
                            .isEqualTo(fieldEnumDescriptor.getName());
                } else {
                    softly.assertThat(fieldEnumType.getName())
                            .isNull();
                    softly.assertThat(fieldEnumType.getStorageName())
                            .isNull();
                }
            } else if (fieldType instanceof Type.Record) {
                final Type.Record fieldRecordType = (Type.Record) fieldType;
                final Descriptors.Descriptor fieldTypeDescriptor = fieldDescriptor.getMessageType();
                if (preserveNames) {
                    softly.assertThat(fieldRecordType.getName())
                            .isEqualTo(ProtoUtils.toUserIdentifier(fieldTypeDescriptor.getName()));
                    softly.assertThat(fieldRecordType.getStorageName())
                            .isEqualTo(fieldTypeDescriptor.getName());
                } else {
                    softly.assertThat(fieldRecordType.getName())
                            .isNull();
                    softly.assertThat(fieldRecordType.getStorageName())
                            .isNull();
                }

                // Recurse down
                assertTypeNames(softly, fieldRecordType, fieldTypeDescriptor, preserveNames);
            }
        }
    }

    /**
     * Send a type to Protobuf and back. This allows us to check that certain features are preserved even in the face
     * of type serialization/deserialization.
     *
     * @param type the type to serialize and deserialize
     * @return the deserialized type
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    private static <T extends Type> T roundTrip(@Nonnull T type) {
        return (T) Type.fromTypeProto(PlanSerializationContext.newForCurrentMode(),
                type.toTypeProto(PlanSerializationContext.newForCurrentMode()));
    }
}
