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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4WrapperProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests for synthesizing a protobuf descriptor from a {@link Type} object and lifting a Java object type into an
 * equivalent {@link Type}.
 */
class TypeTest {

    static class ProtobufRandomMessageProvider implements ArgumentsProvider {

        private static final int seed = 42;
        private static final Random random = new Random(seed);

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
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
                                    .setBytesValue(ByteString.copyFrom(RandomUtils.nextBytes(20))).build()
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
                new Descriptors.FileDescriptor[] {});
        final Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName(typeName.get());
        final Message actual = DynamicMessage.parseFrom(messageDescriptor, message.toByteArray());
        areEqual(message, actual, messageDescriptor);
    }

    static class TypesProvider implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
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
                    Arguments.of(LiteralValue.ofScalar(false), LiteralValue.ofScalar(false).getResultType()),
                    Arguments.of(LiteralValue.ofScalar(42), LiteralValue.ofScalar(42).getResultType()),
                    Arguments.of(LiteralValue.ofScalar(42.1d), LiteralValue.ofScalar(42.1d).getResultType()),
                    Arguments.of(LiteralValue.ofScalar(42.2f), LiteralValue.ofScalar(42.2f).getResultType()),
                    Arguments.of(LiteralValue.ofScalar(43L), LiteralValue.ofScalar(43L).getResultType()),
                    Arguments.of(LiteralValue.ofScalar("foo"), LiteralValue.ofScalar("foo").getResultType()),
                    Arguments.of(LiteralValue.ofScalar(ByteString.copyFrom("bar", Charset.defaultCharset().name())),
                            LiteralValue.ofScalar(ByteString.copyFrom("bar", Charset.defaultCharset().name())).getResultType()),


                    // Primitives
                    Arguments.of(null, Type.nullType()),
                    Arguments.of(false, Type.primitiveType(Type.TypeCode.BOOLEAN, false)),
                    Arguments.of(42, Type.primitiveType(Type.TypeCode.INT, false)),
                    Arguments.of(42.1d, Type.primitiveType(Type.TypeCode.DOUBLE, false)),
                    Arguments.of(42.2f, Type.primitiveType(Type.TypeCode.FLOAT, false)),
                    Arguments.of(43L, Type.primitiveType(Type.TypeCode.LONG, false)),
                    Arguments.of("foo", Type.primitiveType(Type.TypeCode.STRING, false)),
                    Arguments.of(ByteString.copyFrom("bar", Charset.defaultCharset().name()), Type.primitiveType(Type.TypeCode.BYTES, false)),

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
                    Arguments.of(actualMessage, Type.Record.fromFields(List.of(
                            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("start_date")),
                            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("school_name")),
                            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("hometown"))
                    ))),

                    // Unsupported cases
                    Arguments.of(new int[] {1, 2, 3}, Type.any()), // primitive Arrays are not supported
                    Arguments.of(new Integer[] {1, 2, 3}, Type.any()), // object Arrays are not supported
                    Arguments.of(TestRecords2Proto.MyLongRecord.newBuilder().setRecNo(42).setBytesValue(ByteString.copyFrom(RandomUtils.nextBytes(20))).build(), Type.any()), // messages are not supported
                    Arguments.of(Type.TypeCode.ANY, Type.any()) // enums are not supported
            );
        }
    }

    @ParameterizedTest(name = "[{index} Java object {0}, Expected type {1}]")
    @ArgumentsSource(TypesProvider.class)
    void testTypeLifting(@Nullable final Object object, @Nonnull final Type expectedType) {
        Assertions.assertEquals(expectedType, Type.fromObject(object));
    }

    @Test
    void testAnyRecordSerialization() {
        PlanSerializationContext serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.CURRENT_FOR_CONTINUATION);
        Type.AnyRecord r1 = new Type.AnyRecord(false);
        Assertions.assertEquals(r1, Type.AnyRecord.fromProto(serializationContext, r1.toProto(serializationContext)));
    }
}
