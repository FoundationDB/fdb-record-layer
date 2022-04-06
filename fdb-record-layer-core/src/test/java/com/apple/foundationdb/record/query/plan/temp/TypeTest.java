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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords2Proto;
import com.apple.foundationdb.record.TestRecords3Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.query.plan.temp.dynamic.DynamicSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests for synthesizing a protobuf descriptor from a {@link Type} object.
 */
class TypeTest {

    static class ProtobufRandomMessageProvider implements ArgumentsProvider {

        private static final int seed = 42;
        private static final Random random = new Random(seed);

        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
            return Stream.of(
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
                                 final Message actual ) {
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : expectedFieldMap.entrySet()) {
            final Object expectedValue = entry.getValue();
            final Object actualValue = actual.getField(actualDescriptor.findFieldByName(entry.getKey().getName()));
            if (actualValue instanceof DynamicMessage) {
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
        areEqual(new ArrayList<>(expected.getAllFields().keySet()), actualDescriptor.getFields());
        // assert data equality
        areEqual(expected.getAllFields(), actualDescriptor, actual);
    }

    @ParameterizedTest(name = "[{index}] test synthesize {0}")
    @ArgumentsSource(ProtobufRandomMessageProvider.class)
    void recordTypeIsParsable(final String paramTestTitleIgnored, final Message message) throws Exception {
        DynamicSchema.Builder builder = DynamicSchema.newBuilder();
        final Type.Record recordType = Type.Record.fromDescriptor(message.getDescriptorForType());
        final DescriptorProtos.DescriptorProto descriptorProto = recordType.buildDescriptor(builder, "SyntheticDescriptor");
        final DynamicSchema schema = builder.build();
        final Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorProto.newBuilder()
                        .addMessageType(descriptorProto)
                        // add subtypes created indirectly and added to the dynamic schema.
                        .addAllMessageType(schema.getMessageTypes().stream().map(schema::getMessageDescriptor).filter(Objects::nonNull).map(Descriptors.Descriptor::toProto).collect(Collectors.toUnmodifiableList()))
                        .build(),
                new Descriptors.FileDescriptor[]{});
        final Descriptors.Descriptor messageDescriptor = fileDescriptor.findMessageTypeByName("SyntheticDescriptor");
        final Message actual = DynamicMessage.parseFrom(messageDescriptor, message.toByteArray());
        areEqual(message, actual, messageDescriptor);
    }
}
