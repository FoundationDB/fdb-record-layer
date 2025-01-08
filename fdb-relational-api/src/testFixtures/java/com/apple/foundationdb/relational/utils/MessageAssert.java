/*
 * MessageAssert.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.annotation.API;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;

@API(API.Status.EXPERIMENTAL)
public class MessageAssert extends AbstractObjectAssert<MessageAssert, Message> {

    protected MessageAssert(Message message) {
        super(message, MessageAssert.class);
    }

    public static MessageAssert assertThat(Message message) {
        return new MessageAssert(message);
    }

    public MessageAssert isSameAs(Message expected) {
        if (expected == null) {
            isNull();
        } else {
            isNotNull();
            assertMessageMatches(expected, actual, descriptionText());
        }
        return this;
    }

    @Override
    public MessageAssert isEqualTo(Object expected) {
        if (expected == null) {
            isNull();
            return this;
        }
        if (expected instanceof Message) {
            return isSameAs((Message) expected);
        } else {
            return super.isEqualTo(expected);
        }
    }

    static SoftAssertions messagesMatch(Message expected, Message actual, SoftAssertions assertions) {
        final Descriptors.Descriptor expectedDescriptor = expected.getDescriptorForType();
        final Descriptors.Descriptor actualDescriptor = actual.getDescriptorForType();
        for (Descriptors.FieldDescriptor expectedField : expectedDescriptor.getFields()) {
            final Descriptors.FieldDescriptor actualField = actualDescriptor.findFieldByName(expectedField.getName());
            if (actualField == null) {
                assertions.fail("Message missing field ", expectedField.getName());
                return assertions;
            }

            Object expectO = expected.getField(expectedField);
            Object actualO = actual.getField(actualField);
            if (expectO instanceof Message) {
                assertions.assertThat(actualO)
                        .withFailMessage("field <%s> is not a message type", expectedField.getName())
                        .isInstanceOf(Message.class);
                assertions.assertAlso(messagesMatch((Message) expectO, (Message) actualO, new SoftAssertions()));
            } else if (expectO == null) {
                assertions.assertThat(actualO)
                        .describedAs("field <%s>", expectedField.getName())
                        .isNull();
            } else {
                assertions.assertThat(actualO)
                        .describedAs("field <%s>", expectedField.getName())
                        .isEqualTo(expectO);
            }
        }

        return assertions;
    }

    private void assertMessageMatches(Message expected, Message actual, String errorMessage) {
        final Descriptors.Descriptor expectedDescriptor = expected.getDescriptorForType();
        final Descriptors.Descriptor actualDescriptor = actual.getDescriptorForType();
        for (Descriptors.FieldDescriptor expectedField : expectedDescriptor.getFields()) {
            final Descriptors.FieldDescriptor actualField = actualDescriptor.findFieldByName(expectedField.getName());
            if (actualField == null) {
                failWithMessage("%s: Message missing field %s", errorMessage, expectedField.getName());
            }

            Object expectO = expected.getField(expectedField);
            Object actualO = actual.getField(actualField);
            if (expectO instanceof Message) {
                Assertions.assertThat(actualO)
                        .describedAs("%s: field <%s> is not a message type", errorMessage, expectedField.getName())
                        .isInstanceOf(Message.class);
                assertMessageMatches((Message) expectO, (Message) actualO, errorMessage);
            } else if (expectO == null) {
                Assertions.assertThat(actualO)
                        .describedAs("%s: field <%s>")
                        .isNull();
            } else {
                Assertions.assertThat(actualO)
                        .describedAs("%s: field <%s>", errorMessage, expectedField.getName())
                        .isEqualTo(expectO);
            }
        }
    }

}
