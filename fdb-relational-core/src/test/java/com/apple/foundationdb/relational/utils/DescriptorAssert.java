/*
 * DescriptorAssert.java
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

import com.google.protobuf.Descriptors;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.SoftAssertions;

import java.util.List;

public class DescriptorAssert extends AbstractObjectAssert<DescriptorAssert, Descriptors.Descriptor> {
    public DescriptorAssert(Descriptors.Descriptor descriptor) {
        super(descriptor, DescriptorAssert.class);
    }

    DescriptorAssert isSemanticallyEqualTo(Descriptors.Descriptor other) {
        isNotNull();
        SoftAssertions asserts = compareFieldsRecursive(other, actual);
        asserts.assertAll();

        return this;
    }

    public DescriptorAssert isContainedIn(List<Descriptors.Descriptor> messages) {
        for (Descriptors.Descriptor desc : messages) {
            SoftAssertions asserts = compareFieldsRecursive(desc, actual);
            if (asserts.wasSuccess()) {
                //we are in there!
                return this;
            }
        }
        failWithMessage("Was not contained in list!");
        return this;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    SoftAssertions compareFieldsRecursive(Descriptors.Descriptor other, Descriptors.Descriptor actual) {
        SoftAssertions asserts = new SoftAssertions();
        if (!other.getName().equals(actual.getName())) {
            asserts.fail("Incorrect descriptor name");
        }

        final List<Descriptors.FieldDescriptor> expectedFields = other.getFields();
        final List<Descriptors.FieldDescriptor> actualFields = actual.getFields();
        if (expectedFields.size() != actualFields.size()) {
            asserts.fail("Incorrect number of fields!");
        }
        for (Descriptors.FieldDescriptor expectedField : expectedFields) {
            boolean found = false;
            for (Descriptors.FieldDescriptor actualField : actualFields) {
                if (actualField.getName().equalsIgnoreCase(expectedField.getName()) &&
                        actualField.getJavaType().equals(expectedField.getJavaType()) &&
                        expectedField.isRepeated() == actualField.isRepeated()) {
                    if (expectedField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                        asserts.assertAlso(compareFieldsRecursive(expectedField.getMessageType(), actualField.getMessageType()));
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                asserts.fail("Missing expected field " + expectedField.getName());
            }
        }
        return asserts;
    }

}
