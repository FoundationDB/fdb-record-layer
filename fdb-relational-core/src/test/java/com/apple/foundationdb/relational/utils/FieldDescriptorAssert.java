/*
 * FieldDescriptorAssert.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import java.util.List;

public class FieldDescriptorAssert extends AbstractObjectAssert<FieldDescriptorAssert, Descriptors.FieldDescriptor> {
    public FieldDescriptorAssert(Descriptors.FieldDescriptor fieldDescriptor) {
        super(fieldDescriptor, FieldDescriptorAssert.class);
    }

    FieldDescriptorAssert isEqualTo(Descriptors.FieldDescriptor other) {
        if (!other.getName().equalsIgnoreCase(actual.getName())) {
            failWithMessage("Incorrect field name! ==> expected: <%s> but was <%s>", other.getName(), actual.getName());
        }
        if (other.getJavaType() != actual.getJavaType()) {
            failWithMessage("Incorrect field type! => expected: <%s> but was <%s>", other.getJavaType(), actual.getJavaType());
        }
        if (other.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
            new DescriptorAssert(actual.getMessageType())
                    .withFailMessage("Incorrect message type!")
                    .isSemanticallyEqualTo(other.getMessageType());
        }
        return this;
    }

    FieldDescriptorAssert isContainedIn(List<Descriptors.FieldDescriptor> fields) {
        for (Descriptors.FieldDescriptor field : fields) {
            if (field.getName().equalsIgnoreCase(actual.getName()) &&
                    field.getJavaType().equals(actual.getJavaType())) {
                if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    new DescriptorAssert(actual.getMessageType())
                            .withFailMessage("Incorrect message type!")
                            .isSemanticallyEqualTo(field.getMessageType());
                    //we have been found
                    return this;
                }
            }
        }
        failWithMessage("Field not found in list!");
        return this;
    }
}
