/*
 * EnumDefinition.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.dynamic;

import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;

/**
 * EnumDefinition.
 */
public class EnumDefinition
{
    // --- private ---
    private final EnumDescriptorProto enumType;

    private EnumDefinition(EnumDescriptorProto enumType) {
        this.enumType = enumType;
    }

    // --- public static ---

    public static Builder newBuilder(String enumName) {
        return new Builder(enumName);
    }

    // --- public ---

    @Override
    public String toString() {
        return enumType.toString();
    }

    // --- package ---

    EnumDescriptorProto getEnumType() {
        return enumType;
    }

    /**
     * EnumDefinition.Builder
     */
    public static class Builder
    {
        // --- private ---
        private final EnumDescriptorProto.Builder enumTypeBuilder;

        private Builder(String enumName) {
            enumTypeBuilder = EnumDescriptorProto.newBuilder();
            enumTypeBuilder.setName(enumName);
        }

        // --- public ---

        public Builder addValue(String name, int num) {
            EnumValueDescriptorProto.Builder enumValBuilder = EnumValueDescriptorProto.newBuilder();
            enumValBuilder.setName(name).setNumber(num);
            enumTypeBuilder.addValue(enumValBuilder.build());
            return this;
        }

        public EnumDefinition build() {
            return new EnumDefinition(enumTypeBuilder.build());
        }
    }
}
