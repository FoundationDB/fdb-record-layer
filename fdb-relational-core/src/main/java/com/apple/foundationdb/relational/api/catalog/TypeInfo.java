/*
 * TypeInfo.java
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.relational.api.ddl.ProtobufDdlUtil;

import com.google.protobuf.DescriptorProtos;

import java.util.stream.Collectors;

import javax.annotation.Nonnull;

public class TypeInfo {
    @Nonnull
    DescriptorProtos.DescriptorProto descriptor;

    public TypeInfo(@Nonnull DescriptorProtos.DescriptorProto typeDesc) {
        this.descriptor = typeDesc;
    }

    public String getTypeName() {
        return descriptor.getName();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("type: { name:").append(descriptor.getName()).append(", columns: { ");
        sb.append(descriptor.getFieldList().stream()
                .map(fd -> fd.getName() + ":" + ProtobufDdlUtil.getTypeName(fd))
                .collect(Collectors.joining(",")));
        sb.append("}}");
        return sb.toString();
    }

    @Nonnull
    public DescriptorProtos.DescriptorProto getDescriptor() {
        return descriptor;
    }
}
