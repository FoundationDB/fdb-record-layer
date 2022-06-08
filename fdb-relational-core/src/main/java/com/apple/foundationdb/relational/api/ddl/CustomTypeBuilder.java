/*
 * CustomTypeBuilder.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.relational.api.catalog.TypeInfo;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.DescriptorProtos;

import java.util.Map;

// todo: refactor and move to its own package
public class CustomTypeBuilder {
    private final Map<String, TypeInfo> customTypes;
    private final String typeName;
    private final DescriptorProtos.DescriptorProto.Builder typeBuilder = DescriptorProtos.DescriptorProto.newBuilder();
    private int columnId = 1;

    public CustomTypeBuilder(Map<String, TypeInfo> customTypes, String typeName) {
        this.customTypes = customTypes;
        this.typeName = typeName;
        this.typeBuilder.setName(typeName);
    }

    public void registerField(DescriptorProtos.FieldDescriptorProto.Builder field, String messageType) throws RelationalException {
        if (messageType != null) {
            //verify that the message type has been defined
            TypeInfo typeInfo = customTypes.get(messageType);
            if (typeInfo == null) {
                throw new RelationalException("Unknown Message type: < " + messageType + ">", ErrorCode.SYNTAX_ERROR);
            }
            field.setTypeName(messageType);
        }
        field.setNumber(columnId);
        columnId++;
        this.typeBuilder.addField(field);
    }

    public DescriptorProtos.DescriptorProto buildDescriptor() {
        return typeBuilder.build();
    }

    public String getTypeName() {
        return typeName;
    }
}
