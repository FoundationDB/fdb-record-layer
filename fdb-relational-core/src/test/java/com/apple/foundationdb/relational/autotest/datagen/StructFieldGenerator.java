/*
 * StructFieldGenerator.java
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

package com.apple.foundationdb.relational.autotest.datagen;

import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.List;

public class StructFieldGenerator implements FieldGenerator {
    private final String fieldName;
    protected final List<FieldGenerator> nestedFieldGenerators;
    private final RandomDataSource randomSource;
    private final int maxArraySize;
    private final boolean isRepeated;

    public StructFieldGenerator(String fieldName,
                                List<FieldGenerator> nestedFieldGenerators,
                                RandomDataSource randomSource,
                                int maxArraySize,
                                boolean isRepeated) {
        this.fieldName = fieldName;
        this.nestedFieldGenerators = nestedFieldGenerators;
        this.randomSource = randomSource;
        this.maxArraySize = maxArraySize;
        this.isRepeated = isRepeated;
    }

    public StructFieldGenerator(String fieldName,
                                Descriptors.Descriptor structDescriptor,
                                RandomDataSource randomSource,
                                int maxArraySize,
                                boolean isRepeated) {
        this.fieldName = fieldName;
        this.randomSource = randomSource;
        final List<Descriptors.FieldDescriptor> fields = structDescriptor.getFields();
        this.maxArraySize = maxArraySize;
        this.nestedFieldGenerators = new ArrayList<>(fields.size());
        this.isRepeated = isRepeated;
        for (Descriptors.FieldDescriptor field : fields) {
            FieldGenerator fieldGenerator = getFieldGenerator(field);
            nestedFieldGenerators.add(fieldGenerator);
        }
    }

    @Override
    public void generateValue(DynamicMessageBuilder destination) throws RelationalException {
        final DynamicMessageBuilder structBuilder = destination.getNestedMessageBuilder(fieldName);
        for (FieldGenerator fieldGen : nestedFieldGenerators) {
            fieldGen.generateValue(structBuilder);
            if (isRepeated) {
                destination.addRepeatedField(fieldName, structBuilder.build());
            } else {
                destination.setField(fieldName, structBuilder.build());
            }
        }
    }

    private FieldGenerator getFieldGenerator(Descriptors.FieldDescriptor field) {
        FieldGenerator generator;
        switch (field.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTE_STRING:
                generator = new PrimitiveFieldGenerator(field.getName(), field.getJavaType(), randomSource, field.isRepeated());
                break;
            case MESSAGE:
                generator = new StructFieldGenerator(field.getName(), field.getMessageType(), randomSource, maxArraySize, field.isRepeated());
                break;
            default:
                throw new IllegalStateException("Unexpected field type: " + field.getJavaType());
        }
        if (field.isRepeated()) {
            generator = new ArrayFieldGenerator(generator, randomSource, maxArraySize);
        }
        return generator;
    }
}
