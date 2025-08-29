/*
 * StructFieldGenerator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.relational.api.ArrayMetaData;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalStructBuilder;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.apple.foundationdb.relational.api.metadata.DataType.Code.ARRAY;
import static com.apple.foundationdb.relational.api.metadata.DataType.Code.STRUCT;

public class StructFieldGenerator implements FieldGenerator {
    private final String fieldName;
    protected final List<FieldGenerator> nestedFieldGenerators;
    private final RandomDataSource randomSource;
    private final int maxArraySize;

    public StructFieldGenerator(String fieldName,
                                List<FieldGenerator> nestedFieldGenerators,
                                RandomDataSource randomSource,
                                int maxArraySize) {
        this.fieldName = fieldName;
        this.nestedFieldGenerators = nestedFieldGenerators;
        this.randomSource = randomSource;
        this.maxArraySize = maxArraySize;
    }

    public StructFieldGenerator(String fieldName,
                                @Nonnull StructMetaData structMetaData,
                                RandomDataSource randomSource,
                                int maxArraySize) throws SQLException {
        this.fieldName = fieldName;
        this.randomSource = randomSource;
        this.maxArraySize = maxArraySize;
        this.nestedFieldGenerators = new ArrayList<>(structMetaData.getColumnCount());
        for (int i = 0; i < structMetaData.getColumnCount(); i++) {
            final var field = structMetaData.getRelationalDataType().getFields().get(i);
            if (field.getType().getCode() == STRUCT) {
                nestedFieldGenerators.add(getStructFieldGenerator(structMetaData.getColumnName(i + 1), structMetaData.getStructMetaData(i + 1)));
            } else if (field.getType().getCode() == ARRAY) {
                nestedFieldGenerators.add(getArrayFieldGenerator(structMetaData.getColumnName(i + 1), structMetaData.getArrayMetaData(i + 1)));
            } else {
                nestedFieldGenerators.add(getPrimitiveFieldGenerator(structMetaData.getColumnName(i + 1), field.getType()));
            }
        }
    }

    @Override
    public void generateValue(RelationalStructBuilder builder) throws SQLException {
        final var structBuilder = EmbeddedRelationalStruct.newBuilder();
        for (FieldGenerator fieldGen : nestedFieldGenerators) {
            fieldGen.generateValue(structBuilder);
            builder.addStruct(fieldName, structBuilder.build());
        }
    }

    private FieldGenerator getStructFieldGenerator(@Nonnull String name, @Nonnull StructMetaData structMetaData) throws SQLException {
        return new StructFieldGenerator(name, structMetaData, randomSource, maxArraySize);
    }

    private FieldGenerator getArrayFieldGenerator(@Nonnull String name, @Nonnull ArrayMetaData arrayMetaData) throws SQLException {
        FieldGenerator componentGenerator;
        if (arrayMetaData.getElementType() == Types.STRUCT) {
            componentGenerator = getStructFieldGenerator("na", arrayMetaData.getElementStructMetaData());
        } else {
            componentGenerator = getPrimitiveFieldGenerator("na", arrayMetaData.asRelationalType().getElementType());
        }
        return new ArrayFieldGenerator(name, componentGenerator, randomSource, maxArraySize);
    }

    private FieldGenerator getPrimitiveFieldGenerator(@Nonnull String name, DataType dataType) {
        switch (dataType.getCode()) {
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case STRING:
            case BYTES:
                return new PrimitiveFieldGenerator(name, dataType, randomSource);
            default:
                throw new IllegalStateException("Unexpected field type: " + SqlTypeNamesSupport.getSqlTypeName(dataType.getJdbcSqlCode()));
        }
    }
}
