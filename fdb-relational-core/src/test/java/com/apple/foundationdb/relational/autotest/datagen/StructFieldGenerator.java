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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.ArrayMetaData;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalStructBuilder;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

@API(API.Status.EXPERIMENTAL)
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
            final var sqlType = structMetaData.getColumnType(i + 1);
            if (sqlType == Types.STRUCT) {
                nestedFieldGenerators.add(getStructFieldGenerator(structMetaData.getColumnName(i + 1), structMetaData.getStructMetaData(i + 1)));
            } else if (sqlType == Types.ARRAY) {
                nestedFieldGenerators.add(getArrayFieldGenerator(structMetaData.getColumnName(i + 1), structMetaData.getArrayMetaData(i + 1)));
            } else {
                nestedFieldGenerators.add(getPrimitiveFieldGenerator(structMetaData.getColumnName(i + 1), sqlType));
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
            componentGenerator = getPrimitiveFieldGenerator("na", arrayMetaData.getElementType());
        }
        return new ArrayFieldGenerator(name, componentGenerator, randomSource, maxArraySize);
    }

    private FieldGenerator getPrimitiveFieldGenerator(@Nonnull String name, int sqlType) {
        switch (sqlType) {
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.BOOLEAN:
            case Types.VARCHAR:
            case Types.BINARY:
                return new PrimitiveFieldGenerator(name, sqlType, randomSource);
            default:
                throw new IllegalStateException("Unexpected field type: " + SqlTypeNamesSupport.getSqlTypeName(sqlType));
        }
    }
}
