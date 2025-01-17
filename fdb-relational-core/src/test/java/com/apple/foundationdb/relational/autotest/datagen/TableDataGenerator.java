/*
 * TableDataGenerator.java
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

package com.apple.foundationdb.relational.autotest.datagen;

import com.apple.foundationdb.relational.api.RelationalStructBuilder;
import com.apple.foundationdb.relational.autotest.TableDescription;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;

/**
 * A Generator for full table data. Because Tables are really just Structs with primary keys,
 * this inherits the same logic as the StructFieldGenerator does, but does a little extra work
 * to ensure that we aren't acting like a struct.
 */
public class TableDataGenerator extends StructFieldGenerator {
    public TableDataGenerator(List<FieldGenerator> nestedFieldGenerators,
                              RandomDataSource randomSource,
                              int maxArraySize) {
        super("", nestedFieldGenerators, randomSource, maxArraySize);
    }

    public TableDataGenerator(TableDescription tableDescription,
                              RandomDataSource randomSource,
                              int maxArraySize) throws SQLException {
        super("", tableDescription.getMetaData(), randomSource, maxArraySize);
    }

    @Override
    public void generateValue(@Nonnull RelationalStructBuilder builder) throws SQLException {
        for (FieldGenerator fieldGen : nestedFieldGenerators) {
            fieldGen.generateValue(builder);
        }
    }
}
