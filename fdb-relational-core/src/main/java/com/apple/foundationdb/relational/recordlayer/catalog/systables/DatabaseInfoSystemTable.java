/*
 * DatabaseInfoSystemTable.java
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

package com.apple.foundationdb.relational.recordlayer.catalog.systables;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.ddl.DdlListener;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.DescriptorProtos;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * This class represents the {@code DatabaseInfo} system table. This system table contains information
 * about all available databases.
 */
public class DatabaseInfoSystemTable implements SystemTable {

    public static final String TABLE_NAME = "DatabaseInfo";

    private static final String DATABASE_ID = "database_id";

    @Nonnull
    @Override
    public String getName() {
        return TABLE_NAME;
    }

    @Nonnull
    @Override
    public DdlListener.TableBuilder getDefinition(@Nonnull final DdlListener.SchemaTemplateBuilder schemaTemplateBuilder) {

        final DescriptorProtos.FieldDescriptorProto.Builder databaseIdField = DescriptorProtos.FieldDescriptorProto.newBuilder().setName(DATABASE_ID).setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        final DdlListener.TableBuilder result = new DdlListener.TableBuilder(Map.of(), getName());
        result.addPrimaryKeyColumns(List.of(DATABASE_ID));
        try {
            result.registerField(databaseIdField, null);
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException(); // should never happen.
        }
        return result;
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryKeyDefinition() {
        return Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field(DATABASE_ID));
    }

    @Override
    public int getRecordTypeKey() {
        return SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY;
    }
}
