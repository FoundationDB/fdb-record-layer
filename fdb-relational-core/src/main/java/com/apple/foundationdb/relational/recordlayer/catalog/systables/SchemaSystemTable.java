/*
 * SchemaSystemTable.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * This class represents the {@code Schema} system table. This system table contains information
 * about all available schemas.
 */
public class SchemaSystemTable implements SystemTable {

    public static final String TABLE_NAME = SystemTableRegistry.SCHEMAS_TABLE_NAME;

    private static final String SCHEMA_NAME = "schema_name";

    private static final String DATABASE_ID = "database_id";
    private static final String TEMPLATE_NAME = "template_name";
    private static final String TEMPLATE_VERSION = "template_version";
    private static final String METADATA = "meta_data";

    @Nonnull
    @Override
    public String getName() {
        return TABLE_NAME;
    }

    @Override
    public void addDefinition(@Nonnull final TypingContext typingContext) {
        // construct the Index type.
        typingContext.addType(getType());
    }

    @Override
    public TypingContext.TypeDefinition getType() {
        final TypingContext.FieldDefinition dbField = new TypingContext.FieldDefinition(DATABASE_ID, Type.TypeCode.STRING, null, false);
        final TypingContext.FieldDefinition nameField = new TypingContext.FieldDefinition(SCHEMA_NAME, Type.TypeCode.STRING, null, false);
        final TypingContext.FieldDefinition templatNameField = new TypingContext.FieldDefinition(TEMPLATE_NAME, Type.TypeCode.STRING, null, false);
        final TypingContext.FieldDefinition versionField = new TypingContext.FieldDefinition(TEMPLATE_VERSION, Type.TypeCode.LONG, null, false);
        final TypingContext.FieldDefinition metadataField = new TypingContext.FieldDefinition(METADATA, Type.TypeCode.BYTES, null, false);
        final Optional<List<String>> pkFields = Optional.of(List.of(DATABASE_ID, SCHEMA_NAME));
        return new TypingContext.TypeDefinition(TABLE_NAME, List.of(dbField, nameField, templatNameField, versionField, metadataField), true, pkFields);
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryKeyDefinition() {
        return Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.concatenateFields(DATABASE_ID, SCHEMA_NAME));
    }

}
