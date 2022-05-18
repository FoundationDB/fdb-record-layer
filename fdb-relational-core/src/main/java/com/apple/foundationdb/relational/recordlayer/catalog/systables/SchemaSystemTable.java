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

    public static final String TABLE_NAME = "Schema";

    private static final String SCHEMA_NAME = "schema_name";

    private static final String INDEX_DEF = "indexDef";

    private static final String INDEX_ALIAS = "IndexAlias";
    private static final String DATABASE_ID = "database_id";
    private static final String SCHEMA_TEMPLATE_NAME = "schema_template_name";
    private static final String SCHEMA_VERSION = "schema_version";
    private static final String TABLES = "tables";
    private static final String RECORD = "record";

    @Nonnull
    @Override
    public String getName() {
        return TABLE_NAME;
    }

    @Override
    public void addDefinition(@Nonnull final TypingContext typingContext) {
        // construct the Index type.
        {
            final TypingContext.FieldDefinition f1 = new TypingContext.FieldDefinition(SCHEMA_NAME, Type.TypeCode.STRING, null, false);
            final TypingContext.FieldDefinition f2 = new TypingContext.FieldDefinition(INDEX_DEF, Type.TypeCode.BYTES, null, false);
            typingContext.addType(new TypingContext.TypeDefinition(INDEX_ALIAS, List.of(f1, f2), false, Optional.empty()));
        }

        // construct the Table type.
        {
            final TypingContext.FieldDefinition f1 = new TypingContext.FieldDefinition("name", Type.TypeCode.STRING, null, false);
            final TypingContext.FieldDefinition f2 = new TypingContext.FieldDefinition("primary_key", Type.TypeCode.BYTES, null, false);
            final TypingContext.FieldDefinition f3 = new TypingContext.FieldDefinition("indexes", Type.TypeCode.RECORD, INDEX_ALIAS, true);
            typingContext.addType(new TypingContext.TypeDefinition("Table", List.of(f1, f2, f3), false, Optional.empty()));
        }

        // construct the system table
        {
            final TypingContext.FieldDefinition f1 = new TypingContext.FieldDefinition(SCHEMA_NAME, Type.TypeCode.STRING, null, false);
            final TypingContext.FieldDefinition f2 = new TypingContext.FieldDefinition(DATABASE_ID, Type.TypeCode.STRING, null, false);
            final TypingContext.FieldDefinition f3 = new TypingContext.FieldDefinition(SCHEMA_TEMPLATE_NAME, Type.TypeCode.STRING, null, false);
            final TypingContext.FieldDefinition f4 = new TypingContext.FieldDefinition(SCHEMA_VERSION, Type.TypeCode.INT, null, false);
            final TypingContext.FieldDefinition f5 = new TypingContext.FieldDefinition(TABLES, Type.TypeCode.RECORD, "Table", true);
            final TypingContext.FieldDefinition f6 = new TypingContext.FieldDefinition(RECORD, Type.TypeCode.BYTES, null, false);
            typingContext.addType(new TypingContext.TypeDefinition(getName(), List.of(f1, f2, f3, f4, f5, f6), true, Optional.of(List.of(DATABASE_ID, SCHEMA_NAME))));
        }
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryKeyDefinition() {
        return Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.concatenateFields(DATABASE_ID, SCHEMA_NAME));
    }

    @Override
    public int getRecordTypeKey() {
        return SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY;
    }
}
