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
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;

import java.util.List;
import java.util.Optional;

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

    @Override
    public void addDefinition(@Nonnull final TypingContext typingContext) {
        typingContext.addType(getType());
    }

    @Override
    public TypingContext.TypeDefinition getType() {
        final TypingContext.FieldDefinition f1 = new TypingContext.FieldDefinition(DATABASE_ID, Type.TypeCode.STRING, null, false);
        return new TypingContext.TypeDefinition(getName(), List.of(f1), true, Optional.of(List.of(DATABASE_ID)));
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryKeyDefinition() {
        return Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field(DATABASE_ID));
    }

    @Override
    public long getRecordTypeKey() {
        return SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY;
    }
}
