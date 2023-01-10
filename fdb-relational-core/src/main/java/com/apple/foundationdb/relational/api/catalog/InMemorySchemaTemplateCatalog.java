/*
 * InMemorySchemaTemplateCatalog.java
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

import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.ValueTuple;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Quick and dirty in-memory implementation of a SchemaTemplate catalog, for use in testing.
 */
public class InMemorySchemaTemplateCatalog implements SchemaTemplateCatalog {
    private final ConcurrentMap<String, SchemaTemplate> backingStore = new ConcurrentHashMap<>();

    @Nonnull
    @Override
    public SchemaTemplate loadTemplate(@Nonnull Transaction txn, @Nonnull String templateId) throws RelationalException {
        SchemaTemplate template = backingStore.get(templateId);
        if (template == null) {
            throw new RelationalException("Unknown schema template <" + templateId + ">", ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
        return template;
    }

    @Override
    public void updateTemplate(@Nonnull Transaction txn, @Nonnull String templateId, @Nonnull SchemaTemplate newTemplate) throws RelationalException {
        boolean doContinue;
        do {
            SchemaTemplate old = backingStore.get(templateId);
            if (old != null) {
                doContinue = !backingStore.replace(templateId, old, newTemplate);
            } else {
                old = backingStore.putIfAbsent(templateId, newTemplate);
                doContinue = old != null;
            }
        } while (doContinue);
    }

    @Override
    public RelationalResultSet listTemplates(@Nonnull Transaction txn) {
        final Set<String> strings = backingStore.keySet();
        Iterator<Row> iter = strings.stream()
                .map(name -> (Row) new ValueTuple(name))
                .collect(Collectors.toList()).iterator();
        FieldDescription field = FieldDescription.primitive("TEMPLATE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls);
        return new IteratorResultSet(new RelationalStructMetaData(field), iter, 0);
    }

    @Override
    public void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateId) {
        backingStore.remove(templateId);
    }
}
