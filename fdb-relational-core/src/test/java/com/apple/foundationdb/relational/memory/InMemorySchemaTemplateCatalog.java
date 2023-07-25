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

package com.apple.foundationdb.relational.memory;

import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.ValueTuple;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

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
 * We used to use this class as general schema template store but it has been replaced by
 * a persisting store. This class was then moved here into this package and under test as
 * it used by the adjacent {@link com.apple.foundationdb.relational.memory.InMemoryCatalog}
 */
class InMemorySchemaTemplateCatalog implements SchemaTemplateCatalog {
    private final ConcurrentMap<String, ConcurrentMap<Integer, SchemaTemplate>> backingStore = new ConcurrentHashMap<>();

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException {
        try {
            loadSchemaTemplate(txn, templateName);
            return true;
        } catch (RelationalException ex) {
            if (ex.getErrorCode() == ErrorCode.UNKNOWN_SCHEMA_TEMPLATE) {
                return false;
            }
            throw ex;
        }
    }

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName, int version) throws RelationalException {
        try {
            loadSchemaTemplate(txn, templateName, version);
            return true;
        } catch (RelationalException ex) {
            if (ex.getErrorCode() == ErrorCode.UNKNOWN_SCHEMA_TEMPLATE) {
                return false;
            }
            throw ex;
        }
    }

    @Nonnull
    @Override
    public SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException {
        final var versions = backingStore.get(templateName);
        if (versions == null) {
            throw new RelationalException(String.format("Unknown schema template with name %s", templateName), ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
        final var template = versions.entrySet().stream().max((a, b) -> (int) (a.getKey() - b.getKey()));
        if (template.isEmpty()) {
            throw new RelationalException(String.format("Unknown schema template with name %s", templateName), ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
        return template.get().getValue();
    }

    @Nonnull
    @Override
    public SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName, int version) throws RelationalException {
        final var versions = backingStore.get(templateName);
        if (versions == null) {
            throw new RelationalException(String.format("Unknown schema template with name %s and version %d", templateName, version), ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
        final var template = versions.get(version);
        if (template == null) {
            throw new RelationalException(String.format("Unknown schema template with name %s and version %d", templateName, version), ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
        return template;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public void createTemplate(@Nonnull Transaction txn, @Nonnull SchemaTemplate newTemplate) throws RelationalException {
        var versions = backingStore.get(newTemplate.getName());
        if (versions == null) {
            backingStore.putIfAbsent(newTemplate.getName(), new ConcurrentHashMap<>());
            createTemplate(txn, newTemplate);
            return;
        }
        versions = backingStore.get(newTemplate.getName());
        var oldTemplate = versions.get(newTemplate.getVersion());
        if (oldTemplate == null) {
            oldTemplate = versions.putIfAbsent(newTemplate.getVersion(), newTemplate);
            if (oldTemplate != null) {
                createTemplate(txn, newTemplate);
            }
        } else {
            final var replaced = versions.replace(newTemplate.getVersion(), oldTemplate, newTemplate);
            if (!replaced) {
                createTemplate(txn, newTemplate);
            }
        }
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
    public void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateName, boolean throwIfDoesNotExist) throws RelationalException {
        if (backingStore.remove(templateName) == null && throwIfDoesNotExist) {
            throw new RelationalException("Cannot delete unknown schema template " + templateName, ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
    }

    @Override
    public void deleteTemplate(@Nonnull Transaction txn, @Nonnull String templateId, int version, boolean throwIfDoesNotExist) throws RelationalException {
        if (doesSchemaTemplateExist(txn, templateId, version)) {
            backingStore.get(templateId).remove(version);
        } else if (throwIfDoesNotExist) {
            throw new RelationalException("Cannot delete unknown schema template " + templateId, ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
    }
}
