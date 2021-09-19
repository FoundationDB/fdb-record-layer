/*
 * MapRecordMetaDataStore.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapRecordMetaDataStore implements MutableRecordMetaDataStore {
    private final ConcurrentMap<String, RecordLayerTemplate> templateMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<URI, String> schemaToTemplateMap = new ConcurrentHashMap<>();

    @Override
    public RecordLayerTemplate loadMetaData(@Nonnull URI schemaUrl) {
        //TODO(bfines) if we ever use non-path elements of the URI, this will need to change
        URI schemaKey = convertToUpperCase(schemaUrl);
        String templateId = schemaToTemplateMap.get(schemaKey);
        if (templateId == null) {
            throw new RelationalException("No schema template found for schema: <" + schemaUrl + ">",
                    RelationalException.ErrorCode.UNKNOWN_SCHEMA);
        }
        return templateMap.get(templateId);
    }

    @Override
    public void addSchemaTemplate(@Nonnull RecordLayerTemplate schemaTemplate) {
        this.templateMap.put(schemaTemplate.getUniqueId(), schemaTemplate);
    }

    @Override
    public void assignSchemaToTemplate(@Nonnull URI schemaUrl, @Nonnull String templateId) {
        RecordLayerTemplate template = loadTemplate(templateId);
        if(template == null){
            throw new RelationalException("Unknown or non-existing schema template: <" + templateId + ">",
                    RelationalException.ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
        URI schemaKey = convertToUpperCase(schemaUrl);
        if (schemaToTemplateMap.containsKey(schemaKey)) {
            throw new RelationalException("One schema cannot be assigned with a template twice, schemaKey: <" + schemaKey
                    + ">, existingTemplateId: <" + schemaToTemplateMap.get(schemaKey)
                    + ">, new added templateId: <" + templateId + ">",
                    RelationalException.ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
        String existingTemplateId = schemaToTemplateMap.putIfAbsent(schemaKey, templateId);
        if (existingTemplateId != null && !existingTemplateId.equals(templateId)) {
            throw new RelationalException("One schema cannot be assigned with a template twice, schemaKey: <" + schemaKey
                    + ">, existingTemplateId: <" + existingTemplateId
                    + ">, new added templateId: <" + templateId + ">",
                    RelationalException.ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
    }

    @Override
    public void removeSchemaMapping(@Nonnull URI schemaUrl) {
        URI schemaKey = convertToUpperCase(schemaUrl);
        //TODO(bfines) this isn't perfectly safe, but we're going to have lots of concurrency problems with
        // this anyway, so not going to worry about it right now.
        schemaToTemplateMap.remove(schemaKey);
    }

    @Override
    public RecordLayerTemplate loadTemplate(@Nonnull String templateId) {
        return templateMap.get(templateId);
    }

    private static URI convertToUpperCase(@Nonnull URI schemaUrl) {
        return URI.create(KeySpaceUtils.getPath(schemaUrl).toUpperCase(Locale.ROOT));
    }
}
