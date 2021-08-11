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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapRecordMetaDataStore implements MutableRecordMetaDataStore {
    private final ConcurrentMap<URI, RecordMetaDataProvider> metadataMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<URI, RecordMetaDataProvider> schemaMap = new ConcurrentHashMap<>();

    @Override
    public RecordMetaDataProvider loadSchemaMetaData(@Nonnull URI dbUrl, @Nonnull String schemaId) {
        //TODO(bfines) if we ever use non-path elements of the URI, this will need to change
        URI upperCase = URI.create((KeySpaceUtils.getPath(dbUrl) + "/" + schemaId).toUpperCase(Locale.ROOT));
        return schemaMap.get(upperCase);
    }

    @Override
    public void setSchemaTemplateMetaData(@Nonnull URI schemaUuid, RecordMetaDataProvider storeMeta) {
        this.metadataMap.put(schemaUuid,storeMeta);
    }

    @Override
    public void createSchemaMetaData(@Nonnull URI dbUrl, @Nonnull String schemaId, @Nonnull URI templateUri) {
        RecordMetaDataProvider provider = loadTemplateMetaData(templateUri);
        if(provider==null){
            throw new RelationalException("No Schema Template at path <"+templateUri+"> found", RelationalException.ErrorCode.UNKNOWN_SCHEMA_TEMPLATE);
        }
        URI schemaKey =URI.create((KeySpaceUtils.getPath(dbUrl) + "/" + schemaId).toUpperCase(Locale.ROOT));
        schemaMap.put(schemaKey,provider);
    }

    @Override
    public RecordMetaDataProvider loadTemplateMetaData(@Nonnull URI templateUri) {
        return metadataMap.get(templateUri);
    }
}
