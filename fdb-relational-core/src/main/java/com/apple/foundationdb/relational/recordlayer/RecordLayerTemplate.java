/*
 * RecordLayerTemplate.java
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
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordMetaDataStore;

import javax.annotation.Nonnull;

public class RecordLayerTemplate implements SchemaTemplate, RecordMetaDataStore {
    private final String name;

    private final RecordMetaDataStore metaDataProvider;

    public RecordLayerTemplate(String name, RecordMetaDataStore metaDataProvider) {
        this.name = name;
        this.metaDataProvider = metaDataProvider;
    }

    @Override
    public String getUniqueName() {
        return name;
    }

    @Override
    public boolean isValid(@Nonnull DatabaseSchema schema) {
        int version = metaDataProvider.loadMetaData(schema.getSchemaName()).getRecordMetaData().getVersion();
        return schema.getSchemaVersion() >= version;

        //TODO(bfines) validate that all the tables and indexes match what is expected
    }

    @Override
    public RecordMetaDataProvider loadMetaData(String storeUuid) {
        return metaDataProvider.loadMetaData(storeUuid);
    }
}
