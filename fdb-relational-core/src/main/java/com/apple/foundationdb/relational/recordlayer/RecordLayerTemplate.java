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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import javax.annotation.Nonnull;

public class RecordLayerTemplate implements SchemaTemplate, RecordMetaDataProvider {
    private final String templateId;

    /*
     * We use a RecordMetaDataProvider here instead of a RecordMetaDataStore because
     * we have explicitly performed the lookup to find the metadata which describes the template.
     */
    private final RecordMetaDataProvider metaDataProvider;

    public RecordLayerTemplate(String templateId, RecordMetaDataProvider metaDataProvider) {
        this.templateId = templateId;
        this.metaDataProvider = metaDataProvider;
    }

    @Override
    public String getUniqueId() {
        return templateId;
    }

    @Override
    public boolean isValid(@Nonnull DatabaseSchema schema) throws RelationalException {
        try {
            int version = getRecordMetaData().getVersion();
            //TODO(bfines) validate that all the tables and indexes match what is expected
            return schema.getSchemaVersion() >= version;
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() throws RecordCoreException {
        return metaDataProvider.getRecordMetaData();
    }
}
