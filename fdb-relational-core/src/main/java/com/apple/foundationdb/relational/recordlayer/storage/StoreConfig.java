/*
 * StoreConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.storage;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.NoSuchDirectoryException;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Message;

import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public final class StoreConfig {
    private final RecordLayerConfig recordLayerConfig;
    private final String schemaName;
    private final KeySpacePath storePath;
    private final RecordMetaDataProvider metaDataProvider;

    private StoreConfig(RecordLayerConfig recordLayerConfig,
                        String schemaName,
                        KeySpacePath storePath,
                        RecordMetaDataProvider metaDataProvider) {
        this.recordLayerConfig = recordLayerConfig;
        this.schemaName = schemaName;
        this.storePath = storePath;
        this.metaDataProvider = metaDataProvider;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public KeySpacePath getStorePath() {
        return storePath;
    }

    public RecordMetaDataProvider getMetaDataProvider() {
        return metaDataProvider;
    }

    public RecordSerializer<Message> getSerializer() {
        return recordLayerConfig.getSerializer();
    }

    public FDBRecordStoreBase.UserVersionChecker getUserVersionChecker() {
        return recordLayerConfig.getUserVersionChecker();
    }

    public int getFormatVersion() {
        return recordLayerConfig.getFormatVersion();
    }

    public static StoreConfig create(RecordLayerConfig recordLayerConfig,
                                     String schemaName,
                                     RelationalKeyspaceProvider.RelationalDatabasePath databasePath,
                                     RecordMetaDataStore metaDataStore,
                                     Transaction transaction) throws RelationalException {
        //TODO(bfines) error handling if this store doesn't exist

        RelationalKeyspaceProvider.RelationalSchemaPath schemaPath;
        try {
            schemaPath = databasePath.schemaPath(schemaName);
        } catch (NoSuchDirectoryException nsde) {
            throw new RelationalException("Uninitialized Catalog", ErrorCode.INTERNAL_ERROR, nsde);
        } catch (MetaDataException mde) {
            throw new RelationalException(mde.getMessage(), ErrorCode.UNDEFINED_SCHEMA, mde);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }

        URI dbUri = databasePath.toUri();
        RecordMetaDataProvider metaDataProvider = metaDataStore.loadMetaData(transaction, dbUri, schemaName);

        return new StoreConfig(recordLayerConfig, schemaName, schemaPath, metaDataProvider);
    }
}
