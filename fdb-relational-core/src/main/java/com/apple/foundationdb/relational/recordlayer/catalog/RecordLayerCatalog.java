/*
 * RecordLayerCatalog.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.RecordLayerDatabase;
import com.apple.foundationdb.relational.recordlayer.SerializerRegistry;

import javax.annotation.Nonnull;
import java.net.URI;

public class RecordLayerCatalog implements Catalog {
    private static final int DEFAULT_FORMAT_VERSION = 8;
    //pluggable
    private final DatabaseLocator databaseFinder;
    private final MutableRecordMetaDataStore metaDataStore;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final SerializerRegistry serializerRegistry;
    /**
     * The clients needs to make sure the passed in keySpace supports all the {@link KeySpacePath} that its databases will be built on potentially
     * The clients volunteers to have the directories for schemas included in this keySpace. Dynamically creation of new schemas is also supported.
     */
    private final KeySpace keySpace;
    private final int formatVersion;


    private RecordLayerCatalog(DatabaseLocator locator,
                               MutableRecordMetaDataStore metaDataStore,
                               FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                               SerializerRegistry serializerRegistry,
                               KeySpace keySpace,
                               int formatVersion) {
        this.databaseFinder = locator;
        this.metaDataStore = metaDataStore;
        this.userVersionChecker = userVersionChecker;
        this.serializerRegistry = serializerRegistry;
        this.keySpace = keySpace;
        this.formatVersion = formatVersion;
    }

    @Override
    public RelationalResultSet listDatabases(@Nonnull Transaction txn) throws RelationalException {
        return new SystemDatabaseResultSet(txn,new DirectoryScannable(keySpace), QueryProperties.DEFAULT,false);
    }

    @Nonnull
    public SchemaTemplate getSchemaTemplate(@Nonnull String templateId) throws RelationalException {
        return metaDataStore.loadTemplate(templateId);
    }

    @Nonnull
    public RelationalDatabase getDatabase(@Nonnull URI dbUrl) throws RelationalException {
        KeySpacePath dbPath = KeySpaceUtils.uriToPath(dbUrl, keySpace);
        final FDBDatabase fdbDatabase = databaseFinder.locateDatabase(dbPath);
        return new RecordLayerDatabase(fdbDatabase, metaDataStore, userVersionChecker,
                formatVersion, serializerRegistry, dbPath, this);
    }

    @Override
    public void deleteDatabase(@Nonnull URI dbUrl) throws RelationalException {
        throw new OperationUnsupportedException("Unimplemented");
    }

    public KeySpace getKeySpace(){
        return keySpace;
    }

    public KeySpace extendKeySpaceForSchema(@Nonnull KeySpacePath dbPath, @Nonnull String schemaId) {
        return KeySpaceUtils.extendKeySpaceForSchema(keySpace, dbPath, schemaId);
    }

    public static class Builder {
        private DatabaseLocator databaseFinder;
        private MutableRecordMetaDataStore metadataProvider;
        private FDBRecordStoreBase.UserVersionChecker userVersionChecker;
        private SerializerRegistry serializerRegistry;
        private KeySpace keySpace;
        private int formatVersion;

        public Builder setMetadataProvider(@Nonnull MutableRecordMetaDataStore metadataProvider) {
            this.metadataProvider = metadataProvider;
            return this;
        }

        public Builder setUserVersionChecker(@Nonnull FDBRecordStoreBase.UserVersionChecker userVersionChecker) {
            this.userVersionChecker = userVersionChecker;
            return this;
        }

        public Builder setSerializerRegistry(@Nonnull SerializerRegistry serializerRegistry) {
            this.serializerRegistry = serializerRegistry;
            return this;
        }

        public Builder setDatabaseLocator(@Nonnull DatabaseLocator locator) {
            this.databaseFinder = locator;
            return this;
        }

        public Builder setKeySpace(@Nonnull KeySpace keySpace) {
            this.keySpace = keySpace;
            return this;
        }

        public Builder setFormatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        public RecordLayerCatalog build() throws RelationalException {
            if (metadataProvider == null) {
                throw new RelationalException("Metadata provider not supplied", RelationalException.ErrorCode.INVALID_PARAMETER);
            }
            if (serializerRegistry == null) {
                throw new RelationalException("Serializer registry not supplied", RelationalException.ErrorCode.INVALID_PARAMETER);
            }
            if (keySpace == null) {
                throw new RelationalException("Key space not supplied", RelationalException.ErrorCode.INVALID_PARAMETER);
            }
            if (formatVersion <= 0) {
                formatVersion = DEFAULT_FORMAT_VERSION;
            }
            return new RecordLayerCatalog(databaseFinder, metadataProvider, userVersionChecker,
                    serializerRegistry, keySpace, formatVersion);
        }
    }
}
