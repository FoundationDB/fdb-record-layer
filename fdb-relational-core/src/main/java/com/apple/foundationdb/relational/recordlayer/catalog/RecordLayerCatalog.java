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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.recordlayer.RecordLayerDatabase;
import com.apple.foundationdb.relational.recordlayer.RecordLayerTemplate;
import com.apple.foundationdb.relational.recordlayer.SerializerRegistry;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RecordLayerCatalog implements Catalog {
    private static final int DEFAULT_FORMAT_VERSION = 8;
    //pluggable
    private final DatabaseLocator databaseLocator;
    private final RecordMetaDataStore metadataProvider;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final SerializerRegistry serializerRegistry;
    private final KeySpace keySpace;
    private final int formatVersion;
    private final ExistenceCheckerForStore existenceCheckerForStore;

    private RecordLayerCatalog(DatabaseLocator locator,
                               RecordMetaDataStore metadataProvider,
                               FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                               SerializerRegistry serializerRegistry,
                               KeySpace keySpace,
                               int formatVersion,
                               ExistenceCheckerForStore existenceCheckerForStore) {
        this.databaseLocator = locator;
        this.metadataProvider = metadataProvider;
        this.userVersionChecker = userVersionChecker;
        this.serializerRegistry = serializerRegistry;
        this.keySpace = keySpace;
        this.formatVersion = formatVersion;
        this.existenceCheckerForStore = existenceCheckerForStore;
    }

    @Nonnull
    public SchemaTemplate getSchemaTemplate(@Nonnull String templateId) throws RelationalException {
        return new RecordLayerTemplate(templateId, metadataProvider.loadMetaData(templateId));
    }

    @Nonnull
    public RelationalDatabase getDatabase(@Nonnull List<Object> url) throws RelationalException {
        final Pair<FDBDatabase, KeySpacePath> dbAndKeySpace = getFDBDatabaseAndKeySpacePath(url, keySpace);
        final KeySpacePath keySpacePath = dbAndKeySpace.getRight();
        return new RecordLayerDatabase(dbAndKeySpace.getLeft(),metadataProvider, userVersionChecker,
                formatVersion, serializerRegistry, keySpacePath, existenceCheckerForStore);
    }

    @VisibleForTesting
    public Pair<FDBDatabase, KeySpacePath> getFDBDatabaseAndKeySpacePath(@Nonnull List<Object> databaseUrl, @Nonnull KeySpace keySpace) {
        FDBDatabase fdbDatabase = databaseLocator.locateDatabase(databaseUrl);
        final Tuple urlTuple = Tuple.fromList(databaseUrl);
        assert urlTuple.size() > 1 : "Invalid databaseUrl without enough elements";
        final Tuple databaseTuple = TupleHelpers.subTuple(urlTuple, 1, urlTuple.size());
        final KeySpacePath keySpacePath = keySpace.resolveFromKey(fdbDatabase.openContext(), databaseTuple).toPath();
        return Pair.of(fdbDatabase, keySpacePath);
    }

    public void createSchema(KeySpacePath schemaPath, @Nonnull String schemaTemplateUri, Transaction transaction) {
//        KeySpacePath schemaPath = KeySpaceUtils.uriToPath(schemaUri,keySpace);
        FDBRecordContext ctx = transaction.unwrap(FDBRecordContext.class);
        FDBRecordStore.newBuilder()
                .setKeySpacePath(schemaPath)
                .setSerializer(serializerRegistry.loadSerializer(schemaPath))
                .setMetaDataProvider(metadataProvider.loadMetaData(schemaTemplateUri))
                .setUserVersionChecker(userVersionChecker)
                .setFormatVersion(formatVersion)
                .setContext(ctx)
                .createOrOpen(existenceCheckerForStore.forStore(schemaTemplateUri));
    }

    public static class Builder {
        private DatabaseLocator databaseLocator;
        private RecordMetaDataStore metadataProvider;
        private FDBRecordStoreBase.UserVersionChecker userVersionChecker;
        private SerializerRegistry serializerRegistry;
        private KeySpace keySpace;
        private int formatVersion;
        private ExistenceCheckerForStore existenceCheckerForStore;

        public Builder setMetadataProvider(@Nonnull RecordMetaDataStore metadataProvider) {
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
            this.databaseLocator = locator;
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

        public Builder setExistenceCheckerForStore(ExistenceCheckerForStore existenceCheckerForStore) {
            this.existenceCheckerForStore = existenceCheckerForStore;
            return this;
        }

        public RecordLayerCatalog build() {
            if (metadataProvider == null) {
                throw new IllegalStateException("RecordLayerCatalog must have its metadataProvider");
            }
            if (serializerRegistry == null) {
                throw new IllegalStateException("RecordLayerCatalog must have its serializerRegistry");
            }
            if (keySpace == null) {
                throw new IllegalStateException("RecordLayerCatalog must have its keySpace");
            }
            if (formatVersion <= 0) {
                formatVersion = DEFAULT_FORMAT_VERSION;
            }
            if (existenceCheckerForStore == null) {
                existenceCheckerForStore = storeName -> FDBRecordStoreBase.StoreExistenceCheck.NONE;
            }
            return new RecordLayerCatalog(databaseLocator,metadataProvider, userVersionChecker,
                    serializerRegistry, keySpace, formatVersion, existenceCheckerForStore);
        }
    }
}
