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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
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
import java.util.List;

public class RecordLayerCatalog implements Catalog {
    private static final int DEFAULT_FORMAT_VERSION = 8;
    //pluggable
    private final RecordMetaDataStore metadataProvider;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final SerializerRegistry serializerRegistry;
    private final KeySpace keySpace;

    private RecordLayerCatalog(RecordMetaDataStore metadataProvider, FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                               SerializerRegistry serializerRegistry, KeySpace keySpace) {
        this.metadataProvider = metadataProvider;
        this.userVersionChecker = userVersionChecker;
        this.serializerRegistry = serializerRegistry;
        this.keySpace = keySpace;
    }

    @Nonnull
    public SchemaTemplate getSchemaTemplate(@Nonnull String templateId) throws RelationalException {
        return new RecordLayerTemplate(templateId, metadataProvider);
    }

    @Nonnull
    public RelationalDatabase getDatabase(@Nonnull List<Object> url) throws RelationalException {
        final KeySpacePath keySpacePath = getFDBDatabaseAndKeySpacePath(url, keySpace).getRight();
        return new RecordLayerDatabase(metadataProvider, userVersionChecker,
                DEFAULT_FORMAT_VERSION, serializerRegistry, keySpacePath);
    }

    @VisibleForTesting
    public static Pair<FDBDatabase, KeySpacePath> getFDBDatabaseAndKeySpacePath(@Nonnull List<Object> databaseUrl, @Nonnull KeySpace keySpace) {
        final Tuple urlTuple = Tuple.fromList(databaseUrl);
        assert urlTuple.size() > 1 : "Invalid databaseUrl without enough elements";
        final Object clusterFileObject = urlTuple.get(0);
        assert clusterFileObject == null || clusterFileObject instanceof String : "Valid databaseUrl must have valid clusterFile String or null at beginning";
        final String clusterFile = (String) clusterFileObject;
        final FDBDatabase fdbDatabase = FDBDatabaseFactory.instance().getDatabase(clusterFile);
        final Tuple databaseTuple = TupleHelpers.subTuple(urlTuple, 1, urlTuple.size());
        final KeySpacePath keySpacePath = keySpace.resolveFromKey(fdbDatabase.openContext(), databaseTuple).toPath();
        return Pair.of(fdbDatabase, keySpacePath);
    }

    public static class Builder {
        private RecordMetaDataStore metadataProvider;
        private FDBRecordStoreBase.UserVersionChecker userVersionChecker;
        private SerializerRegistry serializerRegistry;
        private KeySpace keySpace;

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

        public Builder setKeySpace(@Nonnull KeySpace keySpace) {
            this.keySpace = keySpace;
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
            return new RecordLayerCatalog(metadataProvider, userVersionChecker, serializerRegistry, keySpace);
        }
    }
}
