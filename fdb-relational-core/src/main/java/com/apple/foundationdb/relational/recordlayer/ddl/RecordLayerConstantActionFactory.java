/*
 * RecordLayerConstantActionFactory.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.SerializerRegistry;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;

import javax.annotation.Nonnull;
import java.net.URI;

public class RecordLayerConstantActionFactory implements ConstantActionFactory {
    public static final int DEFAULT_FORMAT_VERSION = 8;

    private final MutableRecordMetaDataStore metaDataStore;
    private final SerializerRegistry serializerRegistry;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final int formatVersion;
    private final KeySpace baseKeySpace;

    public RecordLayerConstantActionFactory(MutableRecordMetaDataStore metaDataStore,
                                            SerializerRegistry serializerRegistry,
                                            FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                                            int formatVersion,
                                            KeySpace baseKeySpace) {
        this.metaDataStore = metaDataStore;
        this.serializerRegistry = serializerRegistry;
        this.userVersionChecker = userVersionChecker;
        this.formatVersion = formatVersion;
        this.baseKeySpace = baseKeySpace;
    }


    @Nonnull
    @Override
    public ConstantAction getCreateSchemaTemplateConstantAction(@Nonnull SchemaTemplate template, @Nonnull Options templateProperties) {
        return new CreateSchemaTemplateConstantAction(template, metaDataStore);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateDatabaseConstantAction(@Nonnull URI dbPath, @Nonnull DatabaseTemplate template, @Nonnull Options constantActionOptions) {
        return new CreateDatabaseConstantAction(dbPath, template, constantActionOptions, this);
    }

    @Nonnull
    @Override
    public ConstantAction getCreateSchemaConstantAction(@Nonnull URI schemaUrl, @Nonnull String templateId, Options constantActionOptions) {
        return new CreateSchemaConstantAction(schemaUrl, templateId, baseKeySpace,
                metaDataStore, serializerRegistry, userVersionChecker, formatVersion);
    }

    @Nonnull
    @Override
    public ConstantAction getDeleteDatabaseContantAction(@Nonnull URI dbUrl, @Nonnull Options options) {
        return new DropDatabaseConstantAction(dbUrl,baseKeySpace,options,this);
    }

    @Nonnull
    @Override
    public ConstantAction getDropSchemaConstantAction(@Nonnull URI schemaUrl,@Nonnull Options options) {
        return new DropSchemaConstantAction(schemaUrl,baseKeySpace,metaDataStore,options);
    }

    public static class Builder {
        private MutableRecordMetaDataStore metaDataStore;
        private SerializerRegistry serializerRegistry;
        private FDBRecordStoreBase.UserVersionChecker userVersionChecker;
        private int formatVersion = DEFAULT_FORMAT_VERSION;
        private KeySpace baseKeySpace;

        public Builder setMetaDataStore(MutableRecordMetaDataStore metaDataStore) {
            this.metaDataStore = metaDataStore;
            return this;
        }

        public Builder setSerializerRegistry(SerializerRegistry serializerRegistry) {
            this.serializerRegistry = serializerRegistry;
            return this;
        }

        public Builder setUserVersionChecker(FDBRecordStoreBase.UserVersionChecker userVersionChecker) {
            this.userVersionChecker = userVersionChecker;
            return this;
        }

        public Builder setFormatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        public Builder setBaseKeySpace(KeySpace baseKeySpace) {
            this.baseKeySpace = baseKeySpace;
            return this;
        }


        public RecordLayerConstantActionFactory build() {
            return new RecordLayerConstantActionFactory(metaDataStore, serializerRegistry, userVersionChecker, formatVersion, baseKeySpace);
        }
    }
}
