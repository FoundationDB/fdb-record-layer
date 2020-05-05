/*
 * CentralStore.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.centralstore;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.MetaDataProtoEditor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * TODO: CentralStore
 */
public class CentralStore {
    @Nonnull
    private static final CentralStore INSTANCE = new CentralStore();

    @Nullable
    private CentralStoreConfiguration config;
    @Nullable
    private RecordMetaData cachedMetaData;

    @Nonnull
    public static CentralStore instance() {
        if (!isConfigured()) {
            throw new CentralStoreNotSetUpException("CentralStore is not configured");
        }
        return INSTANCE;
    }

    private CentralStore() {}

    public static CentralStoreConfiguration newConfiguration() {
        return new CentralStoreConfiguration();
    }

    static void configure(@Nonnull CentralStoreConfiguration config) {
        INSTANCE.config = config;
    }

    private static boolean isConfigured() {
        return INSTANCE.config != null;
    }

    public static <T> T runIfConfigured(@Nonnull FDBRecordContext context,
                                        @Nonnull Function<? super FDBRecordStore, ? extends T> runnable) {
        if (isConfigured()) {
            return runnable.apply(instance().createOrOpen(context));
        } else {
            return null;
        }
    }

    @Nonnull
    public FDBRecordStore createOrOpen(FDBRecordContext context) {
        return FDBRecordStore.newBuilder()
                .setMetaDataProvider(getMetaData())
                .setContext(context)
                .setKeySpacePath(config.storePath)
                .createOrOpen();
    }

    @Nonnull
    private RecordMetaData getMetaData() {
        if (cachedMetaData == null || cachedMetaData.getVersion() != config.metadataVersion) {
            cachedMetaData = buildMetaData();
        }
        return cachedMetaData;
    }

    @Nonnull
    private RecordMetaData buildMetaData() {
        RecordMetaDataProto.MetaData.Builder protoBuilder = RecordMetaDataProto.MetaData.newBuilder();
        config.types.forEach(type ->
                MetaDataProtoEditor.addRecordType(protoBuilder, type.getTypeDescriptor(),
                        // Primary key.
                        Key.Expressions.concat(Key.Expressions.recordType(), type.getPrimaryFields()))
        );
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(protoBuilder.build(), true);
        config.types.forEach(type ->
                type.getIndexes().forEach(
                        index -> builder.addIndex(type.getTypeDescriptor().getName(), index)
                )
        );
        // The original version comes from how many times MetaDataProtoEditor.addRecordType was called. It
        // should be overwritten by configured version.
        builder.setVersion(config.metadataVersion);
        return builder.build();
    }
}
