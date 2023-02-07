/*
 * RecordLayerConfig.java
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

import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;

import java.util.concurrent.CompletableFuture;

/**
 * Holder object for RecordLayer-specific stuff that isn't directly tied to an actual FDB StorageCluster.
 */
public class RecordLayerConfig {
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final SerializerRegistry serializerRegistry;
    private final int formatVersion;

    public RecordLayerConfig(FDBRecordStoreBase.UserVersionChecker userVersionChecker, SerializerRegistry serializerRegistry, int formatVersion) {
        this.userVersionChecker = userVersionChecker;
        this.serializerRegistry = serializerRegistry;
        this.formatVersion = formatVersion;
    }

    public FDBRecordStoreBase.UserVersionChecker getUserVersionChecker() {
        return userVersionChecker;
    }

    public SerializerRegistry getSerializerRegistry() {
        return serializerRegistry;
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public static RecordLayerConfig getDefault() {
        return new RecordLayerConfig(
                (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion),
                storePath -> DynamicMessageRecordSerializer.instance(),
                FDBRecordStore.DEFAULT_FORMAT_VERSION);
    }
}
