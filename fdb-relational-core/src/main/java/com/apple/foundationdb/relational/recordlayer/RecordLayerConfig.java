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

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.TransformedRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;

import com.google.protobuf.Message;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.zip.Deflater;

/**
 * Holder object for RecordLayer-specific stuff that isn't directly tied to an actual FDB StorageCluster.
 */
public final class RecordLayerConfig {
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final RecordSerializer<Message> serializer;
    private final int formatVersion;
    private final Map<String, IndexState> indexStateMap;

    private static final RecordSerializer<Message> DEFAULT_RELATIONAL_SERIALIZER = TransformedRecordSerializer.newDefaultBuilder()
            .setEncryptWhenSerializing(false)
            .setCompressWhenSerializing(true)
            .setCompressionLevel(Deflater.DEFAULT_COMPRESSION)
            .setWriteValidationRatio(0.0)
            .build();

    private RecordLayerConfig(RecordLayerConfigBuilder builder) {
        this.userVersionChecker = builder.userVersionChecker;
        this.serializer = builder.serializer;
        this.formatVersion = builder.formatVersion;
        this.indexStateMap = builder.indexStateMap;
    }

    public FDBRecordStoreBase.UserVersionChecker getUserVersionChecker() {
        return userVersionChecker;
    }

    public RecordSerializer<Message> getSerializer() {
        return serializer;
    }

    public int getFormatVersion() {
        return formatVersion;
    }

    public Map<String, IndexState> getIndexStateMap() {
        return indexStateMap;
    }

    public static RecordLayerConfig getDefault() {
        return new RecordLayerConfigBuilder().build();
    }

    public static class RecordLayerConfigBuilder {
        private FDBRecordStoreBase.UserVersionChecker userVersionChecker;
        private final RecordSerializer<Message> serializer;
        private int formatVersion;
        private Map<String, IndexState> indexStateMap;

        public RecordLayerConfigBuilder() {
            this.userVersionChecker = (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion);
            this.serializer = DEFAULT_RELATIONAL_SERIALIZER;
            this.formatVersion = FDBRecordStore.DEFAULT_FORMAT_VERSION;
            this.indexStateMap = Map.of();
        }

        public RecordLayerConfigBuilder setIndexStateMap(Map<String, IndexState> indexStateMap) {
            this.indexStateMap = indexStateMap;
            return this;
        }

        public RecordLayerConfigBuilder setFormatVersion(int formatVersion) {
            this.formatVersion = formatVersion;
            return this;
        }

        public RecordLayerConfigBuilder setUserVersionChecker(FDBRecordStoreBase.UserVersionChecker userVersionChecker) {
            this.userVersionChecker = userVersionChecker;
            return this;
        }

        public RecordLayerConfig build() {
            return new RecordLayerConfig(this);
        }
    }
}
