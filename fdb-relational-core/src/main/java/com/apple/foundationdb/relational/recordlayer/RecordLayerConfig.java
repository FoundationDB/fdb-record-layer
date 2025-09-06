/*
 * RecordLayerConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Holder object for RecordLayer-specific stuff that isn't directly tied to an actual FDB StorageCluster.
 */
@API(API.Status.EXPERIMENTAL)
public final class RecordLayerConfig {
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final FormatVersion formatVersion;
    private final Map<String, IndexState> indexStateMap;

    private RecordLayerConfig(RecordLayerConfigBuilder builder) {
        this.userVersionChecker = builder.userVersionChecker;
        this.formatVersion = builder.formatVersion;
        this.indexStateMap = builder.indexStateMap;
    }

    public FDBRecordStoreBase.UserVersionChecker getUserVersionChecker() {
        return userVersionChecker;
    }

    public FormatVersion getFormatVersion() {
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
        private FormatVersion formatVersion;
        private Map<String, IndexState> indexStateMap;

        public RecordLayerConfigBuilder() {
            this.userVersionChecker = (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion);
            this.formatVersion = FormatVersion.getDefaultFormatVersion();
            this.indexStateMap = Map.of();
        }

        public RecordLayerConfigBuilder setIndexStateMap(Map<String, IndexState> indexStateMap) {
            this.indexStateMap = indexStateMap;
            return this;
        }

        @Deprecated(forRemoval = true)
        public RecordLayerConfigBuilder setFormatVersion(int formatVersion) {
            this.formatVersion = FormatVersion.getFormatVersion(formatVersion);
            return this;
        }

        public RecordLayerConfigBuilder setFormatVersion(FormatVersion formatVersion) {
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
