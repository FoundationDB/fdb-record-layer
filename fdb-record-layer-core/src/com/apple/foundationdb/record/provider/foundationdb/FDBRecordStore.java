/*
 * FDBRecordStore.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A multi-type record store.
 *
 * Uses Protobuf dynamic messages to process records.
 */
@API(API.Status.MAINTAINED)
public class FDBRecordStore extends FDBRecordStoreBase<Message> {

    protected FDBRecordStore(@Nonnull FDBRecordContext context,
                             @Nonnull SubspaceProvider subspaceProvider, // Can assume getting subspace from this
                             // provider is not blocking because of the way it is created by the builder.
                             int defaultOutputFormatVersion,
                             @Nonnull RecordMetaDataProvider metaDataProvider,
                             @Nonnull RecordSerializer<Message> serializer,
                             @Nonnull IndexMaintainerRegistry indexMaintainerRegistry,
                             @Nonnull IndexMaintenanceFilter indexMaintenanceFilter,
                             @Nonnull FDBRecordStoreBase.PipelineSizer pipelineSizer) {
        super(context, subspaceProvider, defaultOutputFormatVersion, metaDataProvider, serializer,
              indexMaintainerRegistry, indexMaintenanceFilter, pipelineSizer);
    }

    /**
     * A builder for {@link FDBRecordStore}.
     *
     * <pre><code>
     * FDBRecordStore.newBuilder().setMetaDataProvider(md).setContext(ctx).setSubspace(s).createOrOpen()
     * </code></pre>
     *
     */
    public static class Builder extends FDBRecordStoreBuilder<Message, FDBRecordStore> {
        protected Builder() {
            serializer = DynamicMessageRecordSerializer.instance();
        }

        protected Builder(Builder other) {
            super(other);
        }

        protected Builder(FDBRecordStore store) {
            super(store);
        }

        @Override
        @Nonnull
        public FDBRecordStore build() {
            return new FDBRecordStore(context, subspaceProvider, formatVersion, getMetaDataProviderForBuild(),
                    serializer, indexMaintainerRegistry, indexMaintenanceFilter, pipelineSizer);
        }

        @Override
        @Nonnull
        public Builder copyBuilder() {
            return new Builder(this);
        }

        // Following Overrides only necessary to narrow the return type.

        @Override
        @Nonnull
        public Builder setSerializer(@Nonnull RecordSerializer<Message> serializer) {
            super.setSerializer(serializer);
            return this;
        }

        @Override
        @Nonnull
        public Builder setFormatVersion(int formatVersion) {
            super.setFormatVersion(formatVersion);
            return this;
        }

        @Override
        @Nonnull
        public Builder setMetaDataProvider(@Nullable RecordMetaDataProvider metaDataProvider) {
            super.setMetaDataProvider(metaDataProvider);
            return this;
        }

        @Override
        @Nonnull
        public Builder setSubspaceProvider(@Nullable SubspaceProvider subspaceProvider) {
            this.subspaceProvider = subspaceProvider;
            return this;
        }

        @Override
        @Nonnull
        public Builder setMetaDataStore(@Nullable FDBMetaDataStore metaDataStore) {
            super.setMetaDataStore(metaDataStore);
            if (metaDataStore != null && context == null) {
                context = metaDataStore.getRecordContext();
            }
            return this;
        }

        @Override
        @Nonnull
        public Builder setContext(@Nullable FDBRecordContext context) {
            super.setContext(context);
            return this;
        }

        /**
         * It is recommended to use {@link #setKeySpacePath(KeySpacePath)} instead.
         */
        @Override
        @Nonnull
        @API(API.Status.UNSTABLE)
        public Builder setSubspace(@Nullable Subspace subspace) {
            super.setSubspace(subspace);
            return this;
        }

        @Override
        @Nonnull
        public Builder setKeySpacePath(@Nullable KeySpacePath path) {
            super.setKeySpacePath(path);
            return this;
        }

        @Override
        @Nonnull
        public Builder setUserVersionChecker(@Nullable FDBRecordStoreBase.UserVersionChecker userVersionChecker) {
            super.setUserVersionChecker(userVersionChecker);
            return this;
        }

        @Override
        @Nonnull
        public Builder setIndexMaintainerRegistry(@Nonnull IndexMaintainerRegistry indexMaintainerRegistry) {
            super.setIndexMaintainerRegistry(indexMaintainerRegistry);
            return this;
        }

        @Override
        @Nonnull
        public Builder setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
            super.setIndexMaintenanceFilter(indexMaintenanceFilter);
            return this;
        }

        @Override
        @Nonnull
        public Builder setPipelineSizer(@Nonnull FDBRecordStoreBase.PipelineSizer pipelineSizer) {
            super.setPipelineSizer(pipelineSizer);
            return this;
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    public Builder asBuilder() {
        return new Builder(this);
    }

}
