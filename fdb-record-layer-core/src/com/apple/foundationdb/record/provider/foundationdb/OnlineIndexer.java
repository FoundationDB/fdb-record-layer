/*
 * OnlineIndexer.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

/**
 * Builds an index online, i.e., concurrently with other database operations. In order to minimize
 * the impact that these operations have with other operations, this attempts to minimize the
 * priorities of its transactions. Additionally, it attempts to limit the amount of work it will
 * done in a fashion that will decrease as the number of failures for a given build attempt increases.
 *
 * <p>
 * As ranges of elements are rebuilt, the fact that the range has rebuilt is added to a {@link com.apple.foundationdb.async.RangeSet}
 * associated with the index being built. This {@link com.apple.foundationdb.async.RangeSet} is used to (a) coordinate work between
 * different builders that might be running on different machines to ensure that the same work isn't
 * duplicated and to (b) make sure that non-idempotent indexes (like <code>COUNT</code> or <code>SUM_LONG</code>)
 * don't update themselves (or fail to update themselves) incorrectly.
 * </p>
 *
 * <p>
 * Unlike many other features in the Record Layer core, this has a retry loop.
 * </p>
 *
 * <p>Build an index immediately in the current transaction:</p>
 * <pre><code>
 * try (OnlineIndexer indexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "newIndex")) {
 *     indexBuilder.rebuildIndex(recordStore);
 * }
 * </code></pre>
 *
 * <p>Build an index synchronously in the multiple transactions:</p>
 * <pre><code>
 * try (OnlineIndexer indexBuilder = OnlineIndexer.forRecordStoreAndIndex(recordStore, "newIndex")) {
 *     indexBuilder.buildIndex();
 * }
 * </code></pre>
 */
@API(API.Status.MAINTAINED)
public class OnlineIndexer extends OnlineIndexerBase<Message> {
    protected OnlineIndexer(@Nonnull FDBDatabaseRunner runner,
                            @Nonnull FDBRecordStoreBuilder<Message, ? extends FDBRecordStoreBase<Message>> recordStoreBuilder,
                            @Nonnull Index index, @Nonnull Collection<RecordType> recordTypes,
                            int limit, int maxRetries, int recordsPerSecond) {
        super(runner, recordStoreBuilder, index, recordTypes, limit, maxRetries, recordsPerSecond);
    }

    /**
     * Builder for {@link OnlineIndexer}.
     *
     * <pre><code>
     * OnlineIndexer.newBuilder().setRecordStoreBuilder(recordStoreBuilder).setIndex(index).build()
     * </code></pre>
     *
     * <pre><code>
     * OnlineIndexer.newBuilder().setDatabase(fdb).setMetaData(metaData).setSubspace(subspace).setIndex(index).build()
     * </code></pre>
     *
     */
    @API(API.Status.MAINTAINED)
    public static class Builder extends OnlineIndexerBaseBuilder<Message> {
        protected Builder() {
            super();
        }

        // Following require being able to make a new FDBRecordStoreBuilder

        /**
         * Set the meta-data to use when indexing.
         * @param metaDataProvider meta-data to use
         * @return this builder
         */
        public Builder setMetaData(@Nonnull RecordMetaDataProvider metaDataProvider) {
            if (recordStoreBuilder == null) {
                recordStoreBuilder = FDBRecordStore.newBuilder();
            }
            recordStoreBuilder.setMetaDataProvider(metaDataProvider);
            return this;
        }

        /**
         * Set the subspace of the record store in which to build the index.
         * @param subspaceProvider subspace to use
         * @return this builder
         */
        public Builder setSubspaceProvider(@Nonnull SubspaceProvider subspaceProvider) {
            if (recordStoreBuilder == null) {
                recordStoreBuilder = FDBRecordStore.newBuilder();
            }
            recordStoreBuilder.setSubspaceProvider(subspaceProvider);
            return this;
        }

        /**
         * Set the subspace of the record store in which to build the index.
         * @param subspace subspace to use
         * @return this builder
         */
        public Builder setSubspace(@Nonnull Subspace subspace) {
            if (recordStoreBuilder == null) {
                recordStoreBuilder = FDBRecordStore.newBuilder();
            }
            recordStoreBuilder.setSubspace(subspace);
            return this;
        }

        // Following Overrides only necessary to narrow the return type.

        @Override
        public Builder setRunner(@Nullable FDBDatabaseRunner runner) {
            super.setRunner(runner);
            return this;
        }

        @Override
        public Builder setDatabase(@Nonnull FDBDatabase database) {
            super.setDatabase(database);
            return this;
        }

        @Override
        public Builder setRecordStoreBuilder(@Nonnull FDBRecordStoreBuilder<Message, ? extends FDBRecordStoreBase<Message>> recordStoreBuilder) {
            super.setRecordStoreBuilder(recordStoreBuilder);
            return this;
        }

        @Override
        public Builder setRecordStore(@Nonnull FDBRecordStoreBase<Message> recordStore) {
            super.setRecordStore(recordStore);
            return this;
        }

        @Override
        public Builder setIndex(@Nullable Index index) {
            super.setIndex(index);
            return this;
        }

        @Override
        public Builder setIndex(@Nonnull String indexName) {
            super.setIndex(indexName);
            return this;
        }

        @Override
        public Builder setRecordTypes(@Nullable Collection<RecordType> recordTypes) {
            super.setRecordTypes(recordTypes);
            return this;
        }

        @Override
        public Builder setLimit(int limit) {
            super.setLimit(limit);
            return this;
        }

        @Override
        public Builder setMaxRetries(int maxRetries) {
            super.setMaxRetries(maxRetries);
            return this;
        }

        @Override
        public Builder setRecordsPerSecond(int recordsPerSecond) {
            super.setRecordsPerSecond(recordsPerSecond);
            return this;
        }

        @Override
        public Builder setTimer(@Nullable FDBStoreTimer timer) {
            super.setTimer(timer);
            return this;
        }

        @Override
        public Builder setMdcContext(@Nullable Map<String, String> mdcContext) {
            super.setMdcContext(mdcContext);
            return this;
        }

        @Override
        public Builder setMaxAttempts(int maxAttempts) {
            super.setMaxAttempts(maxAttempts);
            return this;
        }

        @Override
        public Builder setMaxDelayMillis(long maxDelayMillis) {
            super.setMaxDelayMillis(maxDelayMillis);
            return this;
        }

        @Override
        public Builder setInitialDelayMillis(long initialDelayMillis) {
            super.setInitialDelayMillis(initialDelayMillis);
            return this;
        }

        @Override
        public Builder setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
            super.setIndexMaintenanceFilter(indexMaintenanceFilter);
            return this;
        }

        @Override
        public Builder setSerializer(@Nonnull RecordSerializer<Message> serializer) {
            super.setSerializer(serializer);
            return this;
        }

        @Override
        public Builder setFormatVersion(int formatVersion) {
            super.setFormatVersion(formatVersion);
            return this;
        }

        @Override
        public OnlineIndexer build() {
            validate();
            return new OnlineIndexer(runner, recordStoreBuilder, index, recordTypes, limit, maxRetries, recordsPerSecond);
        }
    }

    /**
     * Create an online indexer builder.
     * @return a new online indexer builder
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create an online indexer for the given record store and index.
     * @param recordStore record store in which to index
     * @param index name of index to build
     * @return a new online indexer
     */
    public static OnlineIndexer forRecordStoreAndIndex(@Nonnull FDBRecordStoreBase<Message> recordStore, @Nonnull String index) {
        return newBuilder().setRecordStore(recordStore).setIndex(index).build();
    }

}
