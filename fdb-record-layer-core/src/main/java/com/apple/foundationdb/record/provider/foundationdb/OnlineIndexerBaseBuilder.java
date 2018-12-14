/*
 * OnlineIndexerBaseBuilder.java 
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;

/**
 * A builder for {@link OnlineIndexerBase}.
 * @param <M> type of message
 */
@API(API.Status.UNSTABLE)
public class OnlineIndexerBaseBuilder<M extends Message> {
    @Nullable
    protected FDBDatabaseRunner runner;
    @Nullable
    protected FDBRecordStoreBuilder<M, ? extends FDBRecordStoreBase<M>> recordStoreBuilder;
    @Nullable
    protected Index index;
    @Nullable
    protected Collection<RecordType> recordTypes;

    protected int limit = OnlineIndexerBase.DEFAULT_LIMIT;
    protected int maxRetries = OnlineIndexerBase.DEFAULT_MAX_RETRIES;
    protected int recordsPerSecond = OnlineIndexerBase.DEFAULT_RECORDS_PER_SECOND;

    protected OnlineIndexerBaseBuilder() {
    }

    /**
     * Get the runner that will be used to call into the database.
     * @return the runner that connects to the target database
     */
    @Nullable
    public FDBDatabaseRunner getRunner() {
        return runner;
    }

    /**
     * Set the runner that will be used to call into the database.
     *
     * Normally the runner is gotten from {@link #setDatabase} or {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param runner the runner that connects to the target database
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setRunner(@Nullable FDBDatabaseRunner runner) {
        this.runner = runner;
        return this;
    }

    /**
     * Set the database in which to run the indexing.
     *
     * Normally the database is gotten from {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param database the target database
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setDatabase(@Nonnull FDBDatabase database) {
        this.runner = database.newRunner();
        return this;
    }

    /**
     * Get the record store builder that will be used to open record store instances for indexing.
     * @return the record store builder
     */
    @Nullable
    @SuppressWarnings("squid:S1452")
    public FDBRecordStoreBuilder<M, ? extends FDBRecordStoreBase<M>> getRecordStoreBuilder() {
        return recordStoreBuilder;
    }

    /**
     * Set the record store builder that will be used to open record store instances for indexing.
     * @param recordStoreBuilder the record store builder
     * @return this builder
     * @see #setRecordStore
     */
    public OnlineIndexerBaseBuilder<M> setRecordStoreBuilder(@Nonnull FDBRecordStoreBuilder<M, ? extends FDBRecordStoreBase<M>> recordStoreBuilder) {
        this.recordStoreBuilder = recordStoreBuilder.copyBuilder().setContext(null);
        if (runner == null && recordStoreBuilder.getContext() != null) {
            runner = recordStoreBuilder.getContext().getDatabase().newRunner();
            runner.setTimer(recordStoreBuilder.getContext().getTimer());
            runner.setMdcContext(recordStoreBuilder.getContext().getMdcContext());
        }
        return this;
    }

    /**
     * Set the record store that will be used as a template to open record store instances for indexing.
     * @param recordStore the target record store
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setRecordStore(@Nonnull FDBRecordStoreBase<M> recordStore) {
        recordStoreBuilder = recordStore.asBuilder().setContext(null);
        if (runner == null) {
            runner = recordStore.getRecordContext().getDatabase().newRunner();
            runner.setTimer(recordStore.getTimer());
            runner.setMdcContext(recordStore.getRecordContext().getMdcContext());
        }
        return this;
    }

    /**
     * Get the index to be built.
     * @return the index to be built
     */
    @Nullable
    public Index getIndex() {
        return index;
    }

    /**
     * Set the index to be built.
     * @param index the index to be built
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setIndex(@Nullable Index index) {
        this.index = index;
        return this;
    }

    /**
     * Set the index to be built.
     * @param indexName the index to be built
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setIndex(@Nonnull String indexName) {
        this.index = getRecordMetaData().getIndex(indexName);
        return this;
    }

    /**
     * Get the explicit set of record types to be indexed.
     *
     * Normally, all record types associated with the chosen index will be indexed.
     * @return the record types to be indexed
     */
    @Nullable
    public Collection<RecordType> getRecordTypes() {
        return recordTypes;
    }

    /**
     * Set the explicit set of record types to be indexed.
     *
     * Normally, record types are inferred from {@link #setIndex}.
     * @param recordTypes the record types to be indexed or {@code null} to infer from the index
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setRecordTypes(@Nullable Collection<RecordType> recordTypes) {
        this.recordTypes = recordTypes;
        return this;
    }

    /**
     * Get the maximum number of records to process in one transaction.
     * @return the maximum number of records to process in one transaction
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Set the maximum number of records to process in one transaction.
     *
     * The default limit is {@link OnlineIndexerBase#DEFAULT_LIMIT} = {@value OnlineIndexerBase#DEFAULT_LIMIT}.
     * @param limit the maximum number of records to process in one transaction
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Get the maximum number of times to retry a single range rebuild.
     * @return the maximum number of times to retry a single range rebuild
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Set the maximum number of times to retry a single range rebuild.
     *
     * The default number of retries is {@link OnlineIndexerBase#DEFAULT_MAX_RETRIES} = {@value OnlineIndexerBase#DEFAULT_MAX_RETRIES}.
     * @param maxRetries the maximum number of times to retry a single range rebuild
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Get the maximum number of records to process in a single second.
     * @return the maximum number of records to process in a single second
     */
    public int getRecordsPerSecond() {
        return recordsPerSecond;
    }

    /**
     * Set the maximum number of records to process in a single second.
     *
     * The default number of retries is {@link OnlineIndexerBase#DEFAULT_RECORDS_PER_SECOND} = {@value OnlineIndexerBase#DEFAULT_RECORDS_PER_SECOND}.
     * @param recordsPerSecond the maximum number of records to process in a single second.
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setRecordsPerSecond(int recordsPerSecond) {
        this.recordsPerSecond = recordsPerSecond;
        return this;
    }

    /**
     * Get the timer used in {@link OnlineIndexerBase#buildIndex}.
     * @return the timer or <code>null</code> if none is set
     */
    @Nullable
    public FDBStoreTimer getTimer() {
        if (runner == null) {
            throw new MetaDataException("timer is only known after runner has been set");
        }
        return runner.getTimer();
    }

    /**
     * Set the timer used in {@link OnlineIndexerBase#buildIndex}.
     * @param timer timer to use
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setTimer(@Nullable FDBStoreTimer timer) {
        if (runner == null) {
            throw new MetaDataException("timer can only be set after runner has been set");
        }
        runner.setTimer(timer);
        return this;
    }

    /**
     * Get the logging context used in {@link OnlineIndexerBase#buildIndex}.
     * @return the logging context of <code>null</code> if none is set
     */
    @Nullable
    public Map<String, String> getMdcContext() {
        if (runner == null) {
            throw new MetaDataException("logging context is only known after runner has been set");
        }
        return runner.getMdcContext();
    }

    /**
     * Set the logging context used in {@link OnlineIndexerBase#buildIndex}.
     * @param mdcContext the logging context to set while running
     * @return this builder
     * @see FDBDatabase#openContext(Map,FDBStoreTimer)
     */
    public OnlineIndexerBaseBuilder<M> setMdcContext(@Nullable Map<String, String> mdcContext) {
        if (runner == null) {
            throw new MetaDataException("logging context can only be set after runner has been set");
        }
        runner.setMdcContext(mdcContext);
        return this;
    }

    /**
     * Get the maximum number of transaction retry attempts.
     * @return the maximum number of attempts
     * @see FDBDatabaseRunner#getMaxAttempts
     */
    public int getMaxAttempts() {
        if (runner == null) {
            throw new MetaDataException("maximum attempts is only known after runner has been set");
        }
        return runner.getMaxAttempts();
    }

    /**
     * Set the maximum number of transaction retry attempts.
     * @param maxAttempts the maximum number of attempts
     * @return this builder
     * @see FDBDatabaseRunner#setMaxAttempts
     */
    public OnlineIndexerBaseBuilder<M> setMaxAttempts(int maxAttempts) {
        if (runner == null) {
            throw new MetaDataException("maximum attempts can only be set after runner has been set");
        }
        runner.setMaxAttempts(maxAttempts);
        return this;
    }

    /**
     * Get the maximum delay between transaction retry attempts.
     * @return the maximum delay
     * @see FDBDatabaseRunner#getMaxDelayMillis
     */
    public long getMaxDelayMillis() {
        if (runner == null) {
            throw new MetaDataException("maximum delay is only known after runner has been set");
        }
        return runner.getMaxDelayMillis();
    }

    /**
     * Set the maximum delay between transaction retry attempts.
     * @param maxDelayMillis the maximum delay
     * @return this builder
     * @see FDBDatabaseRunner#setMaxDelayMillis
     */
    public OnlineIndexerBaseBuilder<M> setMaxDelayMillis(long maxDelayMillis) {
        if (runner == null) {
            throw new MetaDataException("maximum delay can only be set after runner has been set");
        }
        runner.setMaxDelayMillis(maxDelayMillis);
        return this;
    }

    /**
     * Get the initial delay between transaction retry attempts.
     * @return the initial delay
     * @see FDBDatabaseRunner#getInitialDelayMillis
     */
    public long getInitialDelayMillis() {
        if (runner == null) {
            throw new MetaDataException("initial delay is only known after runner has been set");
        }
        return runner.getInitialDelayMillis();
    }

    /**
     * Set the initial delay between transaction retry attempts.
     * @param initialDelayMillis the initial delay
     * @return this builder
     * @see FDBDatabaseRunner#setInitialDelayMillis
     */
    public OnlineIndexerBaseBuilder<M> setInitialDelayMillis(long initialDelayMillis) {
        if (runner == null) {
            throw new MetaDataException("initial delay can only be set after runner has been set");
        }
        runner.setInitialDelayMillis(initialDelayMillis);
        return this;
    }

    /**
     * Set the {@link IndexMaintenanceFilter} to use while building the index.
     *
     * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param indexMaintenanceFilter the index filter to use
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setIndexMaintenanceFilter(@Nonnull IndexMaintenanceFilter indexMaintenanceFilter) {
        if (recordStoreBuilder == null) {
            throw new MetaDataException("index filter can only be set after record store builder has been set");
        }
        recordStoreBuilder.setIndexMaintenanceFilter(indexMaintenanceFilter);
        return this;
    }

    /**
     * Set the {@link RecordSerializer} to use while building the index.
     *
     * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param serializer the serializer to use
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setSerializer(@Nonnull RecordSerializer<M> serializer) {
        if (recordStoreBuilder == null) {
            throw new MetaDataException("serializer can only be set after record store builder has been set");
        }
        recordStoreBuilder.setSerializer(serializer);
        return this;
    }

    /**
     * Set the store format version to use while building the index.
     *
     * Normally this is set by {@link #setRecordStore} or {@link #setRecordStoreBuilder}.
     * @param formatVersion the format version to use
     * @return this builder
     */
    public OnlineIndexerBaseBuilder<M> setFormatVersion(int formatVersion) {
        if (recordStoreBuilder == null) {
            throw new MetaDataException("format version can only be set after record store builder has been set");
        }
        recordStoreBuilder.setFormatVersion(formatVersion);
        return this;
    }

    /**
     * Build an {@link OnlineIndexer}.
     * @return a new online indexer
     */
    public OnlineIndexerBase<M> build() {
        validate();
        return new OnlineIndexerBase<>(runner, recordStoreBuilder, index, recordTypes, limit, maxRetries, recordsPerSecond);
    }

    protected void validate() {
        validateIndex();
        validateLimits();
    }

    // Check pointer equality to make sure other objects really came from given metaData.
    // Also resolve record types to use if not specified.
    private void validateIndex() {
        final RecordMetaData metaData = getRecordMetaData();
        if (index == null) {
            throw new MetaDataException("index must be set");
        }
        if (!metaData.hasIndex(index.getName()) || index != metaData.getIndex(index.getName())) {
            throw new MetaDataException("Index " + index.getName() + " not contained within specified metadata");
        }
        if (recordTypes == null) {
            recordTypes = metaData.recordTypesForIndex(index);
        } else {
            for (RecordType recordType : recordTypes) {
                if (recordType != metaData.getRecordTypes().get(recordType.getName())) {
                    throw new MetaDataException("Record type " + recordType.getName() + " not contained within specified metadata");
                }
            }
        }
    }

    @Nonnull
    private RecordMetaData getRecordMetaData() {
        if (recordStoreBuilder == null) {
            throw new MetaDataException("record store must be set");
        }
        if (recordStoreBuilder.getMetaDataProvider() == null) {
            throw new MetaDataException("record store builder must include metadata");
        }
        return recordStoreBuilder.getMetaDataProvider().getRecordMetaData();
    }

    private void validateLimits() {
        checkPositive(maxRetries, "maximum retries");
        checkPositive(limit, "record limit");
        checkPositive(recordsPerSecond, "records per second value");
    }

    private static void checkPositive(int value, String desc) {
        if (value <= 0) {
            throw new RecordCoreException("Non-positive value " + value + " given for " + desc);
        }
    }
}
