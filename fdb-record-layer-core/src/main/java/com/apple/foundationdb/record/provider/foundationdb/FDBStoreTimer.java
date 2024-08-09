/*
 * FDBStoreTimer.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ExtendedDirectoryLayer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryStreamingAggregationPlan;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link StoreTimer} associated with {@link FDBRecordStore} operations.
 */
@API(API.Status.STABLE)
public class FDBStoreTimer extends StoreTimer {

    /**
     * Ordinary top-level events which surround a single body of code.
     */
    public enum Events implements StoreTimer.Event {
        /** The amount of time taken performing a no-op. */
        PERFORM_NO_OP("perform no-op"),
        /**
         * The amount of time taken to get a read version when explicitly called.
         * This metric is recorded only if the transaction is not conducted at batch priority.
         * This includes any injected latency by the client before issuing the request.
         * @see #INJECTED_GET_READ_VERSION_LATENCY
         * @see FDBTransactionPriority
         */
        GET_READ_VERSION("get read version"),
        /**
         * The amount of time taken to get a read version for batch priority transactions.
         * This is a separate metric from {@link #GET_READ_VERSION} (which is recorded on non-batch
         * priority transactions) because the performance of batch priority read reversion requests are
         * is requested to be significantly different than the performance of non-batch priority
         * requests. This is because the read version request is the primary mechanism by which the
         * FoundationDB cluster can introduce back pressure, and the cluster will favor higher priority
         * transactions over lower priority transactions.
         *
         * <p>
         * Like {@link #GET_READ_VERSION}, this includes any latency injected by the client
         * before issuing the request.
         * </p>
         *
         * @see #GET_READ_VERSION
         * @see #INJECTED_GET_READ_VERSION_LATENCY
         * @see FDBTransactionPriority
         */
        BATCH_GET_READ_VERSION("batch priority get read version"),
        /**
         * The amount of time injected by the client prior to getting a read version.
         * @see FDBDatabase#getLatencyToInject(FDBLatencySource)
         */
        INJECTED_GET_READ_VERSION_LATENCY("injected get read version latency"),
        /**
         * The amount of time taken committing transactions successfully.
         * This includes any injected latency added before issuing the request and any time spent performing pre-commit checks.
         * @see #INJECTED_COMMIT_LATENCY
         */
        COMMIT("commit transaction"),
        /**
         * The amount of time injected into committing transactions.
         * @see FDBDatabase#getLatencyToInject(FDBLatencySource)
         */
        INJECTED_COMMIT_LATENCY("injected commit latency"),
        /** The amount of time taken committing transactions that did not actually have any writes. */
        COMMIT_READ_ONLY("commit read-only transaction"),
        /** The amount of time taken committing transactions that did not succeed. */
        COMMIT_FAILURE("commit transaction with failure"),
        /** The amount of time estimating the size of a key range. See {@link FDBRecordStore#estimateStoreSizeAsync()}. */
        ESTIMATE_SIZE("estimate the size of a key range"),
        /** The amount of time taken persisting meta-data to a {@link FDBMetaDataStore}. */
        SAVE_META_DATA("save meta-data"),
        /** The amount of time taken loading meta-data from a {@link FDBMetaDataStore}. */
        LOAD_META_DATA("load meta-data"),
        /** The amount of time taken loading a record store's {@link com.apple.foundationdb.record.RecordStoreState} listing store-specific information. */
        LOAD_RECORD_STORE_STATE("load record store state"),
        /** The amount of time taken loading a record store's {@code DataStoreInfo} header.*/
        LOAD_RECORD_STORE_INFO("load record store info"),
        /** The amount of time taken loading a record store's index meta-data. */
        LOAD_RECORD_STORE_INDEX_META_DATA("load record store index meta-data"),
        /** The amount of time taken getting the current version from a {@link MetaDataCache}. */
        GET_META_DATA_CACHE_VERSION("get meta-data cache version"),
        /** The amount of time taken getting cached meta-data from a {@link MetaDataCache}. */
        GET_META_DATA_CACHE_ENTRY("get meta-data cache entry"),
        /**
         * The amount of time taken saving records.
         * This time includes serialization and secondary index maintenance as well as writing to the current transaction
         * for later committing.
         */
        SAVE_RECORD("save record"),
        /**
         * The amount of time taken loading records.
         * This time includes fetching from the database and deserialization.
         */
        LOAD_RECORD("load record"),
        /** The amount of time taken scanning records directly without any index. */
        SCAN_RECORDS("scan records"),
        /**
         * The amount of time taken scanning the entries of an index.
         * An ordinary index scan from a query will entail both an index entry scan and {@link #LOAD_RECORD} for each record pointed to by an index entry.
         */
        SCAN_INDEX_KEYS("scan index"),
        /**
         * The amount of time taken performing an index prefetch operation.
         * Index prefetch operation is an index scan followed by record fetches, all done at the FDB level.
         */
        SCAN_REMOTE_FETCH_ENTRY("remote fetch index entry"),
        /**
         * The amount of time taken deleting records.
         * This time includes secondary index maintenance as well as writing to the current transaction
         * for later committing.
         */
        DELETE_RECORD("delete record"),
        // TODO: Are these index maintenance related ones really DetailEvents?
        /** The amount of time spent maintaining an index when the entire record is skipped by the {@link IndexMaintenanceFilter}. */
        SKIP_INDEX_RECORD_BY_PREDICATE("skip index record by index predicates"),
        /** The amount of time spent at index predicates when an entry is skipped by the {@link IndexMaintenanceFilter}. */
        USE_INDEX_RECORD_BY_PREDICATE("use index record by index predicates"),
        /** The amount of time spent at index predicates when an entry is used by the {@link IndexMaintenanceFilter}. */
        SKIP_INDEX_RECORD("skip index record"),
        /** The amount of time spent maintaining an index when an entry is skipped by the {@link IndexMaintenanceFilter}. */
        SKIP_INDEX_ENTRY("skip index entry"),
        /** The amount of time spent saving an entry to a secondary index. */
        SAVE_INDEX_ENTRY("save index entry"),
        /** The amount of time spent deleting an entry from a secondary index. */
        DELETE_INDEX_ENTRY("delete index entry"),
        /** The amount of time spent updating an entry in an atomic mutation index. */
        MUTATE_INDEX_ENTRY("mutate index entry"),
        /** The amount of time spent merging an index (explicitly or during indexing).*/
        MERGE_INDEX("merge index"),
        /** The amount of time spent rebuilding an index for any reason. */
        REBUILD_INDEX("rebuild index for any reason"),
        /** The amount of time spent rebuilding an index for a new store. */
        REBUILD_INDEX_NEW_STORE("rebuild index for a new store"),
        /** The amount of time spent rebuilding an index for stores with few records. */
        REBUILD_INDEX_FEW_RECORDS("rebuild index with few records"),
        /** The amount of time spent rebuilding an index when count is unknown. */
        REBUILD_INDEX_COUNTS_UNKNOWN("rebuild index with counts unknown"),
        /** The amount of time spent rebuilding an index during rebuild all. */
        REBUILD_INDEX_REBUILD_ALL("rebuild index during rebuild all"),
        /** The amount of time spent rebuilding an index by explicit request. */
        REBUILD_INDEX_EXPLICIT("rebuild index by an explicit request"),
        /** The amount of time spent rebuilding an index during a test. */
        REBUILD_INDEX_TEST("rebuild index during test"),
        /** The amount of time spent delayed during index builds. This is injected by the indexing process to avoid overwhelming the database server. */
        INDEXER_DELAY("indexer delay"),

        /** The amount of time spent inserting ranges into a {@link com.apple.foundationdb.async.RangeSet}. */
        RANGE_SET_INSERT("insert into range set"),
        /** The amount of time listing missing ranges from a {@link com.google.common.collect.RangeSet}. */
        RANGE_SET_LIST_MISSING("list missing ranges from range set"),
        /** The amount of time finding the first missing range from a {@link com.google.common.collect.RangeSet}. */
        RANGE_SET_FIND_FIRST_MISSING("find first missing range from range set"),
        /** The amount of time checking if a {@link com.google.common.collect.RangeSet} contains a specific key. */
        RANGE_SET_CONTAINS("range set contains key"),
        /** The amount of time checking if a {@link com.google.common.collect.RangeSet} is empty. */
        RANGE_SET_IS_EMPTY("range set is empty"),

        /** The amount of time spent clearing the space taken by an index that has been removed from the meta-data. */
        REMOVE_FORMER_INDEX("remove former index"),
        /** The amount of time spent counting records for the deprecated record count key. */
        RECOUNT_RECORDS("recount records"),
        /** The amount of time spent checking an index for duplicate entries to preserve uniqueness. */
        CHECK_INDEX_UNIQUENESS("check index uniqueness"),
        /**
         * The amount of time in the {@code checkVersion} call.
         * This may include time to rebuild indexes if the record store is small enough.
         */
        CHECK_VERSION("check meta-data version"),
        /** The amount of time spent reading an entry from a directory layer. */
        DIRECTORY_READ("directory read"),
        /** The amount of time spent reading an entry from a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedDirectoryLayer}. */
        SCOPED_DIRECTORY_LAYER_READ("read the value from the scoped directory layer"),
        /** The amount of time spent adding a new entry to a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedDirectoryLayer}. */
        SCOPED_DIRECTORY_LAYER_CREATE("create the value in the scoped directory layer"),
        /** The amount of time spent reading an entry from an {@link ExtendedDirectoryLayer}. */
        EXTENDED_DIRECTORY_LAYER_READ("read the value from the extended directory layer"),
        /** The amount of time spent adding a new entry to an {@link ExtendedDirectoryLayer}. */
        EXTENDED_DIRECTORY_LAYER_CREATE("create the value in the extended directory layer"),
        /** The amount of time spent reading an entry from a {@link com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer}. */
        INTERNING_LAYER_READ("read the value from the interning layer"),
        /** The amount of time spent adding a new entry to a {@link com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer}. */
        INTERNING_LAYER_CREATE("create the value in the interning layer"),
        /** The amount of time spent loading boundary keys. */
        LOAD_BOUNDARY_KEYS("load boundary keys"),
        /** The amount of time spent reading a sample key to measure read latency. */
        READ_SAMPLE_KEY("read sample key"),
        /** The amount of time spent planning a query. */
        PLAN_QUERY("plan query"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan} as part of executing a query. */
        QUERY_FILTER("filter records"),
        /** The amount of time spent in {@link RecordQueryStreamingAggregationPlan} as part of executing a query. */
        QUERY_AGGREGATE("aggregate records"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan} as part of executing a query. */
        QUERY_TYPE_FILTER("filter records by type"),
        /** The amount of time spent filtering by text contents in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan} as part of executing a query. */
        QUERY_TEXT_FILTER("filter records by text contents"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan} as part of executing a query. */
        QUERY_INTERSECTION("compare query records for intersection"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan} as part of executing a query. */
        QUERY_UNION("compare query records for union"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan} as part of executing a query. */
        QUERY_DISTINCT("compare query records for distinct"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan} as part of executing a query. */
        QUERY_PK_DISTINCT("compare record primary key for distinct"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan} as part of executing a query. */
        QUERY_SELECTOR("execute one iteration of selected plan"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan} as part of executing a query. */
        QUERY_COMPARATOR("execute multiple plans and compare results"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardDirectoryOperation}. */
        TIME_WINDOW_LEADERBOARD_GET_DIRECTORY("leaderboard get directory"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardWindowUpdate}. */
        TIME_WINDOW_LEADERBOARD_UPDATE_DIRECTORY("leaderboard update directory"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardScoreTrim}. */
        TIME_WINDOW_LEADERBOARD_TRIM_SCORES("leaderboard trim scores"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardSubDirectoryOperation}. */
        TIME_WINDOW_LEADERBOARD_GET_SUB_DIRECTORY("leaderboard get sub-directory"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardSaveSubDirectory}. */
        TIME_WINDOW_LEADERBOARD_SAVE_SUB_DIRECTORY("leaderboard save sub-directory"),
        /** The amount of time spent during backoff delay on retryable errors in {@link FDBDatabase#run}. */
        RETRY_DELAY("retry delay"),
        /** The total number of timeouts that have happened during asyncToSync and their durations. */
        TIMEOUTS("timeouts"),
        /** Total number and duration of commits. */
        COMMITS("commits"),
        /** Time for FDB fetches.*/
        FETCHES("fetches"),
        /** Total lifetime of a transaction. */
        TRANSACTION_TIME("transaction time")
        ;

        private final String title;
        private final String logKey;

        Events(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.Event.super.logKey();
        }

        Events(String title) {
            this(title, null);
        }


        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }
    }

    /**
     * Standard {@link com.apple.foundationdb.record.provider.common.StoreTimer.DetailEvent}s.
     */
    public enum DetailEvents implements StoreTimer.DetailEvent {
        /** The amount of time spent loading the raw bytes of a record. */
        GET_RECORD_RAW_VALUE("get record raw value"),
        /** The amount of time spent until the first part of a split record is available. */
        GET_RECORD_RANGE_RAW_FIRST_CHUNK("get record range raw first chunk"),
        /** The amount of time spent until the first part of a range scan (such as an index) is available. */
        GET_SCAN_RANGE_RAW_FIRST_CHUNK("get scan range raw first chunk"),
        /** The amount of time spent initializing a {@link com.apple.foundationdb.async.RankedSet} skip list. */
        RANKED_SET_INIT("ranked set init"),
        /** The amount of time spent looking up the next level of a {@link com.apple.foundationdb.async.RankedSet} skip list. */
        RANKED_SET_NEXT_LOOKUP("ranked set next lookup"),
        /** The amount of time spent looking up the next entry of a {@link com.apple.foundationdb.async.RankedSet} skip list. */
        RANKED_SET_NEXT_LOOKUP_KEY("ranked set next lookup key"),
        /** The amount of time spent checking for a key in a {@link com.apple.foundationdb.async.RankedSet} skip list. */
        RANKED_SET_CONTAINS("ranked set contains"),
        /** The amount of time spent adding to the finest level of a {@link com.apple.foundationdb.async.RankedSet} skip list. */
        RANKED_SET_ADD_LEVEL_ZERO_KEY("ranked set add level 0 key"),
        /** The amount of time spent incrementing an existing level key of a {@link com.apple.foundationdb.async.RankedSet} skip list. */
        RANKED_SET_ADD_INCREMENT_LEVEL_KEY("ranked set add increment level key"),
        /** The amount of time spent incrementing an splitting a level of a {@link com.apple.foundationdb.async.RankedSet} skip list by inserting another key. */
        RANKED_SET_ADD_INSERT_LEVEL_KEY("ranked set add insert level key"),
        /** The amount of time spent reading the lock state of a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver}. */
        RESOLVER_STATE_READ("read resolver state"),
        /** The amount of time spent scanning the directory subspace after a hard miss in {@link FDBReverseDirectoryCache}. */
        RD_CACHE_DIRECTORY_SCAN("reverse directory cache hard miss, scanning directory subspace"),
        /** Time taken to register a lock in the registry. */
        LOCKS_REGISTERED("register lock"),
        /** Time spent in waiting for the lock to be acquired. */
        LOCKS_ACQUIRED("acquire lock"),
        ;

        private final String title;
        private final String logKey;

        DetailEvents(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.DetailEvent.super.logKey();
        }

        DetailEvents(String title) {
            this(title, null);
        }


        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }

    }

    /**
     * Standard {@link Wait} events.
     *
     * In a number of cases, a {@code WAIT_XXX} {@code Wait} corresponds to an {@code XXX} {@code Event}.
     * The event measures the time of the operation itself, whereas the wait measures only the time actually waiting for it to complete.
     * Note that not all of these are waited for anywhere in the Record Layer codebase. They are included so that users of the library
     * can make use of them in their own monitoring.
     */
    public enum Waits implements Wait {
        /**
         * A fake wait for a future already known to be completed.
         * The purpose of passing this is to get standard error handling for futures that have completed exceptionally.
         */
        WAIT_ERROR_CHECK("check for error completion"),
        /** Wait for performing a no-op operation.*/
        WAIT_PERFORM_NO_OP("wait for performing a no-op"),
        /** Wait for explicit call to {@link FDBRecordContext#getReadVersionAsync}. */
        WAIT_GET_READ_VERSION("get_read_version"),
        /** Wait for a transaction to commit. */
        WAIT_COMMIT("wait for commit"),
        /** Wait to compute the approximate transaction size. See {@link FDBRecordContext#getApproximateTransactionSize()}. */
        WAIT_APPROXIMATE_TRANSACTION_SIZE("wait to get the approximate transaction size"),
        /** Wait to estimate the size of a key range. See {@link FDBRecordStore#estimateStoreSizeAsync()}. */
        WAIT_ESTIMATE_SIZE("wait to estimate the size of a key range"),
        /** Wait for saving meta-data to a {@link FDBMetaDataStore}. */
        WAIT_SAVE_META_DATA("wait for save meta-data"),
        /** Wait for loading meta-data from a {@link FDBMetaDataStore}. */
        WAIT_LOAD_META_DATA("wait for load meta-data"),
        /** Wait for loading {@link com.apple.foundationdb.record.RecordStoreState}. */
        WAIT_LOAD_RECORD_STORE_STATE("wait for load record store state"),
        /** Wait for loading a record. */
        WAIT_LOAD_RECORD("wait for load record"),
        /** Wait for loading a record's version. */
        WAIT_LOAD_RECORD_VERSION("wait for load record version"),
        /** Wait for saving a record. */
        WAIT_SAVE_RECORD("wait for save record"),
        /** Wait to check if a record exists. */
        WAIT_RECORD_EXISTS("wait to check if a record exists"),
        /** Wait for deleting a record. */
        WAIT_DELETE_RECORD("wait for delete record"),
        /** Wait for resolving directory layer entries. */
        WAIT_DIRECTORY_RESOLVE("wait for directory resolve"),
        /** Wait for check version on a record store. */
        WAIT_CHECK_VERSION("wait for check version"),
        /** Wait for {@link OnlineIndexer} to complete building an index. */
        WAIT_ONLINE_BUILD_INDEX("wait for online build index"),
        /** Wait for {@link OnlineIndexer} to stop ongoing online index builds. */
        WAIT_STOP_ONLINE_INDEX_BUILD("wait for stopping ongoing online index builds"),
        /** Wait for {@link OnlineIndexer} to checking ongoing online index builds. */
        WAIT_CHECK_ONGOING_ONLINE_INDEX_BUILD("wait for checking ongoing online index builds"),
        /** Wait for {@link OnlineIndexer} to complete ongoing online index merge(s). */
        WAIT_ONLINE_MERGE_INDEX("wait for online merge index"),
        /** Wait for {@link OnlineIndexer} to build endpoints. */
        WAIT_BUILD_ENDPOINTS("wait for building endpoints"),
        /** Wait for a record scan without an index. */
        WAIT_SCAN_RECORDS("wait for scan records"),
        /** Wait for a indexed record scan. */
        WAIT_SCAN_INDEX_RECORDS("wait for scan index records"),
        /** Wait for getting index build state. */
        WAIT_GET_INDEX_BUILD_STATE("wait for getting index build state"),
        /** Wait for query execution to complete. */
        WAIT_EXECUTE_QUERY("wait for execute query"),
        /** Wait for a reverse directory scan. */
        WAIT_REVERSE_DIRECTORY_SCAN("wait for reverse directory scan"),
        /** Wait for a reverse directory lookup. */
        WAIT_REVERSE_DIRECTORY_LOOKUP("wait for reverse directory lookup"),
        /** Wait for finding reverse directory location. */
        WAIT_REVERSE_DIRECTORY_LOCATE("wait for finding reverse directory location"),
        /** Wait for an {@link IndexOperation} to complete. */
        WAIT_INDEX_OPERATION("wait for index operation"),
        /** Wait for indexing type stamp operation. */
        WAIT_INDEX_TYPESTAMP_OPERATION("wait for indexing type stamp operation"),
        /** Wait for adding an index. */
        WAIT_ADD_INDEX("wait for adding an index"),
        /** Wait for dropping an index. */
        WAIT_DROP_INDEX("wait for dropping an index"),
        /** Wait for updating records descriptor. */
        WAIT_UPDATE_RECORDS_DESCRIPTOR("wait for updating the records descriptor"),
        /** Wait for meta-data mutation. */
        WAIT_MUTATE_METADATA("wait for meta-data mutation"),
        /** Wait for updating if record versions should be stored. */
        WAIT_UPDATE_STORE_RECORD_VERSIONS("wait for updating if record versions must be stored"),
        /** Wait for enabling splitting long records. */
        WAIT_ENABLE_SPLIT_LONG_RECORDS("wait for enabling splitting long records"),
        /**
         * Wait for the updated version stamp from a committed transaction.
         * This future should normally be completed already, so this is mainly for error checking.
         */
        WAIT_VERSION_STAMP("wait for version stamp"),
        /** Wait to load the the cluster's meta-data version stamp. */
        WAIT_META_DATA_VERSION_STAMP("wait for meta-data version stamp"),
        /** Wait for a synchronous {@link com.apple.foundationdb.record.RecordCursor#getNext()}. */
        WAIT_ADVANCE_CURSOR("wait for advance cursor"),
        /** Wait for scanning a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace} to see whether it has data.*/
        WAIT_KEYSPACE_SCAN("wait scanning keyspace"),
        /** Wait for listing a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace}. */
        WAIT_KEYSPACE_LIST("wait for listing keyspace"),
        /** Wait for clearing a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace}. */
        WAIT_KEYSPACE_CLEAR("wait for clearing keyspace"),
        /** Wait for resolving the path for a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace}. */
        WAIT_KEYSPACE_PATH_RESOLVE("wait for keyspace path resolve"),
        /** Wait for a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverMappingDigest} to be computed. */
        WAIT_LOCATABLE_RESOLVER_COMPUTE_DIGEST("wait for computing directory layer digest"),
        /** Wait for {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverMappingReplicator} to copy a directory layer. */
        WAIT_LOCATABLE_RESOLVER_MAPPING_COPY("wait for copying contents of directory layer"),
        /** Wait for a backoff delay on retryable errors in {@link FDBDatabase#run}. */
        WAIT_RETRY_DELAY("wait for retry delay"),
        /** Wait for statistics to be collected. */
        WAIT_COLLECT_STATISTICS("wait for statistics to be collected of a record store or index"),
        /** Wait for getting boundaries. */
        WAIT_GET_BOUNDARY("wait for boundary result from locality api"),
        /** Wait for setting the store state cacheability. */
        WAIT_SET_STATE_CACHEABILITY("wait to set state cacheability"),
        /** Wait for initializing a synchronized session. */
        WAIT_INIT_SYNC_SESSION("wait for initializing a synchronized session"),
        /** Wait for checking a synchronized session. */
        WAIT_CHECK_SYNC_SESSION("wait for checking a synchronized session"),
        /** Wait for ending a synchronized session. */
        WAIT_END_SYNC_SESSION("wait for ending a synchronized session"),
        /** Wait for editing a header user field. */
        WAIT_EDIT_HEADER_USER_FIELD("wait to edit a header user field"),
        /** Wait to read a key from the FDB system keyspace. */
        WAIT_LOAD_SYSTEM_KEY("wait for reading a key from the FDB system keyspace"),
        /** Wait to perform validation of resolver reverse directory mapping. */
        WAIT_VALIDATE_RESOLVER("wait validating resolver"),
        /** wait to load partition metadata for one or more grouping key. */
        WAIT_LOAD_LUCENE_PARTITION_METADATA("wait to load lucene partition metadata")
        ;

        private final String title;
        private final String logKey;

        Waits(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : Wait.super.logKey();
        }

        Waits(String title) {
            this(title, null);
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }
    }

    /**
     * Standard {@link Count} events.
     */
    public enum Counts implements Count {
        /** The number of times a record context is opened. */
        OPEN_CONTEXT("open record context", false),
        /** The number of times a record context is closed. */
        CLOSE_CONTEXT("close record context", false),
        /** The number of times a record context is closed because it has been open for a long time. */
        CLOSE_CONTEXT_OPEN_TOO_LONG("close record context open too long", false),
        /** The number of times a record store is created in the database. */
        CREATE_RECORD_STORE("create record store", false),
        /** The number of times the store state cache returned a cached result. */
        STORE_STATE_CACHE_HIT("store info cache hit", false),
        /** The number of times the store state cache was unable to return a cached result. */
        STORE_STATE_CACHE_MISS("store info cache miss", false),
        /** The number of record key-value pairs saved. */
        SAVE_RECORD_KEY("number of record keys saved", false, null, true),
        /** The size of keys for record key-value pairs saved. */
        SAVE_RECORD_KEY_BYTES("number of record key bytes saved", true, null, true),
        /** The size of values for record key-value pairs saved. */
        SAVE_RECORD_VALUE_BYTES("number of record value bytes saved", true, null, true),
        /** The number of entries (e.g., key-value pairs or text index entries) loaded by a scan. */
        LOAD_SCAN_ENTRY("number of entries loaded by some scan", false),
        /** The number of key-value pairs loaded by a range scan. */
        LOAD_KEY_VALUE("number of keys loaded", false),
        /** The number of entries loaded when scanning a text index. */
        LOAD_TEXT_ENTRY("number of text entries loaded", false),
        /** The number of record key-value pairs loaded. */
        LOAD_RECORD_KEY("number of record keys loaded", false),
        /** The size of keys for record key-value pairs loaded. */
        LOAD_RECORD_KEY_BYTES("number of record key bytes loaded", true),
        /** The size of values for record key-value pairs loaded. */
        LOAD_RECORD_VALUE_BYTES("number of record value bytes loaded", true),
        /** The number of index key-value pairs saved. */
        SAVE_INDEX_KEY("number of index keys saved", false, null, true),
        /** The size of keys for index key-value pairs saved. */
        SAVE_INDEX_KEY_BYTES("number of index key bytes saved", true, null, true),
        /** The size of values for index key-value pairs saved. */
        SAVE_INDEX_VALUE_BYTES("number of index value bytes saved", true, null, true),
        /** The number of index key-value pairs loaded. */
        LOAD_INDEX_KEY("number of index keys loaded", false),
        /** The size of keys for index key-value pairs loaded. */
        LOAD_INDEX_KEY_BYTES("number of index key bytes loaded", true),
        /** The size of values for index key-value pairs loaded. */
        LOAD_INDEX_VALUE_BYTES("number of index value bytes loaded", true),
        /** The number of index state key-value pairs loaded. */
        LOAD_STORE_STATE_KEY("number of store state keys loaded", false),
        /** The size of keys for index state key-value pairs loaded. */
        LOAD_STORE_STATE_KEY_BYTES("number of store state key bytes loaded", true),
        /** The size of values for index state key-value pairs loaded. */
        LOAD_STORE_STATE_VALUE_BYTES("number of store state value bytes loaded", true),
        /** The number of record key-value pairs deleted. */
        DELETE_RECORD_KEY("number of record keys deleted", false, null, true),
        /** The size of keys for record key-value pairs deleted. */
        DELETE_RECORD_KEY_BYTES("number of record key bytes deleted", true, null, true),
        /** The size of values for record key-value pairs deleted. */
        DELETE_RECORD_VALUE_BYTES("number of record value bytes deleted", true, null, true),
        /** The number of index key-value pairs deleted. */
        DELETE_INDEX_KEY("number of index keys deleted", false, null, true),
        /** The size of keys for index key-value pairs deleted. */
        DELETE_INDEX_KEY_BYTES("number of index key bytes deleted", true, null, true),
        /** The size of values for index key-value pairs deleted. */
        DELETE_INDEX_VALUE_BYTES("number of index value bytes deleted", true, null, true),
        /** The previous size of values for record key-value pairs that are updated. */
        REPLACE_RECORD_VALUE_BYTES("number of record value bytes replaced", true, null, true),
        /** The number of reverse directory cache misses.  */
        REVERSE_DIR_PERSISTENT_CACHE_MISS_COUNT("number of persistent cache misses", false),
        /** The number of reverse directory cache hits.  */
        REVERSE_DIR_PERSISTENT_CACHE_HIT_COUNT("number of persistent cache hits", false),
        /** The number of times an {@link com.apple.foundationdb.async.RangeSet} is cleared. */
        RANGE_SET_CLEAR("range set clears", false),
        /** The number of query plans that use a covering index. */
        PLAN_COVERING_INDEX("number of covering index plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan}. */
        PLAN_FILTER("number of filter plans", false),
        /** The number of query plans that include a {@link RecordQueryStreamingAggregationPlan}. */
        PLAN_AGGREGATE("number of streaming aggregate plans", false),
        /** The number of query plans that include an index. */
        PLAN_INDEX("number of index plans", false),
        /** The number of query plans that include an index scan which overscans to load additional data into a client-side cache. */
        PLAN_OVERSCAN_INDEX("number of overscan index plans", false),
        /** The number of query plans that include an {@code IN} with parameters. */
        PLAN_IN_PARAMETER("number of in plans with parameters", false),
        /** The number of query plans that include a {@code IN} type of Union. */
        PLAN_IN_UNION("number of in union plans", false),
        /** The number of query plans that include an {@code IN} with literal values. */
        PLAN_IN_VALUES("number of in plans with values", false),
        /** The number of query plans that include an {@code IN} with values extracted from a comparison. */
        PLAN_IN_COMPARAND("number of in plans with comparison comparands", false),
        /** The number of query plans that include an {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan}. */
        PLAN_INTERSECTION("number of intersection plans", false),
        /** The number of query plans that include a loading records directly by their primary keys. */
        PLAN_LOAD_BY_KEYS("number of load-by-keys plans", false),
        /** The number of query plans that include a record scan without an index. */
        PLAN_SCAN("number of scan plans", false),
        /** The number of query plans that include translating a rank range into a score range. */
        PLAN_SCORE_FOR_RANK("number of score-for-rank plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan}. */
        PLAN_TYPE_FILTER("number of type filter plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan}. */
        PLAN_UNION("number of union plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan}. */
        PLAN_UNORDERED_UNION("number of unordered union plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan}. */
        PLAN_DISTINCT("number of unordered distinct plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan}. */
        PLAN_PK_DISTINCT("number of unordered distinct plans by primary key", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan}. */
        PLAN_FETCH("number of fetch from partial record plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.bitmap.ComposedBitmapIndexQueryPlan}. */
        PLAN_COMPOSED_BITMAP_INDEX("number of composed bitmap plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan}. */
        PLAN_SORT("number of sort plans", false),
        PLAN_DAM("number of dam plans", false),
        /** The number of synthetic record type plans. */
        PLAN_SYNTHETIC_TYPE("number of synthetic record types plans", false),
        /** The number of records given given to any filter within any plan. */
        QUERY_FILTER_GIVEN("number of records given to any filter within any plan", false),
        /** The number of records passed by any filter within any plan. */
        QUERY_FILTER_PASSED("number of records passed by any filter within any plan", false),
        /** The number of records given to {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan}. */
        QUERY_FILTER_PLAN_GIVEN("number of records given to RecordQueryFilterPlan", false),
        /** The number of records passed by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan}. */
        QUERY_FILTER_PLAN_PASSED("number of records passed by RecordQueryFilterPlan", false),
        /** The number of records given to {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan}. */
        QUERY_TYPE_FILTER_PLAN_GIVEN("number of records given to RecordQueryTypeFilterPlan", false),
        /** The number of records passed by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan}. */
        QUERY_TYPE_FILTER_PLAN_PASSED("number of records passed by RecordQueryTypeFilterPlan", false),
        /** The number of records given to a filter within a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan}. */
        QUERY_TEXT_FILTER_PLAN_GIVEN("number of records given to a filter within a RecordQueryTextIndexPlan", false),
        /** The number of records passed by a filter within a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan}. */
        QUERY_TEXT_FILTER_PLAN_PASSED("number of records passed by a filter within a RecordQueryTextIndexPlan", false),
        /** The number of duplicate records found by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan}. */
        QUERY_DISTINCT_PLAN_DUPLICATES("number of duplicates found by RecordQueryUnorderedDistinctPlan", false),
        /** The number of unique records found by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan}. */
        QUERY_DISTINCT_PLAN_UNIQUES("number of unique records found by RecordQueryUnorderedDistinctPlan", false),
        /** The number of duplicate records found by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan}. */
        QUERY_PK_DISTINCT_PLAN_DUPLICATES("number of duplicates found by RecordQueryUnorderedPrimaryKeyDistinctPlan", false),
        /** The number of unique records found by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan}. */
        QUERY_PK_DISTINCT_PLAN_UNIQUES("number of unique records found by RecordQueryUnorderedPrimaryKeyDistinctPlan", false),
        /** The number of matching records found by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan}. */
        QUERY_INTERSECTION_PLAN_MATCHES("number of matching records found by RecordQueryIntersectionPlan", false),
        /** The number of non-matching records found by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan}. */
        QUERY_INTERSECTION_PLAN_NONMATCHES("number of non-matching records found by RecordQueryIntersectionPlan", false),
        /** The number of duplicate records found by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan}. */
        QUERY_UNION_PLAN_DUPLICATES("number of duplicates found by RecordQueryUnorderedDistinctPlan", false),
        /** The number of unique records found by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan}. */
        QUERY_UNION_PLAN_UNIQUES("number of unique records found by RecordQueryUnorderedDistinctPlan", false),
        /** The number of records filtered out as not matching or duplicate. */
        QUERY_DISCARDED("number of records loaded but filtered out", false),
        /** The number of aggregate groups created by {@link RecordQueryStreamingAggregationPlan}. */
        QUERY_AGGREGATE_GROUPS("number of aggregate groups", false),
        /** The max size of aggregate group created by {@link RecordQueryStreamingAggregationPlan}. */
        QUERY_AGGREGATE_GROUP_MAX_SIZE("max size of aggregate group", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan}. */
        PLAN_COMPARATOR("number of comparator plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan}. */
        PLAN_SELECTOR("number of selector plans", false),
        /** The number of matching records by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan}. */
        QUERY_COMPARATOR_MATCH("number of records matched", false),
        /** The number of comparison failures by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan}. */
        QUERY_COMPARATOR_MISMATCH("number of record comparison mismatched", false),
        /** The number of comparisons made by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan}. */
        QUERY_COMPARATOR_COMPARED("number of comparisons", false),
        /** The number of times the read version was taken from the cache of the last seen version. */
        SET_READ_VERSION_TO_LAST_SEEN("set read version to last seen version", false),
        /** The number of records scanned by {@link OnlineIndexer}. */
        ONLINE_INDEX_BUILDER_RECORDS_SCANNED("number of records scanned by online index build", false),
        /** The number of records indexed by {@link OnlineIndexer}. */
        ONLINE_INDEX_BUILDER_RECORDS_INDEXED("number of records indexed by online index build", false),
        /** The number of {@link OnlineIndexer} range scans terminated after hitting the scan limit. */
        ONLINE_INDEX_BUILDER_RANGES_BY_COUNT("number of indexer iterations terminated by scan limit", false),
        /** The number of {@link OnlineIndexer} range scans terminated after hitting the size limit. */
        ONLINE_INDEX_BUILDER_RANGES_BY_SIZE("number of indexer iterations terminated by write limit", false),
        /** The number of missing index entries detected by the online scrubber. */
        ONLINE_INDEX_BUILDER_RANGES_BY_TIME("number of indexer iterations terminated by time limit", false),
        /** The number of {@link OnlineIndexer} range scans terminated after hitting the time limit. */
        ONLINE_INDEX_BUILDER_RANGES_BY_DEPLETION("number of indexer iterations terminated by cursor's range depletion", false),
        /** The number of {@link OnlineIndexer} range scans terminated after hitting the cursor's range depletion. */
        MUTUAL_INDEXER_FULL_START("counter: start indexing a 'FULL' fragment", false),
        /** {@link IndexingMutuallyByRecords} counter: start indexing a 'FULL' fragment. */
        MUTUAL_INDEXER_FULL_DONE("counter: done indexing a 'FULL' fragment", false),
        /** {@link IndexingMutuallyByRecords} counter: done indexing a 'FULL' fragment. */
        MUTUAL_INDEXER_ANY_START("counter: start indexing an 'ANY' fragment", false),
        /** {@link IndexingMutuallyByRecords} counter: start indexing an 'ANY' fragment. */
        MUTUAL_INDEXER_ANY_DONE("counter: done indexing an 'ANY' fragment", false),
        /** {@link IndexingMutuallyByRecords} counter: done indexing an 'ANY' fragment. */
        MUTUAL_INDEXER_ANY_JUMP("counter: had conflicts while indexing an 'ANY' fragment, jump ahead", false),
        /** {@link IndexingMutuallyByRecords} counter: had conflicts while indexing an 'ANY' fragment, jump ahead. */
        INDEX_SCRUBBER_MISSING_ENTRIES("number of missing index entries detected by online scrubber", false),
        /** The number of dangling index entries detected by online scrubber. */
        INDEX_SCRUBBER_DANGLING_ENTRIES("number of dangling index entries detected by online scrubber", false),
        /** The number of times that a leaderboard update adds a time window. */
        TIME_WINDOW_LEADERBOARD_ADD_WINDOW("number of leaderboard windows added", false),
        /** The number of times that a leaderboard update deleted a time window. */
        TIME_WINDOW_LEADERBOARD_DELETE_WINDOW("number of leaderboard windows deleted", false),
        /** The number of times that a leaderboard needs to be rebuilt because a window was added after a score it should contain. */
        TIME_WINDOW_LEADERBOARD_OVERLAPPING_CHANGED("number of leaderboard conditional rebuilds", false),
        /** The number of times that an index entry does not point to a valid record. */
        BAD_INDEX_ENTRY("number of occurrences of bad index entries", false),
        /** The number of record keys repaired by {@link FDBRecordStore#repairRecordKeys(byte[], com.apple.foundationdb.record.ScanProperties)}. */
        REPAIR_RECORD_KEY("repair record key", false, null, true),
        /** The number of record keys with an invalid split suffix found by {@link FDBRecordStore#repairRecordKeys(byte[], com.apple.foundationdb.record.ScanProperties)}. */
        INVALID_SPLIT_SUFFIX("invalid split suffix", false),
        /** The number of record keys with an incorrect length found by {@link FDBRecordStore#repairRecordKeys(byte[], com.apple.foundationdb.record.ScanProperties)}. */
        INVALID_KEY_LENGTH("invalid record key", false),
        /** The number of indexes that need to be rebuilt in the record store. */
        INDEXES_NEED_REBUILDING("indexes need rebuilding", false),
        /** The number of bytes read. */
        BYTES_READ("bytes read", true),
        /** The number of bytes written, not including deletes. */
        BYTES_WRITTEN("bytes written", true, null, true),
        /** Total number of read (get) operations. */
        READS("reads", false),
        /** Total number of range read (get) operations. */
        RANGE_READS("range reads", false),
        /** Total number of remote fetch (get) operations. */
        REMOTE_FETCH("remote fetch reads", false),
        /** Total number of write operations. */
        WRITES("writes", false, null, true),
        /** Total number of delete (clear) operations. */
        DELETES("deletes", false, null, true),
        /** Total number of range delete (clear) operations. */
        RANGE_DELETES("range deletes", false, null, true),
        /** Total number of mutation operations. */
        MUTATIONS("mutations", false, null, true),
        /** JNI Calls.*/
        JNI_CALLS("jni calls", false),
        /**Bytes read.*/
        BYTES_FETCHED("bytes fetched", false),
        /** Number of network fetches performed.*/
        RANGE_FETCHES("range fetches", false),
        /** Number of Key-values fetched during a range scan.*/
        RANGE_KEYVALUES_FETCHED("range key-values ", false),
        /** Number of chunk reads that failed.*/
        CHUNK_READ_FAILURES("read fails", false),
        /** Count of commits that failed for any reason. */
        COMMITS_FAILED("commits failed", false),
        /** Count failed due to conflict. */
        CONFLICTS("conflicts", false),
        /** Count failed due to commit_unknown. */
        COMMIT_UNKNOWN("commit unknown", false),
        /** Count failed due to transaction_too_large. */
        TRANSACTION_TOO_LARGE("transaction too large", false),
        /** Count the number of scans executed that returned no data. */
        EMPTY_SCANS("empty scans", false),
        MULTIDIMENSIONAL_LEAF_NODE_READS("leaf nodes read", false),
        MULTIDIMENSIONAL_LEAF_NODE_READ_BYTES("leaf node bytes read", true),
        MULTIDIMENSIONAL_LEAF_NODE_WRITES("leaf nodes written", false),
        MULTIDIMENSIONAL_LEAF_NODE_WRITE_BYTES("leaf node bytes written", true),

        MULTIDIMENSIONAL_INTERMEDIATE_NODE_READS("intermediate nodes read", false),
        MULTIDIMENSIONAL_INTERMEDIATE_NODE_READ_BYTES("intermediate node bytes read", true),
        MULTIDIMENSIONAL_INTERMEDIATE_NODE_WRITES("intermediate nodes written", false),
        MULTIDIMENSIONAL_INTERMEDIATE_NODE_WRITE_BYTES("intermediate node bytes written", true),
        MULTIDIMENSIONAL_CHILD_NODE_DISCARDS("child node discards", false),
        /** Count of the locks created. */
        LOCKS_ATTEMPTED("number of attempts to register a lock", false),
        /** Count of the locks released. */
        LOCKS_RELEASED("number of locks released", false),
        ;

        private final String title;
        private final boolean isSize;
        private final String logKey;
        private final boolean delayedUntilCommit;

        Counts(String title, boolean isSize, String logKey, boolean delayedUntilCommit) {
            this.title = title;
            this.isSize = isSize;
            this.logKey = (logKey != null) ? logKey : Count.super.logKey();
            this.delayedUntilCommit = delayedUntilCommit;
        }

        Counts(String title, boolean isSize) {
            this(title, isSize, null, false);
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        public boolean isDelayedUntilCommit() {
            return delayedUntilCommit;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }

        @Override
        public boolean isSize() {
            return isSize;
        }
    }

    /**
     * An aggregate over other count events.
     */
    public enum CountAggregates implements Aggregate, Count {
        /**
         * The number of bytes deleted. This represents the number of bytes cleared by all events
         * instrumented by an {@link FDBStoreTimer}. Note that it is not possible (for efficiency
         * reasons) to track all deleted bytes, specifically for many {@code clear()} operations.
         * A range clear takes a start and end key and efficiently deletes all bytes in between at the
         * server and, thus, the client has no visibility into the number of bytes actually deleted
         * by the operation.
         */
        BYTES_DELETED("bytes deleted",
                Counts.DELETE_RECORD_KEY_BYTES,
                Counts.DELETE_RECORD_VALUE_BYTES,
                Counts.DELETE_INDEX_KEY_BYTES,
                Counts.DELETE_INDEX_VALUE_BYTES,
                // The size of a record that was replaced by another record. The other record will be accounted
                // for in the BYTES_WRITTEN, so BYTES_WRITTEN - BYTES_DELETED should be an accurate(-ish) reflection
                // of on-disk delta.
                Counts.REPLACE_RECORD_VALUE_BYTES
        ),
        ;
        @Nonnull
        private final String title;
        private final boolean isSize;
        @Nonnull
        private final String logKey;
        @Nonnull
        private final Set<Count> events;

        CountAggregates(@Nonnull String title, @Nonnull Count... events) {
            this(title, null, events);
        }

        CountAggregates(@Nonnull String title, @Nullable String logKey, @Nonnull Count... events) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : Aggregate.super.logKey();
            this.events = ImmutableSet.copyOf(validate((first, other) -> {
                if (first.isSize() != other.isSize()) {
                    throw new IllegalArgumentException("All counts must have the same isSize()");
                }
            }, events));
            this.isSize = events[0].isSize();
        }

        @Override
        @Nonnull
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }

        @Override
        public Set<Count> getComponentEvents() {
            return events;
        }

        @Nullable
        @Override
        public Counter compute(@Nonnull StoreTimer storeTimer) {
            return compute(storeTimer, events);
        }

        @Override
        public boolean isSize() {
            return isSize;
        }
    }

    protected static final Set<Aggregate> ALL_AGGREGATES = new ImmutableSet.Builder<Aggregate>()
            .add(CountAggregates.values())
            .build();

    protected static Stream<StoreTimer.Event> possibleEvents() {
        return Stream.of(
                Events.values(),
                DetailEvents.values(),
                Waits.values(),
                Counts.values(),
                CountAggregates.values(),
                RecordSerializer.Events.values()
        ).flatMap(Arrays::stream);
    }

    static {
        checkEventNameUniqueness(possibleEvents());
    }

    public FDBStoreTimer() {
        super();
    }

    @Override
    @Nonnull
    public Set<Aggregate> getAggregates() {
        return ALL_AGGREGATES;
    }

    @Override
    public void recordTimeout(Wait event, long startTime) {
        final long totalNanos = System.nanoTime() - startTime;
        getCounter(Events.TIMEOUTS, true).record(totalNanos);
        getTimeoutCounter(event, true).record(totalNanos);
    }
}
