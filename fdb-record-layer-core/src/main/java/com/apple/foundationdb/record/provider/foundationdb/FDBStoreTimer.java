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

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A {@link StoreTimer} associated with {@link FDBRecordStore} operations.
 */
@API(API.Status.STABLE)
public class FDBStoreTimer extends StoreTimer {

    /**
     * Ordinary top-level events which surround a single body of code.
     */
    public enum Events implements Event {
        /** The amount of time taken committing transactions successfully. */
        COMMIT("commit transaction"),
        /** The amount of time taken committing transactions that did not actually have any writes. */
        COMMIT_READ_ONLY("commit read-only transaction"),
        /** The amount of time taken committing transactions that did not succeed. */
        COMMIT_FAILURE("commit transaction with failure"),
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
        /** The amount of time taken loading record versions. */
        LOAD_RECORD_VERSION("load record version"),
        /** The amount of time taken scanning records directly without any index. */
        SCAN_RECORDS("scan records"),
        /**
         * The amount of time taken scanning the entries of an index.
         * An ordinary index scan from a query will entail both an index entry scan and {@link #LOAD_RECORD} for each record pointed to by an index entry.
         */
        SCAN_INDEX_KEYS("scan index"),
        /**
         * The amount of time taken deleting records.
         * This time includes secondary index maintenance as well as writing to the current transaction
         * for later committing.
         */
        DELETE_RECORD("delete record"),
        // TODO: Are these index maintenance related ones really DetailEvents?
        /** The amount of time spent maintaining an index when the entire record is skipped by the {@link IndexMaintenanceFilter}. */
        SKIP_INDEX_RECORD("skip index record"),
        /** The amount of time spent maintaining an index when an entry is skipped by the {@link IndexMaintenanceFilter}. */
        SKIP_INDEX_ENTRY("skip index entry"),
        /** The amount of time spent saving an entry to a secondary index. */
        SAVE_INDEX_ENTRY("save index entry"),
        /** The amount of time spent deleting an entry from a secondary index. */
        DELETE_INDEX_ENTRY("delete index entry"),
        /** The amount of time spent updating an entry in an atomic mutation index. */
        MUTATE_INDEX_ENTRY("mutate index entry"),
        /** The amount of time spent deleting an entry from a secondary index. */
        REBUILD_INDEX("rebuild index"),
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
        /** The amount of time spent computing boundary keys. */
        COMPUTE_BOUNDARY_KEYS("compute boundary keys"),
        /** The amount of time spent reading a sample key to measure read latency. */
        READ_SAMPLE_KEY("read sample key"),
        /** The amount of time spent planning a query. */
        PLAN_QUERY("plan query"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan} as part of executing a query. */
        QUERY_FILTER("filter records"),
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
        /** The amount of time spent in {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardDirectoryOperation}. */
        TIME_WINDOW_LEADERBOARD_GET_DIRECTORY("leaderboard get directory"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardWindowUpdate}. */
        TIME_WINDOW_LEADERBOARD_UPDATE_DIRECTORY("leaderboard update directory"),
        /** The amount of time spent in {@link com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowLeaderboardScoreTrim}. */
        TIME_WINDOW_LEADERBOARD_TRIM_SCORES("leaderboard trim scores");

        private final String title;
        Events(String title) {
            this.title = title;
        }

        @Override
        public String title() {
            return title;
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
        /** The amount of time spent reading the lock state of a {@link com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver}. */
        RESOLVER_STATE_READ("read resolver state"),
        /** The amount of time spent scanning the directory subspace after a hard miss in {@link FDBReverseDirectoryCache}. */
        RD_CACHE_DIRECTORY_SCAN("reverse directory cache hard miss, scanning directory subspace");

        private final String title;
        DetailEvents(String title) {
            this.title = title;
        }

        @Override
        public String title() {
            return title;
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
        /** Wait for explicit call to {@link FDBDatabase#getReadVersion}. */
        WAIT_GET_READ_VERSION("get_read_version"),
        /** Wait for a transaction to commit. */
        WAIT_COMMIT("wait for commit"),
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
        /** Wait for {@link OnlineIndexer} to build endpoints. */
        WAIT_BUILD_ENDPOINTS("wait for building endpoints"),
        /** Wait for a record scan without an index. */
        WAIT_SCAN_RECORDS("wait for scan records"),
        /** Wait for a indexed record scan. */
        WAIT_SCAN_INDEX_RECORDS("wait for scan index records"),
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
        /** Wait for a synchronous {@link com.apple.foundationdb.record.RecordCursor#next}. */
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
        /** Wait for a backoff delay on retryable error in {@link FDBDatabase#run}. */
        WAIT_RETRY_DELAY("wait for retry delay"),
        /** Wait for statistics to be collected. */
        WAIT_COLLECT_STATISTICS("wait for statistics to be collected of a record store or index"),
        /** Wait for getting boundaries. */
        WAIT_GET_BOUNDARY("wait for boundary result from locality api"),
        ;

        private final String title;
        Waits(String title) {
            this.title = title;
        }

        @Override
        public String title() {
            return title;
        }
    }

    /**
     * Standard {@link Count} events.
     */
    public enum Counts implements Count {
        /** The number of times a record context is opened. */
        OPEN_CONTEXT("open record context", false),
        /** The number of times a record context is closed. */
        CLOSE_CONTEXT("open record context", false),
        /** The number of times a record store is created in the database. */
        CREATE_RECORD_STORE("create record store", false),
        /** The number of times the store state cache returned a cached result. */
        STORE_STATE_CACHE_HIT("store info cache hit", false),
        /** The number of times the store state cache was unable to return a cached result. */
        STORE_STATE_CACHE_MISS("store info cache miss", false),
        /** The number of record key-value pairs saved. */
        SAVE_RECORD_KEY("number of record keys saved", false),
        /** The size of keys for record key-value pairs saved. */
        SAVE_RECORD_KEY_BYTES("number of record key bytes saved", true),
        /** The size of values for record key-value pairs saved. */
        SAVE_RECORD_VALUE_BYTES("number of record value bytes saved", true),
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
        SAVE_INDEX_KEY("number of index keys saved", false),
        /** The size of keys for index key-value pairs saved. */
        SAVE_INDEX_KEY_BYTES("number of index key bytes saved", true),
        /** The size of values for index key-value pairs saved. */
        SAVE_INDEX_VALUE_BYTES("number of index value bytes saved", true),
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
        DELETE_RECORD_KEY("number of record keys deleted", false),
        /** The size of keys for record key-value pairs deleted. */
        DELETE_RECORD_KEY_BYTES("number of record key bytes deleted", true),
        /** The size of values for record key-value pairs deleted. */
        DELETE_RECORD_VALUE_BYTES("number of record value bytes deleted", true),
        /** The number of index key-value pairs deleted. */
        DELETE_INDEX_KEY("number of index keys deleted", false),
        /** The size of keys for index key-value pairs deleted. */
        DELETE_INDEX_KEY_BYTES("number of index key bytes deleted", true),
        /** The size of values for index key-value pairs deleted. */
        DELETE_INDEX_VALUE_BYTES("number of index value bytes deleted", true),
        /** The previous size of values for record key-value pairs that are updated. */
        REPLACE_RECORD_VALUE_BYTES("number of record value bytes replaced", true),
        /** The number of reverse directory cache misses.  */
        REVERSE_DIR_PERSISTENT_CACHE_MISS_COUNT("number of persistent cache misses", false),
        /** The number of reverse directory cache hits.  */
        REVERSE_DIR_PERSISTENT_CACHE_HIT_COUNT("number of persistent cache hits", false),
        /** The number of query plans that use a covering index. */
        PLAN_COVERING_INDEX("number of covering index plans", false),
        /** The number of query plans that include a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan}. */
        PLAN_FILTER("number of filter plans", false),
        /** The number of query plans that include an index. */
        PLAN_INDEX("number of index plans", false),
        /** The number of query plans that include an {@code IN} with parameters. */
        PLAN_IN_PARAMETER("number of in plans with parameters", false),
        /** The number of query plans that include an {@code IN} with literal values. */
        PLAN_IN_VALUES("number of in plans with values", false),
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
        /** The number of records given to a filter within a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan}*/
        QUERY_TEXT_FILTER_PLAN_GIVEN("number of records given to a filter within a RecordQueryTextIndexPlan", false),
        /** The number of records passed by a filter within a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan}*/
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
        /** The number of times the read version was taken from the cache of the last seen version. */
        SET_READ_VERSION_TO_LAST_SEEN("set read version to last seen version", false),
        /** The number of records scanned by {@link OnlineIndexer}. */
        ONLINE_INDEX_BUILDER_RECORDS_SCANNED("number of records scanned by online index build", false),
        /** The number of records indexed by {@link OnlineIndexer}. */
        ONLINE_INDEX_BUILDER_RECORDS_INDEXED("number of records indexed by online index build", false),
        /** The number of times that a leaderboard update adds a time window. */
        TIME_WINDOW_LEADERBOARD_ADD_WINDOW("number of leaderboard windows added", false),
        /** The number of times that a leaderboard update deleted a time window. */
        TIME_WINDOW_LEADERBOARD_DELETE_WINDOW("number of leaderboard windows deleted", false),
        /** The number of times that a leaderboard needs to be rebuilt because a window was added after a score it should contain. */
        TIME_WINDOW_LEADERBOARD_OVERLAPPING_CHANGED("number of leaderboard conditional rebuilds", false),
        /** The number of times that an index entry does not point to a valid record. */
        BAD_INDEX_ENTRY("number of occurrences of bad index entries", false),
        /** The number of record keys repaired by {@link FDBRecordStore#repairRecordKeys(byte[], com.apple.foundationdb.record.ScanProperties)}. */
        REPAIR_RECORD_KEY("repair record key", false),
        /** The number of record keys with an invalid split suffix found by {@link FDBRecordStore#repairRecordKeys(byte[], com.apple.foundationdb.record.ScanProperties)}. */
        INVALID_SPLIT_SUFFIX("invalid split suffix", false),
        /** The number of record keys with an incorrect length found by {@link FDBRecordStore#repairRecordKeys(byte[], com.apple.foundationdb.record.ScanProperties)}. */
        INVALID_KEY_LENGTH("invalid record key", false),
        /** The number of indexes that need to be rebuilt in the record store. */
        INDEXES_NEED_REBUILDING("indexes need rebuilding", false),
        ;

        private final String title;
        private final boolean isSize;
        Counts(String title, boolean isSize) {
            this.title = title;
            this.isSize = isSize;
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        public boolean isSize() {
            return isSize;
        }
    }

    protected static Stream<Event> possibleEvents() {
        return Stream.of(
                Events.values(),
                DetailEvents.values(),
                Waits.values(),
                Counts.values(),
                RecordSerializer.Events.values()
        ).flatMap(Arrays::stream);
    }

    static {
        checkEventNameUniqueness(possibleEvents());
    }

    public FDBStoreTimer() {
        super();
    }
}
