/*
 * FDBStoreTimer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;

/**
 * A {@link StoreTimer} events associated with Lucene operations.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.MissingStaticMethodInNonInstantiatableClass")
public class LuceneEvents {
    private LuceneEvents() {
    }

    /**
     * Main events.
     */
    public enum Events implements StoreTimer.Event {
        /** Time to read a block from Lucene's FDBDirectory. */
        LUCENE_READ_BLOCK("lucene block reads"),
        /** Time to read a schema from Lucene's FDBDirectory. */
        LUCENE_READ_SCHEMA("lucene schema read"),
        /** Time to read stored fields from Lucene's FDBDirectory. */
        LUCENE_READ_STORED_FIELDS("lucene stored fields read"),
        /** Time to read a lucene block from FBB loader. */
        LUCENE_FDB_READ_BLOCK("lucene read from fdb"),
        /** Time to list all files from Lucene's FDBDirectory. */
        LUCENE_LIST_ALL("lucene list all"),
        /** Time to load the file cache for Lucene's FDBDirectory. */
        LUCENE_LOAD_FILE_CACHE("lucene load file cache"),
        /** Number of file length calls in the FDBDirectory. */
        LUCENE_GET_FILE_LENGTH("lucene get file length"),
        /** Number of documents returned from a single Lucene Index Scan. */
        LUCENE_INDEX_SCAN("lucene search returned documents"),
        /** Number of suggestions returned from a single Lucene Auto Complete Scan. */
        LUCENE_AUTO_COMPLETE_SUGGESTIONS_SCAN("lucene search returned auto complete suggestions"),
        /** Number of documents returned from a single Lucene spellcheck scan. */
        LUCENE_SPELLCHECK_SCAN("lucene search returned spellcheck suggestions"),
        /** Number of times new document is added. */
        LUCENE_ADD_DOCUMENT("lucene add document"),
        /** Number of times query is needed for document delete. */
        LUCENE_DELETE_DOCUMENT_BY_QUERY("lucene delete document by query"),
        /** Number of times query is needed for document delete in WriteOnly mode. */
        LUCENE_DELETE_DOCUMENT_BY_QUERY_IN_WRITE_ONLY_MODE("lucene delete document by query in WRITE_ONLY mode"),
        /** Number of times primary key index used for document delete. */
        LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY("lucene delete document by primary key"),
        /** Number of merge calls to the FDBDirectory. */
        LUCENE_MERGE("Lucene merge"),
        /** Number of find merge calls (calculation of lucene's required merges). */
        LUCENE_FIND_MERGES("Lucene find merges"),
        /**
         * Amount of time spent in a transaction doing partition rebalancing.
         */
        LUCENE_REBALANCE_PARTITION_TRANSACTION("Lucene rebalance partition transaction"),
        /**
         * Amount of time spent moving documents during partition rebalancing.
         */
        LUCENE_REBALANCE_PARTITION("Lucene rebalance partition"),
        /** The duration of which the file lock was taken, in nanoseconds. */
        LUCENE_FILE_LOCK_DURATION("Lucene lock duration")
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
     * Detail events.
     */
    @SuppressWarnings("squid:S1144")    // Until there are some actual detail events.
    public enum DetailEvents implements StoreTimer.DetailEvent {
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
     * Wait events.
     */
    public enum Waits implements StoreTimer.Wait {
        /** Wait to delete a file from Lucene's FDBDirectory.*/
        WAIT_LUCENE_DELETE_FILE("lucene delete file"),
        /** Wait to get the length of the a file in Lucene's FDBDirectory.*/
        WAIT_LUCENE_FILE_LENGTH("lucene file length"),
        /** Wait to rename a file in Lucene's FDBDirectory.*/
        WAIT_LUCENE_RENAME("lucene rename"),
        /** Wait to get a new file counter increment. */
        WAIT_LUCENE_GET_INCREMENT("lucene file counter increment"),
        /** Wait to read a file reference. */
        WAIT_LUCENE_GET_FILE_REFERENCE("lucene get file reference"),
        /** Wait to read schema. */
        WAIT_LUCENE_GET_SCHEMA("lucene get schema"),
        /** Wait to read all stored fields for a segment. */
        WAIT_LUCENE_GET_ALL_STORED_FIELDS("lucene get stored all fields"),
        /** Wait to read stored fields. */
        WAIT_LUCENE_GET_STORED_FIELDS("lucene get stored fields"),
        /** Wait to read a data block. */
        WAIT_LUCENE_GET_DATA_BLOCK("lucene get data block"),
        /** Wait for lucene to load the file cache. */
        WAIT_LUCENE_LOAD_FILE_CACHE("lucene load file cache"),
        /** Create a file from FDBDirectory. */
        WAIT_LUCENE_CREATE_OUTPUT("lucene create output"),
        /** Look up primary key segment. */
        WAIT_LUCENE_FIND_PRIMARY_KEY("lucene find primary key"),
        /** Read the field infos data. */
        WAIT_LUCENE_READ_FIELD_INFOS("lucene read field infos"),
        /** Set a file lock. */
        WAIT_LUCENE_FILE_LOCK_SET("lucene set file lock"),
        /** Get a file lock. */
        WAIT_LUCENE_FILE_LOCK_GET("lucene get file lock"),
        /** Clear a file lock. */
        WAIT_LUCENE_FILE_LOCK_CLEAR("lucene clear file lock"),
        /** read the ongoing merge indicator. */
        WAIT_LUCENE_READ_ONGOING_MERGE_INDICATOR("lucene read the ongoing merge indicator"),
        /** Wait to drain the pending queue. */
        WAIT_LUCENE_DRAIN_PENDING_QUEUE("lucene pending queue drain"),
        /** Wait to for the pending queue. */
        WAIT_LUCENE_READ_PENDING_QUEUE("lucene pending queue get item(s)"),
        /** Count number of entries in pending writes queue. */
        WAIT_COUNT_QUEUE_ITEMS("lucene count entries in queue"),
        /** Replay items from queue in a read-only transaction. */
        WAIT_LUCENE_REPLAY_QUEUE("lucene replay pending writes queue");
        ;
        private final String title;
        private final String logKey;

        Waits(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.Wait.super.logKey();
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
     * Count events.
     */
    public enum Counts implements StoreTimer.Count {
        /** Number of times the getIncrement() function is called in the FDBDirectory. */
        LUCENE_GET_INCREMENT_CALLS("lucene increments", false),
        /** The number of block reads that occur against the FDBDirectory.*/
        LUCENE_BLOCK_READS("lucene block reads", false),
        /** The number of block writes that occur against the FDBDirectory.*/
        LUCENE_BLOCK_WRITES("lucene block writes", false),
        /** Matched documents returned from lucene index reader scans. **/
        LUCENE_SCAN_MATCHED_DOCUMENTS("lucene scan matched documents", false),
        /** Matched auto complete suggestions returned from lucene auto complete suggestion lookup. **/
        LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS("lucene scan matched auto complete suggestions", false),
        /** Matched spellchecker suggestions returned from lucene spellchecker suggestion lookup. **/
        LUCENE_SCAN_SPELLCHECKER_SUGGESTIONS("lucene scan matched spellchecker suggestions", false),
        /** Block to read came from shared cache. **/
        LUCENE_SHARED_CACHE_HITS("lucene shared cache hits", false),
        /** Block to read came not in shared cache. **/
        LUCENE_SHARED_CACHE_MISSES("lucene shared cache misses", false),
        /** Plan contains highlight operator. **/
        PLAN_HIGHLIGHT_TERMS("lucene highlight plans", false),
        /** Number of file delete operations on the FDBDirectory. */
        LUCENE_DELETE_FILE("lucene delete file", false),
        /** Number of rename file operations on the FDBDirectory. */
        LUCENE_RENAME_FILE("lucene rename file", false),
        /** Number of documents merged. */
        LUCENE_MERGE_DOCUMENTS("lucene merge document", false),
        /** Number of segments merged. */
        LUCENE_MERGE_SEGMENTS("lucene merge segment", false),
        /** Number of Delete Stored Fields operations on the FDBDirectory. */
        LUCENE_DELETE_STORED_FIELDS("lucene delete stored fields", false),
        /** Number of agile context commits after exceeding size quota. */
        LUCENE_AGILE_COMMITS_SIZE_QUOTA("lucene agile commits size quota", false),
        /** Number of agile context commits after exceeding time quota. */
        LUCENE_AGILE_COMMITS_TIME_QUOTA("lucene agile commits time quota", false),
        /** Count of times a rebalance was called. */
        LUCENE_REPARTITION_CALLS("Count of Lucene repartition calls", false),
        /** Count of the number of times a block was removed from the block cache. */
        LUCENE_BLOCK_CACHE_REMOVE("Count of blocks removed from cache", false),
        /** Count of the number of Writes to the pending writes queue. */
        LUCENE_PENDING_QUEUE_WRITE("Count of Lucene Pending writes queue Writes", false),
        /** Count of the number of Clears in the pending writes queue. */
        LUCENE_PENDING_QUEUE_CLEAR("Count of Lucene Pending writes queue Clears", false);

        private final String title;
        private final boolean isSize;
        private final String logKey;
        private final boolean delayedUntilCommit;

        Counts(String title, boolean isSize, String logKey, boolean delayedUntilCommit) {
            this.title = title;
            this.isSize = isSize;
            this.logKey = (logKey != null) ? logKey : StoreTimer.Count.super.logKey();
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
        @Nonnull
        public String logKey() {
            return this.logKey;
        }

        @Override
        public boolean isDelayedUntilCommit() {
            return delayedUntilCommit;
        }

        @Override
        public boolean isSize() {
            return isSize;
        }
    }

    /**
     * Size Events.
     */
    public enum SizeEvents implements StoreTimer.SizeEvent {

        /** writeFileReference operation in the FDBDirectory.*/
        LUCENE_WRITE_FILE_REFERENCE("lucene write file references"),
        /** writeData operation in FDBDirectory. */
        LUCENE_WRITE("lucene index writes", true),
        /** Write Stored Fields operations on the FDBDirectory. */
        LUCENE_WRITE_STORED_FIELDS("lucene write stored fields"),
        /**
         * The number of docs moved during each transaction as part of partition rebalance.
         */
        LUCENE_REBALANCE_PARTITION_DOCS("lucene rebalance partition count"),
        /** Size of the list of files loaded in loadFileReferenceCacheForMemoization operation in FDBDirectory. */
        LUCENE_FILES_COUNT("lucene files"),
        ;

        private final String title;
        private final boolean delayedUntilCommit;

        SizeEvents(@Nonnull String title) {
            this(title, false);
        }

        SizeEvents(@Nonnull String title, boolean delayedUntilCommit) {
            this.title = title;
            this.delayedUntilCommit = delayedUntilCommit;
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        public boolean isDelayedUntilCommit() {
            return delayedUntilCommit;
        }
    }
}
