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
        /** Time to read a block from Lucene's FDBDirectory.*/
        LUCENE_READ_BLOCK("lucene block reads"),
        /** Time to read a lucene block from FBB loader. */
        LUCENE_FDB_READ_BLOCK("lucene read from fdb"),
        /** Time to list all files from Lucene's FDBDirectory.*/
        LUCENE_LIST_ALL("lucene list all"),
        /** Number of getFileReference calls in the FDBDirectory.*/
        LUCENE_GET_FILE_REFERENCE("lucene get file references"),
        /** Number of documents returned from a single Lucene Index Scan. */
        LUCENE_INDEX_SCAN("lucene search returned documents"),
        /** Number of suggestions returned from a single Lucene Auto Complete Scan. */
        LUCENE_AUTO_COMPLETE_SUGGESTIONS_SCAN("lucene search returned auto complete suggestions"),
        /** Number of documents returned from a single Lucene spellcheck scan. */
        LUCENE_SPELLCHECK_SCAN("lucene search returned spellcheck suggestions")
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
        /** Time to get the length of the a file in Lucene's FDBDirectory.*/
        WAIT_LUCENE_FILE_LENGTH("lucene file length"),
        /** Time to rename a file in Lucene's FDBDirectory.*/
        WAIT_LUCENE_RENAME("lucene rename"),
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
        LUCENE_GET_INCREMENT_CALLS("lucene increments",false),
        /** Number of writeFileReference calls in the FDBDirectory.*/
        LUCENE_WRITE_FILE_REFERENCE_CALL("lucene write file references",false),
        /** Total number of bytes that were attempted to be written (not necessarily committed) for file references in the FDBDirectory. */
        LUCENE_WRITE_FILE_REFERENCE_SIZE("lucene write file reference size", true),
        /** Count of writeData calls in FDBDirectory. */
        LUCENE_WRITE_CALL("lucene index writes", false),
        /** Total number of bytes that were attempted to be written (not necessarily committed) to the FDBDirectory.*/
        LUCENE_WRITE_SIZE("lucene index size",true),
        /** The number of block reads that occur against the FDBDirectory.*/
        LUCENE_BLOCK_READS("lucene block reads", false),
        /** Time to write a file references in Lucene's FDBDirectory.*/
        LUCENE_WRITE_FILE_REFERENCE("lucene write file reference" ,false),
        /** Matched documents returned from lucene index reader scans. **/
        LUCENE_SCAN_MATCHED_DOCUMENTS("lucene scan matched documents", false),
        /** Matched auto complete suggestions returned from lucene auto complete suggestion lookup. **/
        LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS("lucene scan matched auto complete suggestions", false),
        /** Matched spellchecker suggestions returned from lucene spellchecker suggestion lookup. **/
        LUCENE_SCAN_SPELLCHECKER_SUGGESTIONS("lucene scan matched spellchecker suggestions", false),
        ;

        private final String title;
        private final boolean isSize;
        private final String logKey;

        Counts(String title, boolean isSize, String logKey) {
            this.title = title;
            this.isSize = isSize;
            this.logKey = (logKey != null) ? logKey : StoreTimer.Count.super.logKey();
        }

        Counts(String title, boolean isSize) {
            this(title, isSize, null);
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
        public boolean isSize() {
            return isSize;
        }
    }
}
