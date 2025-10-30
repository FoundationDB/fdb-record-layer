/*
 * LucenePrimaryKeySegmentIndex.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * Maintain a B-tree index of primary key to segment and doc id.
 * This allows for efficient deleting of a document given that key, such as when doing index maintenance from an update.
 */
public interface LucenePrimaryKeySegmentIndex {
    @VisibleForTesting
    List<List<Object>> readAllEntries();

    @SuppressWarnings("PMD.CloseResource")
    List<String> findSegments(@Nonnull Tuple primaryKey) throws IOException;

    /**
     * Find document in index for direct delete.
     *
     * @param directoryReader a NRT reader
     * @param primaryKey the document's record's primary key
     *
     * @return an entry with the leaf reader and document id in that segment or {@code null} if not found
     *
     * @see IndexWriter#tryDeleteDocument
     */
    @Nullable
    DocumentIndexEntry findDocument(@Nonnull DirectoryReader directoryReader, @Nonnull Tuple primaryKey) throws IOException;

    /**
     * Add or delete the primary key/segment/docId from the index.
     * @param primaryKey the primary ey of the record
     * @param segmentId the id of the segment (see {@link com.apple.foundationdb.record.lucene.directory.FDBDirectory#primaryKeySegmentId})
     * @param docId the document id within the segment
     * @param add whether to add ({@code true}) or delete ({@code false}) the entry
     * @param segmentName name associated with the segment, for logging
     */
    void addOrDeletePrimaryKeyEntry(@Nonnull byte[] primaryKey, long segmentId, int docId, boolean add, String segmentName);

    /**
     * Clears all the primary key entries for a given segment name.
     * @param segmentName the name of the segment to clear out
     * @throws IOException if the primary keys cannot be parsed from stored fields
     */
    void clearForSegment(String segmentName) throws IOException;

    /**
     * Clear all entries for the given primary key.
     * Note that this operation will clear all entries for the given
     * PK - regardless of how many (if any) are present.
     * @param primaryKey the record primary key to clear
     */
    void clearForPrimaryKey(Tuple primaryKey);

    /**
     * Result of {@link #findDocument}.
     */
    // TODO: Can be a record.
    class DocumentIndexEntry {
        @Nonnull
        public final Tuple primaryKey;
        @Nonnull
        public final byte[] entryKey;
        @Nonnull
        public final IndexReader indexReader;
        @Nonnull
        public final String segmentName;
        public final int docId;

        public DocumentIndexEntry(@Nonnull final Tuple primaryKey, @Nonnull final byte[] entryKey, @Nonnull final IndexReader indexReader,
                                  @Nonnull String segmentName, final int docId) {
            this.primaryKey = primaryKey;
            this.entryKey = entryKey;
            this.indexReader = indexReader;
            this.segmentName = segmentName;
            this.docId = docId;
        }
    }
}
