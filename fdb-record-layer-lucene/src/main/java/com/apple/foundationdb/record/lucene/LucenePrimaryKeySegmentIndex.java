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
    List<String> findSegments(@Nonnull Tuple primaryKey);

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
    DocumentIndexEntry findDocument(@Nonnull DirectoryReader directoryReader, @Nonnull Tuple primaryKey);

    void addOrDeletePrimaryKeyEntry(@Nonnull byte[] primaryKey, long segmentId, int docId, boolean add);

    void clearForSegment(String segmentName) throws IOException;

    /**
     * Result of {@link #findDocument}.
     */
    // TODO: Can be a record.
    public static class DocumentIndexEntry {
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
