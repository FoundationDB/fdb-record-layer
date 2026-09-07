/*
 * LucenePrimaryKeySegmentIndexV2.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedStoredFieldsReader;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Maintain a B-tree index of primary key to segment and doc id.
 * This allows for efficient deleting of a document given that key, such as when doing index maintenance from an update.
 */
public class LucenePrimaryKeySegmentIndexV2 implements LucenePrimaryKeySegmentIndex {

    private static final Logger LOGGER = LoggerFactory.getLogger(LucenePrimaryKeySegmentIndexV2.class);
    private final FDBDirectory directory;
    private final Subspace subspace;

    public LucenePrimaryKeySegmentIndexV2(FDBDirectory directory, Subspace subspace) {
        this.directory = directory;
        this.subspace = subspace;
    }

    @Override
    @VisibleForTesting
    public List<List<Object>> readAllEntries() {
        AtomicReference<List<List<Object>>> list = new AtomicReference<>();
        directory.getAgilityContext().accept(aContext -> readAllEntries(aContext, list));
        // The accept() call above runs synchronously, and readAllEntries() always populates list before returning.
        return Objects.requireNonNull(list.get());
    }

    private void readAllEntries(FDBRecordContext aContext, AtomicReference<List<List<Object>>> list) {
        List<Tuple> tuples;
        try (KeyValueCursor kvs = KeyValueCursor.Builder.newBuilder(subspace)
                .setContext(aContext)
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build();
                 RecordCursor<Tuple> entries = kvs.map(kv -> subspace.unpack(kv.getKey()))) {
            tuples = Objects.requireNonNull(LuceneConcurrency.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FIND_PRIMARY_KEY, entries.asList(), aContext));
        }
        list.set(tuples.stream().map(t -> {
            List<Object> items = t.getItems();
            // Replace segment id with segment name to make inspection easier.
            String name = directory.primaryKeySegmentName((Long)items.get(items.size() - 2));
            if (name != null) {
                items.set(items.size() - 2, name);
            }
            items.set(items.size() - 1, ((Long)items.get(items.size() - 1)).intValue());
            return items;
        }).collect(Collectors.toList()));
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public List<String> findSegments(Tuple primaryKey) throws IOException {
        try {
            return Objects.requireNonNull(directory.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FIND_PRIMARY_KEY,
                    directory.getAgilityContext().apply(context -> {
                        final Subspace keySubspace = subspace.subspace(primaryKey);
                        final KeyValueCursor kvs = KeyValueCursor.Builder.newBuilder(keySubspace)
                                .setContext(context)
                                .setScanProperties(ScanProperties.FORWARD_SCAN)
                                .build();
                        return kvs.map(kv -> {
                            final Tuple segdoc = keySubspace.unpack(kv.getKey());
                            final long segid = segdoc.getLong(0);
                            final String segmentName = directory.primaryKeySegmentName(segid);
                            if (segmentName != null) {
                                return segmentName;
                            } else {
                                return "#" + segid;
                            }
                        }).asList().whenComplete((result, err) -> kvs.close());
                    })));
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    @Override
    @Nullable
    public DocumentIndexEntry findDocument(DirectoryReader directoryReader, Tuple primaryKey) throws IOException {
        try {
            final AtomicReference<DocumentIndexEntry> doc = new AtomicReference<>();
            directory.getAgilityContext().accept(aContext -> findDocument(aContext, doc, directoryReader, primaryKey));
            return doc.get();
        } catch (RecordCoreException ex) {
            throw LuceneExceptions.toIoException(ex, null);
        }
    }

    private void findDocument(FDBRecordContext aContext, AtomicReference<DocumentIndexEntry> doc,
                                            DirectoryReader directoryReader, Tuple primaryKey) {
        final SegmentInfos segmentInfos = ((StandardDirectoryReader)FilterDirectoryReader.unwrap(directoryReader)).getSegmentInfos();
        final Subspace keySubspace = subspace.subspace(primaryKey);
        try (KeyValueCursor kvs = KeyValueCursor.Builder.newBuilder(keySubspace)
                .setContext(aContext)
                .setScanProperties(ScanProperties.FORWARD_SCAN)
                .build();
                RecordCursor<DocumentIndexEntry> documents = kvs.map(kv -> {
                    final Tuple segdoc = keySubspace.unpack(kv.getKey());
                    final long segid = segdoc.getLong(0);
                    final String segmentName = directory.primaryKeySegmentName(segid);
                    if (segmentName == null) {
                        return Optional.<DocumentIndexEntry>empty();
                    }
                    for (int i = 0; i < segmentInfos.size(); i++) {
                        SegmentInfo segmentInfo = segmentInfos.info(i).info;
                        if (segmentInfo.name.equals(segmentName)) {
                            final int docid = (int)segdoc.getLong(1);
                            return Optional.of(new DocumentIndexEntry(primaryKey, kv.getKey(),
                                    directoryReader.leaves().get(i).reader(), segmentName, docid));
                        }
                    }
                    return Optional.<DocumentIndexEntry>empty();
                }).filter(Optional::isPresent).map(Optional::get)) {
            doc.set(Objects.requireNonNull(directory.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_FIND_PRIMARY_KEY,
                    documents.first())).orElse(null));
        }
    }

    @Override
    public void addOrDeletePrimaryKeyEntry(byte[] primaryKey, long segmentId, int docId, boolean add, String segmentName) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("pkey " + (add ? "Adding" : "Deling") + " #" + segmentId + "(" + segmentName + ")" +  Tuple.fromBytes(primaryKey));
        }
        final byte[] entryKey = ByteArrayUtil.join(subspace.getKey(), primaryKey, Tuple.from(segmentId, docId).pack());
        if (add) {
            directory.getAgilityContext().set(entryKey, new byte[0]);
        } else {
            directory.getAgilityContext().clear(entryKey);
        }
    }

    @Override
    public void clearForSegment(final String segmentName) throws IOException {
        final List<byte[]> primaryKeys = LuceneOptimizedStoredFieldsReader.getPrimaryKeys(segmentName, directory);
        final long segmentId = directory.primaryKeySegmentId(segmentName, true);
        for (final byte[] primaryKey : primaryKeys) {
            final byte[] entryKey = ByteArrayUtil.join(subspace.getKey(), primaryKey, Tuple.from(segmentId).pack());
            directory.getAgilityContext().clear(Range.startsWith(entryKey));
        }
    }
}
