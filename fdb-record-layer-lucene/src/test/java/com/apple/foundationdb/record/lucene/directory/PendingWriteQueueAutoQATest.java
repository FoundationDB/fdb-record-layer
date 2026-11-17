/*
 * PendingWriteQueueAutoQATest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional edge-case tests for {@link PendingWriteQueue}.
 */
@Tag(Tags.RequiresFDB)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class PendingWriteQueueAutoQATest extends FDBRecordStoreTestBase {
    private static final Index INDEX = new Index("pendingQueueIndex", field("text"), LuceneIndexTypes.LUCENE);

    private LuceneSerializer serializer;

    @BeforeEach
    void setup() {
        serializer = new LuceneSerializer(true, false, null, true);
    }

    @Test
    void replayQueuedOperationsExactlyAtLimitSucceeds() throws Exception {
        // BUG AQA-11: replay fails with RETURN_LIMIT_REACHED after exactly maxEntriesToReplay rows, even if no more
        // entries exist. Spec: PendingWriteQueue.maxEntriesToReplay javadoc, fails if the queue "has more than that
        // number of entries".
        // a queue holding exactly maxEntriesToReplay entries is not "more than" the limit, so replay must succeed
        final int maxEntriesToReplay = 2;
        final PendingWriteQueue queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, maxEntriesToReplay);
            enqueueDeletes(queue, context, maxEntriesToReplay);
            commit(context);
        }

        try (FDBRecordContext context = openContext();
                ByteBuffersDirectory directory = new ByteBuffersDirectory();
                IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            queue.replayQueuedOperations(context, writer, INDEX).join();
        }
    }

    @Test
    void getQueueCursorWithSkipSkipsEntries() {
        // BUG AQA-12: getQueueCursor clears the skip on the inner cursor and never applies it. Spec:
        // PendingWriteQueue.getQueueCursor javadoc, "row/byte/skip limits are still honored at the entry level".
        // the javadoc promises that the skip of the scan properties is honored at the entry level
        final PendingWriteQueue queue;
        final List<Tuple> primaryKeys;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context, PendingWriteQueue.DEFAULT_MAX_PENDING_ENTRIES_TO_REPLAY);
            primaryKeys = enqueueDeletes(queue, context, 5);
            commit(context);
        }

        final ScanProperties scanProperties = ExecuteProperties.newBuilder()
                .setSkip(2)
                .build()
                .asScanProperties(false);
        try (FDBRecordContext context = openContext()) {
            final List<Tuple> actual = queue.getQueueCursor(context, scanProperties, null).asList().join().stream()
                    .map(PendingWriteQueue.QueueEntry::getPrimaryKeyParsed)
                    .collect(Collectors.toList());
            assertThat(actual).isEqualTo(primaryKeys.subList(2, 5));
        }
    }

    @Nonnull
    private static List<Tuple> enqueueDeletes(@Nonnull PendingWriteQueue queue, @Nonnull FDBRecordContext context, int count) {
        final List<Tuple> primaryKeys = IntStream.range(0, count)
                .mapToObj(i -> Tuple.from("pk", i))
                .collect(Collectors.toList());
        primaryKeys.forEach(primaryKey -> queue.enqueueDelete(context, primaryKey, 0));
        return primaryKeys;
    }

    private PendingWriteQueue getQueue(FDBRecordContext context, int maxEntriesToReplay) {
        return getQueue(context, maxEntriesToReplay, serializer);
    }

    private PendingWriteQueue getQueue(FDBRecordContext context, int maxEntriesToReplay, LuceneSerializer serializerToUse) {
        Subspace queueSpace = path.toSubspace(context).subspace(Tuple.from(0));
        Subspace counterSpace = path.toSubspace(context).subspace(Tuple.from(1));
        return new PendingWriteQueue(queueSpace, counterSpace,
                maxEntriesToReplay,
                PendingWriteQueue.DEFAULT_MAX_PENDING_QUEUE_SIZE,
                serializerToUse, true);
    }

    private static class PassThroughLuceneSerializer extends LuceneSerializer {
        PassThroughLuceneSerializer() {
            super(true, false, null, true);
        }

        @Override
        public byte[] encode(byte[] data) {
            return data;
        }

        @Override
        public byte[] decode(byte[] data) {
            return data;
        }
    }
}
