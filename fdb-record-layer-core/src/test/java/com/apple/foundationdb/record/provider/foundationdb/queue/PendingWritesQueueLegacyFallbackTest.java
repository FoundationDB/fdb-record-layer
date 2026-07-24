/*
 * PendingWritesQueueLegacyFallbackTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.queue;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.record.PendingWritesQueueTestProto.TestQueuePayload;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.InvalidProtocolBufferException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the {@link PendingWritesQueue} legacy-decoder fallback: a queue can read a mix of
 * current-format entries (a versioned envelope with an {@code Any} payload, written by
 * {@link PendingWritesQueue#enqueue}) and "legacy" entries written by a predecessor
 * implementation in a different value layout. On read, an entry that is not the current format
 * (parse fails, or it has no {@code version >= 1}) is handed to the legacy decoder.
 *
 * <p>Here the simulated legacy layout is a directly-serialized {@link TestQueuePayload}, optionally prefixed with a
 * one-byte stand-in for such a serializer; the decoder parses it straight back into a
 * {@link TestQueuePayload}.</p>
 */
@Tag(Tags.RequiresFDB)
class PendingWritesQueueLegacyFallbackTest extends FDBRecordStoreTestBase {

    /** Legacy layout: a directly-serialized {@link TestQueuePayload}, parsed straight back. */
    private static final Function<byte[], TestQueuePayload> BARE_LEGACY_DECODER = raw -> {
        try {
            return TestQueuePayload.parseFrom(raw);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    };

    /** Legacy layout: a one-byte {@code 0x00} prefix (a stand-in serializer) + serialized {@link TestQueuePayload}. */
    private static final Function<byte[], TestQueuePayload> PREFIXED_LEGACY_DECODER = raw -> {
        try {
            return TestQueuePayload.parseFrom(Arrays.copyOfRange(raw, 1, raw.length));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }
    };

    /**
     * A queue reads current-format and legacy (version-0, still-parseable) entries interleaved,
     * in versionstamp order, surfacing each as the bound payload type.
     */
    @Test
    void readsMixOfNewAndLegacyEntries() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
            queue.enqueue(context, payload("new-0"), 0).join();
            writeRawLegacyEntry(context, bareLegacy("legacy-1"));
            queue.enqueue(context, payload("new-2"), 0).join();
            commit(context);
        }

        List<PendingWritesQueueEntry<TestQueuePayload>> entries = readAll(queue, BARE_LEGACY_DECODER);
        assertEquals(3, entries.size());
        assertEquals("new-0", entries.get(0).getPayload().getLabel());
        assertEquals("legacy-1", entries.get(1).getPayload().getLabel()); // via the legacy decoder
        assertEquals("new-2", entries.get(2).getPayload().getLabel());
    }

    /**
     * A legacy entry whose bytes do not even parse as the envelope (the stand-in serializer
     * prefix is an invalid protobuf tag) still routes to the legacy decoder.
     */
    @Test
    void legacyEntryThatFailsEnvelopeParseUsesDecoder() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
            writeRawLegacyEntry(context, prefixedLegacy("wrapped-legacy"));
            queue.enqueue(context, payload("new"), 0).join();
            commit(context);
        }

        List<PendingWritesQueueEntry<TestQueuePayload>> entries = readAll(queue, PREFIXED_LEGACY_DECODER);
        assertEquals(2, entries.size());
        assertEquals("wrapped-legacy", entries.get(0).getPayload().getLabel());
        assertEquals("new", entries.get(1).getPayload().getLabel());
    }

    /**
     * A non-current-format entry read without a legacy decoder is a storage error.
     */
    @Test
    void nonCurrentEntryWithoutLegacyDecoderThrows() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
            writeRawLegacyEntry(context, bareLegacy("orphan"));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            Assertions.assertThatThrownBy(
                            () -> queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null).asList().join())
                    .hasCauseInstanceOf(RecordCoreStorageException.class);
        }
    }

    /**
     * An entry written with an old-format key that lacks the incarnation prefix (a size-1
     * versionstamp-only key) is rejected on read.
     */
    @Test
    void entryWithoutIncarnationInKeyThrows() {
        PendingWritesQueue<TestQueuePayload> queue;
        try (FDBRecordContext context = openContext()) {
            queue = getQueue(context);
            final FDBRecordVersion recordVersion = FDBRecordVersion.incomplete(context.claimLocalVersion());
            // Old-format key: versionstamp only, with no incarnation prefix.
            writeRawEntry(context, bareLegacy("no-incarnation"), Tuple.from(recordVersion.toVersionstamp()));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            Assertions.assertThatThrownBy(
                            () -> queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null, BARE_LEGACY_DECODER).asList().join())
                    .hasCauseInstanceOf(RecordCoreStorageException.class);
        }
    }

    private void writeRawLegacyEntry(@Nonnull FDBRecordContext context, @Nonnull byte[] rawValue) {
        final FDBRecordVersion recordVersion = FDBRecordVersion.incomplete(context.claimLocalVersion());
        writeRawEntry(context, rawValue, Tuple.from(0, recordVersion.toVersionstamp()));
    }

    private void writeRawEntry(@Nonnull FDBRecordContext context, @Nonnull byte[] rawValue, @Nonnull Tuple keyTuple) {
        Subspace queueSubspace = queueSubspaceFor(context);
        SplitHelper.saveWithSplit(context, queueSubspace, keyTuple, rawValue, null, true, false, false, null, null);
        // Keep the size counter consistent with the entry we wrote directly.
        context.ensureActive().mutate(MutationType.ADD, counterSubspaceFor(context).pack(),
                new byte[] {1, 0, 0, 0, 0, 0, 0, 0});
    }

    @Nonnull
    private static byte[] bareLegacy(@Nonnull String text) {
        return TestQueuePayload.newBuilder().setLabel(text).build().toByteArray();
    }

    @Nonnull
    private static byte[] prefixedLegacy(@Nonnull String text) {
        byte[] bare = bareLegacy(text);
        byte[] prefixed = new byte[bare.length + 1];
        prefixed[0] = 0x00; // an invalid protobuf tag, so the envelope parse fails
        System.arraycopy(bare, 0, prefixed, 1, bare.length);
        return prefixed;
    }

    @Nonnull
    private PendingWritesQueue<TestQueuePayload> getQueue(@Nonnull FDBRecordContext context) {
        return new PendingWritesQueue<>(queueSubspaceFor(context), counterSubspaceFor(context), 0,
                TestQueuePayload.class);
    }

    @Nonnull
    private Subspace queueSubspaceFor(@Nonnull FDBRecordContext context) {
        return path.toSubspace(context).subspace(Tuple.from("queue"));
    }

    @Nonnull
    private Subspace counterSubspaceFor(@Nonnull FDBRecordContext context) {
        return path.toSubspace(context).subspace(Tuple.from("counter"));
    }

    @Nonnull
    private List<PendingWritesQueueEntry<TestQueuePayload>> readAll(
            @Nonnull PendingWritesQueue<TestQueuePayload> queue,
            @Nonnull Function<byte[], TestQueuePayload> legacyDecoder) {
        try (FDBRecordContext context = openContext()) {
            return queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null, legacyDecoder).asList().join();
        }
    }

    @Nonnull
    private static TestQueuePayload payload(@Nonnull String label) {
        return TestQueuePayload.newBuilder().setLabel(label).build();
    }
}
