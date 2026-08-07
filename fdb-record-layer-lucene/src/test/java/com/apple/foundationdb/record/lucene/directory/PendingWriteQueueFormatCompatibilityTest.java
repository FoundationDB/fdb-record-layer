/*
 * PendingWriteQueueFormatCompatibilityTest.java
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

import com.apple.foundationdb.record.PendingWritesQueueProto;
import com.apple.foundationdb.record.lucene.LucenePendingWriteQueueProto;
import com.apple.foundationdb.record.provider.common.FixedZeroKeyManager;
import com.apple.foundationdb.record.util.RandomSecretUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.SecretKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * De-risking tests for the Lucene pending-write-queue format migration (Plan #1).
 *
 * <p>The migration stores new-format entries as a plain envelope proto (discriminated by the
 * {@code version} field, &ge;1) and leaves legacy entries exactly as the old queue wrote them
 * ({@code LuceneSerializer.encode(proto)}). The reader parses each entry directly as the new
 * envelope and, only when that fails or yields a non-new version, falls back to the serializer +
 * the legacy descriptor — so new writes are never forced through the serializer.
 * These tests are pure-protobuf (no FDB) and verify the wire-level assumptions the plan relies
 * on:</p>
 * <ul>
 *   <li><b>E1</b> — legacy bytes (fields 1-4) parse cleanly as the new envelope
 *       ({@link PendingWritesQueueProto.PendingWriteItem}, which reserves 1-4 and now has all
 *       fields {@code optional}) without throwing, and report {@code version == 0} with no
 *       {@code payload}; the
 *       legacy content survives as retained unknown fields.</li>
 *   <li>the legacy bytes remain decodable with the legacy descriptor (the reader's legacy
 *       fallback);</li>
 *   <li><b>E3</b> — a new entry round-trips through {@code Any.pack}/{@code unpack};</li>
 *   <li>new bytes are NOT parseable by the legacy descriptor (justifies the read-before-write
 *       gate — an old binary must never see a new-format entry);</li>
 *   <li>new-format entries parse directly with no serializer; legacy (serializer-wrapped)
 *       entries trigger the serializer fallback; and one reader recovers a stream mixing both,
 *       across all compression/encryption modes.</li>
 * </ul>
 */
class PendingWriteQueueFormatCompatibilityTest {

    private static final int NEW_FORMAT_VERSION = 1;

    /**
     * E1: legacy bytes parse as the new envelope without throwing, carry no {@code payload}, and
     * keep the legacy fields as unknown fields.
     */
    @Test
    void legacyBytesParseAsNewEnvelopeWithoutPayload() throws InvalidProtocolBufferException {
        byte[] legacyBytes = legacyInsert("record-1", "hello").toByteArray();

        // This is the crux: with the envelope's fields relaxed to optional, parsing legacy bytes
        // (which have none of fields 10-12) must not throw on missing required fields.
        PendingWritesQueueProto.PendingWriteItem envelope =
                PendingWritesQueueProto.PendingWriteItem.parseFrom(legacyBytes);

        assertThat(envelope.hasPayload()).as("legacy entry has no Any payload").isFalse();
        assertThat(envelope.hasVersion()).as("legacy entry has no version field").isFalse();
        assertThat(envelope.getVersion()).as("absent version defaults to 0").isEqualTo(0);
        assertThat(envelope.hasEnqueueTimestamp()).isFalse();

        // The legacy fields (1-4) are retained as unknown fields rather than lost or erroring.
        assertThat(envelope.getUnknownFields().hasField(1)).as("operation_type retained").isTrue();
        assertThat(envelope.getUnknownFields().hasField(2)).as("primary_key retained").isTrue();
        assertThat(envelope.getUnknownFields().hasField(3)).as("enqueue_timestamp retained").isTrue();
        assertThat(envelope.getUnknownFields().hasField(4)).as("fields retained").isTrue();
    }

    /**
     * The reader's legacy fallback: the same raw bytes are still a valid legacy message, so
     * decoding them with the legacy descriptor recovers the original operation.
     */
    @Test
    void legacyBytesRemainDecodableWithLegacyDescriptor() throws InvalidProtocolBufferException {
        LucenePendingWriteQueueProto.PendingWriteItem original = legacyInsert("record-2", "world");
        byte[] legacyBytes = original.toByteArray();

        LucenePendingWriteQueueProto.PendingWriteItem roundTripped =
                LucenePendingWriteQueueProto.PendingWriteItem.parseFrom(legacyBytes);

        assertThat(roundTripped).isEqualTo(original);
    }

    /**
     * E3: a new entry wraps the (Lucene) payload in an {@code Any}, and round-trips through
     * serialize → parse → unpack.
     */
    @Test
    void newEnvelopeRoundTripsThroughAnyPayload() throws InvalidProtocolBufferException {
        LucenePendingWriteQueueProto.PendingWriteItem payload = legacyInsert("record-3", "payload");
        byte[] newBytes = newEnvelope(payload).toByteArray();

        PendingWritesQueueProto.PendingWriteItem envelope =
                PendingWritesQueueProto.PendingWriteItem.parseFrom(newBytes);

        assertThat(envelope.hasPayload()).as("new entry carries an Any payload").isTrue();
        assertThat(envelope.getVersion()).isEqualTo(NEW_FORMAT_VERSION);
        assertThat(envelope.getPayload().is(LucenePendingWriteQueueProto.PendingWriteItem.class))
                .as("payload type URL matches the Lucene message").isTrue();

        LucenePendingWriteQueueProto.PendingWriteItem unpacked =
                envelope.getPayload().unpack(LucenePendingWriteQueueProto.PendingWriteItem.class);
        assertThat(unpacked).isEqualTo(payload);
    }

    /**
     * New-format bytes must NOT be parseable by the legacy descriptor: the legacy message has
     * required fields 1-3 that a new entry lacks, so {@code parseFrom} throws. This is exactly
     * why an old (pre-migration) binary must never encounter a new-format entry, and hence why
     * new-format writes are gated behind a fleet-wide property.
     */
    @Test
    void newBytesAreNotParseableByLegacyDescriptor() {
        byte[] newBytes = newEnvelope(legacyInsert("record-4", "nope")).toByteArray();

        assertThrows(InvalidProtocolBufferException.class,
                () -> LucenePendingWriteQueueProto.PendingWriteItem.parseFrom(newBytes));
    }

    /**
     * New-format entries are stored as a plain envelope proto (NOT serializer-wrapped), so the
     * reader parses them directly — no serializer needed. This is the common path for an index
     * with no legacy backlog.
     */
    @Test
    void newFormatEntriesParseDirectlyWithoutSerializer() throws InvalidProtocolBufferException {
        List<LucenePendingWriteQueueProto.PendingWriteItem> expected = new ArrayList<>();
        List<byte[]> onDisk = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            LucenePendingWriteQueueProto.PendingWriteItem op = (i % 3 == 0)
                    ? legacyDelete("record-" + i)
                    : legacyInsert("record-" + i, "value-" + i);
            expected.add(op);
            onDisk.add(newEnvelope(op).toByteArray());
        }

        List<LucenePendingWriteQueueProto.PendingWriteItem> decoded = new ArrayList<>();
        for (byte[] raw : onDisk) {
            // No serializer: new entries parse directly, so the fallback is never needed.
            decoded.add(decodeEntry(raw, null));
        }

        assertThat(decoded).containsExactlyElementsOf(expected);
    }

    /**
     * E2: the realistic mixed queue. Legacy entries are stored exactly as the old queue wrote
     * them — {@code serializer.encode(proto)} — while new entries are stored as a plain envelope
     * proto (NOT serializer-wrapped). A single reader parses each entry directly as the new
     * envelope and, only when that fails (or yields a non-new version), falls back to
     * {@link LuceneSerializer#decode} + the legacy descriptor. Exercised across all
     * compression/encryption modes, since only the legacy wrapping varies.
     */
    @ParameterizedTest(name = "compress={0}, encrypt={1}")
    @MethodSource("serializerModes")
    void mixedFormatStreamRoundTripsWithSerializerFallback(boolean compress, boolean encrypt)
            throws InvalidProtocolBufferException {
        final LuceneSerializer serializer = buildSerializer(compress, encrypt);

        final List<LucenePendingWriteQueueProto.PendingWriteItem> expected = new ArrayList<>();
        final List<byte[]> onDisk = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            LucenePendingWriteQueueProto.PendingWriteItem op = (i % 3 == 0)
                    ? legacyDelete("record-" + i)
                    : legacyInsert("record-" + i, "value-" + i);
            expected.add(op);
            // Even indices are LEGACY (serializer-wrapped, as the old queue stored them); odd
            // indices are NEW (plain envelope proto, not wrapped).
            byte[] stored = (i % 2 == 0)
                    ? serializer.encode(op.toByteArray())
                    : newEnvelope(op).toByteArray();
            onDisk.add(stored);
        }
        // Add large, compressible legacy entries so the compression path actually engages.
        LucenePendingWriteQueueProto.PendingWriteItem bigLegacy = legacyInsert("big-legacy", "x".repeat(4096));
        expected.add(bigLegacy);
        onDisk.add(serializer.encode(bigLegacy.toByteArray()));
        LucenePendingWriteQueueProto.PendingWriteItem bigNew = legacyInsert("big-new", "y".repeat(4096));
        expected.add(bigNew);
        onDisk.add(newEnvelope(bigNew).toByteArray());

        final List<LucenePendingWriteQueueProto.PendingWriteItem> decoded = new ArrayList<>();
        for (byte[] stored : onDisk) {
            decoded.add(decodeEntry(stored, serializer));
        }

        assertThat(decoded).containsExactlyElementsOf(expected);
    }

    /**
     * The read-path fallback contract. A new-format entry is a plain envelope proto, so it
     * parses directly (no serializer involved). A legacy entry is {@code serializer.encode}d, so
     * parsing it directly as the new envelope fails — and THAT parse failure is what triggers
     * the reader to fall back to {@link LuceneSerializer#decode} + the legacy descriptor. New
     * entries are never forced through the serializer.
     */
    @Test
    void legacyEntryFallsBackToSerializerWhenEnvelopeParseFails() throws InvalidProtocolBufferException {
        final LuceneSerializer serializer = buildSerializer(false, false);

        // New entry: plain envelope proto, parses directly as the new format.
        final LucenePendingWriteQueueProto.PendingWriteItem newOp = legacyInsert("new-record", "v");
        final byte[] newStored = newEnvelope(newOp).toByteArray();
        assertThat(PendingWritesQueueProto.PendingWriteItem.parseFrom(newStored).getVersion())
                .isEqualTo(NEW_FORMAT_VERSION);
        assertThat(decodeEntry(newStored, serializer)).isEqualTo(newOp);

        // Legacy entry: serializer-wrapped. Parsing it directly as the new envelope fails (even
        // the minimal prefix byte is not a valid protobuf tag), which is the fallback trigger.
        final LucenePendingWriteQueueProto.PendingWriteItem legacyOp = legacyInsert("legacy-record", "v");
        final byte[] legacyStored = serializer.encode(legacyOp.toByteArray());
        assertThrows(InvalidProtocolBufferException.class,
                () -> PendingWritesQueueProto.PendingWriteItem.parseFrom(legacyStored));
        // The reader falls back through the serializer and recovers the original operation.
        assertThat(decodeEntry(legacyStored, serializer)).isEqualTo(legacyOp);
    }

    @Nonnull
    static Stream<Arguments> serializerModes() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(true, true));
    }

    @Nonnull
    private static LuceneSerializer buildSerializer(boolean compress, boolean encrypt) {
        if (!encrypt) {
            return new LuceneSerializer(compress, false, null, true);
        }
        // Deterministic key + IV source so the test is reproducible.
        final Random random = new Random(0xF00DL);
        final SecretKey key = RandomSecretUtil.randomSecretKey(random);
        return new LuceneSerializer(compress, true, new FixedZeroKeyManager(key, null, random), true);
    }

    /**
     * Mirrors the planned reader dispatch. New-format entries are stored as a plain envelope
     * proto, so they parse directly and report {@code version >= }{@link #NEW_FORMAT_VERSION};
     * their payload is unpacked from the {@code Any}. Anything that does not parse as a new-format
     * envelope (a legacy, serializer-wrapped entry) falls back to {@link LuceneSerializer#decode}
     * followed by the legacy descriptor. The serializer is therefore only consulted on the
     * fallback path, and may be {@code null} when no legacy entries are expected.
     */
    @Nonnull
    private static LucenePendingWriteQueueProto.PendingWriteItem decodeEntry(
            @Nonnull byte[] raw, @Nullable LuceneSerializer serializer)
            throws InvalidProtocolBufferException {
        final PendingWritesQueueProto.PendingWriteItem envelope = tryParseEnvelope(raw);
        if (envelope != null && envelope.getVersion() >= NEW_FORMAT_VERSION) {
            return envelope.getPayload().unpack(LucenePendingWriteQueueProto.PendingWriteItem.class);
        }
        // Fallback: a legacy, serializer-wrapped entry (it either failed to parse as the new
        // envelope, or parsed as version 0 — new entries always carry version >= 1).
        if (serializer == null) {
            throw new IllegalStateException("entry is not new-format and no serializer was provided to decode it");
        }
        return LucenePendingWriteQueueProto.PendingWriteItem.parseFrom(serializer.decode(raw));
    }

    /**
     * Parse the bytes as the new envelope, returning {@code null} if they are not a valid
     * envelope (e.g. a serializer-wrapped legacy blob).
     */
    @Nullable
    private static PendingWritesQueueProto.PendingWriteItem tryParseEnvelope(@Nonnull byte[] raw) {
        try {
            return PendingWritesQueueProto.PendingWriteItem.parseFrom(raw);
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    @Nonnull
    private static PendingWritesQueueProto.PendingWriteItem newEnvelope(
            @Nonnull LucenePendingWriteQueueProto.PendingWriteItem payload) {
        return PendingWritesQueueProto.PendingWriteItem.newBuilder()
                .setVersion(NEW_FORMAT_VERSION)
                .setPayload(Any.pack(payload))
                .setEnqueueTimestamp(System.currentTimeMillis())
                .build();
    }

    @Nonnull
    private static LucenePendingWriteQueueProto.PendingWriteItem legacyInsert(
            @Nonnull String recordId, @Nonnull String fieldValue) {
        return LucenePendingWriteQueueProto.PendingWriteItem.newBuilder()
                .setOperationType(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT)
                .setPrimaryKey(ByteString.copyFrom(Tuple.from(recordId).pack()))
                .setEnqueueTimestamp(1234567890L)
                .addFields(LucenePendingWriteQueueProto.DocumentField.newBuilder()
                        .setFieldName("f")
                        .setStored(true)
                        .setSorted(false)
                        .setStringValue(fieldValue)
                        .build())
                .build();
    }

    @Nonnull
    private static LucenePendingWriteQueueProto.PendingWriteItem legacyDelete(@Nonnull String recordId) {
        return LucenePendingWriteQueueProto.PendingWriteItem.newBuilder()
                .setOperationType(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE)
                .setPrimaryKey(ByteString.copyFrom(Tuple.from(recordId).pack()))
                .setEnqueueTimestamp(1234567890L)
                .build();
    }
}
