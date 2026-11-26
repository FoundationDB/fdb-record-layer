/*
 * FDBClientLogEventsParsingTest.java
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

package com.apple.foundationdb.clientlog;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of the parsing of {@link FDBClientLogEvents}. These tests do not require an FDB cluster.
 *
 * <p>Some of them read the log entries in the {@code log-entries} test resource directory. These were captured from
 * real clients by {@link FDBClientLogEventsClusterTest}, so that they check the parser against what FDB wrote and not
 * just against this test's idea of the format. Every file in that directory is picked up by these tests, so covering
 * another protocol version (or another way of configuring the client) only takes adding the files that the cluster test
 * captures for it. The name of each file says what it is: the protocol version that it was written with (in hex),
 * whether the client recorded checksums ({@code checksum}) or not ({@code plain}), and what was done to make the
 * entry ({@code commit}, {@code conflict}, or {@code reads}).
 *
 * <p>The rest of the tests build entries by hand, to cover what a real client does not write (such as an accumulative
 * checksum index), and the layouts of the protocol versions for which no log entry has been captured.
 */
final class FDBClientLogEventsParsingTest {
    private static final String LOG_ENTRIES_DIRECTORY = "/com/apple/foundationdb/clientlog/log-entries/";
    private static final String COMMIT = "commit";
    private static final String CONFLICT = "conflict";
    private static final String READS = "reads";
    private static final List<LogEntry> LOG_ENTRIES = findLogEntries();

    /**
     * A log entry that was captured from a real client and saved as a resource, named
     * {@code <protocol version in hex>-<checksum or plain>-<scenario>.hex}.
     */
    private record LogEntry(@Nonnull String name, long protocolVersion, boolean checksums, @Nonnull String scenario) {
        @Nonnull
        static LogEntry fromFileName(@Nonnull String fileName) {
            final String name = fileName.substring(0, fileName.length() - ".hex".length());
            final String[] parts = name.split("-", 3);
            if (parts.length != 3 || !("checksum".equals(parts[1]) || "plain".equals(parts[1]))) {
                throw new IllegalArgumentException("Log entry file not named <protocol version>-<checksum or plain>-<scenario>.hex: " + fileName);
            }
            return new LogEntry(name, Long.parseLong(parts[0], 16), "checksum".equals(parts[1]), parts[2]);
        }

        @Nonnull
        byte[] load() throws IOException {
            try (InputStream stream = FDBClientLogEventsParsingTest.class.getResourceAsStream(LOG_ENTRIES_DIRECTORY + name + ".hex")) {
                assertThat(stream).as("log entry %s", name).isNotNull();
                return HexFormat.of().parseHex(new String(stream.readAllBytes(), StandardCharsets.US_ASCII).trim());
            }
        }

        // Shown in the name of each run of a parameterized test.
        @Override
        @Nonnull
        public String toString() {
            return name;
        }
    }

    @Nonnull
    private static List<LogEntry> findLogEntries() {
        final URL directory = FDBClientLogEventsParsingTest.class.getResource(LOG_ENTRIES_DIRECTORY);
        assertThat(directory).as("log entries directory").isNotNull();
        try (Stream<Path> files = Files.list(Path.of(directory.toURI()))) {
            return files.map(file -> file.getFileName().toString())
                    .filter(fileName -> fileName.endsWith(".hex"))
                    .sorted()
                    .map(LogEntry::fromFileName)
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    static LongStream protocolVersions() {
        return Arrays.stream(FDBClientLogEvents.SUPPORTED_PROTOCOL_VERSIONS);
    }

    static Stream<LogEntry> logEntries() {
        return LOG_ENTRIES.stream();
    }

    static Stream<LogEntry> commitEntries() {
        return logEntries().filter(logEntry -> COMMIT.equals(logEntry.scenario()));
    }

    static Stream<LogEntry> conflictEntries() {
        return logEntries().filter(logEntry -> CONFLICT.equals(logEntry.scenario()));
    }

    static Stream<LogEntry> readsEntries() {
        return logEntries().filter(logEntry -> READS.equals(logEntry.scenario()));
    }

    // The entry for the scenario from the newest protocol version that there is one for, for tests that need an
    // entry to work with and do not care which.
    @Nonnull
    private static byte[] newestEntry(@Nonnull String scenario) throws IOException {
        return LOG_ENTRIES.stream()
                .filter(logEntry -> scenario.equals(logEntry.scenario()))
                .max(Comparator.comparingLong(LogEntry::protocolVersion).thenComparing(LogEntry::checksums))
                .orElseThrow()
                .load();
    }

    // Log entries from real clients

    @ParameterizedTest
    @MethodSource("logEntries")
    void logEntryIsOfTheProtocolVersionItIsNamedFor(LogEntry logEntry) throws IOException {
        final long protocolVersion = ByteBuffer.wrap(logEntry.load()).order(ByteOrder.LITTLE_ENDIAN).getLong();

        assertThat(protocolVersion).isEqualTo(logEntry.protocolVersion());
        assertThat(FDBClientLogEvents.SUPPORTED_PROTOCOL_VERSIONS).contains(protocolVersion);
    }

    @ParameterizedTest
    @MethodSource("logEntries")
    void mutationChecksumsAreThoseThatWereRecorded(LogEntry logEntry) throws Exception {
        for (FDBClientLogEvents.Mutation mutation : mutationsOf(parse(logEntry.load()))) {
            try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
                if (logEntry.checksums()) {
                    softly.assertThat(mutation.getChecksum()).as("checksum of %s", mutation).isEqualTo(MutationChecksums.forMutation(mutation));
                } else {
                    softly.assertThat(mutation.getChecksum()).as("checksum of %s", mutation).isNull();
                }
                // A client never records the index, which is assigned later by the commit proxy.
                softly.assertThat(mutation.getAccumulativeChecksumIndex()).as("index of %s", mutation).isNull();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("commitEntries")
    void commitEntryHasTheMutationsThatWereCommitted(LogEntry logEntry) throws Exception {
        final FDBClientLogEvents.EventCommit commit = first(parse(logEntry.load()), FDBClientLogEvents.EventCommit.class);
        final FDBClientLogEvents.Mutation[] mutations = commit.getCommitRequest().getMutations();
        // One set, one single key clear, one range clear, and one atomic add.
        assertThat(Arrays.stream(mutations).mapToInt(FDBClientLogEvents.Mutation::getType)).containsExactlyInAnyOrder(
                FDBClientLogEvents.Mutation.SET_VALUE, FDBClientLogEvents.Mutation.CLEAR_RANGE,
                FDBClientLogEvents.Mutation.CLEAR_RANGE, MutationType.ADD.code());
        for (FDBClientLogEvents.Mutation mutation : mutations) {
            if (mutation.getType() == FDBClientLogEvents.Mutation.CLEAR_RANGE) {
                assertThat(ByteArrayUtil.compareUnsigned(mutation.getKey(), mutation.getParam())).isNegative();
            }
        }
        assertThat(Arrays.stream(mutations).filter(mutation -> mutation.getType() == FDBClientLogEvents.Mutation.CLEAR_RANGE))
                .anySatisfy(mutation -> assertThat(mutation.getParam()).isEqualTo(ByteArrayUtil.join(mutation.getKey(), new byte[1])));
        assertThat(commit.getCommitRequest().getWriteConflictRanges()).isNotEmpty();
    }

    @ParameterizedTest
    @MethodSource("conflictEntries")
    void conflictEntryHasTheErrorAndSingleKeyConflictRanges(LogEntry logEntry) throws Exception {
        final FDBClientLogEvents.EventCommitError error = first(parse(logEntry.load()), FDBClientLogEvents.EventCommitError.class);
        assertThat(error.getErrorCode()).isEqualTo(1020);
        final Range[] readRanges = error.getCommitRequest().getReadConflictRanges();
        assertThat(readRanges).isNotEmpty();
        for (Range range : readRanges) {
            assertThat(ByteArrayUtil.compareUnsigned(range.begin, range.end)).as("range %s", range).isNegative();
        }
        // The read of a single key is a conflict range from the key to the next possible key.
        assertThat(readRanges).anySatisfy(range -> assertThat(range.end).isEqualTo(ByteArrayUtil.join(range.begin, new byte[1])));
        assertThat(error.getCommitRequest().isReportConflictingKeys()).isTrue();
        assertThat(error.getCommitRequest().getMutations()).isNotEmpty();
    }

    @ParameterizedTest
    @MethodSource("readsEntries")
    void readsEntryHasAllTheReads(LogEntry logEntry) throws Exception {
        final List<FDBClientLogEvents.Event> events = parse(logEntry.load());
        assertThat(events).anySatisfy(event ->
                assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGet.class,
                        getEvent -> assertThat(getEvent.getSize()).isEqualTo("read me".length()))
        ).anySatisfy(event ->
                assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGetRange.class, getRangeEvent -> {
                    final Range range = getRangeEvent.getRange();
                    assertThat(ByteArrayUtil.compareUnsigned(range.begin, range.end)).isNegative();
                })
        ).anySatisfy(event ->
                assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGetVersion.class,
                        getVersionEvent -> assertThat(getVersionEvent.getReadVersion()).isPositive())
        );
    }

    // Entries made by hand

    @ParameterizedTest
    @MethodSource("protocolVersions")
    void getVersionEventHasTheFieldsOfItsProtocolVersion(long protocolVersion) throws Exception {
        final EntryBuilder builder = new EntryBuilder(protocolVersion);
        builder.eventHeader(FDBClientLogEvents.GET_VERSION_LATENCY, 1234.5);
        builder.putDouble(0.25);
        if (protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_2) {
            builder.putInt(2);
        }
        if (protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_3) {
            builder.putLong(987654321L);
        }

        final FDBClientLogEvents.EventGetVersion event = parseSingle(builder.build(), FDBClientLogEvents.EventGetVersion.class);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(event.getStartTimestampDouble()).isEqualTo(1234.5);
            softly.assertThat(event.getLatency()).isEqualTo(0.25);
            softly.assertThat(event.getPriority()).isEqualTo(protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_2 ? 2 : 0);
            softly.assertThat(event.getReadVersion()).isEqualTo(protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_3 ? 987654321L : 0L);
            softly.assertThat(event.getDcId()).isEqualTo(protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_3 ? "dc1" : "");
            softly.assertThat(event.getTenant()).isEqualTo(protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_7_1 ? "tenant1" : "");
        }
    }

    @ParameterizedTest
    @MethodSource("protocolVersions")
    void commitEventHasTheFieldsOfItsProtocolVersion(long protocolVersion) throws Exception {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        final EntryBuilder builder = new EntryBuilder(protocolVersion);
        builder.eventHeader(FDBClientLogEvents.COMMIT_LATENCY, 10.0);
        builder.putDouble(0.5).putInt(3).putInt(100);
        if (protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_3) {
            builder.putLong(555L);
        }
        builder.commitRequest(
                List.of(new Range(new byte[] {1}, new byte[] {2})),
                List.of(new Range(key, ByteArrayUtil.join(key, new byte[1]))),
                List.of(new MutationSpec(FDBClientLogEvents.Mutation.SET_VALUE, key, new byte[] {9}, null, null)),
                4242L);

        final FDBClientLogEvents.EventCommit event = parseSingle(builder.build(), FDBClientLogEvents.EventCommit.class);
        final FDBClientLogEvents.CommitRequest request = event.getCommitRequest();

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(event.getNumMutations()).isEqualTo(3);
            softly.assertThat(event.getCommitBytes()).isEqualTo(100);
            softly.assertThat(request.getReadConflictRanges()).singleElement().satisfies(range -> {
                softly.assertThat(range.begin).isEqualTo(new byte[] {1});
                softly.assertThat(range.end).isEqualTo(new byte[] {2});
            });
            // The single key range was written in its short form, so this checks that it is expanded.
            softly.assertThat(request.getWriteConflictRanges()).singleElement().satisfies(range -> {
                softly.assertThat(range.begin).isEqualTo(key);
                softly.assertThat(range.end).isEqualTo(ByteArrayUtil.join(key, new byte[1]));
            });
            softly.assertThat(request.getMutations()).singleElement().satisfies(mutation ->
                    assertMutation(mutation, FDBClientLogEvents.Mutation.SET_VALUE, key, new byte[] {9}, null, null));
            softly.assertThat(request.getSnapshotVersion()).isEqualTo(4242L);
            softly.assertThat(request.isReportConflictingKeys()).isEqualTo(protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_3);
            softly.assertThat(request.isLockAware()).isEqualTo(protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_7_1);
            if (protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_7_1) {
                softly.assertThat(request.getSpanContext()).isNotNull();
                if (request.getSpanContext() != null) {
                    softly.assertThat(request.getSpanContext().getSpanID()).isEqualTo(77L);
                }
            } else {
                softly.assertThat(request.getSpanContext()).isNull();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("protocolVersions")
    void otherEventTypesAreParsed(long protocolVersion) throws Exception {
        final byte[] key = "k".getBytes(StandardCharsets.UTF_8);
        final EntryBuilder builder = new EntryBuilder(protocolVersion);
        builder.eventHeader(FDBClientLogEvents.GET_LATENCY, 1.0).putDouble(0.5).putInt(12).putArray(key);
        builder.eventHeader(FDBClientLogEvents.GET_RANGE_LATENCY, 2.0).putDouble(0.75).putInt(34).putKeys(key, new byte[] {'z'});
        builder.eventHeader(FDBClientLogEvents.ERROR_GET, 3.0).putInt(1007).putArray(key);
        builder.eventHeader(FDBClientLogEvents.ERROR_GET_RANGE, 4.0).putInt(1009).putKeys(key, ByteArrayUtil.join(key, new byte[1]));

        final List<FDBClientLogEvents.Event> events = parse(builder.build());

        assertThat(events).satisfiesExactly(
                event -> assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGet.class, get -> {
                    assertThat(get.getSize()).isEqualTo(12);
                    assertThat(get.getKey()).isEqualTo(key);
                }),
                event -> assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGetRange.class, getRange -> {
                    assertThat(getRange.getSize()).isEqualTo(34);
                    assertThat(getRange.getRange().end).isEqualTo(new byte[] {'z'});
                }),
                event -> assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGetError.class, getError -> {
                    assertThat(getError.getErrorCode()).isEqualTo(1007);
                    assertThat(getError.getKey()).isEqualTo(key);
                }),
                event -> assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGetRangeError.class, getRangeError -> {
                    assertThat(getRangeError.getErrorCode()).isEqualTo(1009);
                    assertThat(getRangeError.getRange().begin).isEqualTo(key);
                    assertThat(getRangeError.getRange().end).isEqualTo(ByteArrayUtil.join(key, new byte[1]));
                }));
    }

    @Test
    void mutationWithChecksumAndIndexHasBothRemoved() throws Exception {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        final byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        final FDBClientLogEvents.Mutation mutation = singleMutation(FDBClientLogEvents.PROTOCOL_VERSION_7_4,
                new MutationSpec(FDBClientLogEvents.Mutation.SET_VALUE, key, value, 0xDEADBEEF, 513));

        assertMutation(mutation, FDBClientLogEvents.Mutation.SET_VALUE, key, value, 0xDEADBEEF, 513);
    }

    @Test
    void mutationWithIndexButNoChecksumIsRejected() {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        // FDB only appends an index after a checksum, so this is not something that it wrote.
        assertThatThrownBy(() -> singleMutation(FDBClientLogEvents.PROTOCOL_VERSION_7_4,
                new MutationSpec(FDBClientLogEvents.Mutation.SET_VALUE, key, new byte[] {1, 2, 3}, null, 5)))
                .isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 3})
    void mutationParameterTooShortForItsChecksumIsRejected(int paramLength) {
        final EntryBuilder builder = new EntryBuilder(FDBClientLogEvents.PROTOCOL_VERSION_7_4);
        builder.eventHeader(FDBClientLogEvents.ERROR_COMMIT, 1.0).putInt(1020);
        builder.putInt(0).putInt(0).putInt(1); // no conflict ranges, one mutation
        builder.putByte(FDBClientLogEvents.Mutation.SET_VALUE | 0x80).putArray("key".getBytes(StandardCharsets.UTF_8)).putArray(new byte[paramLength]);

        assertThatThrownBy(() -> parse(builder.build())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void singleKeyClearThatDoesNotEndInZeroIsRejected() {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> singleMutation(FDBClientLogEvents.PROTOCOL_VERSION_7_3,
                new MutationSpec(FDBClientLogEvents.Mutation.CLEAR_RANGE, key, new byte[0], null, null)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void singleKeyConflictRangeThatDoesNotEndInZeroIsRejected() {
        final EntryBuilder builder = new EntryBuilder(FDBClientLogEvents.PROTOCOL_VERSION_7_3);
        builder.eventHeader(FDBClientLogEvents.ERROR_COMMIT, 1.0).putInt(1020);
        builder.commitRequest(List.of(new Range("abc".getBytes(StandardCharsets.UTF_8), new byte[0])), List.of(), List.of(), 1L);

        assertThatThrownBy(() -> parse(builder.build())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rangeReadEventsKeepTheKeysTheyWereWrittenWith() throws Exception {
        // Only conflict ranges have the short form for a single key, so an empty end of a read is just an empty end.
        final byte[] begin = "abc".getBytes(StandardCharsets.UTF_8);
        final EntryBuilder builder = new EntryBuilder(FDBClientLogEvents.PROTOCOL_VERSION_7_4);
        builder.eventHeader(FDBClientLogEvents.GET_RANGE_LATENCY, 1.0).putDouble(0.5).putInt(1).putKeys(begin, new byte[0]);
        builder.eventHeader(FDBClientLogEvents.ERROR_GET_RANGE, 2.0).putInt(1009).putKeys(begin, new byte[0]);

        final List<FDBClientLogEvents.Event> events = parse(builder.build());

        assertThat(events).satisfiesExactly(
                event -> assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGetRange.class, getRange -> {
                    assertThat(getRange.getRange().begin).isEqualTo(begin);
                    assertThat(getRange.getRange().end).isEmpty();
                }),
                event -> assertThat(event).isInstanceOfSatisfying(FDBClientLogEvents.EventGetRangeError.class, getRangeError -> {
                    assertThat(getRangeError.getRange().begin).isEqualTo(begin);
                    assertThat(getRangeError.getRange().end).isEmpty();
                }));
    }

    @Test
    void mutationWithEmptyParameterAndChecksumIsNotMistakenForSingleKeyClear() throws Exception {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        final FDBClientLogEvents.Mutation mutation = singleMutation(FDBClientLogEvents.PROTOCOL_VERSION_7_4,
                new MutationSpec(FDBClientLogEvents.Mutation.SET_VALUE, key, new byte[0], 7, null));

        assertMutation(mutation, FDBClientLogEvents.Mutation.SET_VALUE, key, new byte[0], 7, null);
    }

    @Test
    void singleKeyClearWithChecksumIsExpanded() throws Exception {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        // Serialized as the end of the range in the place of the begin, and nothing but the checksum for the end.
        final FDBClientLogEvents.Mutation mutation = singleMutation(FDBClientLogEvents.PROTOCOL_VERSION_7_4,
                new MutationSpec(FDBClientLogEvents.Mutation.CLEAR_RANGE, ByteArrayUtil.join(key, new byte[1]), new byte[0], 99, null));

        assertMutation(mutation, FDBClientLogEvents.Mutation.CLEAR_RANGE, key, ByteArrayUtil.join(key, new byte[1]), 99, null);
    }

    @Test
    void highTypeBitsAreLeftAloneBeforeChecksumsExisted() throws Exception {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        final byte[] value = "value-with-enough-bytes".getBytes(StandardCharsets.UTF_8);
        // Not something any real client wrote, but the flag bits only mean anything from 7.4 on.
        final FDBClientLogEvents.Mutation mutation = singleMutation(FDBClientLogEvents.PROTOCOL_VERSION_7_3,
                new MutationSpec(0x80, key, value, null, null));

        assertMutation(mutation, 0x80, key, value, null, null);
    }

    @Test
    void mutationToStringShowsChecksumAsUnsignedAndIndex() {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);

        final FDBClientLogEvents.Mutation withBoth = new FDBClientLogEvents.Mutation(FDBClientLogEvents.Mutation.SET_VALUE, key, key,
                0xDEADBEEF, 513);
        final FDBClientLogEvents.Mutation withChecksumOnly = new FDBClientLogEvents.Mutation(FDBClientLogEvents.Mutation.SET_VALUE, key, key,
                7, null);

        // The checksum is a 32 bit pattern that is stored in an Integer, so would be negative if printed as is.
        assertThat(withBoth.toString()).contains("checksum=3735928559", "accumulativeChecksumIndex=513");
        assertThat(withChecksumOnly.toString()).contains("checksum=7").doesNotContain("accumulativeChecksumIndex");
    }

    // The deprecated names are skipped because they share a code with the ones that replaced them, and a code is shown as the newer name.
    @ParameterizedTest
    @EnumSource(value = MutationType.class, mode = EnumSource.Mode.EXCLUDE, names = {"AND", "OR", "XOR"})
    void mutationToStringNamesTheType(MutationType mutationType) {
        final byte[] key = "key".getBytes(StandardCharsets.UTF_8);

        assertThat(new FDBClientLogEvents.Mutation(mutationType.code(), key, key, null, null).toString()).contains("type=" + mutationType.name() + ",");
    }

    @Test
    void protocolVersionsAreAcceptedOrRejected() {
        for (long protocolVersion : FDBClientLogEvents.SUPPORTED_PROTOCOL_VERSIONS) {
            assertThat(parseHeaderOnly(protocolVersion)).as("protocol version %x", protocolVersion).isEmpty();
        }
        // Versions that FDB has never had.
        assertThatThrownBy(() -> parseHeaderOnly(0L)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> parseHeaderOnly(FDBClientLogEvents.PROTOCOL_VERSION_8_0 + 1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unknownEventTypeIsRejected() {
        final EntryBuilder builder = new EntryBuilder(FDBClientLogEvents.PROTOCOL_VERSION_7_4);
        builder.eventHeader(99, 1.0);
        assertThatThrownBy(() -> parse(builder.build())).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void truncatedEntryIsRejected() throws Exception {
        final byte[] whole = newestEntry(COMMIT);
        for (int length : new int[] {whole.length - 1, whole.length / 2, 20}) {
            assertThatThrownBy(() -> parse(Arrays.copyOf(whole, length)))
                    .as("length %d", length)
                    .isInstanceOf(BufferUnderflowException.class);
        }
    }

    // Entries stored in several keys

    @Test
    void entryInSeveralChunksIsPutBackTogether() throws Exception {
        final byte[] whole = newestEntry(COMMIT);
        final List<FDBClientLogEvents.Event> expected = parse(whole);
        final List<KeyValue> chunks = chunk(whole, transactionId(1), 3);

        final List<FDBClientLogEvents.Event> actual = new ArrayList<>();
        final FDBClientLogEvents.EventDeserializer deserializer = collecting(actual);
        for (KeyValue chunk : chunks) {
            deserializer.accept(chunk).get();
        }

        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertThat(actual.get(i)).hasToString(expected.get(i).toString());
        }
        assertThat(deserializer.getLastProcessedKey()).isEqualTo(chunks.get(chunks.size() - 1).getKey());
    }

    @Test
    void entryMissingAChunkIsSkipped() throws Exception {
        final List<KeyValue> chunks = chunk(newestEntry(COMMIT), transactionId(1), 3);
        final List<FDBClientLogEvents.Event> actual = new ArrayList<>();
        final FDBClientLogEvents.EventDeserializer deserializer = collecting(actual);

        deserializer.accept(chunks.get(0)).get();
        deserializer.accept(chunks.get(2)).get();

        assertThat(actual).isEmpty();
        // Resuming from the key of the lone chunk skips it, and nothing before it was complete.
        assertThat(deserializer.getLastProcessedKey()).isEqualTo(chunks.get(2).getKey());
    }

    @Test
    void entriesInSeveralChunksOneAfterTheOtherAreBothParsed() throws Exception {
        final byte[] first = newestEntry(COMMIT);
        final byte[] second = newestEntry(READS);
        final List<FDBClientLogEvents.Event> actual = new ArrayList<>();
        final FDBClientLogEvents.EventDeserializer deserializer = collecting(actual);

        for (KeyValue chunk : chunk(first, transactionId(1), 3)) {
            deserializer.accept(chunk).get();
        }
        for (KeyValue chunk : chunk(second, transactionId(2), 2)) {
            deserializer.accept(chunk).get();
        }

        assertThat(actual).hasSize(parse(first).size() + parse(second).size());
    }

    @Test
    void entryThatStartsOverInTheMiddleOfAnotherIsParsedAndTheInterruptedOneSkipped() throws Exception {
        final byte[] interrupted = newestEntry(COMMIT);
        final byte[] complete = newestEntry(READS);
        final List<FDBClientLogEvents.Event> actual = new ArrayList<>();
        final FDBClientLogEvents.EventDeserializer deserializer = collecting(actual);

        final List<KeyValue> interruptedChunks = chunk(interrupted, transactionId(1), 3);
        deserializer.accept(interruptedChunks.get(0)).get();
        deserializer.accept(interruptedChunks.get(1)).get();
        for (KeyValue chunk : chunk(complete, transactionId(2), 2)) {
            deserializer.accept(chunk).get();
        }

        assertThat(actual).hasSameSizeAs(parse(complete));
    }

    // Helpers

    private static void assertMutation(@Nonnull FDBClientLogEvents.Mutation mutation, int type, @Nonnull byte[] key,
                                       @Nonnull byte[] param, Integer checksum, Integer accumulativeChecksumIndex) {
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(mutation.getType()).isEqualTo(type);
            softly.assertThat(mutation.getKey()).isEqualTo(key);
            softly.assertThat(mutation.getParam()).isEqualTo(param);
            softly.assertThat(mutation.getChecksum()).isEqualTo(checksum);
            softly.assertThat(mutation.getAccumulativeChecksumIndex()).isEqualTo(accumulativeChecksumIndex);
        }
    }

    @Nonnull
    private static byte[] transactionId(int seed) {
        final byte[] id = new byte[16];
        Arrays.fill(id, (byte)seed);
        return id;
    }

    @Nonnull
    private static FDBClientLogEvents.EventDeserializer collecting(@Nonnull List<FDBClientLogEvents.Event> events) {
        return new FDBClientLogEvents.EventDeserializer(event -> {
            events.add(event);
            return AsyncUtil.DONE;
        });
    }

    @Nonnull
    private static List<KeyValue> chunk(@Nonnull byte[] value, @Nonnull byte[] transactionId, int count) {
        final List<KeyValue> chunks = new ArrayList<>();
        final int size = (value.length + count - 1) / count;
        for (int i = 0; i < count; i++) {
            final byte[] key = new byte[FDBClientLogEvents.EVENT_KEY_CHUNK_INDEX + 8 + 5];
            System.arraycopy(transactionId, 0, key, FDBClientLogEvents.EVENT_KEY_ID_START_INDEX, transactionId.length);
            ByteBuffer.wrap(key).position(FDBClientLogEvents.EVENT_KEY_CHUNK_INDEX).putInt(i + 1).putInt(count);
            chunks.add(new KeyValue(key, Arrays.copyOfRange(value, i * size, Math.min(value.length, (i + 1) * size))));
        }
        return chunks;
    }

    @Nonnull
    private static List<FDBClientLogEvents.Mutation> mutationsOf(@Nonnull List<FDBClientLogEvents.Event> events) {
        final List<FDBClientLogEvents.Mutation> mutations = new ArrayList<>();
        for (FDBClientLogEvents.Event event : events) {
            if (event instanceof FDBClientLogEvents.EventCommit commit) {
                mutations.addAll(Arrays.asList(commit.getCommitRequest().getMutations()));
            } else if (event instanceof FDBClientLogEvents.EventCommitError error) {
                mutations.addAll(Arrays.asList(error.getCommitRequest().getMutations()));
            }
        }
        return mutations;
    }

    @Nonnull
    private static <E extends FDBClientLogEvents.Event> E first(@Nonnull List<FDBClientLogEvents.Event> events, @Nonnull Class<E> type) {
        return events.stream().filter(type::isInstance).map(type::cast).findFirst().orElseThrow();
    }

    @Nonnull
    private static FDBClientLogEvents.Mutation singleMutation(long protocolVersion, @Nonnull MutationSpec mutation) throws Exception {
        final EntryBuilder builder = new EntryBuilder(protocolVersion);
        builder.eventHeader(FDBClientLogEvents.ERROR_COMMIT, 1.0).putInt(1020);
        builder.commitRequest(List.of(), List.of(), List.of(mutation), 1L);
        final FDBClientLogEvents.EventCommitError event = parseSingle(builder.build(), FDBClientLogEvents.EventCommitError.class);
        assertThat(event.getCommitRequest().getMutations()).hasSize(1);
        return event.getCommitRequest().getMutations()[0];
    }

    @Nonnull
    private static List<FDBClientLogEvents.Event> parseHeaderOnly(long protocolVersion) {
        final ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(protocolVersion);
        buffer.flip();
        final List<FDBClientLogEvents.Event> events = new ArrayList<>();
        // There is no event following the version, so reading stops at the first one, which proves that the version was accepted.
        try {
            FDBClientLogEvents.deserializeEvents(buffer, event -> {
                events.add(event);
                return AsyncUtil.DONE;
            }).get();
        } catch (BufferUnderflowException e) {
            return events;
        } catch (ExecutionException e) {
            // Running out of data is what is expected, anything else is a failure to parse.
            assertThat(e.getCause()).isInstanceOf(BufferUnderflowException.class);
            return events;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return events;
    }

    @Nonnull
    private static <E extends FDBClientLogEvents.Event> E parseSingle(@Nonnull byte[] entry, @Nonnull Class<E> type) throws Exception {
        final List<FDBClientLogEvents.Event> events = parse(entry);
        assertThat(events).singleElement().isInstanceOf(type);
        return type.cast(events.get(0));
    }

    @Nonnull
    private static List<FDBClientLogEvents.Event> parse(@Nonnull byte[] entry) throws Exception {
        final List<FDBClientLogEvents.Event> events = new ArrayList<>();
        try {
            FDBClientLogEvents.deserializeEvents(ByteBuffer.wrap(entry), event -> {
                events.add(event);
                return AsyncUtil.DONE;
            }).get();
        } catch (ExecutionException e) {
            // Parse failures happen inside the asynchronous loop, so surface the original exception.
            if (e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw e;
        }
        return events;
    }

    /**
     * What to write for a mutation. The suffixes are what FDB appends to the second parameter, and the flags in the
     * type that say they are there are set from them.
     */
    private record MutationSpec(int type, @Nonnull byte[] key, @Nonnull byte[] param, Integer checksum, Integer index) {
    }

    /**
     * Writes entries in the layout of a protocol version. This is derived from the FDB C client's code for
     * doing the same. Note that as this constructs entirely synthetic mutations, this can only validate
     * our model of the format. This means it will give us less confidence than the tests which read log
     * events either directly from FDB or from mutations that we have saved and load as a resource.
     */
    private static final class EntryBuilder {
        private final long protocolVersion;
        private final ByteBuffer buffer = ByteBuffer.allocate(1 << 16).order(ByteOrder.LITTLE_ENDIAN);

        private EntryBuilder(long protocolVersion) {
            this.protocolVersion = protocolVersion;
            buffer.putLong(protocolVersion);
        }

        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder putByte(int value) {
            buffer.put((byte)value);
            return this;
        }

        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder putInt(int value) {
            buffer.putInt(value);
            return this;
        }

        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder putLong(long value) {
            buffer.putLong(value);
            return this;
        }

        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder putDouble(double value) {
            buffer.putDouble(value);
            return this;
        }

        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder putArray(@Nonnull byte[] bytes) {
            buffer.putInt(bytes.length);
            buffer.put(bytes);
            return this;
        }

        // The events for reading a range write the two keys as they are.
        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder putKeys(@Nonnull byte[] begin, @Nonnull byte[] end) {
            putArray(begin);
            putArray(end);
            return this;
        }

        // Conflict ranges of a single key are written as the end of the range followed by an empty string.
        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder putRange(@Nonnull Range range) {
            if (Arrays.equals(range.end, ByteArrayUtil.join(range.begin, new byte[1]))) {
                putArray(range.end);
                putArray(new byte[0]);
            } else {
                putArray(range.begin);
                putArray(range.end);
            }
            return this;
        }

        // The fields shared by all events: type, start time, and, depending on the protocol version, datacenter and tenant.
        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder eventHeader(int type, double startTimestamp) {
            buffer.putInt(type);
            buffer.putDouble(startTimestamp);
            if (protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_3) {
                putArray("dc1".getBytes(StandardCharsets.UTF_8));
            }
            if (protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_7_1) {
                buffer.put((byte)1);
                putArray("tenant1".getBytes(StandardCharsets.UTF_8));
            }
            return this;
        }

        @Nonnull
        @CanIgnoreReturnValue
        EntryBuilder commitRequest(@Nonnull List<Range> readRanges, @Nonnull List<Range> writeRanges,
                                   @Nonnull List<MutationSpec> mutations, long snapshotVersion) {
            buffer.putInt(readRanges.size());
            readRanges.forEach(this::putRange);
            buffer.putInt(writeRanges.size());
            writeRanges.forEach(this::putRange);
            buffer.putInt(mutations.size());
            for (MutationSpec mutation : mutations) {
                final boolean checksums = protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_7_4;
                int type = mutation.type;
                final ByteBuffer suffix = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN);
                if (checksums && mutation.checksum != null) {
                    type |= 0x80;
                    suffix.putInt(mutation.checksum);
                }
                if (checksums && mutation.index != null) {
                    type |= 0x40;
                    suffix.putShort((short)(int)mutation.index);
                }
                suffix.flip();
                buffer.put((byte)type);
                putArray(mutation.key);
                buffer.putInt(mutation.param.length + suffix.remaining());
                buffer.put(mutation.param);
                buffer.put(suffix);
            }
            buffer.putLong(snapshotVersion);
            if (protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_6_3) {
                buffer.put((byte)1); // report conflicting keys
            }
            if (protocolVersion >= FDBClientLogEvents.PROTOCOL_VERSION_7_1) {
                buffer.put((byte)1); // lock aware
                buffer.put((byte)1); // has span context
                buffer.putLong(11L).putLong(22L).putLong(77L).put((byte)1);
            }
            return this;
        }

        @Nonnull
        byte[] build() {
            return Arrays.copyOf(buffer.array(), buffer.position());
        }
    }
}
