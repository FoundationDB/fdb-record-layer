/*
 * FDBClientLogEventsClusterTest.java
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.system.SystemKeyspace;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validate {@link FDBClientLogEvents} against the client log entries written by a real cluster and client library.
 *
 * <p>The protocol version of the log entries is that of the FDB server, which in turn matches the protocol version
 * of the loaded {@code libfdb_c} library. In addition, the {@value #CHECKSUM_PROPERTY} system property can be used
 * to configure the client knob that makes the client attach checksums to the mutations it records (starting with
 * FDB version 7.4). If that property is set, this test will validate that it reads the correct checksums, and
 * it is skipped if the client library is too old to record them.
 *
 * <p>The test sets client knobs, which should only be done before the FDB network is started. For that reason, it
 * cannot share a JVM with other tests that open a database. It is tagged with {@link Tags#ClientLog}, which
 * keeps it out of the {@code test} task. Two gradle tasks are configured to enable running this test with and without
 * checksums:
 *
 * <pre>{@code
 * ./gradlew :fdb-extensions:clientLogTest
 * ./gradlew :fdb-extensions:clientLogChecksumTest
 * }</pre>
 *
 * <p>If the {@value #CAPTURE_DIR_PROPERTY} system property is set to a directory, the raw bytes of the entries that
 * the tests read are written there as hex files. Those are the files that {@link FDBClientLogEventsParsingTest} reads
 * in order to test the parsing logic with different protocol versions. To add the entries for another protocol version
 * or mode, run the test with that property set against that version, and copy the files into the {@code log-entries}
 * test resource directory.
 *
 * <p>The test changes the cluster's client transaction sampling, and puts it back as it was when it is done. The keys
 * that the tests write are in a subspace of their own, with a random part for each test, and are cleared after each one.
 * The entries that the sampling adds to the client log are not removed, as the tests only read entries that were written
 * after they started.
 */
@Tag(Tags.ClientLog)
@Tag(Tags.RequiresFDB)
class FDBClientLogEventsClusterTest {
    static final String CHECKSUM_PROPERTY = "fdb.clientlog.mutation_checksum";
    static final String CAPTURE_DIR_PROPERTY = "fdb.clientlog.capture_dir";
    // By default, the client only flushes its log entries, and picks up changes to the sampling rate, every ten seconds.
    private static final String CSI_STATUS_DELAY_KNOB = "csi_status_delay";
    private static final String ENABLE_MUTATION_CHECKSUM_KNOB = "enable_mutation_checksum";
    private static final byte[] GLOBAL_CONFIG_PREFIX = ByteArrayUtil.join(new byte[] {(byte)0xff, (byte)0xff},
            "/global_config/config/fdb_client_info/".getBytes(StandardCharsets.US_ASCII));
    private static final byte[] SAMPLE_RATE_KEY = ByteArrayUtil.join(GLOBAL_CONFIG_PREFIX,
            "client_txn_sample_rate".getBytes(StandardCharsets.US_ASCII));
    private static final byte[] SIZE_LIMIT_KEY = ByteArrayUtil.join(GLOBAL_CONFIG_PREFIX,
            "client_txn_size_limit".getBytes(StandardCharsets.US_ASCII));
    // Changes to the global configuration are made through the special keys above, but reading them back that way
    // gives what the client has cached, as text. To avoid that, we read through these other keys, which will actually
    // return the value stored in the database when read.
    private static final byte[] STORED_CONFIG_PREFIX = ByteArrayUtil.join(new byte[] {(byte)0xff},
            "/globalConfig/k/config/fdb_client_info/".getBytes(StandardCharsets.US_ASCII));
    private static final byte[] STORED_SAMPLE_RATE_KEY = ByteArrayUtil.join(STORED_CONFIG_PREFIX,
            "client_txn_sample_rate".getBytes(StandardCharsets.US_ASCII));
    private static final byte[] STORED_SIZE_LIMIT_KEY = ByteArrayUtil.join(STORED_CONFIG_PREFIX,
            "client_txn_size_limit".getBytes(StandardCharsets.US_ASCII));
    // Everything that the tests write is in this subspace, and each test uses a random part of it for its own keys.
    private static final Subspace TEST_SUBSPACE = new Subspace(Tuple.from("clientlog"));
    private static final int NOT_COMMITTED = 1020;
    private static final int NETWORK_ALREADY_SETUP = 2201;
    // How much of the log to read in one transaction.
    private static final int READ_PAGE_SIZE = 200;
    private static final long TIMEOUT_SECONDS = 90;

    private static Database database;
    private static Executor executor;
    private static boolean expectChecksums;
    private static long startVersion;
    // What the cluster had configured before the test changed it, to put back afterwards; null where it had nothing.
    private static byte[] previousSampleRate;
    private static byte[] previousSizeLimit;
    private static boolean configCaptured;
    // Written until the client is recording the transactions, and cleared when the tests are done.
    private static byte[] marker;

    // The keys of the test that is running, which are cleared when it is done.
    private byte[] prefix;

    /**
     * A log entry for one transaction, as it was stored and as it was parsed.
     */
    private record Entry(@Nonnull byte[] value, @Nonnull List<FDBClientLogEvents.Event> events) {
    }

    @BeforeAll
    static void openDatabase() throws Exception {
        expectChecksums = Boolean.getBoolean(CHECKSUM_PROPERTY);
        configCaptured = false;
        marker = null;
        startVersion = 0;
        final FDB fdb = FDB.selectAPIVersion(TestDatabaseExtension.getAPIVersion());
        try {
            // Lower the time that the client waits before flushing to 0.5 seconds to lower the overall time taken by these tests.
            fdb.options().setKnob(CSI_STATUS_DELAY_KNOB + "=0.5");
            if (expectChecksums) {
                fdb.options().setKnob(ENABLE_MUTATION_CHECKSUM_KNOB + "=true");
            }
        } catch (FDBException e) {
            if (e.getCode() == NETWORK_ALREADY_SETUP) {
                throw new IllegalStateException("The FDB network was already started by something else in this JVM, "
                        + "so the client knobs cannot be set. Run this test with the clientLogTest Gradle task.", e);
            }
            throw e;
        }
        database = fdb.open(FDBTestEnvironment.randomClusterFile());
        // Fail rather than hang if the client library and the server cannot talk to each other.
        database.options().setTransactionTimeout(TimeUnit.SECONDS.toMillis(30));
        executor = database.getExecutor();
        startVersion = readVersion();
        database.run(tr -> {
            tr.options().setReadSystemKeys();
            previousSampleRate = tr.get(STORED_SAMPLE_RATE_KEY).join();
            previousSizeLimit = tr.get(STORED_SIZE_LIMIT_KEY).join();
            return null;
        });
        configCaptured = true;
        // Configure the database to sample 100% of all keys with no size limit.
        database.run(tr -> {
            tr.options().setSpecialKeySpaceEnableWrites();
            tr.set(SAMPLE_RATE_KEY, Tuple.from(1.0).pack());
            tr.set(SIZE_LIMIT_KEY, Tuple.from(-1L).pack());
            return null;
        });
        // The client only learns of the new rate some time later, so wait until it is actually recording.
        marker = TEST_SUBSPACE.pack(Tuple.from("marker", UUID.randomUUID().toString()));
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        List<Entry> recorded;
        do {
            if (System.nanoTime() > deadline) {
                throw new TimeoutException("Client never started recording transactions");
            }
            database.run(tr -> {
                tr.set(marker, new byte[0]);
                return null;
            });
            delay(1000);
            recorded = findEntries(entry -> hasMutationOnKey(entry, marker));
        } while (recorded.isEmpty());
        // A client that does not know the knob ignores it, which would otherwise show up as missing checksums.
        final long protocolVersion = protocolVersionOf(recorded.get(0));
        Assumptions.assumeFalse(expectChecksums && protocolVersion < FDBClientLogEvents.PROTOCOL_VERSION_7_4,
                () -> "The client library writes protocol version " + Long.toHexString(protocolVersion)
                        + ", which is before 7.4 and so cannot record mutation checksums");
    }

    private static long protocolVersionOf(@Nonnull Entry entry) {
        return ByteBuffer.wrap(entry.value()).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    @AfterAll
    static void closeDatabase() {
        if (database != null) {
            try {
                // If setting up failed before the original values were read, nothing has been changed and there is
                // nothing to put back, and putting back "nothing" would clear a configuration that is not ours.
                if (configCaptured) {
                    database.run(tr -> {
                        tr.options().setSpecialKeySpaceEnableWrites();
                        restore(tr, SAMPLE_RATE_KEY, previousSampleRate);
                        restore(tr, SIZE_LIMIT_KEY, previousSizeLimit);
                        return null;
                    });
                    database.run(tr -> {
                        tr.clear(marker);
                        return null;
                    });
                }
            } finally {
                database.close();
            }
        }
    }

    @BeforeEach
    void choosePrefix() {
        prefix = TEST_SUBSPACE.pack(UUID.randomUUID().toString());
    }

    @AfterEach
    void clearPrefix() {
        database.run(tr -> {
            tr.clear(Range.startsWith(prefix));
            return null;
        });
    }

    private static void restore(@Nonnull Transaction tr, @Nonnull byte[] key, byte[] previous) {
        if (previous == null) {
            tr.clear(key);
        } else {
            tr.set(key, previous);
        }
    }

    @Test
    void commitEventsRoundTripMutations() throws Exception {
        final byte[] setKey = ByteArrayUtil.join(prefix, new byte[] {'s'});
        final byte[] clearKey = ByteArrayUtil.join(prefix, new byte[] {'c'});
        final byte[] rangeBegin = ByteArrayUtil.join(prefix, new byte[] {'r', 1});
        final byte[] rangeEnd = ByteArrayUtil.join(prefix, new byte[] {'r', 2});
        final byte[] addKey = ByteArrayUtil.join(prefix, new byte[] {'a'});
        final byte[] setValue = "some value".getBytes(StandardCharsets.UTF_8);
        final byte[] addValue = new byte[] {1, 0, 0, 0, 0, 0, 0, 0};

        final long before = readVersion();
        database.run(tr -> {
            tr.set(setKey, setValue);
            tr.clear(clearKey);
            tr.clear(rangeBegin, rangeEnd);
            tr.mutate(MutationType.ADD, addKey, addValue);
            return null;
        });

        final FDBClientLogEvents.EventCommit commit = awaitEvent(FDBClientLogEvents.EventCommit.class, before,
                event -> writesKey(event.getCommitRequest(), setKey));
        final Map<ByteBuffer, FDBClientLogEvents.Mutation> mutations = mutationsByKey(commit, prefix);
        assertThat(mutations).hasSize(4);
        assertMutation(mutations.get(ByteBuffer.wrap(setKey)), FDBClientLogEvents.Mutation.SET_VALUE, setKey, setValue);
        // A single key clear is a range clear from the key to the immediately following key.
        assertMutation(mutations.get(ByteBuffer.wrap(clearKey)), FDBClientLogEvents.Mutation.CLEAR_RANGE,
                clearKey, ByteArrayUtil.join(clearKey, new byte[1]));
        assertMutation(mutations.get(ByteBuffer.wrap(rangeBegin)), FDBClientLogEvents.Mutation.CLEAR_RANGE, rangeBegin, rangeEnd);
        assertMutation(mutations.get(ByteBuffer.wrap(addKey)), MutationType.ADD.code(), addKey, addValue);
        assertChecksums(mutations.values());
        capture("commit", entry -> hasMutationOnKey(entry, setKey));
    }

    @Test
    void commitEventsRoundTripEveryAtomicOperation() throws Exception {
        // The client turns the original MIN and AND into their V2 forms, which are what is recorded.
        final Map<MutationType, Integer> expectedCodes = new HashMap<>();
        for (MutationType type : List.of(MutationType.ADD, MutationType.BIT_AND, MutationType.BIT_OR, MutationType.BIT_XOR,
                MutationType.MAX, MutationType.MIN, MutationType.BYTE_MIN, MutationType.BYTE_MAX,
                MutationType.APPEND_IF_FITS, MutationType.COMPARE_AND_CLEAR)) {
            expectedCodes.put(type, type.code());
        }
        expectedCodes.put(MutationType.BIT_AND, FDBClientLogEvents.Mutation.AND_V2);
        expectedCodes.put(MutationType.MIN, FDBClientLogEvents.Mutation.MIN_V2);

        final Map<MutationType, byte[]> keys = new HashMap<>();
        final byte[] param = new byte[] {7, 0, 0, 0, 0, 0, 0, 0};
        for (MutationType type : expectedCodes.keySet()) {
            keys.put(type, ByteArrayUtil.join(prefix, type.name().getBytes(StandardCharsets.US_ASCII)));
        }
        final long before = readVersion();
        database.run(tr -> {
            keys.forEach((type, key) -> tr.mutate(type, key, param));
            return null;
        });

        final byte[] anyKey = keys.values().iterator().next();
        final FDBClientLogEvents.EventCommit commit = awaitEvent(FDBClientLogEvents.EventCommit.class, before,
                event -> writesKey(event.getCommitRequest(), anyKey));
        final Map<ByteBuffer, FDBClientLogEvents.Mutation> mutations = mutationsByKey(commit, prefix);
        assertThat(mutations).hasSameSizeAs(keys);
        expectedCodes.forEach((type, code) ->
                assertMutation(mutations.get(ByteBuffer.wrap(keys.get(type))), code, keys.get(type), param));
        assertChecksums(mutations.values());
    }

    @Test
    void readEventsRoundTrip() throws Exception {
        final byte[] key = ByteArrayUtil.join(prefix, new byte[] {'k'});
        final byte[] value = "read me".getBytes(StandardCharsets.UTF_8);
        final Range range = new Range(ByteArrayUtil.join(prefix, new byte[] {'m'}), ByteArrayUtil.join(prefix, new byte[] {'n'}));
        database.run(tr -> {
            tr.set(key, value);
            return null;
        });

        final long before = readVersion();
        final long readVersion = database.run(tr -> {
            assertThat(tr.get(key).join()).isEqualTo(value);
            tr.getRange(range).asList().join();
            return tr.getReadVersion().join();
        });

        final FDBClientLogEvents.EventGet get = awaitEvent(FDBClientLogEvents.EventGet.class, before,
                event -> Arrays.equals(event.getKey(), key));
        final FDBClientLogEvents.EventGetRange getRange = awaitEvent(FDBClientLogEvents.EventGetRange.class, before,
                event -> Arrays.equals(event.getRange().begin, range.begin));
        // Reading the version is recorded too, and is the only one of these that cannot be recognized by a key.
        awaitEvent(FDBClientLogEvents.EventGetVersion.class, before, event -> event.getReadVersion() == readVersion);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(get.getSize()).isEqualTo(value.length);
            softly.assertThat(get.getLatency()).isGreaterThanOrEqualTo(0.0);
            softly.assertThat(getRange.getRange().end).isEqualTo(range.end);
        }
        capture("reads", entry -> entry.events().stream().anyMatch(event ->
                event instanceof FDBClientLogEvents.EventGet getEvent && Arrays.equals(getEvent.getKey(), key)));
    }

    @Test
    void conflictingCommitIsRecordedAsError() throws Exception {
        final byte[] readKey = ByteArrayUtil.join(prefix, new byte[] {'r'});
        final byte[] writeKey = ByteArrayUtil.join(prefix, new byte[] {'w'});

        final long before = readVersion();
        try (Transaction loser = database.createTransaction(executor)) {
            loser.options().setReportConflictingKeys();
            loser.get(readKey).join();
            database.run(tr -> {
                tr.set(readKey, new byte[] {1});
                return null;
            });
            loser.set(writeKey, new byte[] {2});
            assertThatThrownBy(() -> loser.commit().join())
                    .as("commit should have conflicted")
                    .isInstanceOf(CompletionException.class)
                    .cause()
                    .isInstanceOfSatisfying(FDBException.class, fdbError -> assertThat(fdbError.getCode()).isEqualTo(NOT_COMMITTED));
        }

        final FDBClientLogEvents.EventCommitError error = awaitEvent(FDBClientLogEvents.EventCommitError.class, before,
                event -> writesKey(event.getCommitRequest(), writeKey));
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(error.getErrorCode()).isEqualTo(NOT_COMMITTED);
            softly.assertThat(error.getCommitRequest().isReportConflictingKeys()).isTrue();
            softly.assertThat(error.getCommitRequest().getReadConflictRanges())
                    .as("read conflict ranges %s", Arrays.toString(error.getCommitRequest().getReadConflictRanges()))
                    .anySatisfy(range -> assertThat(range.begin).isEqualTo(readKey));
        }
        capture("conflict", entry -> entry.events().stream().anyMatch(event ->
                event instanceof FDBClientLogEvents.EventCommitError errorEvent && writesKey(errorEvent.getCommitRequest(), writeKey)));
    }

    @Test
    void largeCommitIsReassembledFromChunks() throws Exception {
        final int count = 400;
        final byte[] value = new byte[1000];
        Arrays.fill(value, (byte)'x');
        final long before = readVersion();
        database.run(tr -> {
            for (int i = 0; i < count; i++) {
                tr.set(ByteArrayUtil.join(prefix, Tuple.from(i).pack()), value);
            }
            return null;
        });

        // This is big enough that the client splits it over several keys, which the public API puts back together.
        final byte[] firstKey = ByteArrayUtil.join(prefix, Tuple.from(0).pack());
        final FDBClientLogEvents.EventCommit commit = awaitEvent(FDBClientLogEvents.EventCommit.class, before,
                event -> writesKey(event.getCommitRequest(), firstKey));
        final Map<ByteBuffer, FDBClientLogEvents.Mutation> mutations = mutationsByKey(commit, prefix);
        assertThat(mutations).hasSize(count);
        for (FDBClientLogEvents.Mutation mutation : mutations.values()) {
            assertThat(mutation.getParam()).isEqualTo(value);
        }
        assertChecksums(mutations.values());
        capture("large", entry -> hasMutationOnKey(entry, firstKey));
    }

    private static void assertChecksums(@Nonnull Iterable<FDBClientLogEvents.Mutation> mutations) {
        for (FDBClientLogEvents.Mutation mutation : mutations) {
            // A client never records an accumulative checksum index. The commit proxy assigns them (to the copies of the
            // mutations that it sends on to the transaction logs), after the client has already written its entry.
            try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
                if (expectChecksums) {
                    // Checking the value, and not just its presence, shows that the right bytes were taken off the parameter.
                    softly.assertThat(mutation.getChecksum()).as("checksum of %s", mutation).isEqualTo(MutationChecksums.forMutation(mutation));
                } else {
                    softly.assertThat(mutation.getChecksum()).as("checksum of %s", mutation).isNull();
                }
                softly.assertThat(mutation.getAccumulativeChecksumIndex()).as("index of %s", mutation).isNull();
            }
        }
    }

    private static void assertMutation(FDBClientLogEvents.Mutation mutation, int type,
                                       @Nonnull byte[] key, @Nonnull byte[] param) {
        assertThat(mutation).isNotNull();
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(mutation.getType()).as("type of %s", mutation).isEqualTo(type);
            softly.assertThat(mutation.getKey()).as("key of %s", mutation).isEqualTo(key);
            softly.assertThat(mutation.getParam()).as("param of %s", mutation).isEqualTo(param);
        }
    }

    // The transaction does not record mutations in the order they were issued, so tests look them up by key.
    @Nonnull
    private static Map<ByteBuffer, FDBClientLogEvents.Mutation> mutationsByKey(@Nonnull FDBClientLogEvents.EventCommit commit,
                                                                               @Nonnull byte[] prefix) {
        final Map<ByteBuffer, FDBClientLogEvents.Mutation> mutations = new HashMap<>();
        for (FDBClientLogEvents.Mutation mutation : commit.getCommitRequest().getMutations()) {
            if (ByteArrayUtil.startsWith(mutation.getKey(), prefix)) {
                mutations.put(ByteBuffer.wrap(mutation.getKey()), mutation);
            }
        }
        return mutations;
    }

    private static long readVersion() throws Exception {
        return database.readAsync(ReadTransaction::getReadVersion).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static boolean writesKey(@Nonnull FDBClientLogEvents.CommitRequest request, @Nonnull byte[] key) {
        return Arrays.stream(request.getMutations()).anyMatch(mutation -> Arrays.equals(mutation.getKey(), key));
    }

    private static boolean hasMutationOnKey(@Nonnull Entry entry, @Nonnull byte[] key) {
        return entry.events().stream().anyMatch(event ->
                event instanceof FDBClientLogEvents.EventCommit commit && writesKey(commit.getCommitRequest(), key));
    }

    /**
     * Wait for an event to be in the client log, reading it the way that users of {@link DatabaseClientLogEvents} do.
     * The client writes its log entries in the background after the work is done, and there is nothing to wait on for
     * that, so look again (with a deadline) until the event shows up.
     *
     * @param type the type of event to wait for
     * @param fromVersion the version to start reading the log from, which should be before the work was done
     * @param predicate which of the events of that type is the one that is wanted
     * @param <E> the type of event
     * @return the event, or the last of them if there is more than one
     */
    @Nonnull
    private static <E extends FDBClientLogEvents.Event> E awaitEvent(@Nonnull Class<E> type, long fromVersion,
                                                                     @Nonnull Predicate<E> predicate) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (true) {
            final List<E> matches = new ArrayList<>();
            final DatabaseClientLogEvents.EventConsumer consumer = (tr, event) -> {
                if (type.isInstance(event) && predicate.test(type.cast(event))) {
                    matches.add(type.cast(event));
                }
                return AsyncUtil.DONE;
            };
            DatabaseClientLogEvents.forEachEventBetweenVersions(database, executor, consumer, fromVersion, null,
                    Integer.MAX_VALUE, Long.MAX_VALUE).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!matches.isEmpty()) {
                return matches.get(matches.size() - 1);
            }
            if (System.nanoTime() > deadline) {
                throw new TimeoutException("No client log event found");
            }
            delay(500);
        }
    }

    /**
     * Wait for an entry to be in the client log, reading its raw bytes. The public API only gives the events, and
     * not the bytes that they were parsed from, which are what is wanted for {@link #capture}, and for knowing which
     * protocol version the client wrote.
     */
    @Nonnull
    private static Entry awaitEntry(@Nonnull Predicate<Entry> predicate) throws Exception {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (true) {
            final List<Entry> matches = findEntries(predicate);
            if (!matches.isEmpty()) {
                return matches.get(matches.size() - 1);
            }
            if (System.nanoTime() > deadline) {
                throw new TimeoutException("No client log entry found");
            }
            delay(500);
        }
    }

    /**
     * Read the log entries recorded since the start of the test and put the chunks of each back together. The log is
     * read a page at a time, each in a transaction of its own, so that a busy log (every transaction of every client
     * is sampled while the test runs) does not run into the limits of a single transaction. Only the entries that
     * match are kept.
     */
    @Nonnull
    private static List<Entry> findEntries(@Nonnull Predicate<Entry> predicate) throws Exception {
        final EntryAssembler assembler = new EntryAssembler(predicate);
        // Only look at entries that were written after this test was started.
        byte[] begin = FDBClientLogEvents.eventKeyForVersion(startVersion);
        final byte[] end = ByteArrayUtil.strinc(SystemKeyspace.CLIENT_LOG_KEY_PREFIX);
        while (true) {
            final byte[] pageBegin = begin;
            final List<KeyValue> page = database.readAsync(tr -> {
                tr.options().setReadSystemKeys();
                return tr.getRange(pageBegin, end, READ_PAGE_SIZE).asList();
            }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            for (KeyValue keyValue : page) {
                assembler.add(keyValue);
            }
            if (page.size() < READ_PAGE_SIZE) {
                return assembler.finish();
            }
            // The key immediately after the last one read.
            begin = ByteArrayUtil.join(page.get(page.size() - 1).getKey(), new byte[1]);
        }
    }

    /**
     * Puts the chunks of entries back together as they are read, and keeps the entries that match. The keys of the
     * chunks of an entry sort next to each other and in order, which is what makes doing it this way possible, and
     * allows an entry to be split between two pages.
     */
    private static final class EntryAssembler {
        @Nonnull
        private final Predicate<Entry> predicate;
        @Nonnull
        private final List<Entry> matches = new ArrayList<>();
        @Nonnull
        private final List<KeyValue> chunks = new ArrayList<>();
        private byte[] chunksId;

        private EntryAssembler(@Nonnull Predicate<Entry> predicate) {
            this.predicate = predicate;
        }

        void add(@Nonnull KeyValue keyValue) throws Exception {
            final byte[] id = Arrays.copyOfRange(keyValue.getKey(), FDBClientLogEvents.EVENT_KEY_ID_START_INDEX,
                    FDBClientLogEvents.EVENT_KEY_ID_END_INDEX);
            if (chunksId != null && !Arrays.equals(chunksId, id)) {
                completeEntry();
            }
            chunksId = id;
            chunks.add(keyValue);
        }

        @Nonnull
        List<Entry> finish() throws Exception {
            completeEntry();
            return matches;
        }

        private void completeEntry() throws Exception {
            if (!chunks.isEmpty()) {
                addIfMatching(chunks, predicate, matches);
                chunks.clear();
            }
        }
    }

    private static void addIfMatching(@Nonnull List<KeyValue> chunks, @Nonnull Predicate<Entry> predicate,
                                      @Nonnull List<Entry> matches) throws Exception {
        final ByteBuffer lastKey = ByteBuffer.wrap(chunks.get(chunks.size() - 1).getKey());
        lastKey.position(FDBClientLogEvents.EVENT_KEY_CHUNK_INDEX);
        lastKey.getInt();
        if (lastKey.getInt() != chunks.size()) {
            return; // Some of the chunks have not been written yet.
        }
        int length = 0;
        for (KeyValue chunk : chunks) {
            length += chunk.getValue().length;
        }
        final ByteBuffer joined = ByteBuffer.allocate(length);
        for (KeyValue chunk : chunks) {
            joined.put(chunk.getValue());
        }
        final byte[] value = joined.array();
        final List<FDBClientLogEvents.Event> events = new ArrayList<>();
        FDBClientLogEvents.deserializeEvents(ByteBuffer.wrap(value), event -> {
            events.add(event);
            return AsyncUtil.DONE;
        }).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        final Entry entry = new Entry(value, events);
        if (predicate.test(entry)) {
            matches.add(entry);
        }
    }

    // Does nothing unless the test was asked to write the entries that it reads.
    private static void capture(@Nonnull String scenario, @Nonnull Predicate<Entry> predicate) throws Exception {
        final String directory = System.getProperty(CAPTURE_DIR_PROPERTY);
        if (directory == null || directory.isEmpty()) {
            return;
        }
        final Entry entry = awaitEntry(predicate);
        final long protocolVersion = protocolVersionOf(entry);
        // The parsing test reads the files named like this to find the log entries that it should check.
        final Path path = Paths.get(directory, String.format("%s-%s-%s.hex", Long.toHexString(protocolVersion),
                expectChecksums ? "checksum" : "plain", scenario));
        write(path, entry);
    }

    private static void write(@Nonnull Path path, @Nonnull Entry entry) throws IOException {
        Files.createDirectories(path.getParent());
        Files.writeString(path, HexFormat.of().formatHex(entry.value()) + "\n");
    }

    private static void delay(long millis) throws Exception {
        CompletableFuture.runAsync(() -> { }, CompletableFuture.delayedExecutor(millis, TimeUnit.MILLISECONDS))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }
}
