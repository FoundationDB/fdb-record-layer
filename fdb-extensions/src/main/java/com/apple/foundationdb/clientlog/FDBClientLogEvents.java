/*
 * FDBClientLogEvents.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.SpotBugsSuppressWarnings;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.common.primitives.Longs;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;

/**
 * Parse client latency events from system keyspace.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
public class FDBClientLogEvents {
    public static final int GET_VERSION_LATENCY = 0;
    public static final int GET_LATENCY = 1;
    public static final int GET_RANGE_LATENCY = 2;
    public static final int COMMIT_LATENCY = 3;
    public static final int ERROR_GET = 4;
    public static final int ERROR_GET_RANGE = 5;
    public static final int ERROR_COMMIT = 6;

    public static final long PROTOCOL_VERSION_5_2 = 0x0FDB00A552000001L;
    public static final long PROTOCOL_VERSION_6_0 = 0x0FDB00A570010001L;
    public static final long PROTOCOL_VERSION_6_1 = 0x0FDB00B061060001L;
    public static final long PROTOCOL_VERSION_6_2 = 0x0FDB00B062010001L;
    private static final long[] SUPPORTED_PROTOCOL_VERSIONS = {
            PROTOCOL_VERSION_5_2, PROTOCOL_VERSION_6_0, PROTOCOL_VERSION_6_1, PROTOCOL_VERSION_6_2
    };

    //                                              0         1         2         3         4         5         6         7
    //                                              0123456789012345678901234567890123456789012345678901234567890123456789012345...
    public static final String EVENT_KEY_PATTERN = "FF/fdbClientInfo/client_latency/SSSSSSSSSS/RRRRRRRRRRRRRRRR/NNNNTTTT/XXXX/";
    @SpotBugsSuppressWarnings("MS_PKGPROTECT")
    public static final byte[] EVENT_KEY_PREFIX;
    public static final int EVENT_KEY_VERSION_START_INDEX = 32;
    public static final int EVENT_KEY_VERSION_END_INDEX = 42;
    public static final int EVENT_KEY_ID_START_INDEX = 43;
    public static final int EVENT_KEY_ID_END_INDEX = 59;
    public static final int EVENT_KEY_CHUNK_INDEX = 60;

    static {
        EVENT_KEY_PREFIX = EVENT_KEY_PATTERN.substring(0, EVENT_KEY_VERSION_START_INDEX).getBytes(StandardCharsets.US_ASCII);
        EVENT_KEY_PREFIX[0] = (byte)0xFF;
        EVENT_KEY_PREFIX[1] = (byte)0x02;
    }

    private static final Map<Integer, MutationType> MUTATION_TYPE_BY_CODE = buildMutationTypeMap();

    private static Map<Integer, MutationType> buildMutationTypeMap() {
        final Map<Integer, MutationType> result = new HashMap<>();
        for (MutationType mutationType : MutationType.values()) {
            // NOTE: There are duplicates, but the deprecated ones always come first, so we overwrite.
            result.put(mutationType.code(), mutationType);
        }
        return result;
    }

    /**
     * Asynchronous callback.
     * @param <T> type of the callback argument
     */
    @FunctionalInterface
    public interface AsyncConsumer<T> {
        CompletableFuture<Void> accept(T t);
    }

    /**
     * Base class for parsed events.
     */
    public abstract static class Event {
        protected final double startTimestamp;

        protected Event(double startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

        public abstract int getType();

        public double getStartTimestampDouoble() {
            return startTimestamp;
        }

        public Instant getStartTimestamp() {
            final long seconds = (long)startTimestamp;
            final long nanos = (long)((startTimestamp - seconds) * 1.0e9);
            return Instant.ofEpochSecond(seconds, nanos);
        }

        /**
         * Get start timestamp formatted for the local zone.
         * @return a printable timestamp
         */
        public String getStartTimestampString() {
            Instant startTimestamp = getStartTimestamp();
            return startTimestamp.atOffset(ZoneId.systemDefault().getRules().getOffset(startTimestamp)).toString();
        }
    }

    /**
     * Event callback.
     */
    public interface EventConsumer extends AsyncConsumer<Event> {
        /**
         * Determine whether to continue processing events.
         * @return {@code true} if more events should be processed
         */
        default boolean more() {
            return true;
        }
    }

    /**
     * A GRV latency event.
     */
    public static class EventGetVersion extends Event {
        private final double latency;
        private final int priority;

        public EventGetVersion(double startTimestamp, double latency, int priority) {
            super(startTimestamp);
            this.latency = latency;
            this.priority = priority;
        }

        @Override
        public int getType() {
            return GET_VERSION_LATENCY;
        }

        public double getLatency() {
            return latency;
        }

        public int getPriority() {
            return priority;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("startTimestamp=" + getStartTimestampString())
                    .add("latency=" + latency)
                    .add("priority=" + priority)
                    .toString();
        }
    }

    /**
     * A single key get latency event.
     */
    @SpotBugsSuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
    public static class EventGet extends Event {
        private final double latency;
        private final int size;
        @Nonnull
        private final byte[] key;

        public EventGet(double startTimestamp, double latency, int size, @Nonnull byte[] key) {
            super(startTimestamp);
            this.latency = latency;
            this.size = size;
            this.key = key;
        }

        @Override
        public int getType() {
            return GET_LATENCY;
        }

        public double getLatency() {
            return latency;
        }

        public int getSize() {
            return size;
        }

        @Nonnull
        public byte[] getKey() {
            return key;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("startTimestamp=" + getStartTimestampString())
                    .add("latency=" + latency)
                    .add("size=" + size)
                    .add("key=" + ByteArrayUtil.printable(key))
                    .toString();
        }
    }

    /**
     * A range get latency event.
     */
    public static class EventGetRange extends Event {
        private final double latency;
        private final int size;
        @Nonnull
        private final Range range;

        public EventGetRange(double startTimestamp, double latency, int size, @Nonnull Range range) {
            super(startTimestamp);
            this.latency = latency;
            this.size = size;
            this.range = range;
        }

        @Override
        public int getType() {
            return GET_RANGE_LATENCY;
        }

        public double getLatency() {
            return latency;
        }

        public int getSize() {
            return size;
        }

        @Nonnull
        public Range getRange() {
            return range;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("startTimestamp=" + getStartTimestampString())
                    .add("latency=" + latency)
                    .add("size=" + size)
                    .add("range=" + range)
                    .toString();
        }
    }

    /**
     * A commit latency event.
     */
    public static class EventCommit extends Event {
        private final double latency;
        private final int numMutations;
        private final int commitBytes;
        @Nonnull
        private final CommitRequest commitRequest;

        public EventCommit(double startTimestamp, double latency, int numMutations, int commitBytes,
                           @Nonnull CommitRequest commitRequest) {
            super(startTimestamp);
            this.latency = latency;
            this.numMutations = numMutations;
            this.commitBytes = commitBytes;
            this.commitRequest = commitRequest;
        }

        @Override
        public int getType() {
            return COMMIT_LATENCY;
        }

        public double getLatency() {
            return latency;
        }

        public int getNumMutations() {
            return numMutations;
        }

        public int getCommitBytes() {
            return commitBytes;
        }

        @Nonnull
        public CommitRequest getCommitRequest() {
            return commitRequest;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("startTimestamp=" + getStartTimestampString())
                    .add("latency=" + latency)
                    .add("numMutations=" + numMutations)
                    .add("commitBytes=" + commitBytes)
                    .add("commitRequest=" + commitRequest)
                    .toString();
        }
    }

    /**
     * A failing single key get event.
     */
    @SpotBugsSuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
    public static class EventGetError extends Event {
        private final int errorCode;
        @Nonnull
        private final byte[] key;

        public EventGetError(double startTimestamp, int errorCode, @Nonnull byte[] key) {
            super(startTimestamp);
            this.errorCode = errorCode;
            this.key = key;
        }

        @Override
        public int getType() {
            return ERROR_GET;
        }

        public int getErrorCode() {
            return errorCode;
        }

        @Nonnull
        public byte[] getKey() {
            return key;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("startTimestamp=" + getStartTimestampString())
                    .add("errorCode=" + errorCode)
                    .add("key=" + Arrays.toString(key))
                    .toString();
        }
    }

    /**
     * A failing range get event.
     */
    public static class EventGetRangeError extends Event {
        private final int errorCode;
        @Nonnull
        private final Range range;

        public EventGetRangeError(double startTimestamp, int errorCode, @Nonnull Range range) {
            super(startTimestamp);
            this.errorCode = errorCode;
            this.range = range;
        }

        @Override
        public int getType() {
            return ERROR_GET_RANGE;
        }

        public int getErrorCode() {
            return errorCode;
        }

        @Nonnull
        public Range getRange() {
            return range;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("startTimestamp=" + getStartTimestampString())
                    .add("errorCode=" + errorCode)
                    .add("range=" + range)
                    .toString();
        }
    }

    /**
     * A failing commit event.
     */
    public static class EventCommitError extends Event {
        private final int errorCode;
        @Nonnull
        private final CommitRequest commitRequest;

        public EventCommitError(double startTimestamp, int errorCode, @Nonnull CommitRequest commitRequest) {
            super(startTimestamp);
            this.errorCode = errorCode;
            this.commitRequest = commitRequest;
        }

        @Override
        public int getType() {
            return ERROR_COMMIT;
        }

        public int getErrorCode() {
            return errorCode;
        }

        @Nonnull
        public CommitRequest getCommitRequest() {
            return commitRequest;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("startTimestamp=" + getStartTimestampString())
                    .add("errorCode=" + errorCode)
                    .add("commitRequest=" + commitRequest)
                    .toString();
        }
    }

    /**
     * A single mutation in a {@link CommitRequest}.
     */
    @SpotBugsSuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
    public static class Mutation {
        // These are not in the binding's MutationType enum.
        public static final int SET_VALUE = 0;
        public static final int CLEAR_RANGE = 1;

        private final int type;
        @Nonnull
        private final byte[] key;
        @Nonnull
        private final byte[] param;

        public Mutation(int type, @Nonnull byte[] key, @Nonnull byte[] param) {
            this.type = type;
            this.key = key;
            this.param = param;
        }

        public int getType() {
            return type;
        }

        @Nonnull
        public byte[] getKey() {
            return key;
        }

        @Nonnull
        public byte[] getParam() {
            return param;
        }

        @Override
        public String toString() {
            String typeName;
            if (type == SET_VALUE) {
                typeName = "SET_VALUE";
            } else if (type == CLEAR_RANGE) {
                typeName = "CLEAR_RANGE";
            } else if (MUTATION_TYPE_BY_CODE.containsKey(type)) {
                typeName = MUTATION_TYPE_BY_CODE.get(type).name();
            } else {
                typeName = Integer.toString(type);
            }
            return new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("type=" + typeName)
                    .add("key=" + ByteArrayUtil.printable(key))
                    .add("param=" + ByteArrayUtil.printable(param))
                    .toString();
        }
    }

    /**
     * Information about a commit, successful or not, in an event.
     */
    @SpotBugsSuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
    public static class CommitRequest {
        @Nonnull
        private final Range[] readConflictRanges;
        @Nonnull
        private final Range[] writeConflictRanges;
        @Nonnull
        private final Mutation[] mutations;
        private final long snapshotVersion;

        public CommitRequest(@Nonnull Range[] readConflictRanges, @Nonnull Range[] writeConflictRanges,
                             @Nonnull Mutation[] mutations, long snapshotVersion) {
            this.readConflictRanges = readConflictRanges;
            this.writeConflictRanges = writeConflictRanges;
            this.mutations = mutations;
            this.snapshotVersion = snapshotVersion;
        }

        @Nonnull
        public Range[] getReadConflictRanges() {
            return readConflictRanges;
        }

        @Nonnull
        public Range[] getWriteConflictRanges() {
            return writeConflictRanges;
        }

        @Nonnull
        public Mutation[] getMutations() {
            return mutations;
        }

        public long getSnapshotVersion() {
            return snapshotVersion;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", CommitRequest.class.getSimpleName() + "[", "]")
                    .add("readConflictRanges=" + Arrays.toString(readConflictRanges))
                    .add("writeConflictRanges=" + Arrays.toString(writeConflictRanges))
                    .add("mutations=" + Arrays.toString(mutations))
                    .add("snapshotVersion=" + snapshotVersion)
                    .toString();
        }
    }

    /**
     * Apply a callback to parsed events.
     * @param buffer the transaction's client trace entry
     * @param callback an asynchronous function to apply to each entry
     * @return a future that completes when all items have been processed
     */
    public static CompletableFuture<Void> deserializeEvents(@Nonnull ByteBuffer buffer, @Nonnull AsyncConsumer<Event> callback) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        final long protocolVersion = buffer.getLong();
        if (Longs.indexOf(SUPPORTED_PROTOCOL_VERSIONS, protocolVersion) < 0) {
            throw new IllegalStateException("Unknown protocol version: 0x" + Long.toString(protocolVersion, 16));
        }
        return AsyncUtil.whileTrue(() -> {
            final int type = buffer.getInt();
            final Event event;
            switch (type) {
                case GET_VERSION_LATENCY:
                    event = new EventGetVersion(buffer.getDouble(), buffer.getDouble(),
                            protocolVersion < PROTOCOL_VERSION_6_2 ? 0 : buffer.getInt());
                    break;
                case GET_LATENCY:
                    event = new EventGet(buffer.getDouble(), buffer.getDouble(), buffer.getInt(), deserializeByteArray(buffer));
                    break;
                case GET_RANGE_LATENCY:
                    event = new EventGetRange(buffer.getDouble(), buffer.getDouble(), buffer.getInt(), deserializeRange(buffer));
                    break;
                case COMMIT_LATENCY:
                    event = new EventCommit(buffer.getDouble(), buffer.getDouble(), buffer.getInt(), buffer.getInt(), deserializeCommit(buffer));
                    break;
                case ERROR_GET:
                    event = new EventGetError(buffer.getDouble(), buffer.getInt(), deserializeByteArray(buffer));
                    break;
                case ERROR_GET_RANGE:
                    event = new EventGetRangeError(buffer.getDouble(), buffer.getInt(), deserializeRange(buffer));
                    break;
                case ERROR_COMMIT:
                    event = new EventCommitError(buffer.getDouble(), buffer.getInt(), deserializeCommit(buffer));
                    break;
                default:
                    throw new IllegalStateException("Unknown event type: " + type);
            }
            return callback.accept(event).thenApply(vignore -> buffer.hasRemaining());
        });
    }

    @Nonnull
    protected static byte[] deserializeByteArray(@Nonnull ByteBuffer buffer) {
        final int length = buffer.getInt();
        final byte[] result = new byte[length];
        buffer.get(result);
        return result;
    }

    @Nonnull
    protected static Range deserializeRange(@Nonnull ByteBuffer buffer) {
        return new Range(deserializeByteArray(buffer), deserializeByteArray(buffer));
    }

    @Nonnull
    protected static Range[] deserializeRangeArray(@Nonnull ByteBuffer buffer) {
        final int length = buffer.getInt();
        final Range[] result = new Range[length];
        for (int i = 0; i < length; i++) {
            result[i] = deserializeRange(buffer);
        }
        return result;
    }

    @Nonnull
    protected static Mutation deserializeMutation(@Nonnull ByteBuffer buffer) {
        return new Mutation(buffer.get(), deserializeByteArray(buffer), deserializeByteArray(buffer));
    }

    @Nonnull
    protected static Mutation[] deserializeMutationArray(@Nonnull ByteBuffer buffer) {
        final int length = buffer.getInt();
        final Mutation[] result = new Mutation[length];
        for (int i = 0; i < length; i++) {
            result[i] = deserializeMutation(buffer);
        }
        return result;
    }

    @Nonnull
    protected static CommitRequest deserializeCommit(@Nonnull ByteBuffer buffer) {
        return new CommitRequest(deserializeRangeArray(buffer), deserializeRangeArray(buffer),
                deserializeMutationArray(buffer), buffer.getLong());
    }

    protected static class EventDeserializer implements AsyncConsumer<KeyValue> {
        @Nonnull
        private final AsyncConsumer<Event> callback;
        @Nullable
        private ByteBuffer splitBuffer;
        private byte[] splitId;
        private int splitPosition;
        private byte[] lastProcessedKey;

        public EventDeserializer(@Nonnull AsyncConsumer<Event> callback) {
            this.callback = callback;
        }

        /**
         * Process next key-value pair by calling callback or appending to pending buffer.
         * @param keyValue a key-value pair with client trace events
         * @return a future which completes when the key-value pair has been processed
         */
        @Override
        public CompletableFuture<Void> accept(KeyValue keyValue) {
            final byte[] transactionId = Arrays.copyOfRange(keyValue.getKey(), EVENT_KEY_ID_START_INDEX, EVENT_KEY_ID_END_INDEX);
            final ByteBuffer keyBuffer = ByteBuffer.wrap(keyValue.getKey());
            keyBuffer.position(EVENT_KEY_CHUNK_INDEX);
            final int chunkNumber = keyBuffer.getInt();
            final int numChunks = keyBuffer.getInt();
            if (numChunks == 1) {
                splitBuffer = null;
                lastProcessedKey = keyValue.getKey();
                return deserializeEvents(ByteBuffer.wrap(keyValue.getValue()), callback);
            } else {
                if (chunkNumber == 1) {
                    splitBuffer = ByteBuffer.allocate(numChunks * keyValue.getValue().length);
                    splitId = transactionId;
                    splitPosition = 1;
                } else if (chunkNumber == splitPosition && Arrays.equals(transactionId, splitId)) {
                    if (splitBuffer.remaining() < keyValue.getValue().length) {
                        final ByteBuffer newBuffer = ByteBuffer.allocate(splitBuffer.remaining() + keyValue.getValue().length);
                        splitBuffer.flip();
                        newBuffer.put(splitBuffer);
                        splitBuffer = newBuffer;
                    }
                    splitBuffer.put(keyValue.getValue());
                    splitPosition++;
                    if (splitPosition == numChunks) {
                        splitBuffer.flip();
                        ByteBuffer buffer = splitBuffer;
                        splitBuffer = null;
                        lastProcessedKey = keyValue.getKey();
                        return deserializeEvents(buffer, callback);
                    }
                } else {
                    splitBuffer = null;
                    splitPosition = -1;
                    lastProcessedKey = keyValue.getKey();
                }
            }
            return AsyncUtil.DONE;
        }

        /**
         * Get the last key fully processed.
         *
         * This is not the last key passed to {@link #accept(KeyValue)}, since an early part of a split entry is buffered.
         * This is the key from which to resume in a new transaction.
         * @return the processed key
         */
        @Nullable
        @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
        public byte[] getLastProcessedKey() {
            return lastProcessedKey;
        }
    }

    /**
     * Invoke a callback on each event in a range of the key-value store.
     * @param range the range from which to parse events
     * @param callback the callback to invoke
     * @return a future which completes when all (whole) events in the range have been processed
     */
    @Nonnull
    public static CompletableFuture<byte[]> forEachEvent(@Nonnull AsyncIterable<KeyValue> range, @Nonnull EventConsumer callback) {
        final EventDeserializer deserializer = new EventDeserializer(callback);
        final AsyncIterator<KeyValue> iterator = range.iterator();
        return AsyncUtil.whileTrue(() -> iterator.onHasNext()
                .thenCompose(hasNext -> hasNext ? deserializer.accept(iterator.next()).thenApply(vignore -> callback.more()) : AsyncUtil.READY_FALSE))
                .thenApply(vignore -> deserializer.getLastProcessedKey());
    }

    /**
     * Get the key at which a particular commit version would be recorded.
     * @param version the version, in the same extent as, e.g., {@link com.apple.foundationdb.Transaction#getReadVersion()}
     * @return the encoded key
     */
    @Nonnull
    public static byte[] eventKeyForVersion(@Nonnull long version) {
        // Do not include the two bytes for the transaction number at this version.
        final byte[] result = new byte[EVENT_KEY_VERSION_END_INDEX - 2];
        final ByteBuffer buffer = ByteBuffer.wrap(result);
        buffer.put(EVENT_KEY_PREFIX);
        buffer.putLong(version);
        return result;
    }

    private FDBClientLogEvents() {
    }
}
