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
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.system.SystemKeyspace;
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
    public static final long PROTOCOL_VERSION_6_3 = 0x0FDB00B063010001L;
    public static final long PROTOCOL_VERSION_7_0 = 0x0FDB00B070010001L;
    public static final long PROTOCOL_VERSION_7_1 = 0x0FDB00B071010000L;
    public static final long PROTOCOL_VERSION_7_2 = 0x0FDB00B072000000L;
    public static final long PROTOCOL_VERSION_7_3 = 0x0FDB00B073000000L;
    private static final long[] SUPPORTED_PROTOCOL_VERSIONS = {
            PROTOCOL_VERSION_5_2, PROTOCOL_VERSION_6_0, PROTOCOL_VERSION_6_1, PROTOCOL_VERSION_6_2, PROTOCOL_VERSION_6_3,
            PROTOCOL_VERSION_7_0, PROTOCOL_VERSION_7_1, PROTOCOL_VERSION_7_2, PROTOCOL_VERSION_7_3
    };

    //                    0               1         2         3         4         5         6         7
    //                    0   1   23456789012345678901234567890123456789012345678901234567890123456789012345...
    // Event key pattern: \xff\x02/fdbClientInfo/client_latency/SSSSSSSSSS/RRRRRRRRRRRRRRRR/NNNNTTTT/XXXX/
    public static final int EVENT_KEY_VERSION_END_INDEX = 42;
    public static final int EVENT_KEY_ID_START_INDEX = 43;
    public static final int EVENT_KEY_ID_END_INDEX = 59;
    public static final int EVENT_KEY_CHUNK_INDEX = 60;

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
        protected final String dcId;
        protected final String tenant;

        protected Event(double startTimestamp, String dcId, String tenant) {
            this.startTimestamp = startTimestamp;
            this.dcId = dcId;
            this.tenant = tenant;
        }

        public abstract int getType();

        public double getStartTimestampDouble() {
            return startTimestamp;
        }

        public Instant getStartTimestamp() {
            final long seconds = (long)startTimestamp;
            final long nanos = (long)((startTimestamp - seconds) * 1.0e9);
            return Instant.ofEpochSecond(seconds, nanos);
        }

        /**
         * Get start timestamp formatted for the local time zone.
         * @return a printable timestamp
         */
        public String getStartTimestampString() {
            Instant startTimestamp = getStartTimestamp();
            return startTimestamp.atOffset(ZoneId.systemDefault().getRules().getOffset(startTimestamp)).toString();
        }

        public String getDcId() {
            return dcId;
        }

        public String getTenant() {
            return tenant;
        }

        protected StringJoiner toStringBase() {
            StringJoiner joiner = new StringJoiner(", ", getClass().getSimpleName() + "[", "]")
                    .add("startTimestamp=" + getStartTimestampString());
            if (dcId.length() > 0) {
                joiner.add("dcId=" + dcId);
            }
            if (tenant.length() > 0) {
                joiner.add("tenant=" + tenant);
            }
            return joiner;
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
        private final long readVersion;

        public EventGetVersion(double startTimestamp, String dcId, String tenant, double latency, int priority, long readVersion) {
            super(startTimestamp, dcId, tenant);
            this.latency = latency;
            this.priority = priority;
            this.readVersion = readVersion;
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

        public long getReadVersion() {
            return readVersion;
        }

        @Override
        public String toString() {
            return toStringBase()
                    .add("latency=" + latency)
                    .add("priority=" + priority)
                    .add("readVersion=" + readVersion)
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

        public EventGet(double startTimestamp, String dcId, String tenant, double latency, int size, @Nonnull byte[] key) {
            super(startTimestamp, dcId, tenant);
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
            return toStringBase()
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

        public EventGetRange(double startTimestamp, String dcId, String tenant, double latency, int size, @Nonnull Range range) {
            super(startTimestamp, dcId, tenant);
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
            return toStringBase()
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
        private final long commitVersion;
        @Nonnull
        private final CommitRequest commitRequest;

        public EventCommit(double startTimestamp, String dcId, String tenant, double latency, int numMutations, int commitBytes, long commitVersion,
                           @Nonnull CommitRequest commitRequest) {
            super(startTimestamp, dcId, tenant);
            this.latency = latency;
            this.numMutations = numMutations;
            this.commitBytes = commitBytes;
            this.commitVersion = commitVersion;
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
            return toStringBase()
                    .add("latency=" + latency)
                    .add("numMutations=" + numMutations)
                    .add("commitBytes=" + commitBytes)
                    .add("commitVersion=" + commitVersion)
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

        public EventGetError(double startTimestamp, String dcId, String tenant, int errorCode, @Nonnull byte[] key) {
            super(startTimestamp, dcId, tenant);
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
            return toStringBase()
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

        public EventGetRangeError(double startTimestamp, String dcId, String tenant, int errorCode, @Nonnull Range range) {
            super(startTimestamp, dcId, tenant);
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
            return toStringBase()
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

        public EventCommitError(double startTimestamp, String dcId, String tenant, int errorCode, @Nonnull CommitRequest commitRequest) {
            super(startTimestamp, dcId, tenant);
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
            return toStringBase()
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
        public static final int MIN_V2 = 18;
        public static final int AND_V2 = 19;

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
            } else if (type == MIN_V2) {
                typeName = "MIN_V2";
            } else if (type == AND_V2) {
                typeName = "AND_V2";
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
        private final boolean reportConflictingKeys;
        private final boolean lockAware;
        @Nullable
        private final SpanContext spanContext;

        public CommitRequest(@Nonnull Range[] readConflictRanges,
                             @Nonnull Range[] writeConflictRanges,
                             @Nonnull Mutation[] mutations,
                             long snapshotVersion,
                             boolean reportConflictingKeys,
                             boolean lockAware,
                             @Nullable SpanContext spanContext) {
            this.readConflictRanges = readConflictRanges;
            this.writeConflictRanges = writeConflictRanges;
            this.mutations = mutations;
            this.snapshotVersion = snapshotVersion;
            this.reportConflictingKeys = reportConflictingKeys;
            this.lockAware = lockAware;
            this.spanContext = spanContext;
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

        public boolean isReportConflictingKeys() {
            return reportConflictingKeys;
        }

        public boolean isLockAware() {
            return lockAware;
        }

        @Nullable
        public SpanContext getSpanContext() {
            return spanContext;
        }

        @Override
        public String toString() {
            final StringJoiner joiner = new StringJoiner(", ", CommitRequest.class.getSimpleName() + "[", "]")
                    .add("readConflictRanges=" + Arrays.toString(readConflictRanges))
                    .add("writeConflictRanges=" + Arrays.toString(writeConflictRanges))
                    .add("mutations=" + Arrays.toString(mutations))
                    .add("snapshotVersion=" + snapshotVersion)
                    .add("reportConflictingKeys=" + reportConflictingKeys)
                    .add("lockAware=" + lockAware);
            if (spanContext != null) {
                joiner.add("spanContext=" + spanContext);
            }
            return joiner.toString();
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
            final double startTime = buffer.getDouble();
            String dcId = "";
            if (protocolVersion >= PROTOCOL_VERSION_6_3) {
                int dcIdLength = buffer.getInt();
                if (dcIdLength > 0) {
                    byte[] dcIdBytes = new byte[dcIdLength];
                    buffer.get(dcIdBytes);
                    dcId = new String(dcIdBytes, StandardCharsets.UTF_8);
                }
            }
            String tenant = "";
            if (protocolVersion >= PROTOCOL_VERSION_7_1) {
                boolean present = buffer.get() != 0;
                if (present) {
                    int tenantLength = buffer.getInt();
                    if (tenantLength > 0) {
                        byte[] tenantBytes = new byte[tenantLength];
                        buffer.get(tenantBytes);
                        tenant = new String(tenantBytes, StandardCharsets.UTF_8);
                    }
                }
            }
            final Event event;
            switch (type) {
                case GET_VERSION_LATENCY:
                    event = new EventGetVersion(startTime, dcId, tenant,
                            buffer.getDouble(),
                            protocolVersion < PROTOCOL_VERSION_6_2 ? 0 : buffer.getInt(),
                            protocolVersion < PROTOCOL_VERSION_6_3 ? 0L : buffer.getLong());
                    break;
                case GET_LATENCY:
                    event = new EventGet(startTime, dcId, tenant,
                            buffer.getDouble(), buffer.getInt(), deserializeByteArray(buffer));
                    break;
                case GET_RANGE_LATENCY:
                    event = new EventGetRange(startTime, dcId, tenant,
                            buffer.getDouble(), buffer.getInt(), deserializeRange(buffer));
                    break;
                case COMMIT_LATENCY:
                    event = new EventCommit(startTime, dcId, tenant,
                            buffer.getDouble(), buffer.getInt(), buffer.getInt(),
                            protocolVersion < PROTOCOL_VERSION_6_3 ? 0L : buffer.getLong(),
                            deserializeCommit(protocolVersion, buffer));
                    break;
                case ERROR_GET:
                    event = new EventGetError(startTime, dcId, tenant,
                            buffer.getInt(), deserializeByteArray(buffer));
                    break;
                case ERROR_GET_RANGE:
                    event = new EventGetRangeError(startTime, dcId, tenant,
                            buffer.getInt(), deserializeRange(buffer));
                    break;
                case ERROR_COMMIT:
                    event = new EventCommitError(startTime, dcId, tenant,
                            buffer.getInt(), deserializeCommit(protocolVersion, buffer));
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
    protected static CommitRequest deserializeCommit(long protocolVersion, @Nonnull ByteBuffer buffer) {
        final Range[] readConflictRanges = deserializeRangeArray(buffer);
        final Range[] writeConflictRanges = deserializeRangeArray(buffer);
        final Mutation[] mutations = deserializeMutationArray(buffer);
        final long snapshotVersion = buffer.getLong();
        boolean reportConflictingKeys = false;
        if (protocolVersion >= PROTOCOL_VERSION_6_3) {
            reportConflictingKeys = buffer.get() != 0;
        }
        boolean lockAware = false;
        SpanContext spanContext = null;
        if (protocolVersion >= PROTOCOL_VERSION_7_1) {
            lockAware = buffer.get() != 0;
            boolean present = buffer.get() != 0;
            if (present) {
                spanContext = new SpanContext(new Uid(buffer.getLong(), buffer.getLong()), buffer.getLong(), buffer.get());
            }
        }
        return new CommitRequest(
                readConflictRanges,
                writeConflictRanges,
                mutations,
                snapshotVersion,
                reportConflictingKeys,
                lockAware,
                spanContext);
    }

    protected static class Uid {
        private final long part1;
        private final long part2;

        public Uid(final long part1, final long part2) {
            this.part1 = part1;
            this.part2 = part2;
        }

        @Override
        public String toString() {
            String hexPart1 = Long.toHexString(part1);
            String hexPart2 = Long.toHexString(part2);
            return ("0".repeat(16 - hexPart1.length())) + hexPart1 + ("0".repeat(16 - hexPart2.length())) + hexPart2;
        }
    }

    protected static class SpanContext {
        private final Uid traceID;
        private final long spanID;
        private final byte flags;

        public Uid getTraceID() {
            return traceID;
        }

        public long getSpanID() {
            return spanID;
        }

        public byte getFlags() {
            return flags;
        }

        public SpanContext(final Uid traceID, final long spanID, final byte flags) {
            this.traceID = traceID;
            this.spanID = spanID;
            this.flags = flags;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", CommitRequest.class.getSimpleName() + "[", "]")
                    .add("traceID=" + traceID)
                    .add("spanID=" + spanID)
                    .add("flags=" + flags)
                    .toString();
        }
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
                        final ByteBuffer newBuffer = ByteBuffer.allocate(splitBuffer.position() + keyValue.getValue().length);
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
    public static byte[] eventKeyForVersion(long version) {
        // Do not include the two bytes for the transaction number at this version.
        final byte[] result = new byte[EVENT_KEY_VERSION_END_INDEX - 2];
        final ByteBuffer buffer = ByteBuffer.wrap(result);
        buffer.put(SystemKeyspace.CLIENT_LOG_KEY_PREFIX);
        buffer.putLong(version);
        return result;
    }

    private FDBClientLogEvents() {
    }
}
