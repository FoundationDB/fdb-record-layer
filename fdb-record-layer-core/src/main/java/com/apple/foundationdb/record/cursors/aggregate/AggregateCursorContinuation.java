/*
 * AggregateCursorContinuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors.aggregate;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Wrapper around the {@link com.apple.foundationdb.record.RecordCursorProto.StreamingAggregateContinuation} protobuf class.
 * This class adds convenience handling of the continuation to bridge the protobuf handling with the business logic.
 */
public class AggregateCursorContinuation implements RecordCursorContinuation {
    private boolean seenRecords;
    @Nonnull
    private List<RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState> groupingKeyStates;
    @Nonnull
    private List<RecordCursorProto.StreamingAggregateContinuation.AccumulatorState> accumulatorStates;
    @Nullable
    private ByteString innerContinuation;


    /**
     * Initialize the continuation from an incoming protobuf message. This will deserialize the continuation and
     * populate the fields.
     *
     * @param rawBytes the raw bytes of hte continuation to be deserialized
     */
    public AggregateCursorContinuation(@Nullable byte[] rawBytes) {
        seenRecords = false;
        groupingKeyStates = Collections.emptyList();
        accumulatorStates = Collections.emptyList();
        innerContinuation = null;

        try {
            if (rawBytes != null) {
                RecordCursorProto.StreamingAggregateContinuation continuation = RecordCursorProto.StreamingAggregateContinuation.parseFrom(rawBytes);
                if (continuation.hasSeenRecords()) {
                    seenRecords = continuation.getSeenRecords();
                }
                if (continuation.getGroupingKeyStatesCount() > 0) {
                    groupingKeyStates = continuation.getGroupingKeyStatesList();
                }
                if (continuation.getAccumulatorStatesCount() > 0) {
                    accumulatorStates = continuation.getAccumulatorStatesList();
                }
                if (continuation.hasInnerContinuation()) {
                    innerContinuation = continuation.getInnerContinuation();
                }
            }
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("error parsing continuation", ex)
                    .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(rawBytes));
        }
    }

    /**
     * Construct a new continuation to be serialized from existing fields.
     * @param seenRecords Whether the aggregation has seen any records so far
     * @param groupingKeyStates The values for the current group being aggregated
     * @param accumulatorStates The current states for the aggregators collecting values
     * @param innerContinuation The continuation of the inner cursor
     */
    public AggregateCursorContinuation(boolean seenRecords,
                                       @Nonnull List<ContinuationGroupingKeyState> groupingKeyStates,
                                       @Nonnull List<ContinuationAccumulatorState> accumulatorStates,
                                       @Nullable byte[] innerContinuation) {
        this.seenRecords = seenRecords;
        this.groupingKeyStates = groupingKeyStates.stream().map(ContinuationGroupingKeyState::getState).collect(Collectors.toList());
        this.accumulatorStates = accumulatorStates.stream().map(ContinuationAccumulatorState::getState).collect(Collectors.toList());
        if (innerContinuation != null) {
            this.innerContinuation = ByteString.copyFrom(innerContinuation);
        } else {
            this.innerContinuation = null;
        }
    }

    /**
     * Serialize the continuation to a byte array that can be sent back as protobuf.
     *
     * @return The byte[] representing the serialized continuation
     */
    @Nullable
    @Override
    public byte[] toBytes() {
        RecordCursorProto.StreamingAggregateContinuation continuation = RecordCursorProto.StreamingAggregateContinuation.newBuilder()
                .setSeenRecords(seenRecords)
                .addAllGroupingKeyStates(groupingKeyStates)
                .addAllAccumulatorStates(accumulatorStates)
                .setInnerContinuation(innerContinuation)
                .build();
        return continuation.toByteArray();
    }

    @Override
    public boolean isEnd() {
        // Since the end of the stream is determined by the inner continuation, when this continuation is produced, there
        // is always a record to return. On the next read, if the stream is exhausted, this continuation will not be produced.
        return false;
    }

    public boolean seenRecords() {
        return seenRecords;
    }

    /**
     * Return the GroupingKey state wrappers.
     *
     * @return A list of the wrappers of the continuation GroupingKeys
     */
    @Nonnull
    public List<ContinuationGroupingKeyState> getGroupingKeyStates() {
        return groupingKeyStates.stream()
                .map(ContinuationGroupingKeyState::new)
                .collect(Collectors.toList());
    }

    /**
     * return the Aggregation state wrappers.
     *
     * @return A list of the wrappers of the continuation Aggregators
     */
    @Nonnull
    public List<ContinuationAccumulatorState> getAccumulatorStates() {
        return accumulatorStates.stream()
                .map(ContinuationAccumulatorState::new)
                .collect(Collectors.toList());
    }

    @Nullable
    public ByteString getInnerContinuation() {
        return innerContinuation;
    }

    @Nullable
    public byte[] getInnerContinuationAsBytes() {
        return (innerContinuation == null) ? null : innerContinuation.toByteArray();
    }

    /**
     * Wrapper around the {@link com.apple.foundationdb.record.RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState}.
     * Adds methods to translate to/from primitive types.
     */
    public static class ContinuationGroupingKeyState {
        public static final ContinuationGroupingKeyState EMPTY = new ContinuationGroupingKeyState((Integer)null);

        @Nonnull
        private RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState state;

        public ContinuationGroupingKeyState(final @Nonnull RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState state) {
            this.state = state;
        }

        public ContinuationGroupingKeyState(@Nullable final Integer value) {
            RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.newBuilder();
            if (value != null) {
                builder.setIntValue(value);
            }
            state = builder.build();
        }

        public ContinuationGroupingKeyState(@Nullable final Long value) {
            RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.newBuilder();
            if (value != null) {
                builder.setLongValue(value);
            }
            state = builder.build();
        }

        public ContinuationGroupingKeyState(@Nullable final Float value) {
            RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.newBuilder();
            if (value != null) {
                builder.setFloatValue(value);
            }
            state = builder.build();
        }

        public ContinuationGroupingKeyState(@Nullable final Double value) {
            RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.newBuilder();
            if (value != null) {
                builder.setDoubleValue(value);
            }
            state = builder.build();
        }

        public ContinuationGroupingKeyState(@Nullable final String value) {
            RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState.newBuilder();
            if (value != null) {
                builder.setStringValue(value);
            }
            state = builder.build();
        }

        @Nonnull
        public RecordCursorProto.StreamingAggregateContinuation.GroupingKeyState getState() {
            return state;
        }

        /**
         * Get the "flattened" Java object from the state.
         *
         * @return the Java (boxed primitive) object from the state, if exists
         */
        @Nullable
        public Object getStateValue() {
            if (state.hasIntValue()) {
                return state.getIntValue();
            } else if (state.hasLongValue()) {
                return state.getLongValue();
            } else if (state.hasFloatValue()) {
                return state.getFloatValue();
            } else if (state.hasDoubleValue()) {
                return state.getDoubleValue();
            } else if (state.hasStringValue()) {
                return state.getStringValue();
            } else {
                return null;
            }
        }
    }

    /**
     * Wrapper around the {@link com.apple.foundationdb.record.RecordCursorProto.StreamingAggregateContinuation.AccumulatorState}.
     * Adds methods to translate to/from primitive types.
     */
    public static class ContinuationAccumulatorState {
        public static final ContinuationAccumulatorState EMPTY = new ContinuationAccumulatorState((Integer)null, null);

        @Nonnull
        RecordCursorProto.StreamingAggregateContinuation.AccumulatorState state;

        public ContinuationAccumulatorState(final @Nonnull RecordCursorProto.StreamingAggregateContinuation.AccumulatorState state) {
            this.state = state;
        }

        public ContinuationAccumulatorState(@Nullable final Integer value, @Nullable final Long count) {
            RecordCursorProto.StreamingAggregateContinuation.AccumulatorState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.AccumulatorState.newBuilder();
            if (value != null) {
                builder.setIntValue(value);
            }
            if (count != null) {
                builder.setCount(count);
            }
            state = builder.build();
        }

        public ContinuationAccumulatorState(@Nullable final Long value, @Nullable final Long count) {
            RecordCursorProto.StreamingAggregateContinuation.AccumulatorState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.AccumulatorState.newBuilder();
            if (value != null) {
                builder.setLongValue(value);
            }
            if (count != null) {
                builder.setCount(count);
            }
            state = builder.build();
        }

        public ContinuationAccumulatorState(@Nullable final Float value, @Nullable final Long count) {
            RecordCursorProto.StreamingAggregateContinuation.AccumulatorState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.AccumulatorState.newBuilder();
            if (value != null) {
                builder.setFloatValue(value);
            }
            if (count != null) {
                builder.setCount(count);
            }
            state = builder.build();
        }

        public ContinuationAccumulatorState(@Nullable final Double value, @Nullable final Long count) {
            RecordCursorProto.StreamingAggregateContinuation.AccumulatorState.Builder builder = RecordCursorProto.StreamingAggregateContinuation.AccumulatorState.newBuilder();
            if (value != null) {
                builder.setDoubleValue(value);
            }
            if (count != null) {
                builder.setCount(count);
            }
            state = builder.build();
        }

        /**
         * Factory method to add a count to an existing state.
         *
         * @param newCount the count value to set
         *
         * @return a new AggregatorState with the given count
         */
        @Nonnull
        public ContinuationAccumulatorState withCount(@Nullable Long newCount) {
            if (newCount != null) {
                return new ContinuationAccumulatorState(state.toBuilder().setCount(newCount).build());
            } else {
                return new ContinuationAccumulatorState(state.toBuilder().clearCount().build());
            }
        }

        @Nonnull
        public RecordCursorProto.StreamingAggregateContinuation.AccumulatorState getState() {
            return state;
        }

        /**
         * Get the "flattened" Java object from the state.
         *
         * @return the Java (boxed primitive) object from the state, if exists
         */
        @Nullable
        public Object getValue() {
            if (state.hasIntValue()) {
                return state.getIntValue();
            } else if (state.hasLongValue()) {
                return state.getLongValue();
            } else if (state.hasFloatValue()) {
                return state.getFloatValue();
            } else if (state.hasDoubleValue()) {
                return state.getDoubleValue();
            } else {
                return null;
            }
        }

        @Nullable
        public Long getCount() {
            return (state.hasCount()) ? state.getCount() : null;
        }
    }
}
