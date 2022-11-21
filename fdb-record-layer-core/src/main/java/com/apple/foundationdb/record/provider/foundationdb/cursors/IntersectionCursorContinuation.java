/*
 * IntersectionCursorContinuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

class IntersectionCursorContinuation extends MergeCursorContinuation<RecordCursorProto.IntersectionContinuation.Builder, RecordCursorContinuation> {
    @Nonnull
    private static final RecordCursorProto.IntersectionContinuation.CursorState EXHAUSTED_PROTO = RecordCursorProto.IntersectionContinuation.CursorState.newBuilder()
            .setStarted(true)
            .build();
    @Nonnull
    private static final RecordCursorProto.IntersectionContinuation.CursorState START_PROTO = RecordCursorProto.IntersectionContinuation.CursorState.newBuilder()
            .setStarted(false)
            .build();

    private IntersectionCursorContinuation(@Nonnull List<RecordCursorContinuation> continuations,
                                           @Nullable RecordCursorProto.IntersectionContinuation originalProto) {
        super(continuations, originalProto);
    }

    private IntersectionCursorContinuation(@Nonnull List<RecordCursorContinuation> continuations) {
        this(continuations, null);
    }

    @Override
    protected void setFirstChild(@Nonnull RecordCursorProto.IntersectionContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        ByteString asBytes = continuation.toByteString();
        if (asBytes.isEmpty() && !continuation.isEnd()) { // first cursor has not started
            builder.setFirstStarted(false);
        } else {
            builder.setFirstStarted(true);
            if (!asBytes.isEmpty()) {
                builder.setFirstContinuation(asBytes);
            }
        }
    }

    @Override
    protected void setSecondChild(@Nonnull RecordCursorProto.IntersectionContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        ByteString asBytes = continuation.toByteString();
        if (asBytes.isEmpty() && !continuation.isEnd()) { // second cursor not started
            builder.setSecondStarted(false);
        } else {
            builder.setSecondStarted(true);
            if (!asBytes.isEmpty()) {
                builder.setSecondContinuation(asBytes);
            }
        }
    }

    @Override
    protected void addOtherChild(@Nonnull RecordCursorProto.IntersectionContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        final RecordCursorProto.IntersectionContinuation.CursorState cursorState;
        if (continuation.isEnd()) {
            cursorState = EXHAUSTED_PROTO;
        } else {
            ByteString asBytes = continuation.toByteString();
            if (asBytes.isEmpty() && !continuation.isEnd()) {
                cursorState = START_PROTO;
            } else {
                cursorState = RecordCursorProto.IntersectionContinuation.CursorState.newBuilder()
                        .setStarted(true)
                        .setContinuation(asBytes)
                        .build();
            }
        }
        builder.addOtherChildState(cursorState);
    }

    @Override
    @Nonnull
    protected RecordCursorProto.IntersectionContinuation.Builder newProtoBuilder() {
        return RecordCursorProto.IntersectionContinuation.newBuilder();
    }

    @Override
    public boolean isEnd() {
        // one of the children have actually stopped, so the intersection will have no more records
        return getContinuations().stream().anyMatch(RecordCursorContinuation::isEnd);
    }

    @Nonnull
    static IntersectionCursorContinuation from(@Nonnull IntersectionCursorBase<?, ?> cursor) {
        return new IntersectionCursorContinuation(cursor.getChildContinuations());
    }

    @Nonnull
    static IntersectionCursorContinuation from(@Nullable byte[] bytes, int numberOfChildren) {
        if (bytes == null) {
            return new IntersectionCursorContinuation(Collections.nCopies(numberOfChildren, RecordCursorStartContinuation.START));
        }
        try {
            return IntersectionCursorContinuation.from(RecordCursorProto.IntersectionContinuation.parseFrom(bytes), numberOfChildren);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
        }
    }

    @Nonnull
    static IntersectionCursorContinuation from(@Nonnull RecordCursorProto.IntersectionContinuation parsed, int numberOfChildren) {
        ImmutableList.Builder<RecordCursorContinuation> builder = ImmutableList.builder();
        if (!parsed.getFirstStarted()) {
            builder.add(RecordCursorStartContinuation.START);
        } else if (parsed.hasFirstContinuation()) {
            builder.add(ByteArrayContinuation.fromNullable(parsed.getFirstContinuation().toByteArray()));
        } else {
            builder.add(RecordCursorEndContinuation.END);
        }
        if (!parsed.getSecondStarted()) {
            builder.add(RecordCursorStartContinuation.START);
        } else if (parsed.hasSecondContinuation()) {
            builder.add(ByteArrayContinuation.fromNullable(parsed.getSecondContinuation().toByteArray()));
        } else {
            builder.add(RecordCursorEndContinuation.END);
        }
        for (RecordCursorProto.IntersectionContinuation.CursorState state : parsed.getOtherChildStateList()) {
            if (!state.getStarted()) {
                builder.add(RecordCursorStartContinuation.START);
            } else if (state.hasContinuation()) {
                builder.add(ByteArrayContinuation.fromNullable(state.getContinuation().toByteArray()));
            } else {
                builder.add(RecordCursorEndContinuation.END);
            }
        }
        ImmutableList<RecordCursorContinuation> children = builder.build();
        if (children.size() != numberOfChildren) {
            throw new RecordCoreArgumentException("invalid continuation (extraneous child state information present)")
                    .addLogInfo(LogMessageKeys.EXPECTED_CHILD_COUNT, numberOfChildren - 2)
                    .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, parsed.getOtherChildStateCount());
        }
        return new IntersectionCursorContinuation(children, parsed);
    }

}
