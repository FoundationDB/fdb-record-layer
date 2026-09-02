/*
 * UnionCursorContinuation.java
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
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.jspecify.annotations.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.apple.foundationdb.record.RecordCursorProto.UnionContinuation;

class UnionCursorContinuation extends MergeCursorContinuation<UnionContinuation.Builder, RecordCursorContinuation> {
    private static final UnionContinuation.CursorState EXHAUSTED_PROTO = UnionContinuation.CursorState.newBuilder()
            .setExhausted(true)
            .build();
    private static final UnionContinuation.CursorState START_PROTO = UnionContinuation.CursorState.newBuilder()
            .setExhausted(false)
            .build();

    protected UnionCursorContinuation(List<RecordCursorContinuation> continuations, @Nullable UnionContinuation originalProto) {
        super(continuations, originalProto);
    }

    protected UnionCursorContinuation(List<RecordCursorContinuation> continuations) {
        this(continuations, null);
    }

    @Override
    protected void setFirstChild(UnionContinuation.Builder builder, RecordCursorContinuation continuation) {
        if (continuation.isEnd()) {
            builder.setFirstExhausted(true);
        } else {
            final ByteString asBytes = continuation.toByteString();
            if (!asBytes.isEmpty()) {
                builder.setFirstContinuation(asBytes);
            }
        }
    }

    @Override
    protected void setSecondChild(UnionContinuation.Builder builder, RecordCursorContinuation continuation) {
        if (continuation.isEnd()) {
            builder.setSecondExhausted(true);
        } else {
            final ByteString asBytes = continuation.toByteString();
            if (!asBytes.isEmpty()) {
                builder.setSecondContinuation(asBytes);
            }
        }
    }

    @Override
    protected void addOtherChild(UnionContinuation.Builder builder, RecordCursorContinuation continuation) {
        UnionContinuation.CursorState cursorState;
        if (continuation.isEnd()) {
            cursorState = EXHAUSTED_PROTO;
        } else {
            final ByteString asBytes = continuation.toByteString();
            if (asBytes.isEmpty()) {
                cursorState = START_PROTO;
            } else {
                cursorState = UnionContinuation.CursorState.newBuilder()
                        .setContinuation(asBytes)
                        .build();
            }
        }
        builder.addOtherChildState(cursorState);
    }

    @Override
    protected UnionContinuation.Builder newProtoBuilder() {
        return UnionContinuation.newBuilder();
    }

    @Override
    public boolean isEnd() {
        // As long as at least one child has data, this should not be an end continuation.
        return getContinuations().stream().allMatch(RecordCursorContinuation::isEnd);
    }

    static UnionCursorContinuation from(UnionCursorBase<?, ?> cursor) {
        return new UnionCursorContinuation(cursor.getChildContinuations());
    }

    @SuppressWarnings({"PMD.PreserveStackTrace", "NullAway"}) // NullAway/JSpecify does not currently track @Nullable on array (byte[])
                                                               // parameters, even across explicit null checks.
    static UnionCursorContinuation from(@Nullable byte[] bytes, int numberOfChildren) {
        if (bytes == null) {
            return new UnionCursorContinuation(Collections.nCopies(numberOfChildren, RecordCursorStartContinuation.START));
        }
        try {
            return UnionCursorContinuation.from(UnionContinuation.parseFrom(bytes), numberOfChildren);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, Objects.requireNonNull(ByteArrayUtil2.loggable(bytes)));
        } catch (RecordCoreArgumentException ex) {
            throw ex.addLogInfo(LogMessageKeys.RAW_BYTES, Objects.requireNonNull(ByteArrayUtil2.loggable(bytes)));
        }
    }

    static UnionCursorContinuation from(UnionContinuation parsed, int numberOfChildren) {
        ImmutableList.Builder<RecordCursorContinuation> builder = ImmutableList.builder();
        if (parsed.hasFirstContinuation()) {
            builder.add(ByteArrayContinuation.fromNullable(parsed.getFirstContinuation().toByteArray()));
        } else if (parsed.getFirstExhausted()) {
            builder.add(RecordCursorEndContinuation.END);
        } else {
            builder.add(RecordCursorStartContinuation.START);
        }
        if (parsed.hasSecondContinuation()) {
            builder.add(ByteArrayContinuation.fromNullable(parsed.getSecondContinuation().toByteArray()));
        } else if (parsed.getSecondExhausted()) {
            builder.add(RecordCursorEndContinuation.END);
        } else {
            builder.add(RecordCursorStartContinuation.START);
        }
        for (UnionContinuation.CursorState state : parsed.getOtherChildStateList()) {
            if (state.hasContinuation()) {
                builder.add(ByteArrayContinuation.fromNullable(state.getContinuation().toByteArray()));
            } else if (state.getExhausted()) {
                builder.add(RecordCursorEndContinuation.END);
            } else {
                builder.add(RecordCursorStartContinuation.START);
            }
        }
        ImmutableList<RecordCursorContinuation> children = builder.build();
        if (children.size() != numberOfChildren) {
            throw new RecordCoreArgumentException("invalid continuation (expected continuation count does not match read)")
                    .addLogInfo(LogMessageKeys.EXPECTED_CHILD_COUNT, numberOfChildren)
                    .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, children.size());
        }
        return new UnionCursorContinuation(children, parsed);
    }
}
