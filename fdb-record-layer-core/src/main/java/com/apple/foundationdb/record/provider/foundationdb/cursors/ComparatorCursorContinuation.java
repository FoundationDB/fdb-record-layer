/*
 * ComparatorCursorContinuation.java
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

class ComparatorCursorContinuation extends MergeCursorContinuation<RecordCursorProto.ComparatorContinuation.Builder, RecordCursorContinuation> {

    private final int referencePlanIndex;

    @Nonnull
    private static final RecordCursorProto.ComparatorContinuation.CursorState EXHAUSTED_PROTO = RecordCursorProto.ComparatorContinuation.CursorState.newBuilder()
            .setStarted(true)
            .build();
    @Nonnull
    private static final RecordCursorProto.ComparatorContinuation.CursorState START_PROTO = RecordCursorProto.ComparatorContinuation.CursorState.newBuilder()
            .setStarted(false)
            .build();

    private ComparatorCursorContinuation(@Nonnull List<RecordCursorContinuation> continuations,
                                         @Nullable RecordCursorProto.ComparatorContinuation originalProto,
                                         final int referencePlanIndex) {
        super(continuations, originalProto);
        this.referencePlanIndex = referencePlanIndex;
    }

    private ComparatorCursorContinuation(@Nonnull List<RecordCursorContinuation> continuations, final int referencePlanIndex) {
        this(continuations, null, referencePlanIndex);
    }

    @Override
    protected void setFirstChild(@Nonnull RecordCursorProto.ComparatorContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        addOtherChild(builder, continuation);
    }

    @Override
    protected void setSecondChild(@Nonnull RecordCursorProto.ComparatorContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        addOtherChild(builder, continuation);
    }

    @Override
    protected void addOtherChild(@Nonnull RecordCursorProto.ComparatorContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        final RecordCursorProto.ComparatorContinuation.CursorState cursorState;
        if (continuation.isEnd()) {
            cursorState = EXHAUSTED_PROTO;
        } else {
            byte[] asBytes = continuation.toBytes();
            if (asBytes == null && !continuation.isEnd()) {
                cursorState = START_PROTO;
            } else {
                cursorState = RecordCursorProto.ComparatorContinuation.CursorState.newBuilder()
                        .setStarted(true)
                        .setContinuation(ByteString.copyFrom(asBytes))
                        .build();
            }
        }
        builder.addChildState(cursorState);
    }

    @Override
    @Nonnull
    protected RecordCursorProto.ComparatorContinuation.Builder newProtoBuilder() {
        return RecordCursorProto.ComparatorContinuation.newBuilder();
    }

    @Override
    public boolean isEnd() {
        // The reference plan is the one that decides when to end
        return getContinuations().get(referencePlanIndex).isEnd();
    }

    @Nonnull
    static ComparatorCursorContinuation from(@Nonnull ComparatorCursor<?> cursor) {
        return new ComparatorCursorContinuation(cursor.getChildContinuations(), cursor.getReferencePlanIndex());
    }

    @Nonnull
    static ComparatorCursorContinuation from(@Nullable byte[] bytes, int numberOfChildren, int referencePlanIndex) {
        if (bytes == null) {
            return new ComparatorCursorContinuation(Collections.nCopies(numberOfChildren, RecordCursorStartContinuation.START), referencePlanIndex);
        }
        try {
            return ComparatorCursorContinuation.from(RecordCursorProto.ComparatorContinuation.parseFrom(bytes), numberOfChildren, referencePlanIndex);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
        }
    }

    @Nonnull
    static ComparatorCursorContinuation from(@Nonnull RecordCursorProto.ComparatorContinuation parsed, int numberOfChildren, int referencePlanIndex) {
        ImmutableList.Builder<RecordCursorContinuation> builder = ImmutableList.builder();
        for (RecordCursorProto.ComparatorContinuation.CursorState state : parsed.getChildStateList()) {
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
                    .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, parsed.getChildStateCount());
        }
        return new ComparatorCursorContinuation(children, parsed, referencePlanIndex);
    }

}
