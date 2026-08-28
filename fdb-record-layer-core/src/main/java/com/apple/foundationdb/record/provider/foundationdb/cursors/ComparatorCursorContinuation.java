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
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.jspecify.annotations.Nullable;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.RecordCursorProto.ComparatorContinuation;

class ComparatorCursorContinuation extends MergeCursorContinuation<ComparatorContinuation.Builder, RecordCursorContinuation> {

    private final int referencePlanIndex;

    private static final ComparatorContinuation.CursorState EXHAUSTED_PROTO = ComparatorContinuation.CursorState.newBuilder()
            .setStarted(true)
            .build();
    private static final ComparatorContinuation.CursorState START_PROTO = ComparatorContinuation.CursorState.newBuilder()
            .setStarted(false)
            .build();

    private ComparatorCursorContinuation(List<RecordCursorContinuation> continuations,
                                         @Nullable ComparatorContinuation originalProto,
                                         final int referencePlanIndex) {
        super(continuations, originalProto);
        this.referencePlanIndex = referencePlanIndex;
    }

    private ComparatorCursorContinuation(List<RecordCursorContinuation> continuations, final int referencePlanIndex) {
        this(continuations, null, referencePlanIndex);
    }

    @Override
    protected void setFirstChild(ComparatorContinuation.Builder builder, RecordCursorContinuation continuation) {
        addOtherChild(builder, continuation);
    }

    @Override
    protected void setSecondChild(ComparatorContinuation.Builder builder, RecordCursorContinuation continuation) {
        addOtherChild(builder, continuation);
    }

    @Override
    protected void addOtherChild(ComparatorContinuation.Builder builder, RecordCursorContinuation continuation) {
        final ComparatorContinuation.CursorState cursorState;
        if (continuation.isEnd()) {
            cursorState = EXHAUSTED_PROTO;
        } else {
            ByteString asBytes = continuation.toByteString();
            if (asBytes.isEmpty() && !continuation.isEnd()) {
                cursorState = START_PROTO;
            } else {
                cursorState = ComparatorContinuation.CursorState.newBuilder()
                        .setStarted(true)
                        .setContinuation(asBytes)
                        .build();
            }
        }
        builder.addChildState(cursorState);
    }

    @Override
    protected ComparatorContinuation.Builder newProtoBuilder() {
        return ComparatorContinuation.newBuilder();
    }

    @Override
    public boolean isEnd() {
        // The reference plan is the one that decides when to end
        return getContinuations().get(referencePlanIndex).isEnd();
    }

    static ComparatorCursorContinuation from(ComparatorCursor<?> cursor) {
        return new ComparatorCursorContinuation(cursor.getChildContinuations(), cursor.getReferencePlanIndex());
    }

    static ComparatorCursorContinuation from(@Nullable byte[] bytes, int numberOfChildren, int referencePlanIndex) {
        if (bytes == null) {
            return new ComparatorCursorContinuation(Collections.nCopies(numberOfChildren, RecordCursorStartContinuation.START), referencePlanIndex);
        }
        try {
            return ComparatorCursorContinuation.from(ComparatorContinuation.parseFrom(bytes), numberOfChildren, referencePlanIndex);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
        }
    }

    static ComparatorCursorContinuation from(ComparatorContinuation parsed, int numberOfChildren, int referencePlanIndex) {
        ImmutableList.Builder<RecordCursorContinuation> builder = ImmutableList.builder();
        for (ComparatorContinuation.CursorState state : parsed.getChildStateList()) {
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
