/*
 * ProbableIntersectionCursorContinuation.java
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
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.RecordCursorProto.ProbableIntersectionContinuation;

class ProbableIntersectionCursorContinuation extends MergeCursorContinuation<ProbableIntersectionContinuation.Builder, BloomFilterCursorContinuation> {
    protected ProbableIntersectionCursorContinuation(@Nonnull List<BloomFilterCursorContinuation> continuations, @Nullable Message originalProto) {
        super(continuations, originalProto);
    }

    protected ProbableIntersectionCursorContinuation(@Nonnull List<BloomFilterCursorContinuation> continuations) {
        this(continuations, null);
    }

    private void addChild(@Nonnull ProbableIntersectionContinuation.Builder builder,
                          @Nonnull BloomFilterCursorContinuation continuation) {
        builder.addChildState(continuation.toProto());
    }

    @Override
    protected void setFirstChild(@Nonnull ProbableIntersectionContinuation.Builder builder, @Nonnull BloomFilterCursorContinuation continuation) {
        addChild(builder, continuation);
    }

    @Override
    protected void setSecondChild(@Nonnull ProbableIntersectionContinuation.Builder builder, @Nonnull BloomFilterCursorContinuation continuation) {
        addChild(builder, continuation);
    }

    @Override
    protected void addOtherChild(@Nonnull ProbableIntersectionContinuation.Builder builder, @Nonnull BloomFilterCursorContinuation continuation) {
        addChild(builder, continuation);
    }

    @Nonnull
    @Override
    protected ProbableIntersectionContinuation.Builder newProtoBuilder() {
        return ProbableIntersectionContinuation.newBuilder();
    }

    @Override
    public boolean isEnd() {
        // A bloom cursor continuation is never an "end cursor" as it will never return "null" (because the BloomFilter
        // at least is always serialized). However, if the *child* of each cursor is done, then the whole cursor
        // is at an end.
        return getContinuations().stream().allMatch(BloomFilterCursorContinuation::isChildEnd);
    }

    @Nonnull
    static ProbableIntersectionCursorContinuation from(@Nonnull ProbableIntersectionCursor<?> cursor) {
        // This can't use getChildContinuations() because of the way the type system works
        List<BloomFilterCursorContinuation> childContinuations = cursor.getCursorStates().stream()
                .map(ProbableIntersectionCursorState::getContinuation)
                .collect(Collectors.toList());
        return new ProbableIntersectionCursorContinuation(childContinuations);
    }

    @Nonnull
    static ProbableIntersectionCursorContinuation from(@Nullable byte[] bytes, int numberOfChildren) {
        if (bytes == null) {
            return new ProbableIntersectionCursorContinuation(Collections.nCopies(numberOfChildren,
                    new BloomFilterCursorContinuation(RecordCursorStartContinuation.START, null)));
        }
        try {
            return ProbableIntersectionCursorContinuation.from(ProbableIntersectionContinuation.parseFrom(bytes), numberOfChildren);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
        }
    }

    @Nonnull
    static ProbableIntersectionCursorContinuation from(@Nonnull ProbableIntersectionContinuation parsed, int numberOfChildren) {
        ImmutableList.Builder<BloomFilterCursorContinuation> builder = ImmutableList.builder();
        for (ProbableIntersectionContinuation.CursorState state : parsed.getChildStateList()) {
            if (state.getExhausted()) {
                builder.add(new BloomFilterCursorContinuation(RecordCursorEndContinuation.END, state.getBloomFilter()));
            } else if (state.hasContinuation()) {
                builder.add(new BloomFilterCursorContinuation(ByteArrayContinuation.fromNullable(state.getContinuation().toByteArray()), state.getBloomFilter()));
            } else {
                builder.add(new BloomFilterCursorContinuation(RecordCursorStartContinuation.START, state.getBloomFilter()));
            }
        }
        ImmutableList<BloomFilterCursorContinuation> children = builder.build();
        if (children.size() != numberOfChildren) {
            throw new RecordCoreArgumentException("invalid continuation (extraneous child state information present)")
                    .addLogInfo(LogMessageKeys.EXPECTED_CHILD_COUNT, numberOfChildren)
                    .addLogInfo(LogMessageKeys.READ_CHILD_COUNT, parsed.getChildStateCount());
        }
        return new ProbableIntersectionCursorContinuation(children, parsed);
    }
}
