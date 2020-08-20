/*
 * ComposedBitmapIndexContinuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.bitmap;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.cursors.MergeCursorContinuation;
import com.apple.foundationdb.record.provider.foundationdb.indexes.BitmapValueIndexMaintainer;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A {@link RecordCursor} doing as bit-wise merge of bitmaps from two or more {@code BITMAP_VALUE} indexes.
 *
 * The bit operations can correspond to a Boolean expression over those indexes' rightmost grouping keys.
 *
 * @see BitmapValueIndexMaintainer
 */
@API(API.Status.EXPERIMENTAL)
class ComposedBitmapIndexContinuation extends MergeCursorContinuation<RecordCursorProto.ComposedBitmapIndexContinuation.Builder, RecordCursorContinuation> {
    @Nonnull
    private static final RecordCursorProto.ComposedBitmapIndexContinuation.CursorState EXHAUSTED_PROTO = RecordCursorProto.ComposedBitmapIndexContinuation.CursorState.newBuilder()
            .setExhausted(true)
            .build();

    @Nonnull
    private static final RecordCursorProto.ComposedBitmapIndexContinuation.CursorState START_PROTO = RecordCursorProto.ComposedBitmapIndexContinuation.CursorState.newBuilder()
            .setExhausted(false)
            .build();

    protected ComposedBitmapIndexContinuation(@Nonnull List<RecordCursorContinuation> continuations, @Nullable Message originalProto) {
        super(continuations, originalProto);
    }

    @Override
    protected void setFirstChild(@Nonnull RecordCursorProto.ComposedBitmapIndexContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        addChild(builder, continuation);
    }

    @Override
    protected void setSecondChild(@Nonnull RecordCursorProto.ComposedBitmapIndexContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        addChild(builder, continuation);

    }

    @Override
    protected void addOtherChild(@Nonnull RecordCursorProto.ComposedBitmapIndexContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        addChild(builder, continuation);

    }

    private void addChild(@Nonnull RecordCursorProto.ComposedBitmapIndexContinuation.Builder builder, @Nonnull RecordCursorContinuation continuation) {
        RecordCursorProto.ComposedBitmapIndexContinuation.CursorState cursorState;
        if (continuation.isEnd()) {
            cursorState = EXHAUSTED_PROTO;
        } else {
            final byte[] asBytes = continuation.toBytes();
            if (asBytes == null) {
                cursorState = START_PROTO;
            } else {
                cursorState = RecordCursorProto.ComposedBitmapIndexContinuation.CursorState.newBuilder()
                        .setContinuation(ByteString.copyFrom(asBytes))
                        .build();
            }
        }
        builder.addChildState(cursorState);
    }

    @Nonnull
    @Override
    protected RecordCursorProto.ComposedBitmapIndexContinuation.Builder newProtoBuilder() {
        return RecordCursorProto.ComposedBitmapIndexContinuation.newBuilder();
    }

    @Override
    public boolean isEnd() {
        return getContinuations().stream().allMatch(RecordCursorContinuation::isEnd);
    }

    public RecordCursorContinuation getContinuation(int i) {
        return getContinuations().get(i);
    }

    @Nonnull
    @SuppressWarnings("PMD.PreserveStackTrace")
    static ComposedBitmapIndexContinuation from(@Nullable byte[] bytes, int numberOfChildren) {
        if (bytes == null) {
            return new ComposedBitmapIndexContinuation(Collections.nCopies(numberOfChildren, RecordCursorStartContinuation.START), null);
        }
        try {
            return from(RecordCursorProto.ComposedBitmapIndexContinuation.parseFrom(bytes), numberOfChildren);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("invalid continuation", ex)
                    .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
        } catch (RecordCoreArgumentException ex) {
            throw ex.addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(bytes));
        }
    }

    @Nonnull
    static ComposedBitmapIndexContinuation from(@Nonnull RecordCursorProto.ComposedBitmapIndexContinuation parsed, int numberOfChildren) {
        ImmutableList.Builder<RecordCursorContinuation> builder = ImmutableList.builder();
        for (RecordCursorProto.ComposedBitmapIndexContinuation.CursorState state : parsed.getChildStateList()) {
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
        return new ComposedBitmapIndexContinuation(children, parsed);
    }

}
