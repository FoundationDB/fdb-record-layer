/*
 * MergeCursorContinuation.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

/**
 * Common code for handling the continuations of {@link MergeCursor}s. The continuations for these cursors are constructed
 * from a Protobuf message containing information on the state of each child cursor. This class handles caching
 * that Protobuf message and its byte representation so that subsequent calls to get the continuation are fast. It also
 * abstracts away some of the logic for converting a list of continuations into a single Protobuf message, but this is
 * still somewhat clunky because Protobuf does not support {@link Message} inheritance (which limits the opportunity for
 * polymorphism).
 *
 * @param <B> the builder for message type of the continuation proto message
 * @param <C> type of the continuation
 */
@API(API.Status.INTERNAL)
public abstract class MergeCursorContinuation<B extends Message.Builder, C extends RecordCursorContinuation> implements RecordCursorContinuation {
    @Nonnull
    private final List<C> continuations; // all continuations must themselves be immutable
    @Nullable
    private Message cachedProto;
    @Nullable
    private byte[] cachedBytes;
    @Nullable
    private ByteString cachedByteString;

    protected MergeCursorContinuation(@Nonnull List<C> continuations, @Nullable Message originalProto) {
        this.continuations = continuations;
        this.cachedProto = originalProto;
    }

    /**
     * Fill in the Protobuf builder with the information from the first child. For backwards-compatibility reasons,
     * cursors may handle this differently than the other children. This method will be called before
     * {@link #setSecondChild} and all calls to {@link #addOtherChild}.
     *
     * @param builder a builder for the Protobuf continuation
     * @param continuation the first child's continuation
     */
    protected abstract void setFirstChild(@Nonnull B builder, @Nonnull C continuation);

    /**
     * Fill in the Protobuf builder with the information from the second child. For backwards-compatibility reasons,
     * cursors may handle this differently than the other children. This method will be called after
     * {@link #setFirstChild} and before all calls to {@link #addOtherChild}.
     *
     * @param builder a builder for the Protobuf continuation
     * @param continuation the second child's continuation
     */
    protected abstract void setSecondChild(@Nonnull B builder, @Nonnull C continuation);

    /**
     * Fill in the Protobuf builder with the information for a child other than the first or second child. For
     * backwards-compatibility reasons, cursors may handle those two children differently than the other children.
     * This method will be called in the same order as the continuations in {@link #getContinuations()}.
     *
     * @param builder a builder for the Protobuf continuation
     * @param continuation a child other than the first or second child
     */
    protected abstract void addOtherChild(@Nonnull B builder, @Nonnull C continuation);

    /**
     * Get a new builder instance for the Protobuf message associated with this continuation. This should typically
     * be implemented by calling the {@code newBuilder()} method on the appropriate message type.
     *
     * @return a new builder for the underlying Protobuf message type
     */
    @Nonnull
    protected abstract B newProtoBuilder();

    @Nonnull
    protected Message toProto() {
        if (cachedProto == null) {
            B builder = newProtoBuilder();
            /*
            final Iterator<C> continuationIterator = continuations.iterator();
            setFirstChild(builder, continuationIterator.next());
            setSecondChild(builder, continuationIterator.next());
            while (continuationIterator.hasNext()) {
                addOtherChild(builder, continuationIterator.next());
            }

             */
            setFirstChild(builder, continuations.get(0));
            setSecondChild(builder, continuations.get(1));
            for (int i = 2; i < continuations.size(); i++) {
                addOtherChild(builder, continuations.get(i));
            }
            cachedProto = builder.build();
        }
        return cachedProto;
    }

    @Nullable
    @Override
    @SpotBugsSuppressWarnings("EI")
    public byte[] toBytes() {
        if (isEnd()) {
            return null;
        }
        if (cachedBytes == null) {
            cachedBytes = toByteString().toByteArray();
        }
        return cachedBytes;
    }

    @Override
    @Nonnull
    @SpotBugsSuppressWarnings("EI")
    public ByteString toByteString() {
        if (isEnd()) {
            return ByteString.EMPTY;
        }
        if (cachedByteString == null) {
            cachedByteString = toProto().toByteString();
        }
        return cachedByteString;
    }

    @Nonnull
    protected List<C> getContinuations() {
        return continuations;
    }
}
