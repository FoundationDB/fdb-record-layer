/*
 * RecursiveCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroCopyByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * A cursor that flattens a tree of cursors.
 * A root cursor seeds the output and then each output element is additionally mapped into a child cursor.
 * @param <T> the type of elements of the cursors
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.CloseResource")
public class RecursiveCursor<T> implements RecordCursor<RecursiveCursor.RecursiveValue<T>> {

    @Nonnull
    private final ChildCursorFunction<T> childCursorFunction;
    @Nullable
    private final Function<T, byte[]> checkValueFunction;
    @Nonnull
    private final List<RecursiveNode<T>> nodes;

    private int currentDepth;

    @Nullable
    private RecordCursorResult<RecursiveValue<T>> lastResult;

    private RecursiveCursor(@Nonnull ChildCursorFunction<T> childCursorFunction,
                            @Nullable Function<T, byte[]> checkValueFunction,
                            @Nonnull List<RecursiveNode<T>> nodes) {
        this.childCursorFunction = childCursorFunction;
        this.checkValueFunction = checkValueFunction;
        this.nodes = nodes;
    }

    /**
     * Create a recursive cursor.
     * @param rootCursorFunction a function that given a continuation or {@code null} returns the children of the root
     * @param childCursorFunction a function that given a value and a continuation returns the children of that level
     * @param checkValueFunction a function to recognize changes to the database since a continuation
     * @param continuation a continuation from a previous cursor
     * @param <T> the type of elements of the cursors
     * @return a cursor over the recursive tree determined by the cursor functions
     */
    @Nonnull
    public static <T> RecursiveCursor<T> create(@Nonnull Function<byte[], ? extends RecordCursor<T>> rootCursorFunction,
                                                @Nonnull ChildCursorFunction<T> childCursorFunction,
                                                @Nullable Function<T, byte[]> checkValueFunction,
                                                @Nullable byte[] continuation) {
        final List<RecursiveNode<T>> nodes = new ArrayList<>();
        if (continuation == null) {
            nodes.add(RecursiveNode.forRoot(RecordCursorStartContinuation.START, rootCursorFunction.apply(null)));
        } else {
            RecordCursorProto.RecursiveContinuation parsed;
            try {
                parsed = RecordCursorProto.RecursiveContinuation.parseFrom(continuation);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("error parsing continuation", ex)
                        .addLogInfo("raw_bytes", ByteArrayUtil2.loggable(continuation));
            }
            final int totalDepth = parsed.getLevelsCount();
            for (int depth = 0; depth < totalDepth; depth++) {
                final RecordCursorProto.RecursiveContinuation.LevelCursor parentLevel = parsed.getLevels(depth);
                final RecordCursorContinuation priorContinuation;
                if (parentLevel.hasContinuation()) {
                    priorContinuation = ByteArrayContinuation.fromNullable(parentLevel.getContinuation().toByteArray());
                } else {
                    priorContinuation = RecordCursorStartContinuation.START;
                }
                if (depth == 0) {
                    nodes.add(RecursiveNode.forRoot(priorContinuation, rootCursorFunction.apply(priorContinuation.toBytes())));
                } else {
                    byte[] checkValue = null;
                    if (checkValueFunction != null && depth < totalDepth - 1) {
                        final RecordCursorProto.RecursiveContinuation.LevelCursor childLevel = parsed.getLevels(depth + 1);
                        if (childLevel.hasCheckValue()) {
                            checkValue = childLevel.getCheckValue().toByteArray();
                        }
                    }
                    nodes.add(RecursiveNode.forContinuation(priorContinuation, checkValue));
                }
            }
        }
        return new RecursiveCursor<>(childCursorFunction, checkValueFunction, nodes);
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<RecursiveValue<T>>> onNext() {
        if (lastResult != null && !lastResult.hasNext()) {
            return CompletableFuture.completedFuture(lastResult);
        }
        return AsyncUtil.whileTrue(this::recursionLoop, getExecutor()).thenApply(vignore -> lastResult);
    }

    @Override
    public void close() {
        for (RecursiveNode<T> node : nodes) {
            CompletableFuture<RecordCursorResult<T>> childFuture = node.childFuture;
            if (childFuture != null) {
                if (childFuture.cancel(false)) {
                    node.childFuture = null;
                } else {
                    continue;
                }
            }
            RecordCursor<T> childCursor = node.childCursor;
            if (childCursor != null) {
                childCursor.close();
            }
        }
    }

    @Override
    public boolean isClosed() {
        for (RecursiveNode<T> node : nodes) {
            if (node.childFuture != null) {
                return false;
            }
            RecordCursor<T> childCursor = node.childCursor;
            if (childCursor != null && !childCursor.isClosed()) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return nodes.get(0).childCursor.getExecutor();  // Take from the root cursor.
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            for (RecursiveNode<T> node : nodes) {
                RecordCursor<T> childCursor = node.childCursor;
                if (childCursor != null) {
                    childCursor.accept(visitor);
                }
            }
        }
        return visitor.visitLeave(this);
    }

    /**
     * A value returned by a recursive descent.
     * Includes the depth level and whether there are any descendants.
     * @param <T> the type of elements of the cursors
     */
    public static class RecursiveValue<T> {
        @Nullable
        private final T value;
        private final int depth;
        private final boolean isLeaf;

        public RecursiveValue(@Nullable T value, int depth, boolean isLeaf) {
            this.value = value;
            this.depth = depth;
            this.isLeaf = isLeaf;
        }

        /**
         * Get the value for a recursive result.
         * @return the value associated with the given result
         */
        @Nullable
        public T getValue() {
            return value;
        }

        /**
         * Get the depth for a recursive result.
         * @return the 1-based depth from the root of the given result
         */
        public int getDepth() {
            return depth;
        }

        /**
         * Get whether a recursive result has any descendants.
         * @return {@code true} if the given result is a leaf, {@code false} if it has any descendants
         */
        public boolean isLeaf() {
            return isLeaf;
        }
    }

    /**
     * Function to generate children of a parent value.
     * @param <T> the type of elements of the cursors
     */
    @FunctionalInterface
    public interface ChildCursorFunction<T> {
        /**
         * Return recursion cursor for this level.
         * @param value the value at this level
         * @param depth the 1-based depth of this level
         * @param continuation an optional continuation
         * @return a cursor over children of the given value for the given level
         */
        RecordCursor<T> apply(@Nullable T value, int depth, @Nullable byte[] continuation);
    }

    // This implementation keeps a single stack of open cursors and returns in pre-order.
    // A child is opened and the first element checked in order to be able to include whether a node is a leaf in each result.
    // It would also be possible to keep a tree of open cursors, opening more children before completing their siblings.
    // It would also be possible to only have a single open cursor and return in level-order.
    // These alternatives have even more complicated continuation restoring behavior, though.

    static class RecursiveNode<T> {
        @Nullable
        final T value;
        @Nullable
        final byte[] checkValue;

        boolean emitPending;

        @Nonnull
        RecordCursorContinuation childContinuationBefore;
        @Nonnull
        RecordCursorContinuation childContinuationAfter;
        @Nullable
        RecordCursor<T> childCursor;
        @Nullable
        CompletableFuture<RecordCursorResult<T>> childFuture;

        private RecursiveNode(@Nullable T value, @Nullable byte[] checkValue, boolean emitPending,
                              @Nonnull RecordCursorContinuation childContinuationBefore, @Nullable RecordCursor<T> childCursor) {
            this.value = value;
            this.checkValue = checkValue;
            this.emitPending = emitPending;
            this.childContinuationAfter = this.childContinuationBefore = childContinuationBefore;
            this.childCursor = childCursor;
        }

        static <T> RecursiveNode<T> forRoot(@Nonnull RecordCursorContinuation childContinuationBefore,
                                            @Nonnull RecordCursor<T> childCursor) {
            return new RecursiveNode<>(null, null, false, childContinuationBefore, childCursor);
        }

        static <T> RecursiveNode<T> forValue(@Nullable T value) {
            return new RecursiveNode<>(value, null, true, RecordCursorStartContinuation.START, null);
        }

        public static <T> RecursiveNode<T> forContinuation(@Nonnull RecordCursorContinuation childContinuationBefore,
                                                           @Nullable byte[] checkValue) {
            return new RecursiveNode<>(null, checkValue, false, childContinuationBefore, null);
        }

        public RecursiveNode<T> withCheckedValue(@Nullable T value) {
            return new RecursiveNode<>(value, null, false, childContinuationBefore, null);
        }
    }

    /**
     * Called to advance the recursion.
     * @return a future that completes to {@code true} if the loop should continue or {@code false} if a result is available
     */
    @Nonnull
    CompletableFuture<Boolean> recursionLoop() {
        int depth = currentDepth;
        final RecursiveNode<T> node = nodes.get(depth);
        if (node.childFuture == null) {
            if (node.childCursor == null) {
                node.childCursor = childCursorFunction.apply(node.value, depth, node.childContinuationBefore.toBytes());
            }
            node.childFuture = node.childCursor.onNext();
        }
        if (!node.childFuture.isDone()) {
            return node.childFuture.thenApply(rignore -> true);
        }
        final RecordCursorResult<T> childResult = node.childFuture.join();
        node.childFuture = null;
        if (childResult.hasNext()) {
            node.childContinuationAfter = childResult.getContinuation();
            addChildNode(childResult.get());
            if (node.emitPending) {
                lastResult = RecordCursorResult.withNextValue(
                        new RecursiveValue<>(node.value, depth, false),
                        buildContinuation(depth + 1));
                node.emitPending = false;
                return AsyncUtil.READY_FALSE;
            }
        } else {
            final NoNextReason noNextReason = childResult.getNoNextReason();
            if (noNextReason.isOutOfBand()) {
                final RecordCursorContinuation continuation;
                if (node.emitPending) {
                    // Stop before parent.
                    continuation = buildContinuation(depth - 1);
                } else {
                    // Stop before child.
                    continuation = buildContinuation(depth);
                }
                lastResult = RecordCursorResult.withoutNextValue(continuation, noNextReason);
                return AsyncUtil.READY_FALSE;
            }
            // If the childCursorFunction added a returned row limit, that is not distinguished here.
            // There is no provision for continuing and adding more descendants at an arbitrary depth.
            while (nodes.size() > depth) {
                nodes.remove(depth);
            }
            currentDepth = depth - 1;
            if (depth == 0) {
                lastResult = RecordCursorResult.exhausted();
                return AsyncUtil.READY_FALSE;
            }
            final RecursiveNode<T> parentNode = nodes.get(depth - 1);
            parentNode.childContinuationBefore = parentNode.childContinuationAfter;
            if (node.emitPending) {
                lastResult = RecordCursorResult.withNextValue(
                        new RecursiveValue<>(node.value, depth, true),
                        buildContinuation(depth));
                node.emitPending = false;
                return AsyncUtil.READY_FALSE;
            }
        }
        return AsyncUtil.READY_TRUE;
    }

    private void addChildNode(@Nullable T value) {
        currentDepth++;
        if (currentDepth < nodes.size()) {
            // Have a nested continuation.
            final RecursiveNode<T> continuationChildNode = nodes.get(currentDepth);
            boolean addNode = false;
            if (checkValueFunction != null && continuationChildNode.checkValue != null) {
                final byte[] actualCheckValue = checkValueFunction.apply(value);
                if (actualCheckValue != null && !Arrays.equals(continuationChildNode.checkValue, actualCheckValue)) {
                    // Does not match; discard proposed continuation(s).
                    while (nodes.size() > currentDepth) {
                        nodes.remove(currentDepth);
                    }
                    addNode = true;
                }
            }
            if (!addNode) {
                // Replace check value with actual value so can open cursors below there, but using the loaded continuation.
                nodes.set(currentDepth, continuationChildNode.withCheckedValue(value));
                return;
            }
        }
        final RecursiveNode<T> childNode = RecursiveNode.forValue(value);
        nodes.add(childNode);
    }

    private RecordCursorContinuation buildContinuation(int depth) {
        final List<RecordCursorContinuation> continuations = new ArrayList<>(depth);
        final List<byte[]> checkValues = checkValueFunction == null ? null : new ArrayList<>(depth - 1);
        for (int i = 0; i < depth; i++) {
            continuations.add(nodes.get(i).childContinuationBefore);
            if (checkValues != null && i < depth - 1) {
                checkValues.add(checkValueFunction.apply(nodes.get(i + 1).value));
            }
        }
        return new Continuation(continuations, checkValues);
    }

    private static class Continuation implements RecordCursorContinuation {
        @Nonnull
        private final List<RecordCursorContinuation> continuations;
        @Nullable
        private final List<byte[]> checkValues;
        @Nullable
        private ByteString cachedByteString;
        @Nullable
        private byte[] cachedBytes;

        private Continuation(@Nonnull List<RecordCursorContinuation> continuations, @Nullable List<byte[]> checkValues) {
            this.continuations = continuations;
            this.checkValues = checkValues;
        }

        @Override
        public boolean isEnd() {
            for (RecordCursorContinuation continuation : continuations) {
                if (!continuation.isEnd()) {
                    return false;
                }
            }
            return true;
        }

        @Nonnull
        @Override
        public ByteString toByteString() {
            if (cachedByteString == null) {
                final RecordCursorProto.RecursiveContinuation.Builder builder = RecordCursorProto.RecursiveContinuation.newBuilder();
                for (int i = 0; i < continuations.size(); i++) {
                    final RecordCursorProto.RecursiveContinuation.LevelCursor.Builder levelBuilder = builder.addLevelsBuilder();
                    final RecordCursorContinuation continuation = continuations.get(i);
                    if (continuation.toBytes() != null) {
                        levelBuilder.setContinuation(continuation.toByteString());
                    }
                    if (checkValues != null && i < checkValues.size()) {
                        final byte[] checkValue = checkValues.get(i);
                        if (checkValue != null) {
                            levelBuilder.setCheckValue(ZeroCopyByteString.wrap(checkValue));
                        }
                    }
                }
                cachedByteString = builder.build().toByteString();
            }
            return cachedByteString;
        }

        @Nullable
        @Override
        public byte[] toBytes() {
            if (cachedBytes == null) {
                cachedBytes = toByteString().toByteArray();
            }
            return cachedBytes;
        }
    }
}
