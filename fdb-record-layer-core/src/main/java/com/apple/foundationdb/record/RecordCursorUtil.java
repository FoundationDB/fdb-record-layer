/*
 * RecordCursorUtil.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A variety of utilities for performing operations with {@link RecordCursor}s and related classes.
 */
@API(API.Status.UNSTABLE)
public class RecordCursorUtil {
    private RecordCursorUtil() {
    }

    /**
     * Asynchronously run the given supplier until the {@link RecordCursorResult} that it returns dose not have a next
     * value. The produced future contains the continuation produced by the final execution of the given supplier.
     * @param body a supplier of a {@code RecordCursorResult} to run until it returns one without a next value
     * @param executor an executor to use for execution
     * @return a future that will be complete when the body returns a a result without a next value and contains the last continuation
     */
    public static CompletableFuture<RecordCursorContinuation> whileHasNext(
            @Nonnull Supplier<? extends CompletableFuture<RecordCursorResult<?>>> body,
            @Nonnull Executor executor) {
        return new WhileHasNextLoop(body, executor).run();
    }

    private static class WhileHasNextLoop implements BiFunction<RecordCursorResult<?>, Throwable, Void> {
        @Nonnull
        private final Supplier<? extends CompletableFuture<RecordCursorResult<?>>> body;
        private final CompletableFuture<RecordCursorContinuation> done;
        @Nonnull
        private final Executor executor;

        public WhileHasNextLoop(@Nonnull Supplier<? extends CompletableFuture<RecordCursorResult<?>>> body,
                                @Nonnull Executor executor) {
            this.body = body;
            this.done = new CompletableFuture<>();
            this.executor = executor;
        }

        @Override
        public Void apply(RecordCursorResult<?> result, Throwable error) {
            if (error != null) {
                done.completeExceptionally(error);
                return null;
            }

            while (true) {
                if (!result.hasNext()) {
                    done.complete(result.getContinuation());
                    return null;
                }
                CompletableFuture<RecordCursorResult<?>> nextResult;
                try {
                    nextResult = body.get();
                } catch (Exception e) {
                    done.completeExceptionally(e);
                    return null;
                }
                if (nextResult.isDone()) {
                    if (nextResult.isCompletedExceptionally()) {
                        nextResult.handle(this);
                    } else {
                        result = nextResult.join();
                    }
                } else {
                    nextResult.handleAsync(this, executor);
                    return null;
                }
            }
        }

        public CompletableFuture<RecordCursorContinuation> run() {
            apply(RecordCursorResult.withNextValue(new Object(), RecordCursorStartContinuation.START), null);
            return done;
        }
    }
}

