/*
 * WhileTrue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2019 Apple Inc. and the FoundationDB project authors
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

package com.geophile.z.async;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Helper class for asynchronous iteration.
 */
public class CompletableFutures
{
    public static final CompletableFuture<Void> NULL = CompletableFuture.completedFuture(null);
    public static final CompletableFuture<Boolean> FALSE = CompletableFuture.completedFuture(Boolean.FALSE);
    public static final CompletableFuture<Boolean> TRUE = CompletableFuture.completedFuture(Boolean.TRUE);

    private CompletableFutures() {
    }

    /**
     * Call given body in a loop.
     * @param body the code to run in the loop
     * @return a future that completes after the body has returned {@code false}
     */
    public static CompletableFuture<Void> whileTrue(Supplier<? extends CompletableFuture<Boolean>> body) {
        return new WhileTrue(body).run();
    }

    static class WhileTrue implements BiFunction<Boolean, Throwable, Void> {
        final Supplier<? extends CompletableFuture<Boolean>> body;
        final CompletableFuture<Void> done;

        WhileTrue(Supplier<? extends CompletableFuture<Boolean>> body) {
            this.body = body;
            this.done = new CompletableFuture<>();
        }

        @Override
        public Void apply(Boolean more, Throwable error) {
            if (error != null) {
                done.completeExceptionally(error);
            } else {
                while (true) {
                    if (!more) {
                        done.complete(null);
                        break;
                    }
                    CompletableFuture<Boolean> result;
                    try {
                        result = body.get();
                    } catch (Exception e) {
                        done.completeExceptionally(e);
                        break;
                    }
                    if (result.isDone()) {
                        if (result.isCompletedExceptionally()) {
                            result.handle(this);
                            break;
                        } else {
                            more = result.join();
                        }
                    } else {
                        result.handleAsync(this);
                        break;
                    }
                }
            }

            return null;
        }

        public CompletableFuture<Void> run() {
            apply(true, null);
            return done;
        }
    }

}
