/*
 * MoreMoreAsyncUtil.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * More helpers in the spirit of {@link AsyncUtil} and {@link com.apple.foundationdb.async.MoreAsyncUtil}.
 */
@API(API.Status.UNSTABLE)
public class MoreMoreAsyncUtil {
    /**
     * Compose a handler bi-function to the result of a future.
     * @param future future to compose the handler onto
     * @param handler handler bi-function to compose onto the passed future
     * @param fdbDatabase database for mapping the underlying async exception to a sync {@link RuntimeException}
     * @param <V> return type of original future
     * @param <T> return type of final future
     * @return future with same completion properties as the future returned by the handler
     * @see MoreAsyncUtil#composeWhenComplete(CompletableFuture, BiFunction, Function)
     */
    public static <V, T> CompletableFuture<T> composeWhenComplete(
            @Nonnull CompletableFuture<V> future,
            @Nonnull BiFunction<V,Throwable,? extends CompletableFuture<T>> handler,
            @Nonnull FDBDatabase fdbDatabase) {
        return MoreAsyncUtil.composeWhenComplete(future, handler, fdbDatabase::mapAsyncToSyncException);
    }

    /**
     * This is a static class, and should not be instantiated.
     **/
    private MoreMoreAsyncUtil() {}
}
