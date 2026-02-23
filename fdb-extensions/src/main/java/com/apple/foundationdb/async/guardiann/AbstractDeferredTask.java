/*
 * AbstractDeferredTask.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class AbstractDeferredTask {
    @Nonnull
    private final Locator locator;
    @Nonnull
    private final AccessInfo accessInfo;
    @Nonnull
    private final UUID taskId;

    AbstractDeferredTask(@Nonnull final Locator locator,
                         @Nonnull final AccessInfo accessInfo,
                         @Nonnull final UUID taskId) {
        this.locator = locator;
        this.accessInfo = accessInfo;
        this.taskId = taskId;
    }

    @Nonnull
    public Locator getLocator() {
        return locator;
    }

    @Nonnull
    AccessInfo getAccessInfo() {
        return accessInfo;
    }

    @Nonnull
    public UUID getTaskId() {
        return taskId;
    }

    @Nonnull
    public Config getConfig() {
        return getLocator().getConfig();
    }

    @Nonnull
    public abstract Tuple valueTuple();

    @Nonnull
    public abstract CompletableFuture<Void> runTask(@Nonnull Transaction transaction);

    @Nonnull
    public abstract Kind getKind();

    static AbstractDeferredTask newFromTuples(@Nonnull final Locator locator,
                                              @Nonnull final AccessInfo accessInfo,
                                              @Nonnull final Tuple keyTuple, @Nonnull final Tuple valueTuple) {
        final Kind kind = Kind.fromValueTuple(valueTuple);
        return kind.create(locator, accessInfo, keyTuple, valueTuple);
    }

    public enum Kind {
        SPLIT_MERGE(0, SplitMergeTask::fromTuples),
        REASSIGN(1, ReassignTask::fromTuples);

        private static final Map<Integer, Kind> BY_CODE =
                Arrays.stream(values())
                        .collect(Collectors.toMap(s -> s.code, s -> s));

        private final int code;
        private final TaskCreationFunction taskCreationFunction;

        Kind(final int code, @Nonnull final TaskCreationFunction taskCreationFunction) {
            this.code = code;
            this.taskCreationFunction = taskCreationFunction;
        }

        public int getCode() {
            return code;
        }

        @Nonnull
        private AbstractDeferredTask create(@Nonnull final Locator locator,
                                            @Nonnull final AccessInfo accessInfo,
                                            @Nonnull final Tuple keyTuple,
                                            @Nonnull final Tuple valueTuple) {
            return taskCreationFunction.create(locator, accessInfo, keyTuple, valueTuple);
        }

        public static Kind fromValueTuple(@Nonnull final Tuple valueTuple) {
            return Kind.ofCode(Math.toIntExact(valueTuple.getLong(0)));
        }

        @Nonnull
        public static Kind ofCode(final int code) {
            return Objects.requireNonNull(BY_CODE.getOrDefault(code, null));
        }
    }

    @FunctionalInterface
    private interface TaskCreationFunction {
        AbstractDeferredTask create(@Nonnull Locator locator,
                                    @Nonnull AccessInfo accessInfo,
                                    @Nonnull Tuple keyTuple,
                                    @Nonnull Tuple valueTuple);
    }
}
