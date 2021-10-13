/*
 * IndexUniquenessCommitCheck.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.metadata.Index;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Commit check used to check index uniqueness constraints. This is used internally to differentiate this commit check
 * from other types of commit checks and to associate it with the {@link Index} it is checking.
 */
class IndexUniquenessCommitCheck implements FDBRecordContext.CommitCheckAsync {
    @Nonnull
    private final Index index;
    @Nonnull
    private final FDBRecordContext.CommitCheckAsync underlying;

    IndexUniquenessCommitCheck(@Nonnull Index index, @Nonnull CompletableFuture<Void> check) {
        this.index = index;
        this.underlying = FDBRecordContext.CommitCheckAsync.fromFuture(check);
    }

    @Override
    public boolean isReady() {
        return underlying.isReady();
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> checkAsync() {
        return underlying.checkAsync();
    }

    @Nonnull
    public Index getIndex() {
        return index;
    }
}
