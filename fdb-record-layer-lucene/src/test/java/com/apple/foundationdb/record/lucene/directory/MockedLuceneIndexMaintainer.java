/*
 * MockedLuceneIndexMaintainer.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.lucene.LuceneIndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A mocked version of {@link LuceneIndexMaintainer}.
 * This version allows for the injection of a mocked directory manager (and eventually, a mocked FDBDirectory) through
 * the test execution.
 */
public class MockedLuceneIndexMaintainer extends LuceneIndexMaintainer {
    final InjectedFailureRepository injectedFailures;

    @SuppressWarnings("this-escape")
    public MockedLuceneIndexMaintainer(@Nonnull final IndexMaintainerState state, @Nonnull final Executor executor, final InjectedFailureRepository injectedFailures) {
        super(state, executor);
        this.injectedFailures = injectedFailures;
        // Setting failures has to be done here rather than via a constructor param since createDirectoryManager is called
        // from the super constructor before we can set local state
        ((MockedFDBDirectoryManager)getDirectoryManager()).setInjectedFailures(injectedFailures);
    }

    @Nonnull
    @Override
    public <M extends Message> CompletableFuture<Void> update(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
        if (injectedFailures.hasFlag(InjectedFailureRepository.Flags.LUCENE_MAINTAINER_SKIP_INDEX_UPDATE)) {
            return AsyncUtil.DONE;
        }
        return super.update(oldRecord, newRecord);
    }

    @Nonnull
    @Override
    protected FDBDirectoryManager createDirectoryManager(@Nonnull final IndexMaintainerState state) {
        // Use the mocked manager static factory method to create/return the right type
        return MockedFDBDirectoryManager.getManager(state);
    }
}
