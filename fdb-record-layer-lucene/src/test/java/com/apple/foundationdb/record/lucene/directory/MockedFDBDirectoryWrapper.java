/*
 * MockedFDBDirectoryWrapper.java
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

import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.store.LockFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * A Testing-focused {@link FDBDirectoryWrapper} that allows a mocked-FDBDirectory to be injected into the system.
 */
public class MockedFDBDirectoryWrapper extends FDBDirectoryWrapper {
    MockedFDBDirectoryWrapper(@Nonnull final IndexMaintainerState state,
                              @Nonnull final Tuple key,
                              final int mergeDirectoryCount,
                              @Nonnull final AgilityContext agilityContext,
                              final int blockCacheMaximumSize,
                              @Nonnull final InjectedFailureRepository injectedFailures,
                              @Nonnull final LuceneAnalyzerWrapper writerAnalyzer,
                              @Nullable final Exception exceptionAtCreation) {
        super(state, key, mergeDirectoryCount, agilityContext, blockCacheMaximumSize, writerAnalyzer, exceptionAtCreation);
        // Set the injectedFailures at the end of the constructor since createDirectory() is called from the constructor
        // and we can't pass the injected failures to it yet.
        ((MockedFDBDirectory)getDirectory()).setInjectedFailures(injectedFailures);
    }

    @Nonnull
    @Override
    protected FDBDirectory createFDBDirectory(final Subspace subspace, final Map<String, String> options,
                                              final FDBDirectorySharedCacheManager sharedCacheManager,
                                              final Tuple sharedCacheKey, final boolean useCompoundFile,
                                              final AgilityContext agilityContext,
                                              final @Nullable LockFactory lockFactory,
                                              final int blockCacheMaximumSize) {
        return new MockedFDBDirectory(subspace, options, sharedCacheManager, sharedCacheKey, useCompoundFile,
                agilityContext, lockFactory, blockCacheMaximumSize);
    }
}
