/*
 * AutoCompleteSuggesterCommitCheckAsync.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedWrappedAnalyzingInfixSuggester;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.apple.foundationdb.record.lucene.DirectoryCommitCheckAsync.getOrCreateDirectoryCommitCheckAsync;

/**
 * This class closes the suggester before commit.
 */
@API(API.Status.EXPERIMENTAL)
public class AutoCompleteSuggesterCommitCheckAsync implements FDBRecordContext.CommitCheckAsync {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoCompleteSuggesterCommitCheckAsync.class);
    protected final AnalyzingInfixSuggester suggester;
    protected final Executor executor;

    /**
     * Create a suggester for auto-complete search.
     * @param state the state for the index maintainer
     * @param directoryCommitCheckAsync the directoryCommitCheckAsync for the directory
     * @param indexAnalyzer the analyzer for indexing
     * @param queryAnalyzer the analyzer for query
     * @param highlight whether the suggestions have the search term highlighted
     * @param executor the executor to close the suggester asynchronously
     */
    private AutoCompleteSuggesterCommitCheckAsync(@Nonnull IndexMaintainerState state, @Nonnull DirectoryCommitCheckAsync directoryCommitCheckAsync,
                                                  @Nonnull Analyzer indexAnalyzer, @Nonnull Analyzer queryAnalyzer,
                                                  boolean highlight, @Nonnull Executor executor) {
        this.suggester = LuceneOptimizedWrappedAnalyzingInfixSuggester.getSuggester(state, directoryCommitCheckAsync.getDirectory(), indexAnalyzer, queryAnalyzer, highlight);
        this.executor = executor;
    }

    /**
     * Close suggester.
     *
     * @return CompletableFuture
     */
    @Nonnull
    @Override
    public CompletableFuture<Void> checkAsync() {
        LOGGER.trace("closing suggester check");
        return CompletableFuture.runAsync(() -> {
            try {
                IOUtils.close(suggester);
            } catch (IOException ioe) {
                LOGGER.error("AutoCompleteSuggesterCommitCheckAsync Failed", ioe);
                throw new RecordCoreStorageException("AutoCompleteSuggesterCommitCheckAsync Failed", ioe);
            }
        }, executor);
    }

    @Nonnull
    public static AnalyzingInfixSuggester getOrCreateSuggester(@Nonnull IndexMaintainerState state,
                                                               @Nonnull Analyzer indexAnalyzer, @Nonnull Analyzer queryAnalyzer,
                                                               boolean highlight, @Nonnull Executor executor, @Nonnull final Tuple groupingKey) {
        synchronized (state.context) {
            AutoCompleteSuggesterCommitCheckAsync suggesterAsync = state.context.getInSession(getSuggestionIndexSubspace(state, groupingKey), AutoCompleteSuggesterCommitCheckAsync.class);
            if (suggesterAsync == null) {
                suggesterAsync = new AutoCompleteSuggesterCommitCheckAsync(state,
                        getOrCreateDirectoryCommitCheckAsync(state, getSuggestionIndexSubspace(state, groupingKey)), indexAnalyzer, queryAnalyzer, highlight, executor);
                state.context.addCommitCheck(suggesterAsync);
                state.context.putInSessionIfAbsent(getSuggestionIndexSubspace(state, groupingKey), suggesterAsync);
            }
            return suggesterAsync.suggester;
        }
    }

    @Nonnull
    private static Subspace getSuggestionIndexSubspace(@Nonnull final IndexMaintainerState state, @Nonnull final Tuple groupingKey) {
        return getSuggestionIndexSubspace(state.indexSubspace, groupingKey);
    }

    @VisibleForTesting
    @Nonnull
    public static Subspace getSuggestionIndexSubspace(@Nonnull final Subspace indexSubspace, @Nonnull final Tuple groupingKey) {
        return indexSubspace.subspace(groupingKey).subspace(Tuple.from("s"));
    }
}
