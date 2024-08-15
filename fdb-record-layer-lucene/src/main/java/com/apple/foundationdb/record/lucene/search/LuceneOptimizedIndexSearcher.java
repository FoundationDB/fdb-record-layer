/*
 * LuceneOptimizedIndexSearcher.java
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

package com.apple.foundationdb.record.lucene.search;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.apple.foundationdb.record.util.pair.Pair;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ThreadInterruptedException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.lucene.search.QueryHelper.getQueriesTerms;

/**
 * This class optimizes the current IndexSearcher and attempts to perform operations in parallel in places where
 * data access can occur.
 *
 *
 */
public class LuceneOptimizedIndexSearcher extends IndexSearcher {

    public LuceneOptimizedIndexSearcher(final IndexReader r) {
        super(r);
    }

    public LuceneOptimizedIndexSearcher(final IndexReader r, final Executor executor) {
        super(r, executor);
    }

    /** Lower-level search API.
     *
     * <p>{@link LeafCollector#collect(int)} is called for every matching document.
     *
     * @throws BooleanQuery.TooManyClauses If a query would exceed
     *         {@link BooleanQuery#getMaxClauseCount()} clauses.
     */
    @Override
    public void search(Query query, Collector results)
            throws IOException {
        query = rewrite(query);
        final var executor = getExecutor();
        final var weight = createWeight(query, results.scoreMode(), 1);
        if (executor == null) {
            search(leafContexts, weight, results);
        } else {
            try {
                searchOptimized(executor, weight, leafContexts, results).join();
            } catch (WrapperException we) {
                throw we.unwrap();
            }
        }
    }

    /**
     * Lower-level search API.
     *
     * This is optimized into an attempt to parallelize database access in FDB, for each collector in {@code collectorManager}.
     *
     * <p>
     * {@link LeafCollector#collect(int)} is called for every document. <br>
     *
     * <p>
     * NOTE: this method executes the searches on all given leaves exclusively.
     * To search across all the searchers leaves use {@link #leafContexts}.
     *
     * <p>
     * most of this code is copied verbatim from {@link IndexSearcher#search(Query, CollectorManager)}, the
     * refactored code is, as explained above, takes care of parallelizing database access in FDB for each collector in
     * {@code collectorManager}.
     *
     * @param query
     *          the search query
     * @param collectorManager
     *          manager of collectors that receive hits
     */
    @Override
    @SuppressWarnings({"PMD.EmptyCatchBlock", "PMD.PreserveStackTrace"})
    public <C extends Collector, T> T search(Query query, CollectorManager<C, T> collectorManager) throws IOException {
        final var leafSlices = getSlices();
        final var executor = getExecutor();
        if (executor == null || leafSlices.length <= 1) {
            final C collector = collectorManager.newCollector();
            search(query, collector);
            return collectorManager.reduce(Collections.singletonList(collector));
        } else {
            final List<C> collectors = new ArrayList<>(leafSlices.length);
            ScoreMode scoreMode = null;
            for (int i = 0; i < leafSlices.length; ++i) {
                final C collector = collectorManager.newCollector();
                collectors.add(collector);
                if (scoreMode == null) {
                    scoreMode = collector.scoreMode();
                } else if (scoreMode != collector.scoreMode()) {
                    throw new IllegalStateException("CollectorManager does not always produce collectors with the same score mode");
                }
            }
            if (scoreMode == null) {
                // no segments
                scoreMode = ScoreMode.COMPLETE;
            }
            query = rewrite(query);
            final Weight weight = createWeight(query, scoreMode, 1);
            final List<Future<C>> topDocsFutures = new ArrayList<>(leafSlices.length);

            for (int i = 0; i < leafSlices.length; ++i) {
                final LeafReaderContext[] leaves = leafSlices[i].leaves;
                final C collector = collectors.get(i);
                final CompletableFuture<C> future = searchOptimized(executor, weight, Arrays.asList(leaves), collector);
                topDocsFutures.add(future);
            }
            for (Future<C> future : topDocsFutures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    throw new ThreadInterruptedException(e);
                } catch (ExecutionException e) {
                    throw LuceneExceptions.toIoException(e.getCause(), e);
                } catch (WrapperException we) {
                    throw we.unwrap();
                }
            }
            return collectorManager.reduce(collectors);
        }
    }

    /**
     * WrapperException used for retrieving an {@link IOException} from an asynchronous execution block.
     */
    private static class WrapperException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        @Nonnull
        private final IOException ioe;

        WrapperException(@Nonnull final IOException ioe) {
            this.ioe = ioe;
        }

        @Nonnull
        public IOException unwrap() {
            return ioe;
        }
    }

    /**
     * An optimized version that performs the search on parallel.
     * @param executor The task executor.
     * @param weight Weight of the query.
     * @param leaves List of leaves reader contexts.
     * @param collector Collector that gathers the results.
     * @return a {@code Future} that can be waited upon for the parallel search tasks to finish.
     *
     * @implNote it is possible that an {@link IOException} is thrown during parallel execution of task. Since Java does
     * not permit throwing a checked exception from a lambda, and we need to cascade this exception to the caller, we wrap
     * it using a custom, unchecked {@link WrapperException} and rethrow it. Therefore, the caller of this method should catch
     * {@link WrapperException} and call {@link WrapperException#unwrap()} to get to the underlying {@link IOException}.
     * Similar to {@link IndexSearcher#search(List, Weight, Collector)}, the {@link CollectionTerminatedException} is
     * caught and ignored. For more information about this please have a look at the implementation of the above method,
     * and read the documentation of {@link CollectionTerminatedException}.
     */
    @Nonnull
    @SuppressWarnings({"PMD.EmptyCatchBlock"})
    private <C extends Collector> CompletableFuture<C> searchOptimized(@Nonnull final Executor executor, @Nonnull final Weight weight, @Nonnull final List<LeafReaderContext> leaves, @Nonnull final C collector) {
        // 1. spawn a list of parallel tasks that interact with the DB for better throughput.
        final List<CompletableFuture<Pair<LeafReaderContext, Pair<LeafCollector, BulkScorer>>>> dependencies = leaves.stream().map(ctx -> CompletableFuture.supplyAsync(() -> {
            try {
                final var result = Pair.of(collector.getLeafCollector(ctx), weight.bulkScorer(ctx));
                return Pair.of(ctx, result);
            } catch (CollectionTerminatedException cte) {
                // bail out.
                return Pair.of(ctx, (Pair<LeafCollector, BulkScorer>)null);
            } catch (IOException ioe) {
                throw new WrapperException(ioe); // to be cascaded.
            }
        }, executor)).collect(Collectors.toList());

        // 2. once we finish processing all of them, we can proceed with the final scoring task.
        return AsyncUtil.whenAll(dependencies).thenApplyAsync(ignored -> {
            dependencies.stream().map(CompletableFuture::join).collect(Collectors.toList())
                    .forEach((Pair<LeafReaderContext, Pair<LeafCollector, BulkScorer>> result) -> {
                        final Pair<LeafCollector, BulkScorer> scorer = result.getRight();
                        final LeafReaderContext ctx = result.getLeft();
                        if (scorer != null && scorer.getLeft() != null && scorer.getRight() != null) {
                            try {
                                scorer.getRight().score(scorer.getLeft(), ctx.reader().getLiveDocs());
                            } catch (CollectionTerminatedException cte) {
                                // no-op just ignore.
                            } catch (IOException ioe) {
                                throw new WrapperException(ioe); // to be cascaded.
                            }
                        }
                    });
            return collector;
        }, executor);
    }

    /**
     * This overridden call will attempt to cache the relevant terms low level blocks in the case where we need scores.
     *
     * @param query query implementation
     * @param scoreMode mode for scoring
     * @param boost boost for scores
     * @return Weight of query
     * @throws IOException IOException
     */
    @SuppressWarnings("PMD")
    @Override
    public Weight createWeight(final Query query, final ScoreMode scoreMode, final float boost) throws IOException {
        if (scoreMode.needsScores() && getExecutor() != null) {
            List<Term> terms = getQueriesTerms(query);
            for (Term term : terms) {
                for (final LeafReaderContext ctx : getTopReaderContext().leaves()) {
                    // Do not block on these pre-fetches...
                    CompletableFuture.runAsync(() -> {
                        try {
                            cacheTermsEnum(ctx, term);
                        } catch (Exception e) {
                            // No Op Swallow since this is for pre-caching
                        }
                    }, getExecutor());
                }
            }
        }
        return super.createWeight(query, scoreMode, boost);
    }

    private static void cacheTermsEnum(LeafReaderContext ctx, Term term) throws IOException {
        final Terms terms = ctx.reader().terms(term.field());
        if (terms != null) {
            final TermsEnum termsEnum = terms.iterator();
            termsEnum.seekExact(term.bytes());
        }
    }

}
