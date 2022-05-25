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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

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

    /**
     * Lower-level search API.
     *
     * This is optimized into an attempt to parallelize database access in FDB
     *
     * <p>
     * {@link LeafCollector#collect(int)} is called for every document. <br>
     *
     * <p>
     * NOTE: this method executes the searches on all given leaves exclusively.
     * To search across all the searchers leaves use {@link #leafContexts}.
     *
     * @param leaves
     *          the searchers leaves to execute the searches on
     * @param weight
     *          to match documents
     * @param collector
     *          to receive hits
     * @throws BooleanQuery.TooManyClauses If a query would exceed
     *         {@link BooleanQuery#getMaxClauseCount()} clauses.
     */
    @Override
    @SuppressWarnings("PMD")
    protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector)
            throws IOException {
        Executor executor = getExecutor();
        if (executor == null) {
            super.search(leaves, weight, collector);
        } else {
            // Used to parallelize weight.bulkScorer database access
            Map<LeafReaderContext, CompletableFuture<Pair<LeafCollector, BulkScorer>>> leafScorers =
                    new ConcurrentHashMap<>();
            leaves.forEach(ctx -> leafScorers.put(ctx, CompletableFuture.supplyAsync(() -> {
                try {
                    return Pair.of(collector.getLeafCollector(ctx), weight.bulkScorer(ctx));
                } catch (IOException e) {
                    return null;
                }
            }, getExecutor())));

            // Need to be able to throw IOExceptions out of this stack.
            // For example, attempting to perform phrase search without pos/freqs throws out of this
            // piece of code.
            for (LeafReaderContext ctx : leaves) {
                try {
                    Pair<LeafCollector, BulkScorer> scorer = leafScorers.get(ctx).join();
                    if (scorer != null && scorer.getLeft() != null && scorer.getRight() != null) {
                        scorer.getRight().score(scorer.getLeft(), ctx.reader().getLiveDocs());
                    }
                } catch (CollectionTerminatedException ioe) {
                    // No Op
                }
            }
        }
    }


}
