/*
 * RetrievalMetricsCalculator.java
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

import com.apple.foundationdb.async.common.PrimaryKeyVectorAndDistance;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Measures how well a vector search finds each query's nearest records: recall, nDCG and Kendall similarity (see
 * {@link QueryRetrievalMetrics}), per query and as means over a set of queries (see {@link RetrievalMetrics}).
 *
 * <p>For each query, the calculator obtains the query's ground truth once, runs the search once, and scores the
 * results against the ground truth with {@link #computeQuery}, which is the one method an implementation provides
 * (see {@link PrimaryKeyRetrievalMetricsCalculator}). The ground truth comes from one of two places:
 * <ul>
 *   <li><b>A dataset's ground-truth file</b> ({@link #compute}): the exact nearest records the dataset lists per
 *       query. It describes the index only while the index holds exactly the records the file was computed over.</li>
 *   <li><b>The records live in the index</b> ({@link #computeDynamic}): the exact nearest records, computed by brute
 *       force over the records the caller knows to be live. It stays correct under inserts and deletes.</li>
 * </ul>
 *
 * <p>The search is any function from a query vector and {@code k} to the primary keys found, best first, so the same
 * calculator measures a raw structure, a record-store index, or a search with custom options:
 * <pre>{@code
 * RetrievalMetrics metrics = new PrimaryKeyRetrievalMetricsCalculator()
 *         .computeDynamic((query, k) -> search(query, k), Metric.COSINE_METRIC, queries, liveRecords, 100);
 * double meanRecall = metrics.meanRecall();
 * List<QueryRetrievalMetrics> perQuery = metrics.perQuery();
 * }</pre>
 */
public interface RetrievalMetricsCalculator {
    /**
     * The metrics of the results of one search. Only the first {@code k} records of the ground truth and the first
     * {@code k} results count; the results may hold records outside the ground truth.
     *
     * @param groundTruth the primary keys of the query's best records, best first, each at most once
     * @param results the primary keys the search returned, best first
     * @param k the number of results searched for; positive
     * @return the metrics, {@link QueryRetrievalMetrics#UNDEFINED} if the ground truth is empty
     */
    @Nonnull
    QueryRetrievalMetrics computeQuery(@Nonnull List<Tuple> groundTruth, @Nonnull List<Tuple> results, int k);

    /**
     * The metrics of {@code searcher} against ground truth from a dataset's ground-truth file, whose index {@code i}
     * is the record with primary key {@code Tuple.from((long) i)}, as {@link VecsDatasetLoaders#loadVectors} keys the
     * base vectors. A query with empty ground truth is not searched.
     *
     * <p>nDCG and Kendall similarity depend on the order of the ground truth, so each query's set must iterate best
     * first. The sets of {@link VecsDatasetLoaders#loadGroundTruth} do: they are {@code ImmutableSet}s, which iterate
     * in the order of the file, and the usual datasets list each query's neighbors nearest first. For ground truth by
     * the index's own metric, use {@link #computeDynamic} over the base vectors.
     *
     * @param searcher runs a top-{@code k} search for a query vector and returns the primary keys found, best first
     * @param queries the query vectors
     * @param groundTruth per query, the indices of its best records, iterating best first (see
     *        {@link VecsDatasetLoaders#loadGroundTruth}); must have the same size as {@code queries}
     * @param k the number of results to search for
     * @return the metrics
     */
    @Nonnull
    default RetrievalMetrics compute(@Nonnull final BiFunction<RealVector, Integer, List<Tuple>> searcher,
                            @Nonnull final List<? extends RealVector> queries,
                            @Nonnull final List<? extends Set<Integer>> groundTruth,
                            final int k) {
        Verify.verify(queries.size() == groundTruth.size(),
                "queries (%s) and groundTruth list (%s) must align", queries.size(), groundTruth.size());
        final List<QueryRetrievalMetrics> perQuery = new ArrayList<>(queries.size());
        for (int i = 0; i < queries.size(); i++) {
            final List<Tuple> truth = groundTruth.get(i).stream()
                    .map(index -> Tuple.from((long) index))
                    .toList();
            perQuery.add(searchAndCompute(searcher, queries.get(i), truth, k));
        }
        return RetrievalMetrics.of(perQuery);
    }

    /**
     * The metrics of {@code searcher} against ground truth computed exactly over {@code active}: per query, the
     * distance by {@code metric} to every record of {@code active}, of which the {@code k} nearest, nearest first and
     * ties broken by primary key, are the ground truth. Each query thus costs one distance computation per record.
     *
     * <p>Use it whenever a dataset's ground-truth file no longer describes the index (after inserts or deletes, or
     * for a sample of a dataset), or to rank the records by the index's own metric rather than the one the file was
     * computed with.
     *
     * @param searcher runs a top-{@code k} search for a query vector and returns the primary keys found, best first
     * @param metric the metric the ground truth ranks {@code active} by, normally that of the index
     * @param queries the query vectors
     * @param active the records live in the index, by primary key; at least {@code k} of them
     * @param k the number of results to search for
     * @return the metrics
     */
    @Nonnull
    default RetrievalMetrics computeDynamic(@Nonnull final BiFunction<RealVector, Integer, List<Tuple>> searcher,
                                   @Nonnull final Metric metric,
                                   @Nonnull final List<? extends RealVector> queries,
                                   @Nonnull final Map<Tuple, ? extends RealVector> active,
                                   final int k) {
        Verify.verify(active.size() >= k,
                "active set (%s) must have at least k=%s entries for meaningful metrics", active.size(), k);
        final List<QueryRetrievalMetrics> perQuery = new ArrayList<>(queries.size());
        for (final RealVector query : queries) {
            final List<Tuple> truth = GuardiannTestHelpers.bruteForceTopK(metric, query, active, k).stream()
                    .map(PrimaryKeyVectorAndDistance::primaryKey)
                    .toList();
            perQuery.add(searchAndCompute(searcher, query, truth, k));
        }
        return RetrievalMetrics.of(perQuery);
    }

    /** Searches for {@code query} and measures the results, unless there is no ground truth to measure them by. */
    @Nonnull
    private QueryRetrievalMetrics searchAndCompute(@Nonnull final BiFunction<RealVector, Integer, List<Tuple>> searcher,
                                          @Nonnull final RealVector query, @Nonnull final List<Tuple> groundTruth,
                                          final int k) {
        if (groundTruth.isEmpty()) {
            return QueryRetrievalMetrics.UNDEFINED;
        }
        return computeQuery(groundTruth, searcher.apply(query, k), k);
    }
}
