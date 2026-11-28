/*
 * RetrievalMetrics.java
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

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.ToDoubleFunction;

/**
 * The metrics of a set of queries (see {@link RetrievalMetricsCalculator}): each query's, in query order, and their
 * means over the queries that had ground truth.
 *
 * @param perQuery the metrics of each query, in query order ({@link QueryRetrievalMetrics#UNDEFINED} for a query
 *        without ground truth)
 * @param countedQueries the number of queries that had ground truth
 * @param meanRecall the mean recall
 * @param meanNdcg the mean nDCG
 * @param meanKendallSimilarity the mean Kendall similarity
 */
public record RetrievalMetrics(@Nonnull List<QueryRetrievalMetrics> perQuery, int countedQueries, double meanRecall,
                               double meanNdcg, double meanKendallSimilarity) {
    public RetrievalMetrics {
        perQuery = ImmutableList.copyOf(perQuery);
    }

    /**
     * The metrics of the given queries, with their means over those that had ground truth ({@link Double#NaN} if
     * none had).
     *
     * @param perQuery the metrics of each query, in query order
     * @return the metrics
     */
    @Nonnull
    public static RetrievalMetrics of(@Nonnull final List<QueryRetrievalMetrics> perQuery) {
        final List<QueryRetrievalMetrics> defined = perQuery.stream().filter(QueryRetrievalMetrics::isDefined).toList();
        return new RetrievalMetrics(perQuery, defined.size(), mean(defined, QueryRetrievalMetrics::recall),
                mean(defined, QueryRetrievalMetrics::ndcg), mean(defined, QueryRetrievalMetrics::kendallSimilarity));
    }

    private static double mean(@Nonnull final List<QueryRetrievalMetrics> metrics,
                               @Nonnull final ToDoubleFunction<QueryRetrievalMetrics> metric) {
        return metrics.stream().mapToDouble(metric).average().orElse(Double.NaN);
    }
}
