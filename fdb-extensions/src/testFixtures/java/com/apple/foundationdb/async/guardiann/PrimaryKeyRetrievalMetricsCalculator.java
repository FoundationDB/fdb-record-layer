/*
 * PrimaryKeyRetrievalMetricsCalculator.java
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

import com.apple.foundationdb.tuple.Tuple;
import com.google.common.math.DoubleMath;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * A {@link RetrievalMetricsCalculator} that matches results to the ground truth by primary key: a result is relevant
 * iff its primary key is in the ground truth, and its rank is the position at which the ground truth lists that key.
 * A record the ground truth does not list is not relevant, even one exactly as near the query as a listed one (such as
 * a duplicate vector under another primary key). Set {@code G} the first {@code k} records of the ground truth and
 * {@code R} the first {@code k} distinct results:
 * <ul>
 *     <li>recall is {@code |R ∩ G| / |G|};</li>
 *     <li>nDCG gives the record at rank {@code i} (from 0) of {@code G} a gain of {@code k - i}, discounts the result
 *     at position {@code j} (from 0, over the first {@code k} results as returned) by {@code 1 / log2(j + 2)}, and
 *     divides by the gain of {@code G} itself; a repeated result takes a position but gains nothing;</li>
 *     <li>Kendall similarity is {@code 1 - K / K_max}, with {@code K} Fagin's top-k Kendall distance between the
 *     ground-truth list {@code G} and the result list {@code R} (the number of pairs of records on whose order the
 *     two lists disagree, {@link #UNKNOWN_ORDER_PENALTY} for a pair that one list orders and the other holds neither
 *     record of) and {@code K_max} its value for two lists with no record in common.</li>
 * </ul>
 */
public final class PrimaryKeyRetrievalMetricsCalculator implements RetrievalMetricsCalculator {
    /**
     * Fagin's {@code p}: what the Kendall similarity charges for a pair of records whose relative order only one of
     * the two lists tells, because the other list holds neither record (two records the search missed, or two extra
     * records it returned). {@code 0} would assume the lists agree on every such pair (optimistic), {@code 1} that
     * they disagree (pessimistic). {@code 1/2} is the neutral choice, and the one with which Fagin's distance is a
     * metric.
     */
    public static final double UNKNOWN_ORDER_PENALTY = 0.5d;

    @Nonnull
    @Override
    public QueryRetrievalMetrics computeQuery(@Nonnull final List<Tuple> groundTruth,
                                              @Nonnull final List<Tuple> results,
                                              final int k) {
        final List<Tuple> truth = groundTruth.subList(0, Math.min(k, groundTruth.size()));
        if (truth.isEmpty()) {
            return QueryRetrievalMetrics.UNDEFINED;
        }
        final Map<Tuple, Integer> groundTruthRanks = new HashMap<>();
        for (int rank = 0; rank < truth.size(); rank++) {
            groundTruthRanks.put(truth.get(rank), rank);
        }
        final List<Tuple> distinctResults = results.stream().distinct().limit(k).toList();

        return new QueryRetrievalMetrics(
                recall(groundTruthRanks, distinctResults),
                ndcg(groundTruthRanks, results, k),
                kendallSimilarity(groundTruthRanks, distinctResults)
        );
    }

    private static double recall(@Nonnull final Map<Tuple, Integer> groundTruthRanks,
                                 @Nonnull final List<Tuple> distinctResults) {
        final long hits = distinctResults.stream().filter(groundTruthRanks::containsKey).count();
        return (double) hits / groundTruthRanks.size();
    }

    /**
     * Scores how much of the value of the ground truth the results deliver, and how near the top they deliver it, as
     * the normalized discounted cumulative gain {@code DCG / IDCG}. {@code 1} means the results hold the ground truth
     * in its exact order; {@code 0} means they hold none of it. Unlike recall it is sensitive to where in the results
     * each record is found, and it weighs the top of the list most, where a caller is most likely to look.
     *
     * <p>The score multiplies two independently-motivated weights per result:
     * <ul>
     *   <li><b>Gain</b>: how valuable the record is, by its rank in the ground truth: the record at rank {@code i}
     *       (from 0) gains {@code k - i}, so the nearest record is worth {@code k} and the {@code k}-th worth
     *       {@code 1}. A record the ground truth does not list gains nothing. The gain is linear rather than the
     *       exponential {@code 2^rel - 1} also common for nDCG, which at {@code k = 100} would make the nearest record
     *       worth about {@code 2^99} times the farthest and drown out everything else.</li>
     *   <li><b>Discount</b>: how much the position counts: the result at position {@code j} (from 0) is weighed
     *       {@code 1 / log2(j + 2)}, so {@code 1} at the top, {@code 0.63} second, {@code 0.5} third, and about
     *       {@code 0.15} at position 100. The weight decays logarithmically: steeply over the first few positions,
     *       slowly after, so the tail still counts a little.</li>
     * </ul>
     * {@code DCG} sums Gain x Discount over the first {@code k} returned results. {@code IDCG} is the same sum
     * for the ground truth in its own order, the best any result list can score, so dividing by it puts every query
     * on the same {@code [0, 1]} scale.
     *
     * <p>A record that gains nothing still takes a position in the result list. A record outside the ground truth
     * returned at the top therefore costs twice: its own position, the most heavily weighed, delivers nothing, and
     * every relevant record after it slides one position down to a smaller weight. The same records thus score lower
     * the earlier the misses sit, while recall, which only counts them, cannot tell the two apart. A record returned
     * twice (which a searcher that deduplicates its results, as Guardiann's does, never does) is treated the same way:
     * the repeat takes a position but gains nothing.
     *
     * @param groundTruthRanks the rank (from 0) of each record of the ground truth, cut to the {@code k} that count
     * @param results the results, best first, as returned (repeats included)
     * @param k the number of results that count
     * @return the nDCG in {@code [0, 1]}
     */
    private static double ndcg(@Nonnull final Map<Tuple, Integer> groundTruthRanks, @Nonnull final List<Tuple> results,
                               final int k) {
        // The ideal list is the ground truth itself: the record at rank i at position i.
        final double idealDcg = IntStream.range(0, groundTruthRanks.size())
                .mapToDouble(rank -> (k - rank) * discount(rank))
                .sum();

        // The first k results as returned. A record outside the ground truth, or one already seen, adds nothing but
        // still moves every later result down a position.
        double dcg = 0.0d;
        final Set<Tuple> seen = new HashSet<>();
        final List<Tuple> firstK = results.subList(0, Math.min(k, results.size()));
        for (int position = 0; position < firstK.size(); position++) {
            final Tuple primaryKey = firstK.get(position);
            final Integer rank = groundTruthRanks.get(primaryKey);
            if (seen.add(primaryKey) && rank != null) {
                dcg += (k - rank) * discount(position);
            }
        }
        return dcg / idealDcg;
    }

    /**
     * The weight of the result at {@code position} (from 0): {@code 1 / log2(position + 2)}, the standard DCG
     * discount, {@code 1} at the top and decaying logarithmically after.
     */
    private static double discount(final int position) {
        return 1.0d / DoubleMath.log2(position + 2);
    }

    /**
     * Scores how much the order of the results agrees with that of the ground truth, as {@code 1 - K / K_max}, where
     * {@code K} is Fagin's top-k Kendall distance between the two lists and {@code K_max} its value for two lists with
     * no record in common. {@code 1} means the results hold the ground truth in its exact order; {@code 0} means the
     * two lists share nothing. Unlike recall it is sensitive to order, and unlike nDCG it weighs every pair of records
     * the same, wherever in the lists the pair sits, so it also sees disorder in the tail that nDCG discounts away.
     *
     * <p>A top-k list says more than the order of its own records: it also says that every record it holds ranks
     * above every record it does not, while saying nothing about how two records it does not hold compare. The
     * distance counts, over every pair of records in either list, how often the two lists say opposite things about
     * the pair. With {@code G} the ground truth, {@code R} the distinct results, {@code S = G ∩ R} the records found,
     * {@code M = G \ R} the records missed and {@code E = R \ G} the extra records returned, every pair falls into one
     * of these cases:
     * <ul>
     *   <li><b>Discordant</b>: two found records ({@code S × S}) that the results order differently than the ground
     *       truth: each list states its own order, and they disagree. Costs {@code 1} per pair.</li>
     *   <li><b>Missing above</b>: a missed record and a found one ({@code M × S}) that the ground truth ranks the
     *       missed one above: the ground truth puts the missed record first, while the results, which hold only the
     *       found one, rank the found one first. This costs {@code 1} per pair, so missing a record near the top of
     *       the ground truth costs more than missing one near the bottom, since more found records should have ranked
     *       below it, increasing the number of such pairs.</li>
     *   <li><b>Extra above</b>: an extra record and a found one ({@code E × S}) that the results return the extra
     *       one above: the results put the extra record first, while the ground truth, which holds only the found
     *       one, puts the found one first. Costs {@code 1} per pair, so an extra record returned early costs more
     *       than one returned late, since it pushes more found records below it.</li>
     *   <li><b>Crossed</b>: a missed record and an extra one ({@code M × E}): each list holds exactly one of the
     *       two and so ranks that one first. They always disagree; costs {@code 1} per pair.</li>
     *   <li><b>Unknown</b>: two missed records ({@code M × M}) or two extra ones ({@code E × E}): one list orders
     *       the pair, the other holds neither and says nothing. Costs {@link #UNKNOWN_ORDER_PENALTY} per pair.</li>
     * </ul>
     * Pairs where the two lists agree cost nothing: a found pair in the same order, a missed record that the ground
     * truth ranks below a found one, or an extra record that the results return below a found one.
     *
     * <p>{@code K_max} is the distance between two lists of the same sizes with nothing in common: every pair across
     * them is crossed ({@code |G| * |R|}) and every pair within either is unknown. Dividing by it puts lists of any
     * size on the same {@code [0, 1]} scale. Empty results get {@code 0}, as for recall and nDCG: they agree with
     * nothing.
     *
     * @param groundTruthRanks the rank (from 0) of each record of the ground truth, cut to the {@code k} that count
     * @param distinctResults the first {@code k} distinct results, best first
     * @return the Kendall similarity in {@code [0, 1]}
     */
    private static double kendallSimilarity(@Nonnull final Map<Tuple, Integer> groundTruthRanks,
                                            @Nonnull final List<Tuple> distinctResults) {
        if (distinctResults.isEmpty()) {
            return 0.0d;
        }
        final int truthSize = groundTruthRanks.size();

        // One pass over the results, in their order. A found record contributes its ground-truth rank (in result
        // order, for the discordant pairs below) and marks its rank found (for the missing-above pairs). An extra
        // record is not compared yet; instead every found record that follows it is one extra-above pair, so each
        // found record adds the number of extra records returned before it.
        final List<Integer> sharedRanks = new ArrayList<>();
        final boolean[] found = new boolean[truthSize];
        long extraAbove = 0L;
        int extrasSoFar = 0;
        for (final Tuple primaryKey : distinctResults) {
            final Integer rank = groundTruthRanks.get(primaryKey);
            if (rank == null) {
                extrasSoFar++;
            } else {
                sharedRanks.add(rank);
                found[rank] = true;
                extraAbove += extrasSoFar;
            }
        }

        // Discordant pairs: the found records' ground-truth ranks, listed in result order, would be increasing if the
        // results kept the ground truth's order. Every pair out of order (an inversion) is one discordant pair. With
        // at most k found records, the quadratic count is cheap next to the search.
        long discordant = 0L;
        for (int i = 0; i < sharedRanks.size(); i++) {
            for (int j = i + 1; j < sharedRanks.size(); j++) {
                if (sharedRanks.get(i) > sharedRanks.get(j)) {
                    discordant++;
                }
            }
        }

        // Missing-above pairs: walk the ground truth from its last rank to its first, counting the found records
        // passed so far. Every found record already passed ranks below the current one, so a missed record at the
        // current rank forms a missing-above pair with each of them.
        long missingAbove = 0L;
        int foundBelow = 0;
        for (int rank = truthSize - 1; rank >= 0; rank--) {
            if (found[rank]) {
                foundBelow++;
            } else {
                missingAbove += foundBelow;
            }
        }

        // Crossed pairs are every missed record against every extra one; unknown pairs are every pair within the
        // missed records and every pair within the extra ones.
        final int missing = truthSize - sharedRanks.size();
        final int extra = distinctResults.size() - sharedRanks.size();
        final long crossed = (long) missing * extra;
        final long unknown = pairs(missing) + pairs(extra);
        final double distance = discordant + missingAbove + extraAbove + crossed + UNKNOWN_ORDER_PENALTY * unknown;

        // Two lists of these sizes with nothing in common: every pair across them is crossed, every pair within either
        // is unknown.
        final double maxDistance = (double) truthSize * distinctResults.size()
                + UNKNOWN_ORDER_PENALTY * (pairs(truthSize) + pairs(distinctResults.size()));
        return 1.0d - distance / maxDistance;
    }

    /** The number of unordered pairs of {@code n} records. */
    private static long pairs(final int n) {
        return (long) n * (n - 1) / 2;
    }
}
