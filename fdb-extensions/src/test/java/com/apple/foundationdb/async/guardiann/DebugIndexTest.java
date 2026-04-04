/*
 * DebugIndexTest.java
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.async.common.BaseTest;
import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.common.StorageTransform;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

public class DebugIndexTest implements BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(DebugIndexTest.class);

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    static final NoClearSubspaceExtension subspaceExtension = new NoClearSubspaceExtension(dbExtension);

    @TempDir
    Path tempDir;

    private static Database db;
    private static Guardiann guardiann;
    private static List<PrimaryKeyAndVector> insertedData;

    @Nonnull
    @Override
    public Database getDb() {
        return Objects.requireNonNull(db);
    }

    @Nonnull
    @Override
    public Subspace getSubspace() {
        return subspaceExtension.getSubspace();
    }

    @Nonnull
    @Override
    public Path getTempDir() {
        return tempDir;
    }

    @BeforeAll
    @Timeout(value = 1000, unit = TimeUnit.MINUTES)
    public static void setUpDb() throws Exception {
        db = dbExtension.getDatabase();

        final TestHelpers.TestOnWriteListener onWriteListener = new TestHelpers.TestOnWriteListener();
        final TestHelpers.TestOnReadListener onReadListener = new TestHelpers.TestOnReadListener();

        final Metric metric = Metric.COSINE_METRIC;
        final Config config =
                Guardiann.newConfigBuilder()
                        .setUseRaBitQ(true)
                        .setRaBitQNumExBits(6)
                        .setMetric(metric)
                        .setPrimaryClusterMax(1024)
                        .setPrimaryClusterMin(100)
                        .setDeterministicRandomness(false)
                        .setClusterOverlap(0.15d)
                        .setReplicatedClusterTarget(1000)
                        .setReplicatedClusterMaxWrites(3000)
                        .build(512);

        guardiann = new Guardiann(subspaceExtension.getSubspace(),
                TestExecutors.defaultThreadPool(),
                config,
                onWriteListener,
                onReadListener);

        logger.info("Preparing db and inserting SIFT small dataset...");
        //insertedData = TestHelpers.insertSIFTSmall(db, guardiann);
        insertedData = TestHelpers.insertSIFT100k(db, guardiann, 100_000, 20);
//        insertedData =
//                TestHelpers.loadVectors("/Users/nseemann/downloads/embeddings-unified-model-100k-1.0.0.fvecs",
//                        100000);
    }

    @Test
    void testDebugQuery() throws Exception {
        final int k = 100;
        final String queriesFile = "/Users/nseemann/Downloads/embeddings-unified-model-100k-queries-1.0.0.fvecs";
        final String groundTruthFile = "/Users/nseemann/Downloads/embeddings-unified-model-100k-groundtruth-1.0.0.ivecs";
        debugQuery(getDb(), guardiann, insertedData, queriesFile, groundTruthFile, 795, k);

        final HNSW centroidHnsw = guardiann.getLocator().primitives().getClusterCentroidsHnsw();

        final Set<ResultEntry> centroidEntries = Sets.newConcurrentHashSet();
        SiftTest.scanCentroids(db, centroidHnsw.getSubspace(), centroidHnsw.getConfig(), 0, 100, centroidEntries::add);

        logger.info("checking clusters numCentroids={}", centroidEntries.size());
        final ListMultimap<UUID, Tuple> result = db.run(transaction -> {
            final Search search = guardiann.getLocator().search();
            return search.globalAssignmentCheck(transaction, ImmutableList.copyOf(centroidEntries)).join();
        });

        final RealVector queryVector = findQuery(queriesFile, 795);
        final List<ClusterInfo> clusterInfos = Lists.newArrayList();
        for (final ResultEntry centroidEntry : centroidEntries) {
            final Transformed<RealVector> centroidVector =
                    StorageTransform.identity().transform(Objects.requireNonNull(centroidEntry.getVector()));
            final ClusterInfo clusterInfo =
                    new ClusterInfo(new ClusterIdAndCentroid(centroidEntry.getPrimaryKey().getUUID(0),
                            centroidVector), Metric.COSINE_METRIC.distance(queryVector, centroidVector.getUnderlyingVector()));
            clusterInfos.add(clusterInfo);
        }

        clusterInfos.sort(Comparator.comparing(ClusterInfo::getDistance));

        for (final ClusterInfo clusterInfo : clusterInfos) {
            final UUID clusterId = clusterInfo.getClusterIdAndCentroid().getClusterId();
            logger.info("cluster={}, distance={}, missingItems={}",
                    clusterId, clusterInfo.getDistance(), result.get(clusterId));
        }

        // Replace this with your actual vector lookup.
        final IntFunction<RealVector> vectorProvider = itemId -> insertedData.get(itemId).getVector();

        final List<ClusterData> clusters = new ArrayList<>();

        for (final ClusterInfo clusterInfo : clusterInfos) {
            // Example cluster
            clusters.add(new ClusterData(
                    clusterInfo.getClusterIdAndCentroid().getClusterId(),
                    clusterInfo.getClusterIdAndCentroid().getCentroid().getUnderlyingVector(),
                    result.get(clusterInfo.getClusterIdAndCentroid().getClusterId())
                            .stream()
                            .map(tuple -> Math.toIntExact(tuple.getLong(0))).collect(Collectors.toList())));
        }

        final List<ClusterCompactnessStats> stats =
                computeAllClusterCompactness(clusters, vectorProvider);

        stats.sort(Comparator.comparingDouble(
                ClusterCompactnessStats::getP95Distance).reversed());

        for (final ClusterCompactnessStats s : stats) {
            System.out.println(s);
        }

        // Inspect the farthest members of one suspicious cluster.
        final var worstMembers = findTopNFarthestMembers(clusters.get(0),
                vectorProvider, 20);

        for (final var member : worstMembers) {
            System.out.println(member);
        }

        final ImmutableList<Integer> missedItemIds =
                ImmutableList.of(); // TODO

        Map<Integer, ClusterData> clusterLookupByItemId = Maps.newHashMap();
        for (final Integer missedItemId : missedItemIds) {
            ClusterData homeClusterData = null;
            double distance = Double.POSITIVE_INFINITY;
            for (final ClusterData clusterData : clusters) {
                final RealVector centroidVector = Objects.requireNonNull(clusterData.centroid);
                final double currentDistance =
                        Metric.COSINE_METRIC.distance(vectorProvider.apply(missedItemId), centroidVector);
                if (currentDistance < distance) {
                    distance = currentDistance;
                    homeClusterData = clusterData;
                }
            }
            clusterLookupByItemId.put(missedItemId, homeClusterData);
        }

        final List<MissedItemPercentileResult> missedItemPercentileResults =
                computeMissedItemPercentiles(missedItemIds, vectorProvider,
                        clusterLookupByItemId::get);

        for (final var r : missedItemPercentileResults) {
            logger.info(
                    "missedItemHomeClusterPercentile itemId={}, clusterId={}, clusterSize={}, itemToHomeCentroidDistance={}, " +
                            "fractionMembersCloserToCentroid={}, percentileAscending={}, percentileDescending={}",
                    r.getItemId(),
                    r.getClusterId(),
                    r.getClusterSize(),
                    r.getItemToHomeCentroidDistance(),
                    r.getFractionMembersCloserToCentroid(),
                    r.getPercentileAscending(),
                    r.getPercentileDescending()
            );
        }

        final List<HomeCentroidDistance> sortedCentroids =
                sortCentroidsByDistanceToQuery(queryVector, clusters);

        final Map<UUID, Integer> rankByClusterId =
                buildHomeCentroidRankMap(sortedCentroids);

        final Map<UUID, Double> distanceByClusterId =
                buildHomeCentroidDistanceMap(sortedCentroids);

        final List<HomeCentroidRankResult> rankResults =
                computeHomeCentroidRanks(
                        missedItemIds,
                        rankByClusterId,
                        distanceByClusterId,
                        clusterLookupByItemId::get,
                        sortedCentroids.size()
                );

        for (final var r : rankResults) {
            logger.info(
                    "missedItemHomeCentroidRank itemId={}, clusterId={}, queryToHomeCentroidDistance={}, homeCentroidRank={}, totalCentroids={}",
                    r.getItemId(),
                    r.getHomeClusterId(),
                    r.getQueryToHomeCentroidDistance(),
                    r.getHomeCentroidRank(),
                    r.getTotalCentroids()
            );
        }
    }

    @Test
    void testJustInsert() throws Exception {
        // nothing
    }

    @Test
    void testSpecificQueries() throws Exception {
        final Path siftQueryPath = Paths.get("/Users/nseemann/Downloads/embeddings-100k-queries.fvecs");
        final Path siftGroundTruthPath = Paths.get("/Users/nseemann/Downloads/embeddings-100k-groundtruth.ivecs");

        final Estimator estimator = Estimator.ofMetric(Metric.COSINE_METRIC);

        try (final var queryChannel = FileChannel.open(siftQueryPath, StandardOpenOption.READ);
                 final var groundTruthChannel = FileChannel.open(siftGroundTruthPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);
            final Iterator<List<Integer>> groundTruthIterator = new StoredVecsIterator.StoredIVecsIterator(groundTruthChannel);

            Verify.verify(queryIterator.hasNext() == groundTruthIterator.hasNext());

            final Set<Integer> interestingQueries = ImmutableSet.of(320, 319, 672, 845, 852);
            int i = 0;
            final ImmutableList.Builder<RealVector> vectorsBuilder = ImmutableList.builder();
            while (queryIterator.hasNext()) {
                final HalfRealVector queryVector = queryIterator.next().toHalfRealVector();
                final Set<Integer> groundTruthIndices = ImmutableSet.copyOf(groundTruthIterator.next());
                if (interestingQueries.contains(i)) {
                    logger.info("query index={}, length={}, vector={}", i, queryVector.l2Norm(), queryVector);
                    logger.info("query groundtruth={}, indices={}", i, groundTruthIndices);
                    vectorsBuilder.add(queryVector);
                }
                i ++;
            }

            final ImmutableList<RealVector> vectors = vectorsBuilder.build();

            for (int j = 0; j < vectors.size(); j++) {
                final RealVector outer = vectors.get(j);
                for (int k = 0; k < vectors.size(); k++) {
                    final RealVector inner = vectors.get(k);

                    final double distance = estimator.distance(outer, inner);
                    logger.info("distance j={}, k={}, distance={}", j, k, distance);
                }
            }
        }
    }

    @Nonnull
    static RealVector findQuery(@Nonnull final String queriesFile, final int queryIndex) throws IOException {
        final Path queryPath = Paths.get(queriesFile);

        try (final var queryChannel = FileChannel.open(queryPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);

            int i = 0;
            while (queryIterator.hasNext()) {
                final DoubleRealVector queryVector = queryIterator.next().toDoubleRealVector();

                if (i++ == queryIndex) {
                    return queryVector;
                }
            }
        }
        throw new NoSuchElementException();
    }

    static void debugQuery(@Nonnull final Database db,
                           @Nonnull final Guardiann guardiann,
                           @Nonnull final List<PrimaryKeyAndVector> data,
                           @Nonnull final String queriesFile,
                           @Nonnull final String groundTruthFile,
                           final int queryIndex,
                           final int k) throws IOException {

        final Metric metric = guardiann.getConfig().getMetric();
        final Path siftQueryPath = Paths.get(queriesFile);
        final Path siftGroundTruthPath = Paths.get(groundTruthFile);

        final TestHelpers.TestOnReadListener onReadListener = (TestHelpers.TestOnReadListener)guardiann.getOnReadListener();

        try (final var queryChannel = FileChannel.open(siftQueryPath, StandardOpenOption.READ);
                 final var groundTruthChannel = FileChannel.open(siftGroundTruthPath, StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> queryIterator = new StoredVecsIterator.StoredFVecsIterator(queryChannel);
            final Iterator<List<Integer>> groundTruthIterator = new StoredVecsIterator.StoredIVecsIterator(groundTruthChannel);

            Verify.verify(queryIterator.hasNext() == groundTruthIterator.hasNext());

            int i = 0;
            while (queryIterator.hasNext()) {
                final HalfRealVector queryVector = queryIterator.next().toHalfRealVector();
                final Set<Integer> groundTruthIndices = ImmutableSet.copyOf(groundTruthIterator.next());
                final Set<Integer> notFound = Sets.newHashSet(groundTruthIndices);

                if (i++ != queryIndex) {
                    continue;
                }

                onReadListener.reset();
                final long beginTs = System.nanoTime();
                final List<? extends ResultEntry> results =
                        db.run(tr -> guardiann.kNearestNeighborsSearch(tr, k, 30000,
                                true, queryVector).join());
                final long endTs = System.nanoTime();
                logger.info("retrieved result in elapsedTimeMs={}, reading readBytes={}",
                        TimeUnit.NANOSECONDS.toMillis(endTs - beginTs), onReadListener.getBytesReadByLayer());

                int recallCount = 0;
                for (final ResultEntry resultEntry : results) {
                    final int primaryKeyIndex = (int)resultEntry.getPrimaryKey().getLong(0);

                    //
                    // Assert that the original vector and the reconstructed vector are the same-ish vector
                    // (minus reconstruction errors). The closeness value is dependent on the encoding quality settings,
                    // the dimensionality, and the metric in use. For now, we just set it to 30.0 as that should be
                    // fairly safe with respect to not giving us false-positives and also tripping for actual logic
                    // errors as the expected random distance is far larger.
                    //
                    final RealVector originalVector = data.get(primaryKeyIndex).getVector();
                    assertThat(originalVector).isNotNull();
                    final double distance = metric.distance(originalVector,
                            Objects.requireNonNull(resultEntry.getVector()).toDoubleRealVector());
                    assertThat(distance).isCloseTo(0.0d, within(30.0d));

                    if (groundTruthIndices.contains(primaryKeyIndex)) {
                        recallCount++;
                    }

                    notFound.remove(primaryKeyIndex);
                }

                final double recall = (double)recallCount / k;
                //assertThat(recall).isGreaterThan(0.93);

                logger.info("query returned results recall={}; notFound={}",
                        String.format(Locale.ROOT, "%.2f", recall * 100.0d), notFound);

                for (final int resultIndex : notFound) {
                    final RealVector originalVector = data.get(resultIndex).getVector();
                    assertThat(originalVector).isNotNull();
                    final double distance = metric.distance(originalVector, queryVector.toDoubleRealVector());
                    logger.info("missing result={}; distance={}", resultIndex, distance);
                }

                break;
            }
        }
    }

    private static float[] loadNormalizedVector(final int itemId) {
        throw new UnsupportedOperationException("hook up your vector store here");
    }

    private static float[] loadNormalizedCentroid(final UUID clusterId) {
        throw new UnsupportedOperationException("hook up your centroid store here");
    }

    private static int[] loadMemberIds(final UUID clusterId) {
        throw new UnsupportedOperationException("hook up your postings/assignment store here");
    }

    /**
     * Compactness summary for a single cluster.
     */
    public static final class ClusterCompactnessStats {
        private final UUID clusterId;
        private final int size;
        private final double meanDistance;
        private final double stddevDistance;
        private final double minDistance;
        private final double p50Distance;
        private final double p90Distance;
        private final double p95Distance;
        private final double p99Distance;
        private final double maxDistance;

        public ClusterCompactnessStats(
                final UUID clusterId,
                final int size,
                final double meanDistance,
                final double stddevDistance,
                final double minDistance,
                final double p50Distance,
                final double p90Distance,
                final double p95Distance,
                final double p99Distance,
                final double maxDistance) {
            this.clusterId = clusterId;
            this.size = size;
            this.meanDistance = meanDistance;
            this.stddevDistance = stddevDistance;
            this.minDistance = minDistance;
            this.p50Distance = p50Distance;
            this.p90Distance = p90Distance;
            this.p95Distance = p95Distance;
            this.p99Distance = p99Distance;
            this.maxDistance = maxDistance;
        }

        public UUID getClusterId() {
            return clusterId;
        }

        public int getSize() {
            return size;
        }

        public double getMeanDistance() {
            return meanDistance;
        }

        public double getStddevDistance() {
            return stddevDistance;
        }

        public double getMinDistance() {
            return minDistance;
        }

        public double getP50Distance() {
            return p50Distance;
        }

        public double getP90Distance() {
            return p90Distance;
        }

        public double getP95Distance() {
            return p95Distance;
        }

        public double getP99Distance() {
            return p99Distance;
        }

        public double getMaxDistance() {
            return maxDistance;
        }

        @Override
        public String toString() {
            return "ClusterCompactnessStats{" +
                    "clusterId=" + clusterId +
                    ", size=" + size +
                    ", meanDistance=" + meanDistance +
                    ", stddevDistance=" + stddevDistance +
                    ", minDistance=" + minDistance +
                    ", p50Distance=" + p50Distance +
                    ", p90Distance=" + p90Distance +
                    ", p95Distance=" + p95Distance +
                    ", p99Distance=" + p99Distance +
                    ", maxDistance=" + maxDistance +
                    '}';
        }
    }

    /**
     * One cluster member whose distance to centroid is considered interesting.
     */
    public static final class ClusterOutlierMember {
        private final UUID clusterId;
        private final int itemId;
        private final double distanceToCentroid;

        public ClusterOutlierMember(final UUID clusterId, final int itemId, final double distanceToCentroid) {
            this.clusterId = clusterId;
            this.itemId = itemId;
            this.distanceToCentroid = distanceToCentroid;
        }

        public UUID getClusterId() {
            return clusterId;
        }

        public int getItemId() {
            return itemId;
        }

        public double getDistanceToCentroid() {
            return distanceToCentroid;
        }

        @Override
        public String toString() {
            return "ClusterOutlierMember{" +
                    "clusterId=" + clusterId +
                    ", itemId=" + itemId +
                    ", distanceToCentroid=" + distanceToCentroid +
                    '}';
        }
    }

    /**
     * Minimal input description for a cluster.
     *
     * memberIds are IDs of the vectors assigned to the cluster.
     * centroid must be normalized for cosine.
     */
    public static final class ClusterData {
        private final UUID clusterId;
        private final RealVector centroid;
        private final List<Integer> memberIds;

        public ClusterData(final UUID clusterId, final RealVector centroid, final List<Integer> memberIds) {
            this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
            this.centroid = Objects.requireNonNull(centroid, "centroid");
            this.memberIds = Objects.requireNonNull(memberIds, "memberIds");
        }

        public UUID getClusterId() {
            return clusterId;
        }

        public RealVector getCentroid() {
            return centroid;
        }

        public List<Integer> getMemberIds() {
            return memberIds;
        }
    }

    /**
     * Compute compactness statistics for one cluster.
     *
     * @param cluster cluster metadata
     * @param vectorProvider maps itemId -> normalized vector
     * @return compactness stats
     */
    public static ClusterCompactnessStats computeClusterCompactness(
            final ClusterData cluster,
            final IntFunction<RealVector> vectorProvider) {

        final List<Integer> memberIds = cluster.getMemberIds();
        if (memberIds.isEmpty()) {
            return new ClusterCompactnessStats(
                    cluster.getClusterId(),
                    0,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN,
                    Double.NaN);
        }

        final RealVector centroid = cluster.getCentroid();
        final double[] distances = new double[memberIds.size()];

        double sum = 0.0;
        double sumSq = 0.0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < memberIds.size(); i++) {
            final int itemId = memberIds.get(i);
            final RealVector vector = Objects.requireNonNull(vectorProvider.apply(itemId),
                    "vectorProvider returned null for itemId=" + itemId);

            final double d = cosineDistanceNormalized(vector, centroid);
            distances[i] = d;

            sum += d;
            sumSq += d * d;
            min = Math.min(min, d);
            max = Math.max(max, d);
        }

        Arrays.sort(distances);

        final double mean = sum / distances.length;
        final double variance = Math.max(0.0, (sumSq / distances.length) - (mean * mean));
        final double stddev = Math.sqrt(variance);

        return new ClusterCompactnessStats(
                cluster.getClusterId(),
                distances.length,
                mean,
                stddev,
                min,
                percentileFromSorted(distances, 0.50),
                percentileFromSorted(distances, 0.90),
                percentileFromSorted(distances, 0.95),
                percentileFromSorted(distances, 0.99),
                max
        );
    }

    /**
     * Compute compactness statistics for all clusters.
     */
    public static List<ClusterCompactnessStats> computeAllClusterCompactness(
            final List<ClusterData> clusters,
            final IntFunction<RealVector> vectorProvider) {

        final List<ClusterCompactnessStats> result = new ArrayList<>(clusters.size());
        for (final ClusterData cluster : clusters) {
            result.add(computeClusterCompactness(cluster, vectorProvider));
        }
        return result;
    }

    /**
     * Returns all members whose distance to centroid is >= threshold.
     *
     * Useful for finding members that are likely hurting routing quality.
     */
    public static List<ClusterOutlierMember> findMembersBeyondDistanceThreshold(
            final ClusterData cluster,
            final IntFunction<RealVector> vectorProvider,
            final double thresholdInclusive) {

        final List<ClusterOutlierMember> result = new ArrayList<>();
        final RealVector centroid = cluster.getCentroid();

        for (final int itemId : cluster.getMemberIds()) {
            final RealVector vector = Objects.requireNonNull(vectorProvider.apply(itemId),
                    "vectorProvider returned null for itemId=" + itemId);

            final double d = cosineDistanceNormalized(vector, centroid);
            if (d >= thresholdInclusive) {
                result.add(new ClusterOutlierMember(cluster.getClusterId(), itemId, d));
            }
        }

        result.sort(Comparator.comparingDouble(ClusterOutlierMember::getDistanceToCentroid).reversed());
        return result;
    }

    /**
     * Returns the top-N members farthest from the centroid.
     */
    public static List<ClusterOutlierMember> findTopNFarthestMembers(final ClusterData cluster,
                                                                     final IntFunction<RealVector> vectorProvider,
                                                                     final int topN) {

        final List<ClusterOutlierMember> result = new ArrayList<>();
        final RealVector centroid = cluster.getCentroid();

        for (final int itemId : cluster.getMemberIds()) {
            final RealVector vector = Objects.requireNonNull(vectorProvider.apply(itemId),
                    "vectorProvider returned null for itemId=" + itemId);

            final double d = cosineDistanceNormalized(vector, centroid);
            result.add(new ClusterOutlierMember(cluster.getClusterId(), itemId, d));
        }

        result.sort(Comparator.comparingDouble(ClusterOutlierMember::getDistanceToCentroid).reversed());
        if (result.size() <= topN) {
            return result;
        }
        return new ArrayList<>(result.subList(0, topN));
    }

    /**
     * Cosine distance for already-normalized vectors.
     *
     * distance = 1 - dot(a, b)
     */
    public static double cosineDistanceNormalized(final RealVector a, final RealVector b) {
        return Metric.COSINE_METRIC.distance(a, b);
    }

    /**
     * Percentile from an already-sorted ascending array.
     * Linear interpolation between adjacent ranks.
     */
    public static double percentileFromSorted(final double[] sorted, final double percentile) {
        if (sorted.length == 0) {
            return Double.NaN;
        }
        if (percentile <= 0.0) {
            return sorted[0];
        }
        if (percentile >= 1.0) {
            return sorted[sorted.length - 1];
        }

        final double position = percentile * (sorted.length - 1);
        final int lower = (int)Math.floor(position);
        final int upper = (int)Math.ceil(position);

        if (lower == upper) {
            return sorted[lower];
        }

        final double weight = position - lower;
        return sorted[lower] * (1.0 - weight) + sorted[upper] * weight;
    }

    public static final class MissedItemPercentileResult {
        private final int itemId;
        private final UUID clusterId;
        private final int clusterSize;
        private final double itemToHomeCentroidDistance;
        private final int numMembersCloserToCentroid;
        private final int numMembersAtOrCloserToCentroid;
        private final double fractionMembersCloserToCentroid;
        private final double percentileAscending;
        private final double percentileDescending;

        public MissedItemPercentileResult(
                final int itemId,
                final UUID clusterId,
                final int clusterSize,
                final double itemToHomeCentroidDistance,
                final int numMembersCloserToCentroid,
                final int numMembersAtOrCloserToCentroid,
                final double fractionMembersCloserToCentroid,
                final double percentileAscending,
                final double percentileDescending) {
            this.itemId = itemId;
            this.clusterId = clusterId;
            this.clusterSize = clusterSize;
            this.itemToHomeCentroidDistance = itemToHomeCentroidDistance;
            this.numMembersCloserToCentroid = numMembersCloserToCentroid;
            this.numMembersAtOrCloserToCentroid = numMembersAtOrCloserToCentroid;
            this.fractionMembersCloserToCentroid = fractionMembersCloserToCentroid;
            this.percentileAscending = percentileAscending;
            this.percentileDescending = percentileDescending;
        }

        public int getItemId() {
            return itemId;
        }

        public UUID getClusterId() {
            return clusterId;
        }

        public int getClusterSize() {
            return clusterSize;
        }

        public double getItemToHomeCentroidDistance() {
            return itemToHomeCentroidDistance;
        }

        public int getNumMembersCloserToCentroid() {
            return numMembersCloserToCentroid;
        }

        public int getNumMembersAtOrCloserToCentroid() {
            return numMembersAtOrCloserToCentroid;
        }

        /**
         * Fraction of members with strictly smaller distance to centroid than the target item.
         * Example: 0.98 means the item is farther from the centroid than 98% of the cluster.
         */
        public double getFractionMembersCloserToCentroid() {
            return fractionMembersCloserToCentroid;
        }

        /**
         * Ascending percentile in [0, 1], where higher means farther from centroid.
         * 0.99 means "more outlying than about 99% of the cluster".
         */
        public double getPercentileAscending() {
            return percentileAscending;
        }

        /**
         * Descending percentile in [0, 1], where higher means closer to centroid.
         * Often less useful for your case, but included for completeness.
         */
        public double getPercentileDescending() {
            return percentileDescending;
        }

        @Override
        public String toString() {
            return "MissedItemPercentileResult{" +
                    "itemId=" + itemId +
                    ", clusterId=" + clusterId +
                    ", clusterSize=" + clusterSize +
                    ", itemToHomeCentroidDistance=" + itemToHomeCentroidDistance +
                    ", numMembersCloserToCentroid=" + numMembersCloserToCentroid +
                    ", numMembersAtOrCloserToCentroid=" + numMembersAtOrCloserToCentroid +
                    ", fractionMembersCloserToCentroid=" + fractionMembersCloserToCentroid +
                    ", percentileAscending=" + percentileAscending +
                    ", percentileDescending=" + percentileDescending +
                    '}';
        }
    }

    /**
     * Computes where one item sits in the home cluster's member->centroid distance distribution.
     *
     * Interpretation:
     * - percentileAscending ~= 0.99  => very strong tail/outlier member
     * - percentileAscending ~= 0.50  => ordinary member
     * - percentileAscending ~= 0.10  => unusually close to centroid
     */
    public static MissedItemPercentileResult computeMissedItemPercentileInHomeCluster(
            final ClusterData homeCluster,
            final IntFunction<RealVector> vectorProvider,
            final int itemId) {

        final List<Integer> memberIds = homeCluster.getMemberIds();
        if (memberIds.isEmpty()) {
            throw new IllegalArgumentException("home cluster is empty: " + homeCluster.getClusterId());
        }

        final RealVector centroid = homeCluster.getCentroid();
        final RealVector itemVector = Objects.requireNonNull(
                vectorProvider.apply(itemId),
                "vectorProvider returned null for itemId=" + itemId);

        final double targetDistance = cosineDistanceNormalized(itemVector, centroid);

        int numCloser = 0;
        int numAtOrCloser = 0;

        for (final int memberId : memberIds) {
            final RealVector memberVector = Objects.requireNonNull(
                    vectorProvider.apply(memberId),
                    "vectorProvider returned null for memberId=" + memberId);

            final double d = cosineDistanceNormalized(memberVector, centroid);

            if (d < targetDistance) {
                numCloser++;
            }
            if (d <= targetDistance) {
                numAtOrCloser++;
            }
        }

        final int n = memberIds.size();
        final double fractionCloser = (double) numCloser / n;

        // Higher = farther from centroid / more outlying
        final double percentileAscending = (double) numAtOrCloser / n;

        // Higher = closer to centroid / less outlying
        final double percentileDescending = 1.0 - ((double) numCloser / n);

        return new MissedItemPercentileResult(
                itemId,
                homeCluster.getClusterId(),
                n,
                targetDistance,
                numCloser,
                numAtOrCloser,
                fractionCloser,
                percentileAscending,
                percentileDescending
        );
    }

    /**
     * Batch version for a list of missed items that all belong to the same home cluster.
     */
    public static List<MissedItemPercentileResult> computeMissedItemPercentilesInSameCluster(
            final ClusterData homeCluster,
            final IntFunction<RealVector> vectorProvider,
            final List<Integer> itemIds) {

        final List<MissedItemPercentileResult> result = new ArrayList<>(itemIds.size());
        for (final int itemId : itemIds) {
            result.add(computeMissedItemPercentileInHomeCluster(homeCluster, vectorProvider, itemId));
        }

        result.sort(Comparator.comparingDouble(MissedItemPercentileResult::getPercentileAscending).reversed());
        return result;
    }

    /**
     * Batch version for arbitrary missed items, where clusterLookup resolves the item's home cluster.
     */
    public static List<MissedItemPercentileResult> computeMissedItemPercentiles(
            final List<Integer> missedItemIds,
            final IntFunction<RealVector> vectorProvider,
            final IntFunction<ClusterData> clusterLookupByItemId) {

        final List<MissedItemPercentileResult> result = new ArrayList<>(missedItemIds.size());

        for (final int itemId : missedItemIds) {
            final ClusterData homeCluster = Objects.requireNonNull(
                    clusterLookupByItemId.apply(itemId),
                    "clusterLookupByItemId returned null for itemId=" + itemId);

            result.add(computeMissedItemPercentileInHomeCluster(homeCluster, vectorProvider, itemId));
        }

        result.sort(Comparator.comparingDouble(MissedItemPercentileResult::getPercentileAscending).reversed());
        return result;
    }

    /**
     * Computes the rank of an item's home centroid in the query-sorted list of all centroids.
     *
     * Rank is 1-based:
     *   1 = closest centroid to query
     *   2 = second closest
     *   ...
     */

    public static final class HomeCentroidDistance {
        private final UUID clusterId;
        private final double queryToCentroidDistance;

        public HomeCentroidDistance(final UUID clusterId, final double queryToCentroidDistance) {
            this.clusterId = clusterId;
            this.queryToCentroidDistance = queryToCentroidDistance;
        }

        public UUID getClusterId() {
            return clusterId;
        }

        public double getQueryToCentroidDistance() {
            return queryToCentroidDistance;
        }
    }

    public static final class HomeCentroidRankResult {
        private final int itemId;
        private final UUID homeClusterId;
        private final double queryToHomeCentroidDistance;
        private final int homeCentroidRank;
        private final int totalCentroids;

        public HomeCentroidRankResult(
                final int itemId,
                final UUID homeClusterId,
                final double queryToHomeCentroidDistance,
                final int homeCentroidRank,
                final int totalCentroids) {
            this.itemId = itemId;
            this.homeClusterId = homeClusterId;
            this.queryToHomeCentroidDistance = queryToHomeCentroidDistance;
            this.homeCentroidRank = homeCentroidRank;
            this.totalCentroids = totalCentroids;
        }

        public int getItemId() {
            return itemId;
        }

        public UUID getHomeClusterId() {
            return homeClusterId;
        }

        public double getQueryToHomeCentroidDistance() {
            return queryToHomeCentroidDistance;
        }

        public int getHomeCentroidRank() {
            return homeCentroidRank;
        }

        public int getTotalCentroids() {
            return totalCentroids;
        }

        @Override
        public String toString() {
            return "HomeCentroidRankResult{" +
                    "itemId=" + itemId +
                    ", homeClusterId=" + homeClusterId +
                    ", queryToHomeCentroidDistance=" + queryToHomeCentroidDistance +
                    ", homeCentroidRank=" + homeCentroidRank +
                    ", totalCentroids=" + totalCentroids +
                    '}';
        }
    }

    /**
     * Precompute all centroid distances to the query and sort them once.
     */
    public static List<HomeCentroidDistance> sortCentroidsByDistanceToQuery(
            final RealVector queryVector,
            final List<ClusterData> allClusters) {

        final List<HomeCentroidDistance> result = new ArrayList<>(allClusters.size());
        for (final ClusterData cluster : allClusters) {
            final double d = cosineDistanceNormalized(queryVector,
                    cluster.getCentroid());
            result.add(new HomeCentroidDistance(cluster.getClusterId(), d));
        }

        result.sort(Comparator.comparingDouble(HomeCentroidDistance::getQueryToCentroidDistance)
                .thenComparing(h -> h.getClusterId().toString()));

        return result;
    }

    /**
     * Build clusterId -> 1-based rank.
     */
    public static Map<UUID, Integer> buildHomeCentroidRankMap(
            final List<HomeCentroidDistance> sortedCentroids) {

        final Map<UUID, Integer> rankByClusterId = new HashMap<>(sortedCentroids.size() * 2);
        for (int i = 0; i < sortedCentroids.size(); i++) {
            rankByClusterId.put(sortedCentroids.get(i).getClusterId(), i + 1);
        }
        return rankByClusterId;
    }

    /**
     * Build clusterId -> queryToCentroidDistance.
     */
    public static Map<UUID, Double> buildHomeCentroidDistanceMap(
            final List<HomeCentroidDistance> sortedCentroids) {

        final Map<UUID, Double> distanceByClusterId = new HashMap<>(sortedCentroids.size() * 2);
        for (final HomeCentroidDistance entry : sortedCentroids) {
            distanceByClusterId.put(entry.getClusterId(), entry.getQueryToCentroidDistance());
        }
        return distanceByClusterId;
    }

    /**
     * Compute one item's home-centroid rank.
     *
     * clusterLookupByItemId must return the item's primary/home cluster.
     */
    public static HomeCentroidRankResult computeHomeCentroidRank(
            final int itemId,
            final Map<UUID, Integer> rankByClusterId,
            final Map<UUID, Double> distanceByClusterId,
            final java.util.function.IntFunction<ClusterData> clusterLookupByItemId,
            final int totalCentroids) {

        final ClusterData homeCluster = Objects.requireNonNull(
                clusterLookupByItemId.apply(itemId),
                "clusterLookupByItemId returned null for itemId=" + itemId);

        final UUID homeClusterId = homeCluster.getClusterId();

        final Integer rank = rankByClusterId.get(homeClusterId);
        if (rank == null) {
            throw new IllegalArgumentException("home cluster not found in rank map: " + homeClusterId);
        }

        final Double distance = distanceByClusterId.get(homeClusterId);
        if (distance == null) {
            throw new IllegalArgumentException("home cluster not found in distance map: " + homeClusterId);
        }

        return new HomeCentroidRankResult(
                itemId,
                homeClusterId,
                distance,
                rank,
                totalCentroids
        );
    }

    /**
     * Compute home-centroid rank for a batch of missed items.
     */
    public static List<HomeCentroidRankResult> computeHomeCentroidRanks(
            final List<Integer> itemIds,
            final Map<UUID, Integer> rankByClusterId,
            final Map<UUID, Double> distanceByClusterId,
            final java.util.function.IntFunction<ClusterData> clusterLookupByItemId,
            final int totalCentroids) {

        final List<HomeCentroidRankResult> result = new ArrayList<>(itemIds.size());
        for (final int itemId : itemIds) {
            result.add(computeHomeCentroidRank(
                    itemId,
                    rankByClusterId,
                    distanceByClusterId,
                    clusterLookupByItemId,
                    totalCentroids));
        }

        result.sort(Comparator.comparingInt(HomeCentroidRankResult::getHomeCentroidRank).reversed());
        return result;
    }

    /**
     * Extension for creating a subspace for tests. Each test will be given a unique subspace, and the data
     * will be cleared out during the {@link AfterEachCallback} for this extension. To use, create a member variable
     * of the test and register the extension:
     *
     * <pre>
     * &#64;RegisterExtension
     * static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
     * &#64;RegisterExtension
     * TestSubspaceExtension subspaceExtension = new TestSubspaceExtension(dbExtension);
     * </pre>
     *
     * <p>
     * Within the test, call {@link #getSubspace()} to get the test's allocated exception. As long as all usage
     * for that test goes through the extension, the data will be deleted after the test has completed.
     * </p>
     *
     * @see TestDatabaseExtension
     */
    public static class NoClearSubspaceExtension implements AfterEachCallback {
        private static final Logger LOGGER = LoggerFactory.getLogger(NoClearSubspaceExtension.class);
        private final TestDatabaseExtension dbExtension;
        @Nullable
        private Subspace subspace;

        public NoClearSubspaceExtension(TestDatabaseExtension dbExtension) {
            this.dbExtension = dbExtension;
        }

        @Nonnull
        public Subspace getSubspace() {
            if (subspace == null) {
                subspace = dbExtension.getDatabase().runAsync(tr ->
                        DirectoryLayer.getDefault().createOrOpen(tr, List.of("fdb-extensions-test"))
                                .thenApply(directorySubspace -> directorySubspace.subspace(Tuple.from(new UUID(1, 1))))
                ).join();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("created test subspace subspace=\"{}\"", subspace);
                }
            }
            return subspace;
        }

        @Override
        public void afterEach(final ExtensionContext extensionContext) {
        }
    }

    private static class ClusterInfo {
        @Nonnull
        private final ClusterIdAndCentroid clusterIdAndCentroid;
        private final double distance;

        public ClusterInfo(@Nonnull final ClusterIdAndCentroid clusterIdAndCentroid, final double distance) {
            this.clusterIdAndCentroid = clusterIdAndCentroid;
            this.distance = distance;
        }

        @Nonnull
        public ClusterIdAndCentroid getClusterIdAndCentroid() {
            return clusterIdAndCentroid;
        }

        public double getDistance() {
            return distance;
        }
    }
}
