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

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.Estimator;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class DebugIndexTest {
    private static final Logger logger = LoggerFactory.getLogger(DebugIndexTest.class);


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
}
