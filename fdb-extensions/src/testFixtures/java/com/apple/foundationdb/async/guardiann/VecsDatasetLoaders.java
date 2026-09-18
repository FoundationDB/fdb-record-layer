/*
 * VecsDatasetLoaders.java
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

import com.apple.foundationdb.async.common.PrimaryKeyAndVector;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.StoredVecsIterator;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Dataset-agnostic loaders for the {@code .fvecs}/{@code .ivecs} vector files (SIFT and friends), reading via the
 * main-code {@link StoredVecsIterator}. Lives in {@code testFixtures} so both the fdb-extensions guardiann tests and
 * the record-layer's vector-index tests can share one copy of the file-parsing wrappers rather than each hand-rolling
 * their own. Pair with the concrete dataset paths in {@link SiftTestHelpers}.
 */
public class VecsDatasetLoaders {
    private VecsDatasetLoaders() {
    }

    /**
     * Loads up to {@code numVectors} base vectors from an {@code .fvecs} file, tagging each with a primary key equal to
     * its zero-based position in the stream. Because the primary key is the file index, the entries line up directly
     * with the position-indexed ground truth from {@link #loadGroundTruth}.
     *
     * @param baseFile path to the {@code .fvecs} base file
     * @param numVectors the maximum number of vectors to read
     *
     * @return the loaded {@code (primaryKey, vector)} pairs, in file order
     */
    @Nonnull
    public static List<PrimaryKeyAndVector> loadVectors(@Nonnull final String baseFile,
                                                        final int numVectors) throws IOException {
        final ImmutableList.Builder<PrimaryKeyAndVector> insertedDataBuilder = ImmutableList.builder();

        try (FileChannel fileChannel = FileChannel.open(Paths.get(baseFile), StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> vectorIterator = new StoredVecsIterator.StoredFVecsIterator(fileChannel);

            int i = 0;
            while (vectorIterator.hasNext() && i < numVectors) {
                final DoubleRealVector currentVector = vectorIterator.next();
                insertedDataBuilder.add(new PrimaryKeyAndVector(Tuple.from((long) i++), currentVector));
            }
        }
        return insertedDataBuilder.build();
    }

    /**
     * Loads query vectors from an {@code .fvecs} file as {@link DoubleRealVector}s (the representation used by the
     * insert helpers; a Guardiann's public search API accepts any real vector, so there's no need to pre-quantize to
     * half-precision).
     *
     * @param queriesFile path to the {@code .fvecs} query file
     *
     * @return the loaded query vectors, in file order
     */
    @Nonnull
    public static List<DoubleRealVector> loadQueryVectors(@Nonnull final String queriesFile) throws IOException {
        final ImmutableList.Builder<DoubleRealVector> queries = ImmutableList.builder();
        try (FileChannel channel = FileChannel.open(Paths.get(queriesFile), StandardOpenOption.READ)) {
            final Iterator<DoubleRealVector> iterator = new StoredVecsIterator.StoredFVecsIterator(channel);
            while (iterator.hasNext()) {
                queries.add(iterator.next());
            }
        }
        return queries.build();
    }

    /**
     * Loads per-query ground-truth top-k index sets from an {@code .ivecs} file. Indices greater than {@code maxIndex}
     * are filtered out; pass {@code -1} to keep all.
     *
     * @param groundTruthFile path to the {@code .ivecs} ground-truth file
     * @param maxIndex the largest index to keep, or {@code -1} to keep every index
     *
     * @return one ground-truth index set per query, in file order
     */
    @Nonnull
    public static List<Set<Integer>> loadGroundTruth(@Nonnull final String groundTruthFile,
                                                     final int maxIndex) throws IOException {
        final ImmutableList.Builder<Set<Integer>> truth = ImmutableList.builder();
        try (FileChannel channel = FileChannel.open(Paths.get(groundTruthFile), StandardOpenOption.READ)) {
            final Iterator<List<Integer>> iterator = new StoredVecsIterator.StoredIVecsIterator(channel);
            while (iterator.hasNext()) {
                truth.add(iterator.next().stream()
                        .filter(idx -> maxIndex < 0 || idx <= maxIndex)
                        .collect(ImmutableSet.toImmutableSet()));
            }
        }
        return truth.build();
    }
}
