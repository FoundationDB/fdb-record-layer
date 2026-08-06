/*
 * GuardiannVectorIndexBackPressureTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests the Guardiann primary-cluster hard cap ({@code guardiannPrimaryClusterHardMax}). When deferred maintenance
 * tasks are not drained in the writing transaction, an insert that would push a cluster above its hard cap must
 * back-pressure the writer with a {@link VectorIndexClusterTooLargeException} instead of letting the cluster grow
 * without bound; when they are drained in-transaction the write self-maintains and is never capped.
 * <p>
 * The cluster-size knobs are tiny and the hard cap low so a short insert burst reaches the cap: with splits deferred,
 * primaries pile into a single over-grown cluster, so a handful of inserts crosses the cap.
 */
class GuardiannVectorIndexBackPressureTest extends VectorIndexTestBase {
    // Comfortably above the hard cap below; with splits deferred these all target one cluster, so the cap is crossed
    // well before this many inserts (the loop stops at the first back-pressure).
    private static final int HARD_CAP_FORCING_INSERTS = 64;

    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.<String, String>builder()
                .put(IndexOptions.VECTOR_ENGINE, VectorIndexEngine.Kind.GUARDIANN.name())
                .put(IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name())
                .put(IndexOptions.VECTOR_NUM_DIMENSIONS, "128")
                // tiny clusters and a low hard cap (which must stay strictly above the max) so a short burst trips it
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MIN, "2")
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_MAX, "8")
                .put(IndexOptions.GUARDIANN_PRIMARY_CLUSTER_HARD_MAX, "16")
                .put(IndexOptions.GUARDIANN_COLLAPSE_MIN_DUPLICATES, "4")
                .put(IndexOptions.GUARDIANN_DETERMINISTIC_RANDOMNESS, "true")
                // maintainIndexesInTransaction left at its false default so the split backlog accrues
                .build();
    }

    /**
     * With deferred draining (the default), inserts that outrun the split backlog push a cluster past its hard cap and
     * must back-pressure with {@link VectorIndexClusterTooLargeException}.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL, 0xf00dcafeL})
    void insertPastHardCapBackPressures(final long seed) throws Exception {
        final var generator = getRecordGenerator(new Random(seed), 0.0d);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            try {
                // read-your-writes keeps the cluster's primary count in step within this transaction, so it climbs to
                // the cap over successive inserts and the crossing insert throws.
                for (int i = 0; i < HARD_CAP_FORCING_INSERTS; i++) {
                    recordStore.saveRecord(generator.apply((long)i));
                }
                fail("inserting past the cluster hard cap should have back-pressured");
            } catch (final Exception e) {
                assertThat(isOrHasCause(e, VectorIndexClusterTooLargeException.class))
                        .as("back-pressure must surface as VectorIndexClusterTooLargeException, but got: %s", e)
                        .isTrue();
            }
        }
    }

    /**
     * An index that drains deferred tasks in-transaction self-maintains: splits are applied inline so the cluster stays
     * bounded and the hard-cap check is disabled, so the same insert burst never back-pressures.
     */
    @ParameterizedTest
    @RandomSeedSource({0x5ca1ab1eL})
    void inTransactionDrainingNeverBackPressures(final long seed) throws Exception {
        final var generator = getRecordGenerator(new Random(seed), 0.0d);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(true);
            for (int i = 0; i < HARD_CAP_FORCING_INSERTS; i++) {
                recordStore.saveRecord(generator.apply((long)i));
            }
            commit(context);
        }
    }

    private static boolean isOrHasCause(@Nonnull final Throwable throwable,
                                        @Nonnull final Class<? extends Throwable> type) {
        for (Throwable current = throwable; current != null && current != current.getCause();
                current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }
}
