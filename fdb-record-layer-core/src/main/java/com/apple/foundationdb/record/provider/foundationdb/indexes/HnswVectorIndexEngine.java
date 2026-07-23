/*
 * HnswVectorIndexEngine.java
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

import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.async.hnsw.Config;
import com.apple.foundationdb.async.hnsw.Config.ConfigBuilder;
import com.apple.foundationdb.async.hnsw.HNSW;
import com.apple.foundationdb.async.hnsw.Node;
import com.apple.foundationdb.async.hnsw.NodeReference;
import com.apple.foundationdb.async.hnsw.OnReadListener;
import com.apple.foundationdb.async.hnsw.OnWriteListener;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.allowChange;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.applyBoolean;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.applyDouble;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.applyInteger;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexOptionsHelper.disallowChange;

/**
 * The {@link VectorIndexEngine} backed by an {@link HNSW} graph. Holds the parsed HNSW {@link Config} and, on each
 * operation, constructs a fresh {@code HNSW} bound to the partition subspace (the graph itself is stateless beyond the
 * subspace, so per-call construction is cheap and matches how the maintainer already worked). All HNSW-specific concerns
 * — config parsing from index options, the per-query {@code efSearch}/return-vectors knob derivation, and the
 * read/write listeners that attribute node-level work to the timer — live here so the maintainer stays engine-neutral.
 */
@SuppressWarnings("PMD.TooManyStaticImports")
final class HnswVectorIndexEngine implements VectorIndexEngine {
    // Stats and concurrency knobs are the only mutable options; everything else is immutable (see
    // validateChangedOptions). Each key already covers its current and legacy names.
    private static final List<VectorOptionKey<?>> MUTABLE_OPTIONS = ImmutableList.of(
            VectorIndexOptionKeys.SAMPLE_VECTOR_STATS_PROBABILITY,
            VectorIndexOptionKeys.MAINTAIN_STATS_PROBABILITY,
            VectorIndexOptionKeys.STATS_THRESHOLD,
            VectorIndexOptionKeys.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES,
            VectorIndexOptionKeys.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES,
            VectorIndexOptionKeys.HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER);

    @Nonnull
    private final Config config;

    HnswVectorIndexEngine(@Nonnull final Config config) {
        this.config = config;
    }

    @Nonnull
    @Override
    public CompletableFuture<List<? extends ResultEntry>> search(@Nonnull final FDBRecordContext context,
                                                                 final boolean snapshot,
                                                                 @Nonnull final Subspace subspace,
                                                                 @Nonnull final VectorIndexScanBounds scanBounds) {
        final HNSW hnsw = new HNSW(subspace, context.getExecutor(), config, OnWriteListener.NOOP,
                OnRead.fromTimer(context.getTimer()));
        return hnsw.kNearestNeighborsSearch(context.readTransaction(snapshot), scanBounds.getAdjustedLimit(),
                efSearch(scanBounds), VectorIndexOptionsHelper.returnVectors(scanBounds, config.useRaBitQ()),
                Objects.requireNonNull(scanBounds.getQueryVector()));
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> insert(@Nonnull final FDBRecordContext context,
                                          @Nonnull final Subspace subspace,
                                          @Nonnull final Tuple primaryKey,
                                          @Nonnull final RealVector vector) {
        // Insert traverses the graph greedily from the entry point, so it reads many nodes; wire a real read listener
        // in addition to the write listener so that read work is instrumented too.
        final FDBStoreTimer timer = context.getTimer();
        final HNSW hnsw = new HNSW(subspace, context.getExecutor(), config, OnWrite.fromTimer(timer),
                OnRead.fromTimer(timer));
        return hnsw.insert(context.ensureActive(), primaryKey, vector, null);
    }

    @Nonnull
    @Override
    public CompletableFuture<Void> delete(@Nonnull final FDBRecordContext context,
                                          @Nonnull final Subspace subspace,
                                          @Nonnull final Tuple primaryKey,
                                          @Nonnull final RealVector vector) {
        // HNSW keys nodes on the primary key alone, so the vector is not needed to locate the node to delete. Delete
        // reads heavily to repair the graph around the removed node, so it also gets a real read listener.
        final FDBStoreTimer timer = context.getTimer();
        final HNSW hnsw = new HNSW(subspace, context.getExecutor(), config, OnWrite.fromTimer(timer),
                OnRead.fromTimer(timer));
        return hnsw.delete(context.ensureActive(), primaryKey);
    }

    private int efSearch(@Nonnull final VectorIndexScanBounds scanBounds) {
        final VectorIndexScanOptions scanOptions = scanBounds.getVectorIndexScanOptions();
        final Integer efSearchOptionValue = scanOptions.getOption(VectorIndexScanOptions.HNSW_EF_SEARCH);
        if (efSearchOptionValue != null) {
            return efSearchOptionValue;
        }
        final var k = scanBounds.getAdjustedLimit();
        return Math.min(Math.max(4 * k, 64), Math.max(k, 400));
    }

    /**
     * Builds the HNSW engine for an index, parsing its {@link Config} from the index options.
     *
     * @param index the index definition
     * @return the HNSW engine
     */
    @Nonnull
    static HnswVectorIndexEngine fromIndex(@Nonnull final Index index) {
        return new HnswVectorIndexEngine(parseConfig(index));
    }

    /**
     * Parses an HNSW {@link Config} from an index's options via the {@link VectorIndexOptionKeys} catalog: each key
     * resolves its value under the current name or a legacy alias and parses it to the right type. Unspecified options
     * fall back to the {@link Config} defaults.
     *
     * @param index the index definition
     * @return the parsed HNSW config
     */
    @Nonnull
    static Config parseConfig(@Nonnull final Index index) {
        final ConfigBuilder builder = HNSW.newConfigBuilder();

        builder.setMetric(VectorIndexOptionKeys.METRIC.read(index, Metric.EUCLIDEAN_METRIC));
        final int numDimensions = VectorIndexOptionsHelper.getNumDimensions(index);

        applyBoolean(VectorIndexOptionKeys.HNSW_USE_INLINING, index, builder::setUseInlining);
        applyInteger(VectorIndexOptionKeys.HNSW_M, index, builder::setM);
        applyInteger(VectorIndexOptionKeys.HNSW_M_MAX, index, builder::setMMax);
        applyInteger(VectorIndexOptionKeys.HNSW_M_MAX_0, index, builder::setMMax0);
        applyInteger(VectorIndexOptionKeys.HNSW_EF_CONSTRUCTION, index, builder::setEfConstruction);
        applyInteger(VectorIndexOptionKeys.HNSW_EF_REPAIR, index, builder::setEfRepair);
        applyBoolean(VectorIndexOptionKeys.HNSW_EXTEND_CANDIDATES, index, builder::setExtendCandidates);
        applyBoolean(VectorIndexOptionKeys.HNSW_KEEP_PRUNED_CONNECTIONS, index, builder::setKeepPrunedConnections);
        applyDouble(VectorIndexOptionKeys.SAMPLE_VECTOR_STATS_PROBABILITY, index,
                builder::setSampleVectorStatsProbability);
        applyDouble(VectorIndexOptionKeys.MAINTAIN_STATS_PROBABILITY, index, builder::setMaintainStatsProbability);
        applyInteger(VectorIndexOptionKeys.STATS_THRESHOLD, index, builder::setStatsThreshold);
        applyBoolean(VectorIndexOptionKeys.USE_RABITQ, index, builder::setUseRaBitQ);
        applyInteger(VectorIndexOptionKeys.RABITQ_NUM_EX_BITS, index, builder::setRaBitQNumExBits);
        applyInteger(VectorIndexOptionKeys.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES, index,
                builder::setMaxNumConcurrentNodeFetches);
        applyInteger(VectorIndexOptionKeys.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES, index,
                builder::setMaxNumConcurrentNeighborhoodFetches);
        applyInteger(VectorIndexOptionKeys.HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER, index,
                builder::setMaxNumConcurrentDeleteFromLayer);
        return builder.build(numDimensions);
    }

    /**
     * Validates which HNSW options may change on an existing index. Comparison is on <em>effective</em> config values
     * (parsed and defaulted), so setting an immutable option to a value equal to its default is not treated as a
     * change. Structural options (metric, dimensions, graph connectivity, construction/repair, and RaBitQ encoding) are
     * immutable; the stats and concurrency options are mutable. Each key covers both its current and legacy names, so a
     * change expressed through either is caught. Handled options are removed from {@code changedOptions}.
     *
     * @param oldIndex the pre-change index
     * @param newIndex the post-change index
     * @param changedOptions the mutable set of changed option names; handled options are removed
     */
    static void validateChangedOptions(@Nonnull final Index oldIndex, @Nonnull final Index newIndex,
                                       @Nonnull final Set<String> changedOptions) {
        final Config oldConfig = parseConfig(oldIndex);
        final Config newConfig = parseConfig(newIndex);
        final String name = newIndex.getName();

        // Immutable structural options.
        disallowChange(changedOptions, VectorIndexOptionKeys.METRIC, oldConfig.metric(), newConfig.metric(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.NUM_DIMENSIONS, oldConfig.numDimensions(),
                newConfig.numDimensions(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.HNSW_USE_INLINING, oldConfig.useInlining(),
                newConfig.useInlining(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.HNSW_M, oldConfig.m(), newConfig.m(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.HNSW_M_MAX, oldConfig.mMax(), newConfig.mMax(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.HNSW_M_MAX_0, oldConfig.mMax0(), newConfig.mMax0(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.HNSW_EF_CONSTRUCTION, oldConfig.efConstruction(),
                newConfig.efConstruction(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.HNSW_EF_REPAIR, oldConfig.efRepair(),
                newConfig.efRepair(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.HNSW_EXTEND_CANDIDATES, oldConfig.extendCandidates(),
                newConfig.extendCandidates(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.HNSW_KEEP_PRUNED_CONNECTIONS,
                oldConfig.keepPrunedConnections(), newConfig.keepPrunedConnections(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.USE_RABITQ, oldConfig.useRaBitQ(),
                newConfig.useRaBitQ(), name);
        disallowChange(changedOptions, VectorIndexOptionKeys.RABITQ_NUM_EX_BITS, oldConfig.raBitQNumExBits(),
                newConfig.raBitQNumExBits(), name);

        // Mutable tuning options — stats and concurrency.
        for (final VectorOptionKey<?> key : MUTABLE_OPTIONS) {
            allowChange(changedOptions, key);
        }
    }

    /**
     * Read listener that attributes HNSW node reads to the store timer.
     */
    static final class OnRead implements OnReadListener {
        @Nonnull
        private final FDBStoreTimer timer;

        OnRead(@Nonnull final FDBStoreTimer timer) {
            this.timer = timer;
        }

        @Override
        public <N extends NodeReference, T extends Node<N>> CompletableFuture<T> onAsyncRead(@Nonnull CompletableFuture<T> future) {
            return timer.instrument(VectorIndexHelper.Events.VECTOR_SCAN, future);
        }

        @Override
        public void onNodeRead(final int layer, @Nonnull final Node<? extends NodeReference> node) {
            if (layer == 0) {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE0_READS);
            } else {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE_READS);
            }
        }

        @Override
        public void onKeyValueRead(final int layer, @Nonnull final byte[] key, @Nullable final byte[] value) {
            VectorIndexInstrumentation.recordKeyValueRead(timer, key, value);

            final int totalLength = key.length + (value == null ? 0 : value.length);
            if (layer == 0) {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE0_READ_BYTES, totalLength);
            } else {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE_READ_BYTES, totalLength);
            }
        }

        @Nonnull
        private static OnReadListener fromTimer(@Nullable final FDBStoreTimer timer) {
            return timer == null ? OnReadListener.NOOP : new OnRead(timer);
        }
    }

    /**
     * Write listener that attributes HNSW node writes to the store timer.
     */
    static final class OnWrite implements OnWriteListener {
        @Nonnull
        private final FDBStoreTimer timer;

        OnWrite(@Nonnull final FDBStoreTimer timer) {
            this.timer = timer;
        }

        @Override
        public void onNodeWritten(final int layer, @Nonnull final Node<? extends NodeReference> node) {
            if (layer == 0) {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE0_WRITES);
            } else {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE_WRITES);
            }
        }

        @Override
        public void onKeyValueWritten(final int layer, @Nonnull final byte[] key, @Nonnull final byte[] value) {
            final int totalLength = key.length + value.length;
            VectorIndexInstrumentation.recordKeyValueWritten(timer, key, value);

            if (layer == 0) {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE0_WRITE_BYTES, totalLength);
            } else {
                timer.increment(FDBStoreTimer.Counts.VECTOR_NODE_WRITE_BYTES, totalLength);
            }
        }

        @Nonnull
        private static OnWriteListener fromTimer(@Nullable final FDBStoreTimer timer) {
            return timer == null ? OnWriteListener.NOOP : new OnWrite(timer);
        }
    }
}
