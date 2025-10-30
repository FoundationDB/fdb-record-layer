/*
 * Config.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.linear.Metric;
import com.google.common.base.Objects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;

/**
 * Configuration settings for a {@link HNSW}.
 */
@SuppressWarnings("checkstyle:MemberName")
public class Config {
    public static final long DEFAULT_RANDOM_SEED = 0L;
    @Nonnull public static final Metric DEFAULT_METRIC = Metric.EUCLIDEAN_METRIC;
    public static final boolean DEFAULT_USE_INLINING = false;
    public static final int DEFAULT_M = 16;
    public static final int DEFAULT_M_MAX_0 = 2 * DEFAULT_M;
    public static final int DEFAULT_M_MAX = DEFAULT_M;
    public static final int DEFAULT_EF_CONSTRUCTION = 200;
    public static final boolean DEFAULT_EXTEND_CANDIDATES = false;
    public static final boolean DEFAULT_KEEP_PRUNED_CONNECTIONS = false;
    // stats
    public static final double DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY = 0.5d;
    public static final double DEFAULT_MAINTAIN_STATS_PROBABILITY = 0.05d;
    public static final int DEFAULT_STATS_THRESHOLD = 1000;
    // RaBitQ
    public static final boolean DEFAULT_USE_RABITQ = false;
    public static final int DEFAULT_RABITQ_NUM_EX_BITS = 4;
    private final long randomSeed;
    @Nonnull
    private final Metric metric;
    private final int numDimensions;
    private final boolean useInlining;
    private final int m;
    private final int mMax;
    private final int mMax0;
    private final int efConstruction;
    private final boolean extendCandidates;
    private final boolean keepPrunedConnections;

    private final double sampleVectorStatsProbability;
    private final double maintainStatsProbability;
    private final int statsThreshold;

    private final boolean useRaBitQ;
    private final int raBitQNumExBits;

    private Config(final long randomSeed, @Nonnull final Metric metric, final int numDimensions,
                   final boolean useInlining, final int m, final int mMax, final int mMax0,
                   final int efConstruction, final boolean extendCandidates, final boolean keepPrunedConnections,
                   final double sampleVectorStatsProbability, final double maintainStatsProbability,
                   final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits) {
        this.randomSeed = randomSeed;
        this.metric = metric;
        this.numDimensions = numDimensions;
        this.useInlining = useInlining;
        this.m = m;
        this.mMax = mMax;
        this.mMax0 = mMax0;
        this.efConstruction = efConstruction;
        this.extendCandidates = extendCandidates;
        this.keepPrunedConnections = keepPrunedConnections;
        this.sampleVectorStatsProbability = sampleVectorStatsProbability;
        this.maintainStatsProbability = maintainStatsProbability;
        this.statsThreshold = statsThreshold;
        this.useRaBitQ = useRaBitQ;
        this.raBitQNumExBits = raBitQNumExBits;
    }

    public long getRandomSeed() {
        return randomSeed;
    }

    @Nonnull
    public Metric getMetric() {
        return metric;
    }

    public int getNumDimensions() {
        return numDimensions;
    }

    public boolean isUseInlining() {
        return useInlining;
    }

    public int getM() {
        return m;
    }

    public int getMMax() {
        return mMax;
    }

    public int getMMax0() {
        return mMax0;
    }

    public int getEfConstruction() {
        return efConstruction;
    }

    public boolean isExtendCandidates() {
        return extendCandidates;
    }

    public boolean isKeepPrunedConnections() {
        return keepPrunedConnections;
    }

    public double getSampleVectorStatsProbability() {
        return sampleVectorStatsProbability;
    }

    public double getMaintainStatsProbability() {
        return maintainStatsProbability;
    }

    public int getStatsThreshold() {
        return statsThreshold;
    }

    public boolean isUseRaBitQ() {
        return useRaBitQ;
    }

    public int getRaBitQNumExBits() {
        return raBitQNumExBits;
    }

    @Nonnull
    public ConfigBuilder toBuilder() {
        return new ConfigBuilder(getRandomSeed(), getMetric(), isUseInlining(), getM(), getMMax(), getMMax0(),
                getEfConstruction(), isExtendCandidates(), isKeepPrunedConnections(),
                getSampleVectorStatsProbability(), getMaintainStatsProbability(), getStatsThreshold(),
                isUseRaBitQ(), getRaBitQNumExBits());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Config)) {
            return false;
        }
        final Config config = (Config)o;
        return randomSeed == config.randomSeed && numDimensions == config.numDimensions &&
                useInlining == config.useInlining && m == config.m && mMax == config.mMax && mMax0 == config.mMax0 &&
                efConstruction == config.efConstruction && extendCandidates == config.extendCandidates &&
                keepPrunedConnections == config.keepPrunedConnections &&
                Double.compare(sampleVectorStatsProbability, config.sampleVectorStatsProbability) == 0 &&
                Double.compare(maintainStatsProbability, config.maintainStatsProbability) == 0 &&
                statsThreshold == config.statsThreshold && useRaBitQ == config.useRaBitQ &&
                raBitQNumExBits == config.raBitQNumExBits && metric == config.metric;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(randomSeed, metric, numDimensions, useInlining, m, mMax, mMax0, efConstruction,
                extendCandidates, keepPrunedConnections, sampleVectorStatsProbability, maintainStatsProbability,
                statsThreshold, useRaBitQ, raBitQNumExBits);
    }

    @Override
    @Nonnull
    public String toString() {
        return "Config[randomSeed=" + getRandomSeed() + ", metric=" + getMetric() +
                ", numDimensions=" + getNumDimensions() + ", isUseInlining=" + isUseInlining() + ", M=" + getM() +
                ", MMax=" + getMMax() + ", MMax0=" + getMMax0() + ", efConstruction=" + getEfConstruction() +
                ", isExtendCandidates=" + isExtendCandidates() +
                ", isKeepPrunedConnections=" + isKeepPrunedConnections() +
                ", sampleVectorStatsProbability=" + getSampleVectorStatsProbability() +
                ", mainStatsProbability=" + getMaintainStatsProbability() +
                ", statsThreshold=" + getStatsThreshold() +
                ", useRaBitQ=" + isUseRaBitQ() +
                ", raBitQNumExBits=" + getRaBitQNumExBits() + "]";
    }

    /**
     * Builder for {@link Config}.
     *
     * @see HNSW#newConfigBuilder
     */
    @CanIgnoreReturnValue
    @SuppressWarnings("checkstyle:MemberName")
    public static class ConfigBuilder {
        private long randomSeed = DEFAULT_RANDOM_SEED;
        @Nonnull
        private Metric metric = DEFAULT_METRIC;
        private boolean useInlining = DEFAULT_USE_INLINING;
        private int m = DEFAULT_M;
        private int mMax = DEFAULT_M_MAX;
        private int mMax0 = DEFAULT_M_MAX_0;
        private int efConstruction = DEFAULT_EF_CONSTRUCTION;
        private boolean extendCandidates = DEFAULT_EXTEND_CANDIDATES;
        private boolean keepPrunedConnections = DEFAULT_KEEP_PRUNED_CONNECTIONS;

        private double sampleVectorStatsProbability = DEFAULT_SAMPLE_VECTOR_STATS_PROBABILITY;
        private double maintainStatsProbability = DEFAULT_MAINTAIN_STATS_PROBABILITY;
        private int statsThreshold = DEFAULT_STATS_THRESHOLD;

        private boolean useRaBitQ = DEFAULT_USE_RABITQ;
        private int raBitQNumExBits = DEFAULT_RABITQ_NUM_EX_BITS;

        public ConfigBuilder() {
        }

        public ConfigBuilder(final long randomSeed, @Nonnull final Metric metric, final boolean useInlining,
                             final int m, final int mMax, final int mMax0, final int efConstruction,
                             final boolean extendCandidates, final boolean keepPrunedConnections,
                             final double sampleVectorStatsProbability, final double maintainStatsProbability,
                             final int statsThreshold, final boolean useRaBitQ, final int raBitQNumExBits) {
            this.randomSeed = randomSeed;
            this.metric = metric;
            this.useInlining = useInlining;
            this.m = m;
            this.mMax = mMax;
            this.mMax0 = mMax0;
            this.efConstruction = efConstruction;
            this.extendCandidates = extendCandidates;
            this.keepPrunedConnections = keepPrunedConnections;
            this.sampleVectorStatsProbability = sampleVectorStatsProbability;
            this.maintainStatsProbability = maintainStatsProbability;
            this.statsThreshold = statsThreshold;
            this.useRaBitQ = useRaBitQ;
            this.raBitQNumExBits = raBitQNumExBits;
        }

        public long getRandomSeed() {
            return randomSeed;
        }

        @Nonnull
        public ConfigBuilder setRandomSeed(final long randomSeed) {
            this.randomSeed = randomSeed;
            return this;
        }

        @Nonnull
        public Metric getMetric() {
            return metric;
        }

        @Nonnull
        public ConfigBuilder setMetric(@Nonnull final Metric metric) {
            this.metric = metric;
            return this;
        }

        public boolean isUseInlining() {
            return useInlining;
        }

        @Nonnull
        public ConfigBuilder setUseInlining(final boolean useInlining) {
            this.useInlining = useInlining;
            return this;
        }

        public int getM() {
            return m;
        }

        @Nonnull
        public ConfigBuilder setM(final int m) {
            this.m = m;
            return this;
        }

        public int getMMax() {
            return mMax;
        }

        @Nonnull
        public ConfigBuilder setMMax(final int mMax) {
            this.mMax = mMax;
            return this;
        }

        public int getMMax0() {
            return mMax0;
        }

        @Nonnull
        public ConfigBuilder setMMax0(final int mMax0) {
            this.mMax0 = mMax0;
            return this;
        }

        public int getEfConstruction() {
            return efConstruction;
        }

        @Nonnull
        public ConfigBuilder setEfConstruction(final int efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        public boolean isExtendCandidates() {
            return extendCandidates;
        }

        @Nonnull
        public ConfigBuilder setExtendCandidates(final boolean extendCandidates) {
            this.extendCandidates = extendCandidates;
            return this;
        }

        public boolean isKeepPrunedConnections() {
            return keepPrunedConnections;
        }

        @Nonnull
        public ConfigBuilder setKeepPrunedConnections(final boolean keepPrunedConnections) {
            this.keepPrunedConnections = keepPrunedConnections;
            return this;
        }

        public double getSampleVectorStatsProbability() {
            return sampleVectorStatsProbability;
        }

        @Nonnull
        public ConfigBuilder setSampleVectorStatsProbability(final double sampleVectorStatsProbability) {
            this.sampleVectorStatsProbability = sampleVectorStatsProbability;
            return this;
        }

        public double getMaintainStatsProbability() {
            return maintainStatsProbability;
        }

        @Nonnull
        public ConfigBuilder setMaintainStatsProbability(final double maintainStatsProbability) {
            this.maintainStatsProbability = maintainStatsProbability;
            return this;
        }

        public int getStatsThreshold() {
            return statsThreshold;
        }

        @Nonnull
        public ConfigBuilder setStatsThreshold(final int statsThreshold) {
            this.statsThreshold = statsThreshold;
            return this;
        }

        public boolean isUseRaBitQ() {
            return useRaBitQ;
        }

        @Nonnull
        public ConfigBuilder setUseRaBitQ(final boolean useRaBitQ) {
            this.useRaBitQ = useRaBitQ;
            return this;
        }

        public int getRaBitQNumExBits() {
            return raBitQNumExBits;
        }

        @Nonnull
        public ConfigBuilder setRaBitQNumExBits(final int raBitQNumExBits) {
            this.raBitQNumExBits = raBitQNumExBits;
            return this;
        }

        public Config build(final int numDimensions) {
            return new Config(getRandomSeed(), getMetric(), numDimensions, isUseInlining(), getM(), getMMax(),
                    getMMax0(), getEfConstruction(), isExtendCandidates(), isKeepPrunedConnections(),
                    getSampleVectorStatsProbability(), getMaintainStatsProbability(), getStatsThreshold(),
                    isUseRaBitQ(), getRaBitQNumExBits());
        }
    }
}
