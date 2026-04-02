/*
 * PlannerConfiguration.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.PlanningRuleSet;
import com.apple.foundationdb.relational.api.Options;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.relational.api.Options.Name.DISABLE_PLANNER_REWRITING;
import static com.apple.foundationdb.relational.api.Options.Name.PRODUCE_RIGHT_DEEP_PLANS_ONLY;

/**
 * This contains a set of configurations given to the planner that fine-tunes its behavior.
 *
 * <p>
 * Note: Currently this only contains a list of <em>readable</em> indexes, but in the future
 * more configurations will be added, mostly reflecting what is already defined in {@link RecordQueryPlannerConfiguration}
 * and consolidate some of the configurations defined in {@link com.apple.foundationdb.relational.api.Options}.
 * </p>
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public final class PlannerConfiguration {

    @Nonnull
    private final Optional<Set<String>> readableIndexes;

    @Nonnull
    private final IndexFetchMethod indexFetchMethod;

    private final boolean disabledAllPlannerRules;

    @Nonnull
    private final Set<String> disabledPlannerRewriteRules;

    private final boolean produceRightDeepPlansOnly;

    @Nonnull
    private final RecordQueryPlannerConfiguration recordQueryPlannerConfiguration;

    private final int memoizedHash;

    private PlannerConfiguration(@Nonnull final Optional<Set<String>> readableIndexes,
                                 @Nonnull final IndexFetchMethod indexFetchMethod,
                                 @Nonnull final Set<String> disabledPlannerRewriteRules,
                                 boolean disabledAllPlannerRules,
                                 boolean produceRightDeepPlansOnly) {
        this.readableIndexes = readableIndexes;
        this.indexFetchMethod = indexFetchMethod;
        this.disabledAllPlannerRules = disabledAllPlannerRules;
        this.disabledPlannerRewriteRules = ImmutableSet.copyOf(disabledPlannerRewriteRules);
        this.produceRightDeepPlansOnly = produceRightDeepPlansOnly;
        this.memoizedHash = computeHash();
        this.recordQueryPlannerConfiguration = buildRecordQueryPlannerConfiguration();
    }

    @Nonnull
    public Optional<Set<String>> getReadableIndexes() {
        return readableIndexes;
    }

    @Nonnull
    public RecordQueryPlannerConfiguration getRecordQueryPlannerConfiguration() {
        return recordQueryPlannerConfiguration;
    }

    /**
     * Returns a {@link PlannerConfiguration} identical to this one except with {@code produceRightDeepPlansOnly} set
     * to {@code newProduceRightDeepPlansOnly}.
     * <p>
     * This is used by {@link com.apple.foundationdb.relational.recordlayer.query.AstNormalizer} to override
     * the connection-level configuration with a query-level {@code OPTIONS (PRODUCE RIGHT DEEP PLANS ONLY)}
     * clause, ensuring the resulting {@link com.apple.foundationdb.relational.recordlayer.query.cache.QueryCacheKey}
     * reflects the planner settings actually in effect for that query.
     * </p>
     * <p>
     * <em>Note:</em> this method is a narrow, per-option escape hatch. It should eventually be replaced by a more
     * general mechanism that allows any subset of {@link com.apple.foundationdb.relational.api.Options} to override
     * the corresponding fields of a {@link PlannerConfiguration}, so that new query-level planner options do not
     * each require a dedicated {@code withXxx} method here.
     * </p>
     *
     * @param newProduceRightDeepPlansOnly the desired value of the flag
     * @return {@code this} if the flag is already equal to {@code newProduceRightDeepPlansOnly},
     *         otherwise a new {@link PlannerConfiguration} with the flag overridden
     */
    @Nonnull
    public PlannerConfiguration withProduceRightDeepPlansOnly(final boolean newProduceRightDeepPlansOnly) {
        if (this.produceRightDeepPlansOnly == newProduceRightDeepPlansOnly) {
            return this;
        }
        return new PlannerConfiguration(readableIndexes, indexFetchMethod, disabledPlannerRewriteRules, disabledAllPlannerRules, newProduceRightDeepPlansOnly);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final var that = (PlannerConfiguration) other;
        return Objects.equals(readableIndexes, that.readableIndexes)
                && this.indexFetchMethod.equals(that.indexFetchMethod)
                && this.disabledAllPlannerRules == that.disabledAllPlannerRules
                && this.disabledPlannerRewriteRules.equals(that.disabledPlannerRewriteRules)
                && this.produceRightDeepPlansOnly == that.produceRightDeepPlansOnly;
    }

    private int computeHash() {
        return Objects.hash(readableIndexes, indexFetchMethod, disabledAllPlannerRules, disabledPlannerRewriteRules, produceRightDeepPlansOnly);
    }

    @Override
    public int hashCode() {
        return memoizedHash;
    }

    @Nonnull
    private RecordQueryPlannerConfiguration buildRecordQueryPlannerConfiguration() {
        final var configurationBuilder = RecordQueryPlannerConfiguration.builder()
                .setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_INDEX)
                .setAttemptFailedInJoinAsUnionMaxSize(24);
        configurationBuilder.setIndexFetchMethod(indexFetchMethod);
        configurationBuilder.setDisabledTransformationRuleNames(disabledPlannerRewriteRules, PlanningRuleSet.DEFAULT);
        if (disabledAllPlannerRules) {
            configurationBuilder.disableRewritingRules();
        }
        configurationBuilder.setJoinRightDeep(produceRightDeepPlansOnly);
        return configurationBuilder.build();
    }

    @Nonnull
    public static PlannerConfiguration of(@Nonnull final Optional<Set<String>> readableIndexesMaybe,
                                          @Nonnull final Options options) {
        final var disabledPlannerRules = ImmutableSet.copyOf(options.<Collection<String>>getOption(Options.Name.DISABLED_PLANNER_RULES));
        return new PlannerConfiguration(readableIndexesMaybe, OptionsUtils.getIndexFetchMethod(options),
                disabledPlannerRules, options.getOption(DISABLE_PLANNER_REWRITING),
                options.getOption(PRODUCE_RIGHT_DEEP_PLANS_ONLY));
    }

    @Nonnull
    public static PlannerConfiguration ofAllAvailableIndexes() {
        return of(Optional.empty(), Options.none());
    }

    @Nonnull
    public static PlannerConfiguration ofAllAvailableIndexes(@Nonnull final Options options) {
        return of(Optional.empty(), options);
    }
}
