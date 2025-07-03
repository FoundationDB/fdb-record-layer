/*
 * OptionUtils.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.PlanningRuleSet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

public final class OptionsUtils {

    @Nonnull
    public static PlanHashable.PlanHashMode getCurrentPlanHashMode(@Nonnull final Options options) {
        final String planHashModeAsString = options.getOption(Options.Name.CURRENT_PLAN_HASH_MODE);
        return planHashModeAsString == null ?
               PlanHashable.CURRENT_FOR_CONTINUATION :
               PlanHashable.PlanHashMode.valueOf(planHashModeAsString);
    }

    @Nonnull
    public static Set<PlanHashable.PlanHashMode> getValidPlanHashModes(@Nonnull final Options options) {
        final String planHashModesAsString = options.getOption(Options.Name.VALID_PLAN_HASH_MODES);
        if (planHashModesAsString == null) {
            return ImmutableSet.of(PlanHashable.CURRENT_FOR_CONTINUATION);
        }

        return Arrays.stream(planHashModesAsString.split(","))
                .map(planHashModeAsString -> PlanHashable.PlanHashMode.valueOf(planHashModeAsString.trim()))
                .collect(ImmutableSet.toImmutableSet());
    }

    public static boolean getContinuationsContainsCompiledStatements(@Nonnull final Options options) {
        return Objects.requireNonNull(options.getOption(Options.Name.CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS));
    }

    @Nonnull
    public static RecordQueryPlannerConfiguration createPlannerConfigurations(@Nonnull final Options options)
            throws RelationalException {
        final var configurationBuilder = RecordQueryPlannerConfiguration.builder()
                .setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_INDEX)
                .setAttemptFailedInJoinAsUnionMaxSize(24);
        // TODO: Interaction between planner configurations and query cache
        Options.IndexFetchMethod indexFetchMethod = options.getOption(Options.Name.INDEX_FETCH_METHOD);
        if (indexFetchMethod == null) {
            configurationBuilder.setIndexFetchMethod(IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK);
        } else {
            switch (indexFetchMethod) {
                case SCAN_AND_FETCH:
                    configurationBuilder.setIndexFetchMethod(IndexFetchMethod.SCAN_AND_FETCH);
                    break;
                case USE_REMOTE_FETCH:
                    configurationBuilder.setIndexFetchMethod(IndexFetchMethod.USE_REMOTE_FETCH);
                    break;
                case USE_REMOTE_FETCH_WITH_FALLBACK:
                    configurationBuilder.setIndexFetchMethod(IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK);
                    break;
                default:
                    Assert.fail("Can not convert index fetch method '" + indexFetchMethod.name() + "' to planner configuration");
                    break; // make pmd happy.
            }
        }
        // TODO: Expose planner configuration parameters like index scan preference
        configurationBuilder.setDisabledTransformationRuleNames(ImmutableSet.copyOf(options.<Collection<String>>getOption(Options.Name.DISABLED_PLANNER_RULES)),
                PlanningRuleSet.DEFAULT);
        if (options.getOption(Options.Name.DISABLE_PLANNER_REWRITING)) {
            configurationBuilder.disableRewritingRules();
        }
        return configurationBuilder.build();
    }
}
