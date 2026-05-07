/*
 * RecordQueryPlannerConfigurationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.TestRecordsEnumProto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test of the semantics of setting configuration options on {@link RecordQueryPlanner}.
 * This is mostly "executable documentation" that makes sure that we never change the contract without meaning to.
 */
class RecordQueryPlannerConfigurationTest {
    private static RecordQueryPlanner blankPlanner() {
        return new RecordQueryPlanner(
                RecordMetaData.build(TestRecordsEnumProto.getDescriptor()),
                new RecordStoreState(null, null));
    }

    @Test
    void configurationOverridesIndexScanPreference() {
        RecordQueryPlanner planner = blankPlanner();

        // Set index scan preference first
        planner.setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_PRIMARY_KEY_INDEX);
        planner.setConfiguration(RecordQueryPlannerConfiguration.builder()
                .setAttemptFailedInJoinAsOr(true)
                .setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_INDEX)
                .build());
        // Configuration overrides index scan preference
        assertEquals(QueryPlanner.IndexScanPreference.PREFER_INDEX, planner.getIndexScanPreference());
    }

    @Test
    void scanPreferenceOverridesConfiguationScanPreference() {
        RecordQueryPlanner planner = blankPlanner();

        // Set configuration, then set an index scan preference.
        planner.setConfiguration(RecordQueryPlannerConfiguration.builder()
                .setAttemptFailedInJoinAsOr(true)
                .setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_INDEX)
                .build());
        planner.setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_PRIMARY_KEY_INDEX);
        // Index scan preference overrides configuration
        assertEquals(QueryPlanner.IndexScanPreference.PREFER_PRIMARY_KEY_INDEX, planner.getIndexScanPreference());
        // Configuration is still there
        RecordQueryPlannerConfiguration configuration = planner.getConfiguration();
        assertTrue(configuration.shouldAttemptFailedInJoinAsOr());
        assertEquals(QueryPlanner.IndexScanPreference.PREFER_PRIMARY_KEY_INDEX, configuration.getIndexScanPreference());
    }

    enum PlannerConfigurationFlag {
        ATTEMPT_FAILED_IN_JOIN_AS_OR(RecordQueryPlannerConfiguration.Builder::setAttemptFailedInJoinAsOr, RecordQueryPlannerConfiguration::shouldAttemptFailedInJoinAsOr),
        CHECK_FOR_DUPLICATE_CONDITIONS(RecordQueryPlannerConfiguration.Builder::setCheckForDuplicateConditions, RecordQueryPlannerConfiguration::shouldCheckForDuplicateConditions),
        DEFER_FETCH_AFTER_UNION_AND_INTERSECTION(RecordQueryPlannerConfiguration.Builder::setDeferFetchAfterUnionAndIntersection, RecordQueryPlannerConfiguration::shouldDeferFetchAfterUnionAndIntersection),
        DEFER_FETCH_AFTER_IN_JOIN_AND_IN_UNION(RecordQueryPlannerConfiguration.Builder::setDeferFetchAfterInJoinAndInUnion, RecordQueryPlannerConfiguration::shouldDeferFetchAfterInJoinAndInUnion),
        OPTIMIZE_FOR_INDEX_FILTERS(RecordQueryPlannerConfiguration.Builder::setOptimizeForIndexFilters, RecordQueryPlannerConfiguration::shouldOptimizeForIndexFilters),
        OPTIMIZE_FOR_REQUIRED_RESULTS(RecordQueryPlannerConfiguration.Builder::setOptimizeForRequiredResults, RecordQueryPlannerConfiguration::shouldOptimizeForRequiredResults),
        USE_FULL_KEY_FOR_VALUE_INDEX(RecordQueryPlannerConfiguration.Builder::setUseFullKeyForValueIndex, RecordQueryPlannerConfiguration::shouldUseFullKeyForValueIndex, true),
        DEFER_CROSS_PRODUCTS(RecordQueryPlannerConfiguration.Builder::setDeferCrossProducts, RecordQueryPlannerConfiguration::shouldDeferCrossProducts, true),
        PLAN_OTHER_ATTEMPT_WHOLE_FILTER(RecordQueryPlannerConfiguration.Builder::setPlanOtherAttemptWholeFilter, RecordQueryPlannerConfiguration::shouldPlanOtherAttemptWholeFilter),
        NORMALIZE_NESTED_FIELDS(RecordQueryPlannerConfiguration.Builder::setNormalizeNestedFields, RecordQueryPlannerConfiguration::shouldNormalizeNestedFields),
        OMIT_PRIMARY_KEY_IN_ORDERING_KEY_FOR_IN_UNION(RecordQueryPlannerConfiguration.Builder::setOmitPrimaryKeyInOrderingKeyForInUnion, RecordQueryPlannerConfiguration::shouldOmitPrimaryKeyInOrderingKeyForInUnion),
        JOIN_RIGHT_DEEP(RecordQueryPlannerConfiguration.Builder::setJoinRightDeep, RecordQueryPlannerConfiguration::shouldJoinRightDeep),
        ;

        private final BiFunction<RecordQueryPlannerConfiguration.Builder, Boolean, RecordQueryPlannerConfiguration.Builder> setter;
        private final Function<RecordQueryPlannerConfiguration, Boolean> getter;

        private final boolean defaultValue;

        PlannerConfigurationFlag(BiFunction<RecordQueryPlannerConfiguration.Builder, Boolean, RecordQueryPlannerConfiguration.Builder> setter, Function<RecordQueryPlannerConfiguration, Boolean> getter) {
            this(setter, getter, false);
        }

        PlannerConfigurationFlag(BiFunction<RecordQueryPlannerConfiguration.Builder, Boolean, RecordQueryPlannerConfiguration.Builder> setter, Function<RecordQueryPlannerConfiguration, Boolean> getter, boolean defaultValue) {
            this.setter = setter;
            this.getter = getter;
            this.defaultValue = defaultValue;
        }

        public RecordQueryPlannerConfiguration.Builder set(RecordQueryPlannerConfiguration.Builder builder, boolean value) {
            return setter.apply(builder, value);
        }

        public boolean get(RecordQueryPlannerConfiguration configuration) {
            return getter.apply(configuration);
        }

        public boolean getDefaultValue() {
            return defaultValue;
        }
    }

    @ParameterizedTest
    @EnumSource(PlannerConfigurationFlag.class)
    void validateBooleanFlagsCanBeSet(@Nonnull PlannerConfigurationFlag flag) {
        assertEquals(flag.getDefaultValue(), flag.get(RecordQueryPlannerConfiguration.defaultPlannerConfiguration()));

        // Set value to false
        final RecordQueryPlannerConfiguration built1 = flag.set(RecordQueryPlannerConfiguration.builder(), false).build();
        assertFalse(flag.get(built1));

        // Rebuild--value should be preserved
        final RecordQueryPlannerConfiguration built2 = built1.asBuilder().build();
        assertFalse(flag.get(built2));

        // Set value to true
        final RecordQueryPlannerConfiguration built3 = flag.set(built2.asBuilder(), true).build();
        assertTrue(flag.get(built3));

        // Rebuild--value should be preserved
        final RecordQueryPlannerConfiguration built4 = built3.asBuilder().build();
        assertTrue(flag.get(built4));
    }

    static Stream<Arguments> validateBooleanFlagsAreSetIndependently() {
        return Stream.of(PlannerConfigurationFlag.values()).flatMap(flag1 ->
                Stream.of(PlannerConfigurationFlag.values()).filter(flag2 -> flag2 != flag1).map(flag2 ->
                        Arguments.of(flag1, flag2)));
    }

    /**
     * For each pair of Boolean flags, make sure that they can be set independently. The flags in the planner
     * configuration all use a shared bitset, and so if the constants where misconfigured in a way that led
     * to multiple flags sharing the same bit, then this would fail.
     *
     * @param flag1 the first Boolean flag
     * @param flag2 the second Boolean flag
     */
    @ParameterizedTest
    @MethodSource
    void validateBooleanFlagsAreSetIndependently(@Nonnull PlannerConfigurationFlag flag1, @Nonnull PlannerConfigurationFlag flag2) {
        for (final boolean flagValue1 : new boolean[]{false, true}) {
            for (final boolean flagValue2 : new boolean[]{false, true}) {
                RecordQueryPlannerConfiguration.Builder builder = RecordQueryPlannerConfiguration.builder();
                builder = flag1.set(builder, flagValue1);
                builder = flag2.set(builder, flagValue2);
                final RecordQueryPlannerConfiguration configuration = builder.build();
                assertEquals(flagValue1, flag1.get(configuration));
                assertEquals(flagValue2, flag2.get(configuration));
            }
        }
    }
}
