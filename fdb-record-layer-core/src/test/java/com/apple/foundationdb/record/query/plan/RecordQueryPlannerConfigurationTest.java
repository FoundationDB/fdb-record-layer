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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test of the semantics of setting configuration options on {@link RecordQueryPlanner}.
 * This is mostly "executable documentation" that makes sure that we never change the contract without meaning to.
 */
public class RecordQueryPlannerConfigurationTest {
    private static RecordQueryPlanner blankPlanner() {
        return new RecordQueryPlanner(
                RecordMetaData.build(TestRecordsEnumProto.getDescriptor()),
                new RecordStoreState(null, null));
    }

    @Test
    public void configurationOverridesIndexScanPreference() {
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
    public void scanPreferenceOverridesConfiguationScanPreference() {
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
}
