/*
 * CopyPlanTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.PlanHashable;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Unit tests of {@link CopyPlan}.
 */
class CopyPlanTest {

    private static MutablePlanGenerationContext createContext(String sql) {
        final PreparedParams preparedParams = PreparedParams.of(Map.of(), Map.of());
        return new MutablePlanGenerationContext(preparedParams,
                PlanHashable.PlanHashMode.VC1, sql, sql, 0);
    }

    private static MutablePlanGenerationContext createImportContext(String sql) {
        final PreparedParams preparedParams = PreparedParams.of(Map.of(1, List.of()), Map.of());
        final MutablePlanGenerationContext context = new MutablePlanGenerationContext(preparedParams,
                PlanHashable.PlanHashMode.VC1, sql, sql, 0);
        context.processUnnamedPreparedParam(1);
        return context;
    }

    @Test
    void exportPlanHashIsStable() {
        final String path = "/FRL/MY_DATABASE";
        final String sql = "COPY " + path + " PRESERVE INCARNATION";
        final CopyPlan plan = CopyPlan.getCopyExportAction(path, createContext(sql), false);

        // This value must remain constant across JVM restarts.
        assertEquals(879232729, plan.getPlanHash(),
                "CopyPlan EXPORT hash should be stable across JVM restarts");
    }

    @Test
    void importPlanHashIsStable() {
        final String path = "/FRL/MY_DATABASE";
        final String sql = "COPY " + path + " FROM ?";
        final CopyPlan plan = CopyPlan.getCopyImportAction(path, createImportContext(sql));

        // This value must remain constant across JVM restarts.
        assertEquals(-1912066422, plan.getPlanHash(),
                "CopyPlan IMPORT hash should be stable across JVM restarts");
    }

    @Test
    void differentPathsProduceDifferentStableHashes() {
        final CopyPlan plan1 = CopyPlan.getCopyExportAction("/FRL/DB_A",
                createContext("COPY /FRL/DB_A PRESERVE INCARNATION"), false);
        final CopyPlan plan2 = CopyPlan.getCopyExportAction("/FRL/DB_B",
                createContext("COPY /FRL/DB_B PRESERVE INCARNATION"), false);

        // Different paths must produce different hashes (and both must be stable)
        assertEquals(2116157195, plan1.getPlanHash(),
                "CopyPlan hash for /FRL/DB_A should be stable");
        assertEquals(2116157226, plan2.getPlanHash(),
                "CopyPlan hash for /FRL/DB_B should be stable");
    }

    @Test
    void exportAndImportProduceDifferentHashes() {
        final String path = "/FRL/MY_DATABASE";
        final CopyPlan exportPlan = CopyPlan.getCopyExportAction(path,
                createContext("COPY " + path + " PRESERVE INCARNATION"), false);
        final CopyPlan importPlan = CopyPlan.getCopyImportAction(path,
                createImportContext("COPY " + path + " FROM ?"));

        // Export and import for the same path must produce different hashes
        assertNotEquals(exportPlan.getPlanHash(), importPlan.getPlanHash(),
                "EXPORT and IMPORT plans for the same path should have different hashes");
    }

    @Test
    void preserveAndIncrementIncarnationProduceDifferentStableHashes() {
        final String path = "/FRL/MY_DATABASE";
        final CopyPlan preservePlan = CopyPlan.getCopyExportAction(path,
                createContext("COPY " + path + " PRESERVE INCARNATION"), false);
        final CopyPlan incrementPlan = CopyPlan.getCopyExportAction(path,
                createContext("COPY " + path + " INCREMENT INCARNATION"), true);

        // PRESERVE and INCREMENT incarnation must produce different stable hashes
        assertEquals(879232729, preservePlan.getPlanHash(),
                "PRESERVE INCARNATION hash should be stable");
        assertEquals(879232723, incrementPlan.getPlanHash(),
                "INCREMENT INCARNATION hash should be stable");
    }
}
