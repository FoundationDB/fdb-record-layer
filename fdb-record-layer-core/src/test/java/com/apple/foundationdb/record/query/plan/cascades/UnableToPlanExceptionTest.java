/*
 * UnableToPlanExceptionTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link UnableToPlanException}.
 */
public class UnableToPlanExceptionTest {
    @Test
    public void testWithPlanCacheInfo() {
        final UnableToPlanException exception = new UnableToPlanException("Test message");
        final String cacheInfo = "Plan Cache Tertiary Layer:\n  [key1][key2]: 5 entries\n    - entry1\n    - entry2";

        final UnableToPlanException result = exception.withPlanCacheInfo(cacheInfo);

        // Verify it returns the same instance (fluent API)
        assertSame(exception, result);

        // Verify the cache info was set
        assertEquals(cacheInfo, exception.getPlanCacheInfo());

        // Verify other fields are still null
        assertNull(exception.getMatchCandidatesInfo());
        assertNull(exception.getConnectionOptionsInfo());
    }

    @Test
    public void testWithMatchCandidatesInfo() {
        final UnableToPlanException exception = new UnableToPlanException("Test message");
        final String matchCandidatesInfo = "Match Candidates:\n  - candidate1\n  - candidate2";

        final UnableToPlanException result = exception.withMatchCandidatesInfo(matchCandidatesInfo);

        // Verify it returns the same instance (fluent API)
        assertSame(exception, result);

        // Verify the match candidates info was set
        assertEquals(matchCandidatesInfo, exception.getMatchCandidatesInfo());

        // Verify other fields are still null
        assertNull(exception.getPlanCacheInfo());
        assertNull(exception.getConnectionOptionsInfo());
    }

    @Test
    public void testWithConnectionOptionsInfo() {
        final UnableToPlanException exception = new UnableToPlanException("Test message");
        final String optionsInfo = "Connection Options:\n  MAX_ROWS = 100\n  TRANSACTION_TIMEOUT = 5000";

        final UnableToPlanException result = exception.withConnectionOptionsInfo(optionsInfo);

        // Verify it returns the same instance (fluent API)
        assertSame(exception, result);

        // Verify the options info was set
        assertEquals(optionsInfo, exception.getConnectionOptionsInfo());

        // Verify other fields are still null
        assertNull(exception.getPlanCacheInfo());
        assertNull(exception.getMatchCandidatesInfo());
    }

    @Test
    public void testWithNullValues() {
        final UnableToPlanException exception = new UnableToPlanException("Test message");

        // Set to non-null values first
        exception.withPlanCacheInfo("cache");
        exception.withMatchCandidatesInfo("match");
        exception.withConnectionOptionsInfo("options");

        // Now set to null
        exception.withPlanCacheInfo(null);
        exception.withMatchCandidatesInfo(null);
        exception.withConnectionOptionsInfo(null);

        // Verify all are null
        assertNull(exception.getPlanCacheInfo());
        assertNull(exception.getMatchCandidatesInfo());
        assertNull(exception.getConnectionOptionsInfo());
    }
}
