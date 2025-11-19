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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link UnableToPlanException}.
 */
public class UnableToPlanExceptionTest {
    @Test
    public void testBasicExceptionCreation() {
        final UnableToPlanException exception = new UnableToPlanException("Test message");

        assertNotNull(exception);
        assertEquals("Test message", exception.getMessage());
        assertNull(exception.getMatchCandidatesInfo());
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
    }

    @Test
    public void testWithNullValue() {
        final UnableToPlanException exception = new UnableToPlanException("Test message");

        // Set to non-null value first
        exception.withMatchCandidatesInfo("match");
        assertEquals("match", exception.getMatchCandidatesInfo());

        // Now set to null
        exception.withMatchCandidatesInfo(null);
        assertNull(exception.getMatchCandidatesInfo());
    }

    @Test
    public void testExceptionWithKeyValues() {
        final UnableToPlanException exception = new UnableToPlanException(
                "Test message",
                "key1", "value1",
                "key2", "value2"
        );

        assertNotNull(exception);
        assertEquals("Test message", exception.getMessage());

        // Add diagnostic info
        exception.withMatchCandidatesInfo("candidates info");
        assertEquals("candidates info", exception.getMatchCandidatesInfo());
    }

    @Test
    public void testOverwritingValue() {
        final UnableToPlanException exception = new UnableToPlanException("Test message");

        // Set initial value
        exception.withMatchCandidatesInfo("match1");
        assertEquals("match1", exception.getMatchCandidatesInfo());

        // Overwrite with new value
        exception.withMatchCandidatesInfo("match2");
        assertEquals("match2", exception.getMatchCandidatesInfo());
    }
}
