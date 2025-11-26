/*
 * PlanGeneratorCacheLoggingTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.recordlayer.query.cache.PhysicalPlanEquivalence;
import com.apple.foundationdb.relational.recordlayer.query.cache.QueryCacheKey;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PlanGenerator#logPlanCacheOnFailure(RelationalPlanCache, org.apache.logging.log4j.Logger)}.
 */
public class PlanGeneratorCacheLoggingTest {

    private TestAppender testAppender;
    private org.apache.logging.log4j.core.Logger logger;

    @BeforeEach
    public void setup() {
        testAppender = new TestAppender("TestAppender");
        testAppender.start();
        logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(PlanGenerator.class);
        logger.addAppender(testAppender);
        logger.setLevel(Level.ERROR);
    }

    @AfterEach
    public void tearDown() {
        logger.removeAppender(testAppender);
        testAppender.stop();
    }

    @Test
    public void testColdCacheScenario() {
        // Create a cache with no entries (cold cache)
        final RelationalPlanCache cache = RelationalPlanCache.buildWithDefaults();

        // Log the cache state
        PlanGenerator.logPlanCacheOnFailure(cache, logger);

        // Should log even with empty cache
        assertFalse(testAppender.getEvents().isEmpty(),
                "Should log even when cache is empty");

        final LogEvent event = testAppender.getEvents().get(0);
        final String message = event.getMessage().getFormattedMessage();

        // Verify it contains cache stats showing empty cache
        assertTrue(message.contains("Plan cache state when unable to plan"),
                "Message should indicate plan cache state");
        assertTrue(message.contains("size=0") || message.contains("planCachePrimarySize"),
                "Should show cache size of 0 for cold cache");
    }

    @Test
    public void testWarmCacheScenario() {
        // Create a cache and populate it with some entries
        final RelationalPlanCache cache = RelationalPlanCache.buildWithDefaults();

        // Add a mock plan entry to warm up the cache
        final String schemaName = "testSchema";
        final var queryCacheKey = QueryCacheKey.of(
                "SELECT * FROM test",
                PlannerConfiguration.ofAllAvailableIndexes(),
                "testDbPath",
                123456,
                1,
                0);
        final var planEquivalence = PhysicalPlanEquivalence.of(EvaluationContext.empty());

        // Create a simple mock plan
        final Plan<String> mockPlan = new Plan<String>("SELECT * FROM test") {
            @Override
            public boolean isUpdatePlan() {
                return false;
            }

            @Override
            public Plan<String> optimize(@Nonnull CascadesPlanner planner, @Nonnull PlanContext planContext,
                                         @Nonnull com.apple.foundationdb.record.PlanHashable.PlanHashMode currentPlanHashMode) {
                return this;
            }

            @Override
            protected String executeInternal(@Nonnull ExecutionContext c) {
                return "mock result";
            }

            @Override
            @Nonnull
            public QueryPlanConstraint getConstraint() {
                return QueryPlanConstraint.noConstraint();
            }

            @Override
            @Nonnull
            public Plan<String> withExecutionContext(@Nonnull QueryExecutionContext queryExecutionContext) {
                return this;
            }

            @Override
            @Nonnull
            public String explain() {
                return "Mock Plan";
            }
        };

        // Populate cache
        cache.reduce(
                schemaName,
                queryCacheKey,
                planEquivalence,
                () -> {
                    // Return plan with constraint
                    return NonnullPair.of(PhysicalPlanEquivalence.of(QueryPlanConstraint.noConstraint()), mockPlan);
                },
                plan -> plan,
                stream -> stream.findFirst().orElse(null),
                event -> { }
        );

        // Log the cache state
        PlanGenerator.logPlanCacheOnFailure(cache, logger);

        // Should log with cache content
        assertFalse(testAppender.getEvents().isEmpty(),
                "Should log when cache has entries");

        final LogEvent event = testAppender.getEvents().get(0);
        final String message = event.getMessage().getFormattedMessage();

        // Verify it contains cache information
        assertTrue(message.contains("Plan cache state when unable to plan"),
                "Message should indicate plan cache state");
        assertTrue(message.contains(schemaName),
                "Should show schema name from cache");
        assertTrue(message.contains("entries") || message.contains("planCachePrimarySize"),
                "Should show cache has entries");
    }

    /**
     * Custom Log4j2 appender for capturing log events in tests.
     */
    private static class TestAppender extends AbstractAppender {
        private final List<LogEvent> events = new ArrayList<>();

        protected TestAppender(String name) {
            super(name, null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }

        public List<LogEvent> getEvents() {
            return events;
        }
    }
}
