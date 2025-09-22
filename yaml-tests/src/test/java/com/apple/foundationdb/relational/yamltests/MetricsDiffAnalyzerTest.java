/*
 * MetricsDiffAnalyzerTest.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class MetricsDiffAnalyzerTest {

    @Nonnull
    private static final PlannerMetricsProto.CountersAndTimers BASE_DUMMY_COUNTERS = PlannerMetricsProto.CountersAndTimers.newBuilder()
            .setTaskCount(10)
            .setTaskTotalTimeNs(1000000L)
            .setTransformCount(5)
            .setTransformTimeNs(500000L)
            .setTransformYieldCount(1)
            .setInsertTimeNs(100000L)
            .setInsertNewCount(2)
            .setInsertReusedCount(3)
            .build();

    @Test
    void testLoadMetricsFromYamlFile() throws Exception {
        // Load test metrics file
        final var testFile = getTestResourcePath("test-base.metrics.yaml");
        final var metrics = YamlExecutionContext.loadMetricsFromYamlFile(testFile);

        assertThat(metrics).hasSize(4); // 3 basic + 1 complex query

        // Verify one of the loaded metrics
        final var basicIdentifier = PlannerMetricsProto.Identifier.newBuilder()
                .setBlockName("basic_queries")
                .setQuery("SELECT id FROM users WHERE id = ?")
                .build();

        assertThat(metrics).containsKey(basicIdentifier);
        final var metricsInfo = metrics.get(basicIdentifier);
        assertThat(metricsInfo.getExplain()).isEqualTo("Index(users_by_id [[?, ?]])");
        assertThat(metricsInfo.getCountersAndTimers().getTaskCount()).isEqualTo(10);
        assertThat(metricsInfo.getCountersAndTimers().getTransformCount()).isEqualTo(5);
    }

    @Test
    void testCompareMetricsDetectsChanges() throws Exception {
        // Load base and head metrics
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-base.metrics.yaml"));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-head.metrics.yaml"));

        // Create analyzer and run comparison
        final var analyzer = new MetricsDiffAnalyzer("base", Paths.get("."));
        final var analysisBuilder = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder();
        final var filePath = Paths.get("test.metrics.yaml");

        analyzer.compareMetrics(baseMetrics, headMetrics, filePath, analysisBuilder);
        final var result = analysisBuilder.build();

        // Verify we detect changes
        assertThat(result.getNewQueries()).hasSize(1); // "SELECT email FROM users WHERE email = ?"
        assertThat(result.getDroppedQueries()).isEmpty();
        assertThat(result.getPlanAndMetricsChanged()).hasSize(2); // two queries have plan change
        assertThat(result.getMetricsOnlyChanged()).hasSize(1); // users query has only metrics changes

        // Verify specific changes
        final var newQuery = result.getNewQueries().get(0);
        assertThat(newQuery.identifier.getQuery()).contains("SELECT email FROM users WHERE email = ?");

        for (MetricsDiffAnalyzer.QueryChange planChanged : result.getPlanAndMetricsChanged()) {
            assertThat(planChanged.newInfo).isNotNull();
            assertThat(planChanged.oldInfo).isNotNull();
            final String query = planChanged.identifier.getQuery();
            final String newExplain = planChanged.newInfo.getExplain();
            final String oldExplain = planChanged.oldInfo.getExplain();

            if (query.contains("SELECT * FROM orders WHERE customer_id = ?")) {
                assertThat(newExplain).contains("Covering_Index");
                assertThat(oldExplain).contains("Index(orders_by_customer");
            } else if (query.contains("SELECT name FROM users WHERE name LIKE 'John%'")) {
                assertThat(newExplain).contains("Index(users_by_name [[John, John~]]) [IMPROVED]");
                assertThat(oldExplain).contains("Index(users_by_name [[John, John~]])");
            } else {
                fail("Unexpected query: " + query);
            }
        }

        final var metricsChanged = result.getMetricsOnlyChanged().get(0);
        assertThat(metricsChanged.newInfo).isNotNull();
        assertThat(metricsChanged.oldInfo).isNotNull();
        assertThat(metricsChanged.newInfo.getExplain())
                .isEqualTo(metricsChanged.oldInfo.getExplain())
                .contains("GroupBy(Join(Index(users), Index(orders_by_customer)))");
    }

    @Test
    void testStatisticalAnalysis() {
        // Create test data with known statistical properties
        final var baseMetrics = createTestMetricsWithStatistics();
        final var headMetrics = createModifiedTestMetrics();

        final var analyzer = new MetricsDiffAnalyzer("base", Paths.get("."));
        final var analysisBuilder = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder();
        final var filePath = Paths.get("test.metrics.yaml");

        analyzer.compareMetrics(baseMetrics, headMetrics, filePath, analysisBuilder);
        final var result = analysisBuilder.build();

        // Generate report and verify statistical analysis
        final var report = result.generateReport();

        assertThat(report)
                .contains("Statistical Summary")
                .contains("Average change:")
                .contains("Median change:")
                .contains("Standard deviation:")
                .contains("Range:")
                .contains("Queries affected:");
    }

    @Test
    void testOutlierDetection() {
        // Create metrics with one clear outlier
        final var baseMetrics = createMetricsWithOutliers();
        final var headMetrics = createMetricsWithOutlierChanges();

        final var analyzer = new MetricsDiffAnalyzer("base", Paths.get("."));
        final var analysisBuilder = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder();
        final var filePath = Paths.get("test.metrics.yaml");

        analyzer.compareMetrics(baseMetrics, headMetrics, filePath, analysisBuilder);
        final var result = analysisBuilder.build();

        final var report = result.generateReport();

        // Should detect the outlier
        assertThat(report)
                .contains("- Plan unchanged + metrics changed: 11")
                .contains("Significant Changes (Only Metrics Changed)")
                .contains("outlier_query");
    }

    @Test
    void testReportGeneration() throws Exception {
        // Load test metrics
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-base.metrics.yaml"));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-head.metrics.yaml"));

        final var analyzer = new MetricsDiffAnalyzer("base", Paths.get("."));
        final var analysisBuilder = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder();
        final var filePath = Paths.get("test.metrics.yaml");

        analyzer.compareMetrics(baseMetrics, headMetrics, filePath, analysisBuilder);
        final var result = analysisBuilder.build();

        final var report = result.generateReport();

        // Verify report structure
        assertThat(report)
                .contains("# Metrics Diff Analysis Report")
                .contains("## Summary")
                .contains("- New queries: 1")
                .contains("- Dropped queries: 0")
                .contains("- Plan changed + metrics changed: 2")
                .contains("- Plan unchanged + metrics changed: 1")
                .contains("## New Queries")
                .contains("## Plan and Metrics Changed")
                .contains("## Only Metrics Changed");
    }

    @Test
    void testSignificantChangesDetection() {
        // Test with dropped queries (should be significant)
        final MetricsDiffAnalyzer.QueryChange change = createDummyQueryChange();
        final var analysisWithDropped = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder()
                .addDroppedQuery(change.filePath, change.identifier, change.oldInfo)
                .build();
        assertThat(analysisWithDropped.hasSignificantChanges()).isTrue();

        // Test with metrics-only changes (should be significant)
        final var analysisWithMetricsOnly = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder()
                .addMetricsOnlyChanged(change.filePath, change.identifier, change.oldInfo, change.newInfo)
                .build();
        assertThat(analysisWithMetricsOnly.hasSignificantChanges()).isTrue();

        // Test with only new queries and plan changes (should not be significant)
        final var analysisWithoutSignificant = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder()
                .addNewQuery(change.filePath, change.identifier, change.newInfo)
                .addPlanAndMetricsChanged(change.filePath, change.identifier, change.oldInfo, change.newInfo)
                .build();
        assertThat(analysisWithoutSignificant.hasSignificantChanges()).isFalse();
    }

    @Nonnull
    private Path getTestResourcePath(String filename) {
        final var classLoader = Thread.currentThread().getContextClassLoader();
        final var resource = classLoader.getResource("metrics-diff/" + filename);
        assertNotNull(resource, "Test resource not found: metrics-diff/" + filename);
        return Paths.get(resource.getPath());
    }

    @Nonnull
    private Map<PlannerMetricsProto.Identifier, MetricsInfo> createTestMetricsWithStatistics() {
        // Create metrics with predictable statistical properties
        final var builder = ImmutableMap.<PlannerMetricsProto.Identifier, MetricsInfo>builder();

        // Add several queries with varying metrics for statistics
        for (int i = 1; i <= 5; i++) {
            final var identifier = PlannerMetricsProto.Identifier.newBuilder()
                    .setBlockName("test_block")
                    .setQuery("SELECT * FROM table" + i + " WHERE id = ?")
                    .build();

            final var countersAndTimers = PlannerMetricsProto.CountersAndTimers.newBuilder()
                    .setTaskCount(10 * i)
                    .setTaskTotalTimeNs(1000000L * i)
                    .setTransformCount(5 * i)
                    .setTransformTimeNs(500000L * i)
                    .setTransformYieldCount(i)
                    .setInsertTimeNs(100000L * i)
                    .setInsertNewCount(2 * i)
                    .setInsertReusedCount(3 * i)
                    .build();

            final var info = PlannerMetricsProto.Info.newBuilder()
                    .setExplain("Index(table" + i + "_by_id [[?, ?]])")
                    .setCountersAndTimers(countersAndTimers)
                    .build();

            builder.put(identifier, new MetricsInfo(info, Paths.get("test.yaml"), i));
        }

        return builder.build();
    }

    @Nonnull
    private Map<PlannerMetricsProto.Identifier, MetricsInfo> createModifiedTestMetrics() {
        // Create modified versions with predictable changes
        final var builder = ImmutableMap.<PlannerMetricsProto.Identifier, MetricsInfo>builder();

        for (int i = 1; i <= 5; i++) {
            final var identifier = PlannerMetricsProto.Identifier.newBuilder()
                    .setBlockName("test_block")
                    .setQuery("SELECT * FROM table" + i + " WHERE id = ?")
                    .build();

            // Add consistent changes: +2 to task_count, +1 to transform_count
            final var countersAndTimers = PlannerMetricsProto.CountersAndTimers.newBuilder()
                    .setTaskCount(10 * i + 2)
                    .setTaskTotalTimeNs(1000000L * i)
                    .setTransformCount(5 * i + 1)
                    .setTransformTimeNs(500000L * i)
                    .setTransformYieldCount(i)
                    .setInsertTimeNs(100000L * i)
                    .setInsertNewCount(2 * i)
                    .setInsertReusedCount(3 * i)
                    .build();

            final var info = PlannerMetricsProto.Info.newBuilder()
                    .setExplain("Index(table" + i + "_by_id [[?, ?]])")
                    .setCountersAndTimers(countersAndTimers)
                    .build();

            builder.put(identifier, new MetricsInfo(info, Paths.get("test.yaml"), i));
        }

        return builder.build();
    }

    @Nonnull
    private Map<PlannerMetricsProto.Identifier, MetricsInfo> createMetricsWithOutliers() {
        final var builder = ImmutableMap.<PlannerMetricsProto.Identifier, MetricsInfo>builder();

        // Add normal queries
        for (int i = 1; i <= 10; i++) {
            final var identifier = PlannerMetricsProto.Identifier.newBuilder()
                    .setBlockName("test_block")
                    .setQuery("normal_query_" + i)
                    .build();

            final var info = PlannerMetricsProto.Info.newBuilder()
                    .setExplain("Index(normal_index)")
                    .setCountersAndTimers(BASE_DUMMY_COUNTERS)
                    .build();

            builder.put(identifier, new MetricsInfo(info, Paths.get("test.yaml"), i));
        }

        // Add outlier query with much different metrics
        final var outlierIdentifier = PlannerMetricsProto.Identifier.newBuilder()
                .setBlockName("test_block")
                .setQuery("outlier_query")
                .build();

        final var outlierCounters = BASE_DUMMY_COUNTERS.toBuilder()
                .setTaskCount(BASE_DUMMY_COUNTERS.getTaskCount() * 10) // Much higher than normal
                .setTransformCount(BASE_DUMMY_COUNTERS.getTransformCount() + 100) // Much higher than normal
                .build();

        final var outlierInfo = PlannerMetricsProto.Info.newBuilder()
                .setExplain("Index(outlier_index)")
                .setCountersAndTimers(outlierCounters)
                .build();

        builder.put(outlierIdentifier, new MetricsInfo(outlierInfo, Paths.get("test.yaml"), 4));

        return builder.build();
    }

    @Nonnull
    private Map<PlannerMetricsProto.Identifier, MetricsInfo> createMetricsWithOutlierChanges() {
        final var builder = ImmutableMap.<PlannerMetricsProto.Identifier, MetricsInfo>builder();

        // Normal queries with small changes
        final PlannerMetricsProto.CountersAndTimers normalIncreaseCounters = BASE_DUMMY_COUNTERS.toBuilder()
                .setTaskCount(BASE_DUMMY_COUNTERS.getTaskCount() + 1)
                .setTransformCount(BASE_DUMMY_COUNTERS.getTransformCount() + 1)
                .build();

        for (int i = 1; i <= 10; i++) {
            final var identifier = PlannerMetricsProto.Identifier.newBuilder()
                    .setBlockName("test_block")
                    .setQuery("normal_query_" + i)
                    .build();

            final var info = PlannerMetricsProto.Info.newBuilder()
                    .setExplain("Index(normal_index)")
                    .setCountersAndTimers(normalIncreaseCounters)
                    .build();

            builder.put(identifier, new MetricsInfo(info, Paths.get("test.yaml"), i));
        }

        // Outlier query with huge changes
        final var outlierIdentifier = PlannerMetricsProto.Identifier.newBuilder()
                .setBlockName("test_block")
                .setQuery("outlier_query")
                .build();

        final var outlierCounters = BASE_DUMMY_COUNTERS.toBuilder()
                .setTaskCount(BASE_DUMMY_COUNTERS.getTaskCount() * 10 + 100) // +100 change (much larger than others)
                .setTransformCount(BASE_DUMMY_COUNTERS.getTransformCount() + 50) // -50 change (much larger than others)
                .build();

        final var outlierInfo = PlannerMetricsProto.Info.newBuilder()
                .setExplain("Index(outlier_index)")
                .setCountersAndTimers(outlierCounters)
                .build();

        builder.put(outlierIdentifier, new MetricsInfo(outlierInfo, Paths.get("test.yaml"), 4));

        return builder.build();
    }

    @Nonnull
    private MetricsDiffAnalyzer.QueryChange createDummyQueryChange() {
        final var identifier = PlannerMetricsProto.Identifier.newBuilder()
                .setBlockName("test_block")
                .setQuery("dummy_query")
                .build();
        final var info = PlannerMetricsProto.Info.newBuilder()
                .setExplain("dummy_explain")
                .setCountersAndTimers(BASE_DUMMY_COUNTERS)
                .build();

        final var metricsInfo = new MetricsInfo(info, Paths.get("test.yaml"), 1);

        return new MetricsDiffAnalyzer.QueryChange(
                Paths.get("test.metrics.yaml"),
                identifier,
                metricsInfo,
                metricsInfo
        );
    }
}
