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
import java.util.List;
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

    @Nonnull
    private MetricsDiffAnalyzer.MetricsAnalysisResult analyze(@Nonnull Map<PlannerMetricsProto.Identifier, MetricsInfo> baseMetrics,
                                                              @Nonnull Map<PlannerMetricsProto.Identifier, MetricsInfo> headMetrics) {
        final var analyzer = new MetricsDiffAnalyzer("base", "head", Paths.get("."), null);
        final var analysisBuilder = analyzer.newAnalysisBuilder();
        final var filePath = Paths.get("test.metrics.yaml");

        analyzer.compareMetrics(baseMetrics, headMetrics, filePath, analysisBuilder);
        return analysisBuilder.build();
    }

    @Nonnull
    private Path getTestResourcePath(String filename) {
        final var classLoader = Thread.currentThread().getContextClassLoader();
        final var resource = classLoader.getResource("metrics-diff/" + filename);
        assertNotNull(resource, "Test resource not found: metrics-diff/" + filename);
        return Paths.get(resource.getPath());
    }


    @Test
    void testCompareMetricsDetectsChanges() throws Exception {
        // Load base and head metrics
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-base.metrics.yaml"));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-head.metrics.yaml"));
        final MetricsDiffAnalyzer.MetricsAnalysisResult analysis = analyze(baseMetrics, headMetrics);

        // Verify we detect changes
        assertThat(analysis.getNewQueries()).hasSize(1); // "SELECT email FROM users WHERE email = ?"
        assertThat(analysis.getDroppedQueries()).isEmpty();
        assertThat(analysis.getPlanAndMetricsChanged()).hasSize(2); // two queries have plan change
        assertThat(analysis.getMetricsOnlyChanged()).hasSize(1); // users query has only metrics changes

        // Verify specific changes
        final var newQuery = analysis.getNewQueries().get(0);
        assertThat(newQuery.identifier.getQuery()).contains("SELECT email FROM users WHERE email = ?");

        for (MetricsDiffAnalyzer.QueryChange planChanged : analysis.getPlanAndMetricsChanged()) {
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

        final var metricsChanged = analysis.getMetricsOnlyChanged().get(0);
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
        final MetricsDiffAnalyzer.MetricsAnalysisResult result = analyze(baseMetrics, headMetrics);

        // Generate report and verify statistical analysis
        final String report = result.generateReport();

        assertThat(report)
                .contains("Statistical Summary")
                .contains("Average change:")
                .contains("Median change:")
                .contains("Standard deviation:")
                .contains("Range:")
                .contains("Queries affected:");
    }

    @Test
    void testReportGeneration() throws Exception {
        // Load test metrics
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-base.metrics.yaml"));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-head.metrics.yaml"));
        final MetricsDiffAnalyzer.MetricsAnalysisResult analysis = analyze(baseMetrics, headMetrics);

        final String report = analysis.generateReport();

        // Verify report structure
        assertThat(report)
                .contains("# 📊 Metrics Diff Analysis Report")
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
    void testOutlierDetection() {
        // Create metrics with one clear outlier
        final var baseMetrics = createMetricsWithOutliers();
        final var headMetrics = createMetricsWithOutlierChanges();
        final MetricsDiffAnalyzer.MetricsAnalysisResult analysis = analyze(baseMetrics, headMetrics);
        final List<MetricsDiffAnalyzer.QueryChange> outliers = analysis.findAllOutliers();
        assertThat(outliers)
                .as("should have found a single outlier")
                .hasSize(1);
        final MetricsDiffAnalyzer.QueryChange expected = outliers.get(0);
        assertThat(expected.newInfo)
                .isNotNull();

        final String report = analysis.generateReport();

        // Should report the outlier in the larger report
        assertThat(report)
                .contains("- Plan unchanged + metrics changed: 11")
                .contains("Significant Changes (Only Metrics Changed)")
                .contains("outlier_query");

        // Should also be able to see the query in the outlier query report
        final String outlierReport = analysis.generateOutlierQueryReport();
        assertThat(outlierReport).isNotEmpty();
        String[] outlierText = outlierReport.split("\n\n");
        assertThat(outlierText)
                .hasSize(1);
        assertThat(outlierText[0])
                .contains(expected.identifier.getQuery())
                .contains("" + expected.newInfo.getLineNumber());
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
