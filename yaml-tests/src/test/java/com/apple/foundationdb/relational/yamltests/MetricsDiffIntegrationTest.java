/*
 * MetricsDiffIntegrationTest.java
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

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration tests for the complete metrics diff analysis workflow.
 */
class MetricsDiffIntegrationTest {

    @Test
    void testCompleteWorkflowWithStatistics() throws Exception {
        // Load test resources
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("metrics-diff-test-base.metrics.yaml"));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("metrics-diff-test-head.metrics.yaml"));
        final Path basePath = Paths.get(".");

        final var analyzer = new MetricsDiffAnalyzer("HEAD", basePath);

        final var analysisBuilder = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder(basePath);
        analyzer.compareMetrics(baseMetrics, headMetrics, Paths.get("test.metrics.yaml"), analysisBuilder);
        final var result = analysisBuilder.build();

        // Verify the analysis detected expected changes
        assertThat(result.getNewQueries()).hasSize(1); // table4 is new
        assertThat(result.getDroppedQueries()).isEmpty();
        assertThat(result.getPlanAndMetricsChanged()).isEmpty(); // no plan changes
        assertThat(result.getMetricsOnlyChanged()).hasSize(3); // table1, table2, table3 have metrics changes

        // Generate and verify report
        final var report = result.generateReport();
        assertThat(report)
                .contains("# Metrics Diff Analysis Report")
                .contains("## Summary")
                .contains("- New queries: 1")
                .contains("- Dropped queries: 0")
                .contains("- Plan changed + metrics changed: 0")
                .contains("- Plan unchanged + metrics changed: 3")
                // Verify statistical analysis
                .contains("## Only Metrics Changed")
                .contains("### Statistical Summary (Only Metrics Changed)")
                .contains("**`task_count`**:")
                .contains("Average change: 2.0") // All queries have +2 task_count
                .contains("**`transform_count`**:")
                .contains("Average change: 1.0"); // All queries have +1 transform_count

        // Test significant changes detection - should be true due to metrics-only changes
        assertThat(result.hasSignificantChanges()).isTrue();
    }

    @Test
    void testOutlierDetection() throws Exception {
        // Load test resources with outlier data
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("metrics-outlier-test-base.metrics.yaml"));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("metrics-outlier-test-head.metrics.yaml"));
        final Path basePath = Paths.get(".");

        final var analyzer = new MetricsDiffAnalyzer("HEAD", basePath);
        final var analysisBuilder = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder(basePath);

        analyzer.compareMetrics(baseMetrics, headMetrics, Paths.get("outlier-test.metrics.yaml"), analysisBuilder);
        final var result = analysisBuilder.build();

        final var report = result.generateReport();

        assertThat(report)
                // Should detect the outlier query
                .contains("### Significant Changes (Only Metrics Changed)")
                .contains("outlier_query")
                // Normal queries should have small changes (+1), outlier should have large changes (+100, +50)
                .contains("`task_count`: 100 -> 200 (+100)")
                .contains("`transform_count`: 50 -> 100 (+50)");
    }

    @Test
    void testReportFormatting() throws Exception {
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-base.metrics.yaml"));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-head.metrics.yaml"));
        final Path basePath = Paths.get(".");

        final var analyzer = new MetricsDiffAnalyzer("HEAD", basePath);
        final var analysisBuilder = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder(basePath);

        analyzer.compareMetrics(baseMetrics, headMetrics, Paths.get("format-test.metrics.yaml"), analysisBuilder);
        final var result = analysisBuilder.build();

        final var report = result.generateReport();

        assertThat(report)
                // Verify markdown formatting
                .startsWith("# Metrics Diff Analysis Report")
                .contains("## Summary")
                .contains("## New Queries")
                .contains("## Plan and Metrics Changed")
                .contains("## Only Metrics Changed")
                // Verify statistical formatting patterns
                .containsPattern("\\*\\*`\\w+`\\*\\*:")
                .containsPattern("- Average change: -?\\d+\\.\\d+")
                .containsPattern("- Average absolute change: -?\\d+\\.\\d+")
                .containsPattern("- Median change: -?\\d+")
                .containsPattern("- Median absolute change: -?\\d++")
                .containsPattern("- Standard deviation: \\d+\\.\\d+")
                .containsPattern("- Standard absolute deviation: \\d+\\.\\d+")
                .containsPattern("- Range: -?\\d+ to -?\\d+")
                .containsPattern("- Range of absolute values: -?\\d+ to -?\\d+")
                .containsPattern("- Queries affected: \\d+");
    }

    @Test
    void testEmptyResultsReporting() throws Exception {
        // Test with identical files (no changes)
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-base.metrics.yaml"));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath("test-base.metrics.yaml")); // Same file
        final Path basePath = Paths.get(".");

        final var analyzer = new MetricsDiffAnalyzer("main", basePath);
        final var analysisBuilder = MetricsDiffAnalyzer.MetricsAnalysisResult.newBuilder(basePath);

        analyzer.compareMetrics(baseMetrics, headMetrics, Paths.get("no-changes.metrics.yaml"), analysisBuilder);
        final var result = analysisBuilder.build();

        final var report = result.generateReport();

        // Should show no changes
        assertThat(report)
                .contains("- New queries: 0")
                .contains("- Dropped queries: 0")
                .contains("- Plan changed + metrics changed: 0")
                .contains("- Plan unchanged + metrics changed: 0");

        // Should not be significant
        assertThat(result.hasSignificantChanges()).isFalse();
    }

    @Nonnull
    private Path getTestResourcePath(String filename) {
        final var classLoader = Thread.currentThread().getContextClassLoader();
        final var resource = classLoader.getResource("metrics-diff/" + filename);
        assertNotNull(resource, "Test resource not found: metrics-diff/" + filename);
        return Paths.get(resource.getPath());
    }
}
