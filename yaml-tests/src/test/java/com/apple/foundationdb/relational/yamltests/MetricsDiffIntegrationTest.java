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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
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
        final MetricsDiffAnalyzer.MetricsAnalysisResult analysis = analyze(
                "metrics-diff-test-base.metrics.yaml",
                "metrics-diff-test-head.metrics.yaml");

        // Verify the analysis detected expected changes
        assertThat(analysis.getNewQueries()).hasSize(1); // table4 is new
        assertThat(analysis.getDroppedQueries()).isEmpty();
        assertThat(analysis.getPlanAndMetricsChanged()).isEmpty(); // no plan changes
        assertThat(analysis.getMetricsOnlyChanged()).hasSize(3); // table1, table2, table3 have metrics changes

        // Generate and verify report
        final var report = analysis.generateReport();
        assertThat(report)
                .contains("# 📊 Metrics Diff Analysis Report")
                .contains("## Summary")
                .contains("- New queries: 1")
                .contains("test.metrics.yaml: 1")
                .contains("- Dropped queries: 0")
                .contains("- Plan changed + metrics changed: 0")
                .contains("- Plan unchanged + metrics changed: 3")
                // Verify statistical analysis
                .contains("## Only Metrics Changed")
                .contains("### Statistical Summary (Only Metrics Changed)")
                .contains("**`task_count`**:")
                .contains("Average change: +2.0") // All queries have +2 task_count
                .contains("**`transform_count`**:")
                .contains("Average change: +1.0"); // All queries have +1 transform_count
    }

    @Test
    void testCompleteWorkflowBackwards() throws Exception {
        final MetricsDiffAnalyzer.MetricsAnalysisResult analysis = analyze(
                "metrics-diff-test-head.metrics.yaml",
                "metrics-diff-test-base.metrics.yaml");

        // Verify the analysis detected expected changes
        assertThat(analysis.getNewQueries()).isEmpty();
        assertThat(analysis.getDroppedQueries()).hasSize(1);
        assertThat(analysis.getPlanAndMetricsChanged()).isEmpty(); // no plan changes
        assertThat(analysis.getMetricsOnlyChanged()).hasSize(3); // table1, table2, table3 have metrics changes

        // Generate and verify report
        final var report = analysis.generateReport();
        assertThat(report)
                .contains("# 📊 Metrics Diff Analysis Report")
                .contains("## Summary")
                .contains("- New queries: 0")
                .contains("- Dropped queries: 1")
                .contains("[test.metrics.yaml:35](https://example.com/repo/blob/base/test.metrics.yaml#L35): `SELECT * FROM table4`")
                .contains("- Plan changed + metrics changed: 0")
                .contains("- Plan unchanged + metrics changed: 3")
                // Verify statistical analysis
                .contains("## Only Metrics Changed")
                .contains("### Statistical Summary (Only Metrics Changed)")
                .contains("**`task_count`**:")
                .contains("Average change: -2.0") // All queries have -2 task_count
                .contains("**`transform_count`**:")
                .contains("Average change: -1.0"); // All queries have -1 transform_count
    }

    @Test
    void testOutlierDetection() throws Exception {
        // Load test files with outlier case
        final MetricsDiffAnalyzer.MetricsAnalysisResult analysis = analyze(
                "metrics-outlier-test-base.metrics.yaml",
                "metrics-outlier-test-head.metrics.yaml");

        final var report = analysis.generateReport();

        assertThat(report)
                // Should detect the outlier query
                .contains("### Significant Changes (Only Metrics Changed)")
                .contains("outlier_query")
                .contains("[test.metrics.yaml:145](https://example.com/repo/blob/head/test.metrics.yaml#L145): `outlier_query`")
                // Normal queries should have small changes (+1), outlier should have large changes (+100, +50)
                .contains("`task_count`: 100 -> 200 (+100)")
                .contains("`transform_count`: 50 -> 100 (+50)");
    }

    @Test
    void testReportFormatting() throws Exception {
        final MetricsDiffAnalyzer.MetricsAnalysisResult analysis = analyze(
                "test-base.metrics.yaml",
                "test-head.metrics.yaml");

        final var report = analysis.generateReport();

        assertThat(report)
                // Verify markdown formatting
                .contains("# 📊 Metrics Diff Analysis Report")
                .contains("## Summary")
                .contains("## New Queries")
                .contains("## Plan and Metrics Changed")
                .contains("## Only Metrics Changed")
                // Verify statistical formatting patterns
                .containsPattern("\\*\\*`\\w+`\\*\\*:")
                .containsPattern("- Average change: [+-]?\\d+\\.\\d+")
                .containsPattern("- Average absolute change: \\+?\\d+\\.\\d+")
                .containsPattern("- Median change: [+-]?\\d+")
                .containsPattern("- Median absolute change: \\+?\\d++")
                .containsPattern("- Standard deviation: \\d+\\.\\d+")
                .containsPattern("- Standard absolute deviation: \\d+\\.\\d+")
                .containsPattern("- Range: [+-]?\\d+ to [+-]?\\d+")
                .containsPattern("- Range of absolute values: \\+?\\d+ to \\+?\\d+")
                .containsPattern("- Queries affected: \\d+");
    }

    @Test
    void testEmptyResultsReporting() throws Exception {
        // Test with identical files (no changes)
        final MetricsDiffAnalyzer.MetricsAnalysisResult analysis = analyze("test-base.metrics.yaml", "test-base.metrics.yaml");
        final var report = analysis.generateReport();

        // Should show no changes
        assertThat(report)
                .contains("- New queries: 0")
                .contains("- Dropped queries: 0")
                .contains("- Plan changed + metrics changed: 0")
                .contains("- Plan unchanged + metrics changed: 0");
    }

    @Nonnull
    private static MetricsDiffAnalyzer.MetricsAnalysisResult analyze(@Nonnull String baseResource, @Nonnull String headResource) throws RelationalException {
        // Load test resources
        final var baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath(baseResource));
        final var headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(
                getTestResourcePath(headResource));
        final Path basePath = Paths.get(".");

        final MetricsDiffAnalyzer analyzer = new MetricsDiffAnalyzer("base", "head", basePath, "https://example.com/repo/blob");
        final MetricsDiffAnalyzer.MetricsAnalysisResult.Builder analysisBuilder = analyzer.newAnalysisBuilder();
        analyzer.compareMetrics(baseMetrics, headMetrics, Paths.get("test.metrics.yaml"), analysisBuilder);
        return analysisBuilder.build();
    }

    @Nonnull
    private static Path getTestResourcePath(String filename) {
        final var classLoader = Thread.currentThread().getContextClassLoader();
        final var resource = classLoader.getResource("metrics-diff/" + filename);
        assertNotNull(resource, "Test resource not found: metrics-diff/" + filename);
        return Paths.get(resource.getPath());
    }
}
