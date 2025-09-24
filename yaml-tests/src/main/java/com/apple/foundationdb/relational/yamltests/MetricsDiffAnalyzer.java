/*
 * MetricsDiffAnalyzer.java
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

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Main analyzer for comparing metrics between different git references.
 * This tool identifies queries that have been added, removed, or changed, and provides
 * statistical analysis of metrics differences.
 */
public final class MetricsDiffAnalyzer {
    private static final Logger logger = LogManager.getLogger(MetricsDiffAnalyzer.class);

    @Nonnull
    private final String baseRef;
    @Nonnull
    private final String headRef;
    @Nonnull
    private final Path repositoryRoot;
    @Nullable
    private final String urlBase;

    /**
     * Command line arguments for the metrics diff analyzer.
     */
    public static class Arguments {
        @Parameter(names = {"--base-ref", "-b"}, description = "Git reference for baseline (e.g., 'main', commit SHA)", order = 0)
        public String baseRef = "main";

        @Parameter(names = {"--head-ref"}, description = "Git reference for new code (e.g., 'HEAD', commit SHA)", order = 0)
        public String headRef = "HEAD";

        @Parameter(names = {"--repository-root", "-r"}, description = "Path to git repository root", order = 1)
        public String repositoryRoot = ".";

        @Parameter(names = {"--output", "-o"}, description = "Output file path (writes to stdout if not specified)", order = 2)
        public String outputPath;

        @Parameter(names = {"--outlier-queries"}, description = "Output file path to specifically list data for outlier queries", order = 5)
        public String outlierQueryPath;

        @Parameter(names = {"--url-base"}, description = "Base of the URL to use for linking to queries", order = 6)
        public String urlBase;

        @Parameter(names = {"--help", "-h"}, description = "Show help message", help = true, order = 4)
        public boolean help;
    }

    public MetricsDiffAnalyzer(@Nonnull final String baseRef,
                               @Nonnull final String headRef,
                               @Nonnull final Path repositoryRoot,
                               @Nullable final String urlBase) {
        this.baseRef = baseRef;
        this.headRef = headRef;
        this.repositoryRoot = repositoryRoot;
        this.urlBase = urlBase;
    }

    /**
     * Main entry point for command-line usage.
     */
    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(String[] args) {
        final Arguments arguments = new Arguments();
        final JCommander commander = JCommander.newBuilder()
                .addObject(arguments)
                .programName("MetricsDiffAnalyzer")
                .build();

        try {
            commander.parse(args);
        } catch (ParameterException e) {
            System.err.println("Error: " + e.getMessage());
            commander.usage();
            System.exit(1);
        }

        if (arguments.help) {
            commander.usage();
            return;
        }


        try {
            final Path repositoryRoot = arguments.repositoryRoot == null ? Paths.get(".") : Paths.get(arguments.repositoryRoot);
            final String baseRef = GitMetricsFileFinder.getCommitHash(repositoryRoot, arguments.baseRef);
            final String headRef = GitMetricsFileFinder.getCommitHash(repositoryRoot, arguments.headRef);
            final Path outputPath = arguments.outputPath != null ? Paths.get(arguments.outputPath) : null;

            final var analyzer = new MetricsDiffAnalyzer(baseRef, headRef, repositoryRoot, arguments.urlBase);
            final var analysis = analyzer.analyze();
            final var report = analysis.generateReport();

            if (outputPath != null) {
                Files.writeString(outputPath, report);
                if (logger.isInfoEnabled()) {
                    logger.info(KeyValueLogMessage.of(
                            "Wrote metrics diff report",
                            "output", outputPath));
                }
            } else {
                System.out.println(report);
            }

            if (arguments.outlierQueryPath != null) {
                String outlierReport = analysis.generateOutlierQueryReport();
                if (!outlierReport.isEmpty()) {
                    final Path outlierQueryPath = Paths.get(arguments.outlierQueryPath);
                    Files.writeString(outlierQueryPath, outlierReport);
                    if (logger.isInfoEnabled()) {
                        logger.info(KeyValueLogMessage.of("Wrote outlier report",
                                "output", outlierQueryPath));
                    }
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to analyze metrics diff", e);
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Performs the metrics diff analysis.
     *
     * @return analysis results
     * @throws RelationalException if analysis fails
     */
    @Nonnull
    public MetricsAnalysisResult analyze() throws RelationalException {
        if (logger.isInfoEnabled()) {
            logger.info(KeyValueLogMessage.of("Starting metrics diff analysis",
                    "base", baseRef));
        }

        // Find all changed metrics files
        final var changedFiles = GitMetricsFileFinder.findChangedMetricsYamlFiles(baseRef, headRef, repositoryRoot);
        if (logger.isInfoEnabled()) {
            logger.info(KeyValueLogMessage.of("Found changed metrics files",
                    "base", baseRef,
                    "head", headRef,
                    "file_count", changedFiles.size()));
        }

        final var analysisBuilder = newAnalysisBuilder();

        for (final var filePath : changedFiles) {
            analyzeYamlFile(filePath, analysisBuilder);
        }

        return analysisBuilder.build();
    }

    /**
     * Analyzes a single metrics file for changes.
     */
    private void analyzeYamlFile(@Nonnull final Path yamlPath,
                                 @Nonnull final MetricsAnalysisResult.Builder analysisBuilder) throws RelationalException {
        if (logger.isDebugEnabled()) {
            logger.debug(KeyValueLogMessage.of("Analyzing YAML metrics file",
                    "path", yamlPath));
        }

        // Make sure this is a YAML metrics file
        if (!yamlPath.toString().endsWith(".metrics.yaml")) {
            throw new RelationalException("Unexpected file type for file (expected .metrics.yaml): " + yamlPath, ErrorCode.INTERNAL_ERROR);
        }

        // Load base (old) metrics
        final Map<PlannerMetricsProto.Identifier, MetricsInfo> baseMetrics = loadMetricsAtRef(yamlPath, baseRef);

        // Load head (new) metrics
        final Map<PlannerMetricsProto.Identifier, MetricsInfo> headMetrics = loadMetricsAtRef(yamlPath, headRef);

        // Compare the metrics
        compareMetrics(baseMetrics, headMetrics, yamlPath, analysisBuilder);
    }

    @Nonnull
    private Map<PlannerMetricsProto.Identifier, MetricsInfo> loadMetricsAtRef(@Nonnull Path yamlPath, @Nonnull String ref) throws RelationalException {
        try {
            final var yamlFile = GitMetricsFileFinder.getFileAtReference(
                    repositoryRoot.relativize(yamlPath).toString(), ref, repositoryRoot);
            if (yamlFile == null) {
                return ImmutableMap.of();
            }
            try {
                return YamlExecutionContext.loadMetricsFromYamlFile(yamlFile);
            } finally {
                Files.deleteIfExists(yamlFile); // Clean up temp file
            }
        } catch (IOException e) {
            throw new RelationalException("unable to delete temporary file", ErrorCode.INTERNAL_ERROR, e);
        }
    }

    /**
     * Compares metrics between base and head versions.
     */
    @VisibleForTesting
    public void compareMetrics(@Nonnull final Map<PlannerMetricsProto.Identifier, MetricsInfo> baseMetrics,
                               @Nonnull final Map<PlannerMetricsProto.Identifier, MetricsInfo> headMetrics,
                               @Nonnull final Path filePath,
                               @Nonnull final MetricsAnalysisResult.Builder analysisBuilder) {

        // Find new and updated queries
        int newCount = 0;
        int changedCount = 0;
        for (final Map.Entry<PlannerMetricsProto.Identifier, MetricsInfo> entry : headMetrics.entrySet()) {
            var identifier = entry.getKey();
            final MetricsInfo headMetricInfo = entry.getValue();
            final MetricsInfo baseMetricInfo = baseMetrics.get(identifier);
            if (baseMetricInfo == null) {
                // Query only in new metrics. Mark as new
                newCount++;
                analysisBuilder.addNewQuery(filePath, identifier, headMetricInfo);
            } else {
                // Query in both old and new. Mark as changed
                changedCount++;
                final MetricsInfo baseInfo = baseMetrics.get(identifier);
                final MetricsInfo headInfo = headMetrics.get(identifier);

                final var planChanged = !baseInfo.getExplain().equals(headInfo.getExplain());
                final var metricsChanged = MetricsInfo.areMetricsDifferent(baseInfo, headInfo);

                if (planChanged && metricsChanged) {
                    changedCount++;
                    analysisBuilder.addPlanAndMetricsChanged(filePath, identifier, baseInfo, headInfo);
                } else if (!planChanged && metricsChanged) {
                    changedCount++;
                    analysisBuilder.addMetricsOnlyChanged(filePath, identifier, baseInfo, headInfo);
                }
                // If plan changed but metrics didn't change, we don't report it. This could happen if
                // there's some cosmetic change to the plan, like one operator's serialization is changed.
                // It could also be something more interesting, but we're generally more concerned
                // with changed metrics anyway
            }
        }

        // Find dropped queries (in base but not in head)
        int droppedCount = 0;
        for (final Map.Entry<PlannerMetricsProto.Identifier, MetricsInfo> entry : baseMetrics.entrySet()) {
            var identifier = entry.getKey();
            final MetricsInfo baseMetricsInfo = entry.getValue();
            if (!headMetrics.containsKey(identifier)) {
                droppedCount++;
                analysisBuilder.addDroppedQuery(filePath, identifier, baseMetricsInfo);
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug(KeyValueLogMessage.of("Analyzed metrics file",
                    "path", filePath,
                    "new", newCount,
                    "dropped", droppedCount,
                    "changed", changedCount));
        }
    }

    @Nonnull
    public MetricsAnalysisResult.Builder newAnalysisBuilder() {
        return new MetricsAnalysisResult.Builder(baseRef, headRef, repositoryRoot, urlBase);
    }

    /**
     * Results of metrics analysis containing all detected changes.
     */
    public static class MetricsAnalysisResult {
        private final List<QueryChange> newQueries;
        private final List<QueryChange> droppedQueries;
        private final List<QueryChange> planAndMetricsChanged;
        private final List<QueryChange> metricsOnlyChanged;
        @Nonnull
        private final String baseRef;
        @Nonnull
        private final String headRef;
        @Nonnull
        private final Path repositoryRoot;
        @Nullable
        private final String urlBase;

        private MetricsAnalysisResult(@Nonnull final List<QueryChange> newQueries,
                                      @Nonnull final List<QueryChange> droppedQueries,
                                      @Nonnull final List<QueryChange> planAndMetricsChanged,
                                      @Nonnull final List<QueryChange> metricsOnlyChanged,
                                      @Nonnull final String baseRef,
                                      @Nonnull final String headRef,
                                      @Nonnull final Path repositoryRoot,
                                      @Nullable final String urlBase) {
            this.newQueries = newQueries;
            this.droppedQueries = droppedQueries;
            this.planAndMetricsChanged = planAndMetricsChanged;
            this.metricsOnlyChanged = metricsOnlyChanged;
            this.baseRef = baseRef;
            this.headRef = headRef;
            this.repositoryRoot = repositoryRoot;
            this.urlBase = urlBase;
        }

        public List<QueryChange> getNewQueries() {
            return newQueries;
        }

        public List<QueryChange> getDroppedQueries() {
            return droppedQueries;
        }

        public List<QueryChange> getPlanAndMetricsChanged() {
            return planAndMetricsChanged;
        }

        public List<QueryChange> getMetricsOnlyChanged() {
            return metricsOnlyChanged;
        }

        @Nonnull
        private Path relativePath(@Nonnull Path path) {
            return repositoryRoot.relativize(path);
        }

        @Nonnull
        private String formatQueryDisplay(@Nonnull final QueryChange change) {
            return formatQueryDisplay(change, true);
        }

        /**
         * Helper method to format a query change for display with relative path and line number.
         */
        @Nonnull
        private String formatQueryDisplay(@Nonnull final QueryChange change, boolean asLink) {
            int lineNumber = -1;
            String ref = null;
            if (change.newInfo != null) {
                lineNumber = change.newInfo.getLineNumber();
                ref = headRef;
            } else if (change.oldInfo != null) {
                lineNumber = change.oldInfo.getLineNumber();
                ref = baseRef;
            }
            final String lineInfo = lineNumber > 0 ? (":" + lineNumber) : "";
            String locationText = relativePath(change.filePath) + lineInfo;
            if (asLink && urlBase != null) {
                locationText = "[" + locationText + "](" + urlBase + "/" + ref + "/" + relativePath(change.filePath);
                if (lineNumber > 0) {
                    locationText += "#L" + lineNumber;
                }
                locationText += ")";
            }
            return locationText + ": `" + change.identifier.getQuery() + "`";
        }

        /**
         * Generate a markdown formatted report summarizing metrics differences. It contains a few
         * different sections, including information on the set of queries that have been added and
         * dropped, as well as modifications. This is intended to be presented as a single document
         * for a reviewer to use to aid in assessing the scope of planner metrics changes.
         *
         * @return the contents of a report comparing query metrics
         */
        @Nonnull
        public String generateReport() {
            final var report = new StringBuilder();
            report.append("# 📊 Metrics Diff Analysis Report\n\n");

            report.append("## Summary\n\n")
                    .append("- New queries: ").append(newQueries.size()).append("\n")
                    .append("- Dropped queries: ").append(droppedQueries.size()).append("\n")
                    .append("- Plan changed + metrics changed: ").append(planAndMetricsChanged.size()).append("\n")
                    .append("- Plan unchanged + metrics changed: ").append(metricsOnlyChanged.size()).append("\n")
                    .append("\n");

            report.append("<details>\n\n")
                    .append("<summary>ℹ️ About this analysis</summary>\n\n")
                    .append("This automated analysis compares query planner metrics between the base branch and this PR. It categorizes changes into:\n\n")
                    .append(" - **New queries**: Queries added in this PR\n")
                    .append(" - **Dropped queries**: Queries removed in this PR. These should be reviewed to ensure we are not losing coverage.\n")
                    .append(" - **Plan changed + metrics changed**: The query plan has changed along with planner metrics.\n")
                    .append(" - **Metrics only changed**: Same plan but different metrics\n\n")
                    .append("The last category in particular may indicate planner regressions that should be investigated.\n\n")
                    .append("</details>\n\n");


            if (!newQueries.isEmpty()) {
                // Only list a count of new queries by file type. Having more test cases is generally a good thing,
                // so we only want to get a rough overview
                report.append("## New Queries\n\nCount of new queries by file:\n\n");
                Map<Path, Integer> countByFile = new TreeMap<>(); // tree map so that we sort by file name
                for (final var change : newQueries) {
                    countByFile.compute(change.filePath, (ignore, oldValue) -> oldValue == null ? 1 : oldValue + 1);
                }
                for (Map.Entry<Path, Integer> countEntry : countByFile.entrySet()) {
                    report.append("- ")
                            .append(relativePath(countEntry.getKey()))
                            .append(": ")
                            .append(countEntry.getValue())
                            .append("\n");
                }
                report.append("\n");
            }

            if (!droppedQueries.isEmpty()) {
                // List out each individual dropped query. These represent a potential lack of coverage, and
                // so we want these listed out more explicitly
                report.append("## Dropped Queries\n\nThe following queries with metrics were removed:\n\n");
                final int maxQueries = 20;
                final List<QueryChange> changes = droppedQueries.size() < maxQueries ? droppedQueries : droppedQueries.subList(0, maxQueries);
                for (final var change : changes) {
                    report.append("- ").append(formatQueryDisplay(change)).append("\n");
                }
                report.append("\n");

                if (droppedQueries.size() > maxQueries) {
                    int remaining = droppedQueries.size() - maxQueries;
                    report.append("This list has been truncated. There ")
                            .append(remaining == 1 ? "is " : "are ")
                            .append(remaining)
                            .append(" remaining dropped queries.\n\n");
                }

                report.append("The reviewer should double check that these queries were removed intentionally to avoid a loss of coverage.\n\n");
            }

            appendChangesList(report, planAndMetricsChanged, "Plan and Metrics Changed",
                    "These queries experienced both plan and metrics changes. This generally indicates that there was some planner change\n"
                            + "that means the planning for this query may be substantially different. Some amount of query plan metrics change is expected,\n"
                            + "but the reviewer should still validate that these changes are not excessive.\n");
            appendChangesList(report, metricsOnlyChanged, "Only Metrics Changed",
                    "These queries experienced *only* metrics changes without any plan changes. If these metrics have substantially changed,\n"
                            + "then a planner change has been made which affects planner performance but does not correlate with any new outcomes,\n"
                            + "which could indicate a regression.\n");

            return report.toString();
        }

        /**
         * Print out a simple list of outlier queries. This format is kept simple so that
         * it can be processed by tools to generate in-line comments. Each query is separated
         * by a blank line. The first line contains the file and line number as well as the query
         * text, and then the metrics differences follow. This is intended to be used to generate
         * a list of in-line comments to highlight each of these queries.
         *
         * @return a report of outlier queries
         */
        @Nonnull
        public String generateOutlierQueryReport() {
            final List<QueryChange> outliers = findAllOutliers();
            if (outliers.isEmpty()) {
                return "";
            }

            final StringBuilder report = new StringBuilder();
            for (QueryChange queryChange : outliers) {
                // One query per line. Do not use format as a link
                report.append(formatQueryDisplay(queryChange, false)).append("\n");
                appendMetricsDiff(report, queryChange);
                report.append("\n");
            }
            return report.toString();
        }

        @Nonnull
        private String sign(double d) {
            // For adding an explicit plus to positive values. Rely on usual toString of negative values
            // to add the minus sign
            return d > 0 ? "+" : "";
        }

        @Nonnull
        private String sign(long l) {
            // See: sign(double)
            return l > 0 ? "+" : "";
        }

        private void appendStatisticalSummary(@Nonnull final StringBuilder report, @Nonnull final MetricsStatistics stats) {
            for (final var fieldName : YamlExecutionContext.TRACKED_METRIC_FIELDS) {
                final var fieldStats = stats.getFieldStatistics(fieldName);
                final var regressionFieldStats = stats.getRegressionStatistics(fieldName);
                if (fieldStats.hasChanges() || regressionFieldStats.hasChanges()) {
                    report.append(String.format("**`%s`**:%n", fieldName));
                    report.append(String.format("  - Average change: %s%.1f%n", sign(fieldStats.getMean()), fieldStats.getMean()));
                    if (regressionFieldStats.hasChanges()) {
                        report.append(String.format("  - Average regression: %s%.1f%n", sign(regressionFieldStats.getMean()), regressionFieldStats.getMean()));
                    }
                    report.append(String.format("  - Median change: %s%d%n", sign(fieldStats.getMedian()), fieldStats.getMedian()));
                    if (regressionFieldStats.hasChanges()) {
                        report.append(String.format("  - Median regression: %s%d%n", sign(regressionFieldStats.getMedian()), regressionFieldStats.getMedian()));
                    }
                    report.append(String.format("  - Standard deviation: %.1f%n", fieldStats.getStandardDeviation()));
                    if (regressionFieldStats.hasChanges()) {
                        report.append(String.format("  - Standard deviation of regressions: %.1f%n", regressionFieldStats.getStandardDeviation()));
                    }
                    report.append(String.format("  - Range: %s%d to %s%d%n", sign(fieldStats.getMin()), fieldStats.getMin(), sign(fieldStats.getMax()), fieldStats.getMax()));
                    if (regressionFieldStats.hasChanges()) {
                        report.append(String.format("  - Range of regressions: %s%d to %s%d%n", sign(regressionFieldStats.getMin()), regressionFieldStats.getMin(), sign(regressionFieldStats.getMax()), regressionFieldStats.getMax()));
                    }
                    report.append(String.format("  - Queries changed: %d%n", fieldStats.getChangedCount()));
                    if (regressionFieldStats.hasChanges()) {
                        report.append(String.format("  - Queries regressed: %d%n", regressionFieldStats.getChangedCount()));
                    } else {
                        report.append("  - No regressions! 🎉\n");
                    }
                    report.append("\n"); // End with blank line
                }
            }
        }

        private void appendChangesList(@Nonnull final StringBuilder report, @Nonnull List<QueryChange> changes, @Nonnull String title, @Nonnull String explanation) {
            if (changes.isEmpty()) {
                return;
            }
            report.append("## ").append(title).append("\n\n");
            report.append(explanation).append("\n");
            report.append("Total: ").append(changes.size()).append(" quer").append(changes.size() == 1 ? "y" : "ies").append("\n\n");

            // Statistical analysis
            final var summary = calculateMetricsStatistics(changes);
            report.append("### Statistical Summary (").append(title).append(")\n\n");
            appendStatisticalSummary(report, summary);

            // Show outliers for metrics-only changes (these are more concerning)
            final var metricsOutliers = findOutliers(changes, summary);
            if (metricsOutliers.isEmpty()) {
                report.append("There were no queries with significant regressions detected.\n\n");
            } else {
                report.append("### Significant Regressions (").append(title).append(")\n\n");
                report.append("There ")
                        .append(metricsOutliers.size() == 1 ? "was " : "were ")
                        .append(metricsOutliers.size())
                        .append(" outlier").append(metricsOutliers.size() == 1 ? "" : "s")
                        .append(" detected. Outlier queries have a significant regression in at least one field. Statistically, "
                                + "this represents either an increase of more than two standard deviations above "
                                + "the mean or a large absolute increase (e.g., 100).\n\n");

                for (final var change : metricsOutliers) {
                    report.append(String.format("- %s", formatQueryDisplay(change))).append("\n");
                    if (change.oldInfo != null && change.newInfo != null) {
                        final String oldExplain = change.oldInfo.getExplain();
                        final String newExplain = change.newInfo.getExplain();
                        if (oldExplain.equals(newExplain)) {
                            report.append("  - explain: `").append(oldExplain).append("`\n");
                        } else {
                            report.append("  - old explain: `").append(oldExplain).append("`\n");
                            report.append("  - new explain: `").append(newExplain).append("`\n");
                        }
                        appendMetricsDiff(report, change);
                    }
                }
                report.append("\n");
            }

            if (changes.size() > metricsOutliers.size()) {
                int minorChanges = changes.size() - metricsOutliers.size();
                report.append("### Minor Changes (").append(title).append(")\n\nIn addition, there ")
                        .append(minorChanges == 1 ? "was " : "were ")
                        .append(minorChanges)
                        .append(" quer").append(minorChanges == 1 ? "y" : "ies")
                        .append(" with minor changes.\n\n");
            }
        }

        private MetricsStatistics calculateMetricsStatistics(@Nonnull final List<QueryChange> changes) {
            final var statisticsBuilder = new MetricsStatistics.Builder();

            for (final var change : changes) {
                if (change.oldInfo != null && change.newInfo != null) {
                    final var oldMetrics = change.oldInfo.getCountersAndTimers();
                    final var newMetrics = change.newInfo.getCountersAndTimers();
                    final var descriptor = oldMetrics.getDescriptorForType();

                    for (final var fieldName : YamlExecutionContext.TRACKED_METRIC_FIELDS) {
                        final var field = descriptor.findFieldByName(fieldName);
                        final var oldValue = (long)oldMetrics.getField(field);
                        final var newValue = (long)newMetrics.getField(field);

                        if (oldValue != newValue) {
                            statisticsBuilder.addDifference(fieldName, oldValue, newValue);
                        }
                    }
                }
            }

            return statisticsBuilder.build();
        }

        @Nonnull
        public List<QueryChange> findAllOutliers() {
            return ImmutableList.<QueryChange>builder()
                    .addAll(findOutliers(planAndMetricsChanged))
                    .addAll(findOutliers(metricsOnlyChanged))
                    .build();
        }

        @Nonnull
        private List<QueryChange> findOutliers(@Nonnull final List<QueryChange> changes) {
            final MetricsStatistics summary = calculateMetricsStatistics(changes);
            return findOutliers(changes, summary);
        }

        @Nonnull
        private List<QueryChange> findOutliers(@Nonnull final List<QueryChange> changes, @Nonnull final MetricsStatistics stats) {
            if (changes.size() < 3) {
                // Not enough data for meaningful outlier detection
                return changes;
            }

            final ImmutableList.Builder<QueryChange> outliers = ImmutableList.builder();

            for (final var change : changes) {
                if (isOutlier(change, stats)) {
                    outliers.add(change);
                }
            }

            return outliers.build();
        }

        private boolean isOutlier(@Nonnull QueryChange change, @Nonnull MetricsStatistics stats) {
            if (change.oldInfo == null || change.newInfo == null) {
                return false;
            }
            final var oldMetrics = change.oldInfo.getCountersAndTimers();
            final var newMetrics = change.newInfo.getCountersAndTimers();
            final var descriptor = oldMetrics.getDescriptorForType();

            for (final var fieldName : YamlExecutionContext.TRACKED_METRIC_FIELDS) {
                final var field = descriptor.findFieldByName(fieldName);
                final var oldValue = (long)oldMetrics.getField(field);
                final var newValue = (long)newMetrics.getField(field);

                if (oldValue != newValue) {
                    final var difference = newValue - oldValue;
                    if (difference > 0 && isOutlierValue(stats.getRegressionStatistics(fieldName), difference)) {
                        return true;
                    }
                }
            }

            return false;
        }

        private boolean isOutlierValue(MetricsStatistics.FieldStatistics fieldStats, long difference) {
            // Consider it an outlier if it's more than 2 standard deviations from the mean
            // or if it's a very large absolute change
            Assert.thatUnchecked(fieldStats.hasChanges(), "Field stats should have at least one difference");
            final var zScore = Math.abs((difference - fieldStats.getMean()) / fieldStats.getStandardDeviation());
            final var isLargeAbsoluteChange = Math.abs(difference) > Math.max(100, Math.abs(fieldStats.getMean()) * 2);
            return zScore > 2.0 || isLargeAbsoluteChange;
        }

        private void appendMetricsDiff(@Nonnull final StringBuilder report,
                                       @Nonnull final QueryChange queryChange) {
            Assert.thatUnchecked(queryChange.oldInfo != null, "old info must be set to display metrics diff");
            Assert.thatUnchecked(queryChange.newInfo != null, "new info must be set to display metrics diff");

            final PlannerMetricsProto.CountersAndTimers oldMetrics = queryChange.oldInfo.getCountersAndTimers();
            final PlannerMetricsProto.CountersAndTimers newMetrics = queryChange.newInfo.getCountersAndTimers();

            final var descriptor = oldMetrics.getDescriptorForType();

            for (final var fieldName : YamlExecutionContext.TRACKED_METRIC_FIELDS) {
                final Descriptors.FieldDescriptor field = descriptor.findFieldByName(fieldName);
                final long oldValue = (long)oldMetrics.getField(field);
                final long newValue = (long)newMetrics.getField(field);
                if (oldValue != newValue) {
                    final long change = newValue - oldValue;
                    report.append("  - `").append(fieldName).append("`: ")
                            .append(oldValue).append(" -> ").append(newValue)
                            .append(" (").append(sign(change)).append(change).append(")\n");
                }
            }
        }

        public static class Builder {
            private final ImmutableList.Builder<QueryChange> newQueries = ImmutableList.builder();
            private final ImmutableList.Builder<QueryChange> droppedQueries = ImmutableList.builder();
            private final ImmutableList.Builder<QueryChange> planAndMetricsChanged = ImmutableList.builder();
            private final ImmutableList.Builder<QueryChange> metricsOnlyChanged = ImmutableList.builder();
            @Nonnull
            private final String baseRef;
            @Nonnull
            private final String headRef;
            @Nonnull
            private final Path repositoryRoot;
            @Nullable
            private final String urlBase;

            public Builder(@Nonnull String baseRef, @Nonnull String headRef, @Nonnull final Path repositoryRoot, @Nullable final String urlBase) {
                this.baseRef = baseRef;
                this.headRef = headRef;
                this.repositoryRoot = repositoryRoot;
                this.urlBase = urlBase;
            }

            @Nonnull
            public Builder addNewQuery(@Nonnull final Path filePath,
                                       @Nonnull final PlannerMetricsProto.Identifier identifier,
                                       @Nonnull final MetricsInfo info) {
                newQueries.add(new QueryChange(filePath, identifier, null, info));
                return this;
            }

            @Nonnull
            public Builder addDroppedQuery(@Nonnull final Path filePath,
                                           @Nonnull final PlannerMetricsProto.Identifier identifier,
                                           @Nonnull final MetricsInfo info) {
                droppedQueries.add(new QueryChange(filePath, identifier, info, null));
                return this;
            }

            @Nonnull
            public Builder addPlanAndMetricsChanged(@Nonnull final Path filePath,
                                                    @Nonnull final PlannerMetricsProto.Identifier identifier,
                                                    @Nonnull final MetricsInfo oldInfo,
                                                    @Nonnull final MetricsInfo newInfo) {
                planAndMetricsChanged.add(new QueryChange(filePath, identifier, oldInfo, newInfo));
                return this;
            }

            @Nonnull
            public Builder addMetricsOnlyChanged(@Nonnull final Path filePath,
                                                 @Nonnull final PlannerMetricsProto.Identifier identifier,
                                                 @Nonnull final MetricsInfo oldInfo,
                                                 @Nonnull final MetricsInfo newInfo) {
                metricsOnlyChanged.add(new QueryChange(filePath, identifier, oldInfo, newInfo));
                return this;
            }

            @Nonnull
            public MetricsAnalysisResult build() {
                return new MetricsAnalysisResult(
                        newQueries.build(),
                        droppedQueries.build(),
                        planAndMetricsChanged.build(),
                        metricsOnlyChanged.build(),
                        baseRef,
                        headRef,
                        repositoryRoot,
                        urlBase
                );
            }
        }
    }

    /**
     * Represents a change to a query's metrics.
     */
    public static class QueryChange {
        public final Path filePath;
        public final PlannerMetricsProto.Identifier identifier;
        @Nullable
        public final MetricsInfo oldInfo;
        @Nullable
        public final MetricsInfo newInfo;

        public QueryChange(@Nonnull final Path filePath,
                           @Nonnull final PlannerMetricsProto.Identifier identifier,
                           @Nullable final MetricsInfo oldInfo,
                           @Nullable final MetricsInfo newInfo) {
            this.filePath = filePath;
            this.identifier = identifier;
            this.oldInfo = oldInfo;
            this.newInfo = newInfo;
        }
    }
}
