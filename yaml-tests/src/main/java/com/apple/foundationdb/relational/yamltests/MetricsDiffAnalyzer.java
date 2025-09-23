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

    /**
     * Command line arguments for the metrics diff analyzer.
     */
    public static class Arguments {
        @Parameter(names = {"--base-ref", "-b"}, description = "Git reference for baseline (e.g., 'main', commit SHA)", order = 0)
        public String baseRef = "main";

        @Parameter(names = {"--repository-root", "-r"}, description = "Path to git repository root", order = 1)
        public String repositoryRoot = ".";

        @Parameter(names = {"--output", "-o"}, description = "Output file path (writes to stdout if not specified)", order = 2)
        public String outputPath;

        @Parameter(names = {"--outlier-queries"}, description = "Output file path to specifically list data for outlier queries", order = 3)
        public String outlierQueryPath;

        @Parameter(names = {"--url-base"}, description = "Base of the URL to use for linking to queries", order = 4)
        public String urlBase;

        @Parameter(names = {"--help", "-h"}, description = "Show help message", help = true, order = 4)
        public boolean help;
    }

    @Nonnull
    private final String baseRef;
    @Nonnull
    private final String headRef;
    @Nullable
    private final Path repositoryRoot;
    @Nullable
    private final String urlBase;

    public MetricsDiffAnalyzer(@Nonnull final String baseRef,
                               @Nonnull final String headRef,
                               @Nullable final Path repositoryRoot,
                               @Nullable final String urlBase) {
        this.baseRef = baseRef;
        this.headRef = headRef;
        this.repositoryRoot = repositoryRoot;
        this.urlBase = urlBase;
    }

    /**
     * Main entry point for command-line usage.
     */
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

        final String baseRef = arguments.baseRef;
        final Path repositoryRoot = Paths.get(arguments.repositoryRoot);
        final Path outputPath = arguments.outputPath != null ? Paths.get(arguments.outputPath) : null;

        try {
            final var analyzer = new MetricsDiffAnalyzer(baseRef, GitMetricsFileFinder.getHeadReference(repositoryRoot), repositoryRoot, arguments.urlBase);
            final var analysis = analyzer.analyze();
            final var report = analysis.generateReport();

            if (outputPath != null) {
                Files.writeString(outputPath, report);
                logger.info(KeyValueLogMessage.of(
                        "Wrote metrics diff report",
                        "output", outputPath));
            } else {
                System.out.println(report);
            }

            if (arguments.outlierQueryPath != null) {
                String outlierReport = analysis.generateOutlierQueryReport();
                if (!outlierReport.isEmpty()) {
                    final Path outlierQueryPath = Paths.get(arguments.outlierQueryPath);
                    Files.writeString(outlierQueryPath, outlierReport);
                    logger.info(KeyValueLogMessage.of("Wrote outlier report",
                            "output", outlierQueryPath));
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
     *
     * @throws RelationalException if analysis fails
     */
    @Nonnull
    public MetricsAnalysisResult analyze() throws RelationalException {
        logger.info(KeyValueLogMessage.of("Starting metrics diff analysis",
                "base", baseRef));

        // Find all changed metrics files
        final var changedFiles = GitMetricsFileFinder.findChangedMetricsYamlFiles(baseRef, repositoryRoot);
        logger.info(KeyValueLogMessage.of("Found changed metrics files",
                "base", baseRef,
                "file_count", changedFiles.size()));

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
        Map<PlannerMetricsProto.Identifier, MetricsInfo> baseMetrics;
        try {
            final var baseYamlFile = GitMetricsFileFinder.getFileAtReference(
                    repositoryRoot.relativize(yamlPath).toString(), baseRef, repositoryRoot);
            baseMetrics = YamlExecutionContext.loadMetricsFromYamlFile(baseYamlFile);
            Files.deleteIfExists(baseYamlFile); // Clean up temp file
        } catch (final RelationalException e) {
            // File might not exist in base ref (new file)
            if (logger.isDebugEnabled()) {
                logger.debug(KeyValueLogMessage.of("Could not load base metrics",
                        "path", yamlPath), e);
            }
            baseMetrics = ImmutableMap.of();
        } catch (IOException e) {
            throw new RelationalException("unable to delete temporary file", ErrorCode.INTERNAL_ERROR, e);
        }

        // Load head (new) metrics
        final Map<PlannerMetricsProto.Identifier, MetricsInfo> headMetrics;
        if (GitMetricsFileFinder.fileExists(yamlPath)) {
            headMetrics = YamlExecutionContext.loadMetricsFromYamlFile(yamlPath);
        } else {
            // File was deleted
            headMetrics = ImmutableMap.of();
        }

        // Compare the metrics
        compareMetrics(baseMetrics, headMetrics, yamlPath, analysisBuilder);
    }

    /**
     * Compares metrics between base and head versions.
     */
    @VisibleForTesting
    public void compareMetrics(@Nonnull final Map<PlannerMetricsProto.Identifier, MetricsInfo> baseMetrics,
                                @Nonnull final Map<PlannerMetricsProto.Identifier, MetricsInfo> headMetrics,
                                @Nonnull final Path filePath,
                                @Nonnull final MetricsAnalysisResult.Builder analysisBuilder) {

        final var baseIdentifiers = baseMetrics.keySet();
        final var headIdentifiers = headMetrics.keySet();

        // Find new queries (in head but not in base)
        int newCount = 0;
        for (final var identifier : headIdentifiers) {
            if (!baseIdentifiers.contains(identifier)) {
                newCount++;
                analysisBuilder.addNewQuery(filePath, identifier, headMetrics.get(identifier));
            }
        }

        // Find dropped queries (in base but not in head)
        int droppedCount = 0;
        for (final var identifier : baseIdentifiers) {
            if (!headIdentifiers.contains(identifier)) {
                droppedCount++;
                analysisBuilder.addDroppedQuery(filePath, identifier, baseMetrics.get(identifier));
            }
        }

        // Find changed queries (in both base and head)
        int changedCount = 0;
        for (final var identifier : headIdentifiers) {
            if (baseIdentifiers.contains(identifier)) {
                final MetricsInfo baseInfo = baseMetrics.get(identifier);
                final MetricsInfo headInfo = headMetrics.get(identifier);

                final var planChanged = !baseInfo.getExplain().equals(headInfo.getExplain());
                final var metricsChanged = YamlExecutionContext.areMetricsDifferent(
                        baseInfo.getCountersAndTimers(), headInfo.getCountersAndTimers());

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
        @Nullable
        private final Path repositoryRoot;
        @Nullable
        private final String urlBase;

        private MetricsAnalysisResult(@Nonnull final List<QueryChange> newQueries,
                                      @Nonnull final List<QueryChange> droppedQueries,
                                      @Nonnull final List<QueryChange> planAndMetricsChanged,
                                      @Nonnull final List<QueryChange> metricsOnlyChanged,
                                      @Nonnull final String baseRef,
                                      @Nonnull final String headRef,
                                      @Nullable final Path repositoryRoot,
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
            return repositoryRoot == null ? path : repositoryRoot.relativize(path);
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
                    .append(String.format("- New queries: %d\n", newQueries.size()))
                    .append(String.format("- Dropped queries: %d\n", droppedQueries.size()))
                    .append(String.format("- Plan changed + metrics changed: %d\n", planAndMetricsChanged.size()))
                    .append(String.format("- Plan unchanged + metrics changed: %d\n", metricsOnlyChanged.size()))
                    .append("\n");

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
                for (final var change : droppedQueries) {
                    report.append(String.format("- %s\n", formatQueryDisplay(change)));
                }
                report.append("\n");
            }

            appendChangesList(report, planAndMetricsChanged, "Plan and Metrics Changed");
            appendChangesList(report, metricsOnlyChanged, "Only Metrics Changed");

            report.append("<details>\n\n")
                    .append("<summary>ℹ️ About this analysis</summary>\n\n")
                    .append("This automated analysis compares query planner metrics between the base branch and this PR. It categorizes changes into:\n\n")
                    .append(" - **New queries**: Queries added in this PR\n")
                    .append(" - **Dropped queries**: Queries removed in this PR\n")
                    .append(" - **Plan changed + metrics changed**: The query plan has changed along with planner metrics\n")
                    .append(" - **Metrics only changed**: Same plan but different metrics\n\n")
                    .append("The last category in particular may indicate planner regressions that should be investigated.\n\n")
                    .append("</details>\n");

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
                final var absoluteFieldStats = stats.getAbsoluteFieldStatistics(fieldName);
                if (fieldStats.hasChanges() || absoluteFieldStats.hasChanges()) {
                    report.append(String.format("**`%s`**:\n", fieldName));
                    report.append(String.format("  - Average change: %s%.1f\n", sign(fieldStats.getMean()), fieldStats.getMean()));
                    report.append(String.format("  - Average absolute change: %s%.1f\n", sign(absoluteFieldStats.getMean()), absoluteFieldStats.getMean()));
                    report.append(String.format("  - Median change: %s%d\n", sign(fieldStats.getMedian()), fieldStats.getMedian()));
                    report.append(String.format("  - Median absolute change: %s%d\n", sign(absoluteFieldStats.getMedian()), absoluteFieldStats.getMedian()));
                    report.append(String.format("  - Standard deviation: %.1f\n", fieldStats.getStandardDeviation()));
                    report.append(String.format("  - Standard absolute deviation: %.1f\n", absoluteFieldStats.getStandardDeviation()));
                    report.append(String.format("  - Range: %s%d to %s%d\n", sign(fieldStats.getMin()), fieldStats.getMin(), sign(fieldStats.getMax()), fieldStats.getMax()));
                    report.append(String.format("  - Range of absolute values: %s%d to %s%d\n", sign(absoluteFieldStats.getMin()), absoluteFieldStats.getMin(), sign(absoluteFieldStats.getMax()), absoluteFieldStats.getMax()));
                    report.append(String.format("  - Queries affected: %d\n\n", fieldStats.getChangedCount()));
                }
            }
        }

        private void appendChangesList(@Nonnull final StringBuilder report, @Nonnull List<QueryChange> changes, @Nonnull String title) {
            if (changes.isEmpty()) {
                return;
            }
            report.append("## ").append(title).append("\n\n");
            report.append("Total: ").append(changes.size()).append(" quer").append(changes.size() == 1 ? "y" : "ies").append("\n\n");

            // Statistical analysis
            final var summary = calculateMetricsStatistics(changes);
            report.append("### Statistical Summary (").append(title).append(")\n\n");
            appendStatisticalSummary(report, summary);

            // Show outliers for metrics-only changes (these are more concerning)
            final var metricsOutliers = findOutliers(changes, summary);
            report.append("### Significant Changes (").append(title).append(")\n\n");
            if (metricsOutliers.isEmpty()) {
                report.append("No outliers detected.\n\n");
            } else {
                for (final var change : metricsOutliers) {
                    report.append(String.format("- %s", formatQueryDisplay(change))).append("\n");
                    if (change.oldInfo != null && change.newInfo != null) {
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
                        final var oldValue = (long) oldMetrics.getField(field);
                        final var newValue = (long) newMetrics.getField(field);

                        if (oldValue != newValue) {
                            final var difference = newValue - oldValue;
                            statisticsBuilder.addDifference(fieldName, difference);
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

        private List<QueryChange> findOutliers(@Nonnull final List<QueryChange> changes) {
            final MetricsStatistics summary = calculateMetricsStatistics(changes);
            return findOutliers(changes, summary);
        }

        private List<QueryChange> findOutliers(@Nonnull final List<QueryChange> changes, @Nonnull final MetricsStatistics stats) {
            if (changes.size() < 3) {
                // Not enough data for meaningful outlier detection
                return changes;
            }

            final ImmutableList.Builder<QueryChange> outliers = ImmutableList.builder();

            for (final var change : changes) {
                if (change.oldInfo != null && change.newInfo != null) {
                    final var oldMetrics = change.oldInfo.getCountersAndTimers();
                    final var newMetrics = change.newInfo.getCountersAndTimers();
                    final var descriptor = oldMetrics.getDescriptorForType();

                    boolean isOutlier = false;
                    for (final var fieldName : YamlExecutionContext.TRACKED_METRIC_FIELDS) {
                        final var field = descriptor.findFieldByName(fieldName);
                        final var oldValue = (long) oldMetrics.getField(field);
                        final var newValue = (long) newMetrics.getField(field);

                        if (oldValue != newValue) {
                            final var difference = newValue - oldValue;
                            if (isOutlierValue(stats.getFieldStatistics(fieldName), difference)
                                    || isOutlierValue(stats.getAbsoluteFieldStatistics(fieldName), Math.abs(difference))) {
                                isOutlier = true;
                                break;
                            }
                        }
                    }

                    if (isOutlier) {
                        outliers.add(change);
                    }
                }
            }

            return outliers.build();
        }

        private boolean isOutlierValue(MetricsStatistics.FieldStatistics fieldStats, long difference) {
            // Consider it an outlier if it's more than 2 standard deviations from the mean
            // or if it's a very large absolute change
            Assert.thatUnchecked(fieldStats.hasChanges(), "Field stats should have at least one difference");
            final var zScore = Math.abs((difference - fieldStats.mean) / fieldStats.standardDeviation);
            final var isLargeAbsoluteChange = Math.abs(difference) > Math.max(100, Math.abs(fieldStats.mean) * 2);
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
                    final String changeStr = sign(change) + change;
                    report.append(String.format("  - `%s`: %d -> %d (%s)\n", fieldName, oldValue, newValue, changeStr));
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
            @Nullable
            private final Path repositoryRoot;
            @Nullable
            private final String urlBase;

            public Builder(@Nonnull String baseRef, @Nonnull String headRef, @Nullable final Path repositoryRoot, @Nullable final String urlBase) {
                this.baseRef = baseRef;
                this.headRef = headRef;
                this.repositoryRoot = repositoryRoot;
                this.urlBase = urlBase;
            }

            public Builder addNewQuery(@Nonnull final Path filePath,
                                    @Nonnull final PlannerMetricsProto.Identifier identifier,
                                    @Nonnull final MetricsInfo info) {
                newQueries.add(new QueryChange(filePath, identifier, null, info));
                return this;
            }

            public Builder addDroppedQuery(@Nonnull final Path filePath,
                                        @Nonnull final PlannerMetricsProto.Identifier identifier,
                                        @Nonnull final MetricsInfo info) {
                droppedQueries.add(new QueryChange(filePath, identifier, info, null));
                return this;
            }

            public Builder addPlanAndMetricsChanged(@Nonnull final Path filePath,
                                                 @Nonnull final PlannerMetricsProto.Identifier identifier,
                                                 @Nonnull final MetricsInfo oldInfo,
                                                 @Nonnull final MetricsInfo newInfo) {
                planAndMetricsChanged.add(new QueryChange(filePath, identifier, oldInfo, newInfo));
                return this;
            }

            public Builder addMetricsOnlyChanged(@Nonnull final Path filePath,
                                              @Nonnull final PlannerMetricsProto.Identifier identifier,
                                              @Nonnull final MetricsInfo oldInfo,
                                              @Nonnull final MetricsInfo newInfo) {
                metricsOnlyChanged.add(new QueryChange(filePath, identifier, oldInfo, newInfo));
                return this;
            }

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
