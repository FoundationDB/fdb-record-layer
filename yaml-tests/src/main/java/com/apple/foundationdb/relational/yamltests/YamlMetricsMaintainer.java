/*
 * YamlMetricsMaintainer.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.yamltests.generated.stats.PlannerMetricsProto;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedListMultimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.assertj.core.util.VisibleForTesting;
import org.junit.jupiter.api.Assertions;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Manages planner metrics for a single {@code .yamsql} test file across a test run.
 *
 * <p>At construction, the expected metrics are loaded from the companion
 * {@code .metrics.binpb} binary proto file. During test execution, each {@code EXPLAIN} result
 * is recorded via {@link #putMetrics} — either preserving the expected value on a match or
 * storing the actual value on a mismatch. When a maintenance config is active
 * (e.g. {@code CorrectMetrics}, {@code CorrectExplains}), {@link #markDirty()} signals that
 * the on-disk files should be updated. At teardown, {@link #saveIfNeeded()} rewrites both the
 * {@code .metrics.binpb} binary proto and the {@code .metrics.yaml} human-readable file with
 * the accumulated actual metrics.
 *
 * <p>Only the fields listed in {@link #TRACKED_METRIC_FIELDS} are compared between runs;
 * timing fields are stored for context but not checked.
 */
@SuppressWarnings({"PMD.GuardLogStatement"})
public class YamlMetricsMaintainer {
    private static final Logger logger = LogManager.getLogger(YamlMetricsMaintainer.class);

    /**
     * List of metrics field names that are tracked for planner comparison.
     * These are the core metrics that should be consistent between runs, excluding timing
     * information which can vary.
     */
    public static final List<String> TRACKED_METRIC_FIELDS = List.of(
            "task_count",
            "transform_count",
            "transform_yield_count",
            "insert_new_count",
            "insert_reused_count"
    );

    @Nonnull
    private final YamlReference.YamlResource resource;
    @Nonnull
    private final ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> expectedMetricsMap;
    @Nonnull
    private final Map<QueryAndLocation, PlannerMetricsProto.Info> actualMetricsMap;
    private volatile boolean isDirty = false;

    private static final Comparator<QueryAndLocation> ACTUAL_METRICS_ORDER =
            Comparator.comparing(QueryAndLocation::getReference)
                    .thenComparing(QueryAndLocation::getBlockName)
                    .thenComparing(QueryAndLocation::getQuery);

    public YamlMetricsMaintainer(@Nonnull YamlReference.YamlResource resource) throws RelationalException {
        this.resource = resource;
        this.expectedMetricsMap = loadMetricsResource(resource);
        this.actualMetricsMap = new TreeMap<>(ACTUAL_METRICS_ORDER);
    }

    /** Testing constructor: bypasses file loading. */
    YamlMetricsMaintainer(@Nonnull YamlReference.YamlResource resource,
                          @Nonnull ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> expectedMetrics) {
        this.resource = resource;
        this.expectedMetricsMap = expectedMetrics;
        this.actualMetricsMap = new TreeMap<>(ACTUAL_METRICS_ORDER);
    }

    @VisibleForTesting
    boolean isMetricsDirty() {
        return isDirty;
    }

    @VisibleForTesting
    @Nullable
    PlannerMetricsProto.Info getActualMetrics(@Nonnull final PlannerMetricsProto.Identifier identifier) {
        return actualMetricsMap.entrySet().stream()
                .filter(e -> e.getKey().getIdentifier().equals(identifier))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }

    @Nullable
    public PlannerMetricsProto.Info getMetrics(@Nonnull PlannerMetricsProto.Identifier identifier) {
        return expectedMetricsMap.get(identifier);
    }

    @SuppressWarnings("UnusedReturnValue")
    public synchronized PlannerMetricsProto.Info putMetrics(@Nonnull final PlannerMetricsProto.Identifier identifier,
                                                            @Nonnull final YamlReference reference,
                                                            @Nonnull final PlannerMetricsProto.Info info) {
        return actualMetricsMap.put(new QueryAndLocation(identifier, reference), info);
    }

    public synchronized void markDirty() {
        this.isDirty = true;
    }

    public void saveIfNeeded() {
        if (!isDirty) {
            return;
        }
        saveMetricsAsBinaryProto();
        saveMetricsAsYaml();
    }

    private void saveMetricsAsBinaryProto() {
        //
        // It is possible that some queries are repeated within the same block. These explain queries, if served from
        // the cache contain their original planner metrics when they were planned, thus they are identical and we
        // pick one of them. If not served from the cache (for instance by explicitly switching it off) we should
        // still see the same counters which is all we test for at this moment. If someone adds a testcase that
        // switches off the cache, executes an explain, changes something about the schema and then runs the same
        // query in the same block a second time, there will be pain. Don't do that! We log a warning for this case
        // but continue.
        //
        final var condensedMetricsMap = new LinkedHashMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info>();
        for (final var entry : actualMetricsMap.entrySet()) {
            final var queryAndLocation = entry.getKey();
            final var identifier = queryAndLocation.getIdentifier();
            if (condensedMetricsMap.containsKey(identifier)) {
                logger.warn("⚠️ Repeated query in block {} at {}", queryAndLocation.getBlockName(),
                        queryAndLocation.getReference());
            } else {
                condensedMetricsMap.put(identifier,
                        entry.getValue());
            }
        }

        final var fileName = Path.of(System.getProperty("user.dir"))
                .resolve(Path.of("src", "test", "resources", metricsBinaryProtoFileName(resource.getPath())))
                .toAbsolutePath().toString();
        try (var fos = new FileOutputStream(fileName)) {
            for (final var entry : condensedMetricsMap.entrySet()) {
                PlannerMetricsProto.Entry.newBuilder()
                        .setIdentifier(entry.getKey())
                        .setInfo(entry.getValue())
                        .build()
                        .writeDelimitedTo(fos);
            }
            logger.info("🟢 Planner metrics file {} replaced.", fileName);
        } catch (final IOException iOE) {
            logger.error("⚠️ Source file {} could not be replaced with corrected file.", fileName);
            Assertions.fail(iOE);
        }
    }

    private void saveMetricsAsYaml() {
        final var mmap = LinkedListMultimap.<String, Map<String, Object>>create();
        for (final var entry : actualMetricsMap.entrySet()) {
            final var identifier = entry.getKey().getIdentifier();
            final var info = entry.getValue();
            final var countersAndTimers = info.getCountersAndTimers();
            final var infoMap = new LinkedHashMap<String, Object>();
            infoMap.put("query", identifier.getQuery());
            // only include setup if it is non-empty, in part so that the PR that adds setup doesn't change every
            // metric in the yaml files
            if (identifier.getSetupsCount() > 0) {
                infoMap.put("setup", identifier.getSetupsList());
            }
            infoMap.put("ref", entry.getKey().getReference().toString());
            infoMap.put("explain", info.getExplain());
            infoMap.put("task_count", countersAndTimers.getTaskCount());
            infoMap.put("task_total_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getTaskTotalTimeNs()));
            infoMap.put("transform_count", countersAndTimers.getTransformCount());
            infoMap.put("transform_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getTransformTimeNs()));
            infoMap.put("transform_yield_count", countersAndTimers.getTransformYieldCount());
            infoMap.put("insert_time_ms", TimeUnit.NANOSECONDS.toMillis(countersAndTimers.getInsertTimeNs()));
            infoMap.put("insert_new_count", countersAndTimers.getInsertNewCount());
            infoMap.put("insert_reused_count", countersAndTimers.getInsertReusedCount());
            mmap.put(identifier.getBlockName(), infoMap);
        }

        final var fileName = Path.of(System.getProperty("user.dir"))
                .resolve(Path.of("src", "test", "resources", metricsYamlFileName(resource.getPath())))
                .toAbsolutePath().toString();
        try (var fos = new FileOutputStream(fileName)) {
            DumperOptions options = new DumperOptions();
            options.setIndent(4);
            options.setPrettyFlow(true);
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            Yaml yaml = new Yaml(options);
            yaml.dump(mmap.asMap(), new PrintWriter(fos, false, StandardCharsets.UTF_8));
            logger.info("🟢 Planner metrics file {} replaced.", fileName);
        } catch (final IOException iOE) {
            logger.error("⚠️ Source file {} could not be replaced with corrected file.", fileName);
            Assertions.fail(iOE);
        }
    }

    @Nonnull
    private static ImmutableMap<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info> loadMetricsResource(@Nonnull final YamlReference.YamlResource resource) throws RelationalException {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final var fis = classLoader.getResourceAsStream(metricsBinaryProtoFileName(resource.getPath()));
        final var resultMapBuilder =
                ImmutableMap.<PlannerMetricsProto.Identifier, PlannerMetricsProto.Info>builder();
        if (fis == null) {
            return resultMapBuilder.build();
        }
        try {
            while (true) {
                final var entry = PlannerMetricsProto.Entry.parseDelimitedFrom(fis);
                if (entry == null) {
                    return resultMapBuilder.build();
                }
                resultMapBuilder.put(entry.getIdentifier(), entry.getInfo());
            }
        } catch (final IOException e) {
            throw new RelationalException(ErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Nonnull
    private static String baseName(@Nonnull final String resourcePath) {
        final var tokens = resourcePath.split("\\.(?=[^\\.]+$)");
        Verify.verify(tokens.length == 2);
        Verify.verify("yamsql".equals(tokens[1]));
        return tokens[0];
    }

    @Nonnull
    private static String metricsBinaryProtoFileName(@Nonnull final String resourcePath) {
        return baseName(resourcePath) + ".metrics.binpb";
    }

    @Nonnull
    private static String metricsYamlFileName(@Nonnull final String resourcePath) {
        return baseName(resourcePath) + ".metrics.yaml";
    }

    private static class QueryAndLocation {
        @Nonnull
        private final PlannerMetricsProto.Identifier identifier;
        @Nonnull
        private final YamlReference reference;

        public QueryAndLocation(@Nonnull final PlannerMetricsProto.Identifier identifier, @Nonnull final YamlReference reference) {
            this.identifier = identifier;
            this.reference = reference;
        }

        @Nonnull
        public PlannerMetricsProto.Identifier getIdentifier() {
            return identifier;
        }

        @Nonnull
        public String getBlockName() {
            return identifier.getBlockName();
        }

        @Nonnull
        public String getQuery() {
            return identifier.getQuery();
        }

        @Nonnull
        public YamlReference getReference() {
            return reference;
        }

        @Override
        public boolean equals(final Object o) {
            if (!(o instanceof QueryAndLocation)) {
                return false;
            }
            final QueryAndLocation that = (QueryAndLocation)o;
            return reference.equals(that.reference) && Objects.equals(identifier, that.identifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(identifier, reference);
        }
    }
}
