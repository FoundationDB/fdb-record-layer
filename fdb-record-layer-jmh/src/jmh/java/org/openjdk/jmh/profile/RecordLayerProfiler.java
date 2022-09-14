/*
 * RecordLayerProfiler.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package org.openjdk.jmh.profile;

import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.Aggregator;
import org.openjdk.jmh.results.Defaults;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ResultRole;
import org.openjdk.jmh.results.ScalarDerivativeResult;
import org.openjdk.jmh.results.ThroughputResult;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.util.ListStatistics;
import org.openjdk.jmh.util.SampleBuffer;
import org.openjdk.jmh.util.Statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A JMH Profiler that handles reporting metrics from FDB and RecordLayer as part of the iteration
 * cleanup.
 */
public class RecordLayerProfiler implements InternalProfiler {
    /*
     * We use this instead of a ThreadLocal here so that we can iterate over all the timers and report
     * their results at the end of the iteration
     */
    public static ConcurrentHashMap<String, FDBStoreTimer> iterationStoreTimer = new ConcurrentHashMap<>();

    public static FDBStoreTimer getStoreTimer() {
        String threadName = Thread.currentThread().getName();
        return iterationStoreTimer.computeIfAbsent(threadName, (tName) -> new FDBStoreTimer());
    }

    private final String outputDir;
    private final String primaryMetric;

    public RecordLayerProfiler(String initLint) throws ProfilerException {
        OptionParser parser = new OptionParser();

        OptionSpec<String> outDir = parser.accepts("dir", "Output directory.")
                .withRequiredArg().ofType(String.class).describedAs("dir");

        OptionSpec<String> primaryMetric = parser.accepts("metric", "The Main metric to print.")
                .withRequiredArg()
                .ofType(String.class).describedAs("metric");

        OptionSet set = ProfilerUtils.parseInitLine(initLint, parser);
        if (set.has(outDir)) {
            this.outputDir = set.valueOf(outDir);
        } else {
            this.outputDir = null;
        }
        if (set.has(primaryMetric)) {
            this.primaryMetric = set.valueOf(primaryMetric);
        } else {
            this.primaryMetric = "TRANSACTION_TIME";
        }
    }

    @Override
    public void beforeIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {
    }

    @Override
    public Collection<? extends Result> afterIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams, final IterationResult result) {
        if (outputDir != null) {
            try {
                dumpFullMetrics();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        List<ScalarDerivativeResult> results = new ArrayList<>();
        if (primaryMetric != null) {
            List<StoreTimer.Counter> counters = iterationStoreTimer.values().stream()
                    .map(timer -> timer.getEvents().stream()
                            .filter(event -> event.name().equalsIgnoreCase(primaryMetric))
                            .findAny()
                            .map(timer::getCounter))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());

            if (!counters.isEmpty()) {
                long time = counters.get(0).getTimeNanos();
                //if it's time, use a sample time result
                if (time != 0L) {
                    ListStatistics stats = new ListStatistics();
                    final double conversion =TimeUnit.NANOSECONDS.convert(1,benchmarkParams.getTimeUnit());
                    counters.forEach(counter -> {
                        double rateNanos = counter.getCount() / ((double)counter.getTimeNanos());
                        stats.addValue(rateNanos * conversion);
                    });

                    String unitLabel = "ops/" + TimeValue.tuToString(benchmarkParams.getTimeUnit());
                    String baseMetricLabel = primaryMetric + Defaults.PREFIX;

                    results.add(new ScalarDerivativeResult(baseMetricLabel + "avg", stats.getMean(), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.00", stats.getPercentile(0d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.50", stats.getPercentile(50d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.90", stats.getPercentile(90d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.95", stats.getPercentile(95d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.99", stats.getPercentile(99d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.999", stats.getPercentile(99.9d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.9999", stats.getPercentile(99.99d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p1.00", stats.getPercentile(100d), unitLabel, AggregationPolicy.AVG));

                } else {
                    //without time, it's just values so sample them as counts
                    SampleBuffer buffer = new SampleBuffer();
                    counters.forEach(counter -> buffer.add(counter.getCount()));

                    Statistics stats = buffer.getStatistics(1d);

                    String unitLabel = "";
                    String baseMetricLabel = primaryMetric + Defaults.PREFIX;
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "avg", stats.getMean(), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.00", stats.getPercentile(0d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.50", stats.getPercentile(50d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.90", stats.getPercentile(90d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.95", stats.getPercentile(95d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.99", stats.getPercentile(99d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.999", stats.getPercentile(99.9d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p0.9999", stats.getPercentile(99.99d), unitLabel, AggregationPolicy.AVG));
                    results.add(new ScalarDerivativeResult(baseMetricLabel + "p1.00", stats.getPercentile(100d), unitLabel, AggregationPolicy.AVG));
                }
            }
        }

        iterationStoreTimer.clear();
        return results;
    }


    @Override
    public String getDescription() {
        return "RecordLayer Profiling using the FDBStoreTimer.";
    }

    private void dumpFullMetrics() throws IOException {

    }

    private static class NestedThroughputResult extends ThroughputResult {

        public NestedThroughputResult(final ResultRole role, final String label, final double operations, final long durationNs, final TimeUnit outputTimeUnit) {
            super(role, label, operations, durationNs, outputTimeUnit);
        }

        @Override
        protected Aggregator<ThroughputResult> getIterationAggregator() {
            return super.getIterationAggregator();
        }
    }

}
