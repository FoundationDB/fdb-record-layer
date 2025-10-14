/*
 * ExponentialDelayTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.runners;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.RepeatedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ExponentialDelayTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExponentialDelayTest.class);

    @RepeatedTest(value = 5, name = "delayIncreases({currentRepetition} of {totalRepetitions})")
    void delayIncreases() {
        final Random random = new Random();
        final long initialDelayMillis = random.nextInt(10) + 45;
        final long maxDelayMillis = random.nextInt(1000) + 9500;
        final NoActualDelay exponentialDelay = new NoActualDelay(initialDelayMillis, maxDelayMillis);
        final int minCount = 100;
        final int maxCount = 100000;
        final double minAverage = maxDelayMillis / 2.0 - 100;
        final List<Long> nextDelays = runUntilAverageMax(exponentialDelay, minCount, maxCount, minAverage);

        assertThat(nextDelays.get(0), Matchers.lessThanOrEqualTo(initialDelayMillis));
        assertThat(nextDelays.size(), Matchers.allOf(Matchers.greaterThanOrEqualTo(minCount), Matchers.lessThan(maxCount)));
        assertThat(nextDelays, Matchers.everyItem(Matchers.lessThan(maxDelayMillis)));
        assertEquals(nextDelays, exponentialDelay.requestedDelays);
    }

    @RepeatedTest(value = 20, name = "delayIncreasesSlowly({currentRepetition} of {totalRepetitions})")
    void delayIncreasesSlowly() {
        final Random random = new Random();
        final long initialDelayMillis = random.nextInt(90) + 10;
        final long maxDelayMillis = (long)(Math.pow(2, 20) * initialDelayMillis);
        final int minCount = 100;
        final int maxCount = 100000;
        final double minAverage = maxDelayMillis / 2.0 - 100;
        List<List<Long>> allDelays = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allDelays.add(runUntilAverageMax(new NoActualDelay(initialDelayMillis, maxDelayMillis),
                    minCount, maxCount, minAverage));
        }

        while (true) {
            assertThat(allDelays, Matchers.hasSize(Matchers.lessThan(500)));
            allDelays.add(runUntilAverageMax(new NoActualDelay(initialDelayMillis, maxDelayMillis),
                    minCount, maxCount, minAverage));
            final long minLength = allDelays.stream().mapToLong(List::size).min().orElseThrow();
            final List<Double> averaged = new ArrayList<>();
            for (int i = 0; i < minLength; i++) {
                averaged.add(average(forAllGet(allDelays, i)));
            }
            assertThat(averaged.size(), Matchers.greaterThan(10));
            // we want to repeat until the averages are within the bounds of exponential growth
            final int firstBadIndex = areAveragesApproximatelyExponential(initialDelayMillis, maxDelayMillis, averaged);
            if (firstBadIndex == -1) {
                break;
            }
            if (allDelays.size() == 500) {
                LOGGER.debug("avg " + averaged.stream()
                        .map(val -> String.format(Locale.ROOT, "%10d", val.longValue()))
                        .collect(Collectors.joining(" ")));
                Stream.concat(Stream.of("^^^^^^^^^^"),
                                Stream.of((averaged.get(firstBadIndex - 1)).longValue(),
                                        (long)(averaged.get(firstBadIndex - 1) * 1.5),
                                        (long)(averaged.get(firstBadIndex - 1) * 2),
                                        (long)(averaged.get(firstBadIndex - 1) * 2.5),
                                        (long)(maxDelayMillis * 0.4),
                                        (long)(maxDelayMillis * 0.6)).map(l -> String.format(Locale.ROOT, "%010d", l)))
                        .forEachOrdered(
                                str -> {
                                    LOGGER.debug("    " + IntStream.range(0, averaged.size())
                                            .mapToObj(i -> i == firstBadIndex ? str : "          ")
                                            .collect(Collectors.joining(" ")));
                                });
                LOGGER.debug("----" + averaged.stream().map(vignore -> "--------").collect(Collectors.joining()));
            }
        }
    }

    @RepeatedTest(value = 50, name = "jitters({currentRepetition} of {totalRepetitions})")
    void jitters() {
        // If we have multiple different delays running at the same time, we don't want them both to be delaying the
        // same amount everytime
        final Random random = new Random();
        final long initialDelayMillis = random.nextInt(500) + 1000;
        final long maxDelayMillis = (long)(Math.pow(2, 20) * initialDelayMillis);
        final int minCount = 100;
        final int maxCount = 100000;
        final double minAverage = maxDelayMillis / 2.0 - 100;
        List<Set<Long>> allDelays = new ArrayList<>();
        allDelays.add(new HashSet<>());
        boolean foundDupe = false;
        // run 10 different sequences, after that, we should be able to run a sequence, where every delay is different
        // from the same nth delay.
        int totalCount = 0;
        while (allDelays.get(0).size() < 10 || foundDupe) {
            List<Long> delaySequence = runUntilAverageMax(new NoActualDelay(initialDelayMillis, maxDelayMillis),
                    minCount, maxCount, minAverage);
            foundDupe = false;
            for (int j = 0; j < delaySequence.size(); j++) {
                while (j >= allDelays.size()) {
                    allDelays.add(new HashSet<>());
                }
                if (!allDelays.get(j).add(delaySequence.get(j))) {
                    foundDupe = true;
                }
            }
            // just so the test doesn't run until it times out (or runs out of heap space)
            totalCount++;
            assertThat(totalCount, Matchers.lessThan(100));
        }
    }

    private int areAveragesApproximatelyExponential(final long initialDelayMillis, final long maxDelayMillis,
                                                    final List<Double> averaged) {
        if (averaged.get(0) <= initialDelayMillis * 0.25 || averaged.get(0) >= initialDelayMillis * 0.75) {
            return 0;
        }
        for (int i = 1; i < averaged.size(); i++) {
            if (averaged.get(i) < Math.min(averaged.get(i - 1) * 1.5, maxDelayMillis * 0.4) ||
                    averaged.get(i) > Math.min(averaged.get(i - 1) * 2.5, maxDelayMillis * 0.6)) {
                return i;
            }
        }
        return -1;
    }

    @Nonnull
    private List<Long> forAllGet(final List<List<Long>> allDelays, final int index) {
        return allDelays.stream().map(delays -> delays.get(index)).collect(Collectors.toList());
    }

    private List<Long> runUntilAverageMax(final NoActualDelay exponentialDelay,
                                          final int minCount, final int maxCount,
                                          final double minAverage) {
        List<Long> nextDelays = new ArrayList<>();
        AsyncUtil.whileTrue(() -> {
            nextDelays.add(exponentialDelay.getNextDelayMillis());
            return exponentialDelay.delay().thenCompose(vignore -> {
                if (nextDelays.size() < minCount) {
                    return AsyncUtil.READY_TRUE;
                } else {
                    if (nextDelays.size() >= maxCount) {
                        // don't just let the test run forever
                        return AsyncUtil.READY_FALSE;
                    } else {
                        double recentAverage = average(nextDelays.subList(nextDelays.size() - 20, nextDelays.size()));
                        return CompletableFuture.completedFuture(recentAverage < minAverage);
                    }
                }
            });
        }).join();
        return nextDelays;
    }

    private double average(final List<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElseThrow();
    }


    /**
     * A helper implementation to make the test substantially faster.
     */
    private static class NoActualDelay extends ExponentialDelay {
        List<Long> requestedDelays = new ArrayList<>();

        public NoActualDelay(final long initialDelayMillis, final long maxDelayMillis) {
            super(initialDelayMillis, maxDelayMillis, MoreAsyncUtil.getDefaultScheduledExecutor());
        }

        @Nonnull
        @Override
        protected CompletableFuture<Void> delayedFuture(final long nextDelayMillis) {
            requestedDelays.add(nextDelayMillis);
            return AsyncUtil.DONE;
        }
    }

}
