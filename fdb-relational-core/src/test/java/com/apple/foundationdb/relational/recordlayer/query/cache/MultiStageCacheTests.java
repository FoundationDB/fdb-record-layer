/*
 * MultiStageCacheTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.google.common.testing.FakeTicker;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This tests different behavioural aspects of {@link MultiStageCache}. Namely:
 * <ul>
 *     <li>eviction policy of main cache</li>
 *     <li>eviction policy of secondary cache</li>
 *     <li>invalidation of main cache entry if its value becomes empty</li>
 * </ul>
 * To make easier to reason about time-based expiration strategy of the cache, a logical clock is used
 * whose ticks can be advanced programmatically, see {@link FakeTicker}.
 * <br>
 * The test harness assumes a test cache with the following layout:
 * <br>
 * <pre>
 * {@code
 *  +---------+----------------------+
 *  |         | +----------+------+  |
 *  | Country | | Category | Item |  |
 *  |         | +----------+------+  |
 *  +---------+---- ^ ---------------+
 *     ^            |
 *     |            |
 *     |            +------------------------ Secondary Cache
 *     +------------------------------------- Main Cache
 * }
 * </pre>
 * Here is a list of countries, categories, and items that we use in the tests:
 * <table>
 *     <tbody>
 *         <tr><th>Country</th><th>Item</th><th>Category</th></tr>
 *         <tr><td>U.S.</td><td>Animal</td><td>American Alligator</td></tr><tr><td>U.S.</td><td>Landform</td><td>Colorado River</td></tr><tr><td>U.S.</td><td>Train</td><td>the Acela</td></tr>
 *         <tr><td>E.U.</td><td>Animal</td><td>European Deer</td></tr><tr><td>E.U.</td><td>Landform</td><td>Southern Carpathians</td></tr><tr><td>E.U.</td><td>Train</td><td>TGV</td></tr>
 *         <tr><td>Japan</td><td>Animal</td><td>Japanese Giant Salamander</td></tr><tr><td>Japan</td><td>Landform</td><td>Mount Fuji</td></tr><tr><td>Japan</td><td>Train</td><td>Shinkansen</td></tr>
 *         <tr><td>China</td><td>Animal</td><td>Sichuan Giant Panda</td></tr><tr><td>China</td><td>Landform</td><td>Qinling Mountains Forest</td></tr><tr><td>China</td><td>Train</td><td>Shanghai</td></tr>
 *     </tbody>
 * </table>
 */
public class MultiStageCacheTests {

    @Nonnull
    private static final Map<String, String> animal = Map.of("U.S.", "American Alligator", "E.U.", "European Deer", "Japan", "Japanese Giant Salamander", "China", "Sichuan Giant Panda");

    @Nonnull
    private static final Map<String, String> landforms = Map.of("U.S.", "Colorado River", "E.U.", "Southern Carpathians", "Japan", "Mount Fuji", "China", "Qinling Mountains Forest");

    @Nonnull
    private static final Map<String, String> trains = Map.of("U.S.", "the Acela", "E.U.", "TGV", "Japan", "Shinkansen", "China", "Shanghai");

    @Nullable
    private static <V> V pickFirst(@Nonnull final Stream<V> stream) {
        return stream.findFirst().orElse(null);
    }

    @Nonnull
    private static String fetchFromCache(@Nonnull final String in) {
        return "restored " + in + " from cache";
    }

    @Nonnull
    private static Pair<String, String> produceAnimal(@Nonnull final String in) {
        return Pair.of("Animal", animal.get(in));
    }

    @Nonnull
    private static Pair<String, String> produceLandform(@Nonnull final String in) {
        return Pair.of("Landform", landforms.get(in));
    }

    @Nonnull
    private static Pair<String, String> produceTrain(@Nonnull final String in) {
        return Pair.of("Train", trains.get(in));
    }

    private static void shouldBe(@Nonnull final MultiStageCache<String, String, String> cache, Map<String, Map<String, String>> expectedLayout) {
        final Set<String> keys = cache.getStats().getAllKeys();
        final Map<String, Map<String, String>> result = keys
                .stream()
                .map(key -> new AbstractMap.SimpleEntry<>(key, cache.getStats().getAllSecondaryMappings(key)))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        Assertions.assertThat(result).isEqualTo(expectedLayout);
    }

    @Test
    void cacheStoresDataCorrectly() {
        final var builder = MultiStageCache.<String, String, String>newMultiStageCacheBuilder();
        final MultiStageCache<String, String, String> testCache = builder.setSize(2).setSecondarySize(2).build();
        final var result1 = testCache.reduce("U.S.", "Animal", () -> produceAnimal("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result1).isEqualTo("American Alligator");
        final var result2 = testCache.reduce("U.S.", "Animal", () -> produceAnimal("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result2).isEqualTo("restored American Alligator from cache");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator")));
    }

    @Test
    void primaryCacheEvictsDataCorrectly() {
        final var builder = MultiStageCache.<String, String, String>newMultiStageCacheBuilder();
        final var ticker = new FakeTicker();
        final MultiStageCache<String, String, String> testCache = builder
                .setSize(2)
                .setSecondarySize(2)
                .setTtl(10)
                .setSecondaryTtl(5000) // we don't care about observing state of secondary cache for now, see test <code>secondaryCacheEvictsDataCorrectly</code>.
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();

        // warm up cache
        var result = testCache.reduce("U.S.", "Animal", () -> produceAnimal("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("American Alligator");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator")));

        result = testCache.reduce("U.S.", "Landform", () -> produceLandform("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("Colorado River");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator", "Landform", "Colorado River")));

        ticker.advance(Duration.of(1, ChronoUnit.MILLIS));

        result = testCache.reduce("E.U.", "Landform", () -> produceLandform("E.U."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("Southern Carpathians");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator", "Landform", "Colorado River"), "E.U.", Map.of("Landform", "Southern Carpathians")));

        ticker.advance(Duration.of(8, ChronoUnit.MILLIS));

        // let's refresh U.S. entry by accessing it.

        result = testCache.reduce("U.S.", "Landform", () -> produceLandform("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("restored Colorado River from cache");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator", "Landform", "Colorado River"), "E.U.", Map.of("Landform", "Southern Carpathians")));

        ticker.advance(Duration.of(4, ChronoUnit.MILLIS)); // E.U. entry should've expired ...

        // ... leaving space for the new entry of "Japan".
        result = testCache.reduce("Japan", "Train", () -> produceTrain("Japan"), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("Shinkansen");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator", "Landform", "Colorado River"), "Japan", Map.of("Train", "Shinkansen")));
    }

    @Test
    void secondaryCacheEvictsDataCorrectly() {
        final var builder = MultiStageCache.<String, String, String>newMultiStageCacheBuilder();
        final var ticker = new FakeTicker();
        final MultiStageCache<String, String, String> testCache = builder
                .setSize(2)
                .setSecondarySize(2) // two slots only for secondary cache items (à la plan-family in production).
                .setTtl(10)
                .setSecondaryTtl(5)
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();

        // warm up the cache
        // add U.S. -> Animal -> American Alligator
        var result = testCache.reduce("U.S.", "Animal", () -> produceAnimal("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("American Alligator");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator")));

        // advance time
        ticker.advance(Duration.of(2, ChronoUnit.MILLIS));

        // let's add another item: U.S. -> Landform -> Colorado River
        result = testCache.reduce("U.S.", "Landform", () -> produceLandform("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("Colorado River");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator", "Landform", "Colorado River")));

        // advance time
        ticker.advance(Duration.of(4, ChronoUnit.MILLIS)); // U.S. -> Animal -> American Alligator should have expired

        // let's add one more item: U.S. -> Train -> the Acela
        result = testCache.reduce("U.S.", "Train", () -> produceTrain("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("the Acela");

        // we're over the max size limit of secondary -> evication should take place.
        // i.e. adding train evicts the _expired_ element, which is U.S. -> Animal -> American Alligator.
        // let's verify that
        shouldBe(testCache, Map.of("U.S.", Map.of("Landform", "Colorado River", "Train", "the Acela")));
    }

    @Test
    void removeCacheKeyIfSecondaryIsEmptyWorks() {
        final var builder = MultiStageCache.<String, String, String>newMultiStageCacheBuilder();
        final var ticker = new FakeTicker();
        final MultiStageCache<String, String, String> testCache = builder
                .setSize(2)
                .setSecondarySize(2) // two slots only for secondary cache items (à la plan-family in production).
                .setTtl(1000) // set to high value because we want to prove that eviction of cache key happens when the secondary cache becomes empty.
                .setSecondaryTtl(5)
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();

        // warm up the cache
        // add U.S. -> Animal -> American Alligator
        var result = testCache.reduce("U.S.", "Animal", () -> produceAnimal("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("American Alligator");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator")));

        // advance time
        ticker.advance(Duration.of(2, ChronoUnit.MILLIS));

        // let's add another item: U.S. -> Landform -> Colorado River
        result = testCache.reduce("U.S.", "Landform", () -> produceLandform("U.S."), MultiStageCacheTests::fetchFromCache, MultiStageCacheTests::pickFirst);
        Assertions.assertThat(result).isEqualTo("Colorado River");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", "American Alligator", "Landform", "Colorado River")));

        // advance time
        // U.S. -> Animal -> American Alligator, and U.S. -> Landform -> Colorado River should have expired
        ticker.advance(Duration.of(10, ChronoUnit.MILLIS));

        // this is needed because expiration is done passively for better performance.
        // cleanup causes expiration handling to kick in immediately resulting in deterministic behavior which is necessary for testing.
        testCache.cleanUp();

        // let's verify that
        shouldBe(testCache, Map.of());
    }
}
