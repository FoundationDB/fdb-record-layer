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

import com.apple.foundationdb.record.util.pair.NonnullPair;

import com.google.common.testing.FakeTicker;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
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
    private static final Map<String, Map<String, Map<String, String>>> entries = Map.of(
            "U.S.", Map.of(
                    "Animal", Map.of("river", "American Alligator", "mountain", "Mountain Goat", "sea", "California Sea Lion", "forest", "American Black Bear"),
                    "Landform", Map.of("river", "Mississippi River", "mountain", "Rocky Mountains", "sea", "Pacific Ocean", "forest", "Smoky Mountains National Park"),
                    "Capital", Map.of("California", "Sacramento", "Texas", "Austin", "Hawaii", "Honolulu", "Alaska", "Juneau")),
            "E.U.", Map.of(
                    "Animal", Map.of("river", "European Beaver", "mountain", "European BeaverI", "sea", "Mediterranean Monk Seal", "forest", "European Wild Boar"),
                    "Landform", Map.of("mountain", "Alps", "sea", "Mediterranean Sea", "forest", "Black Forest"),
                    "Capital", Map.of("France", "Paris", "Germany", "Berlin", "Spain", "Madrid", "Slovakia", "Bratislava")),
            "Japan", Map.of(
                    "Animal", Map.of("river", "Lambda 4S", "mountain", "Japanese Macaque", "sea", "Japanese Sea Turtle", "forest", "Japanese Giant Salamander"),
                    "Landform", Map.of("river", "Kiso", "mountain", "Kiso", "sea", "Sea of Japan", "forest", "Shiretoko Forest")),
            "China", Map.of(
                    "Animal", Map.of("mountain", "Sichuan Giant Panda", "sea", "Chinese White Dolphin", "forest", "Chinese White DolphinB"),
                    "Landform", Map.of("river", "Yangtze River", "mountain", "Mount Hua"),
                    "Capital", Map.of("Beijing", "Beijing", "Anhui", "Hefei", "Fujian", "Fuzhou", "Gansu", "Lanzhou")));

    @Nullable
    private static <V> V pickFirst(@Nonnull final Stream<V> stream) {
        return stream.findFirst().orElse(null);
    }

    @Nonnull
    private static String fetchFromCache(@Nonnull final String in) {
        return "restored " + in + " from cache";
    }

    @Nonnull
    private static Pair<String, String> produceAnimal(@Nonnull final String k2, @Nonnull final String k3) {
        return Pair.of(k2, entries.get("Animal").get(k2).get(k3));
    }

    @Nonnull
    private static Pair<String, String> produceLandform(@Nonnull final String k2, @Nonnull final String k3) {
        return Pair.of(k2, entries.get("Landform").get(k2).get(k3));
    }

    @Nonnull
    private static Pair<String, String> produceCapital(@Nonnull final String k2, @Nonnull final String k3) {
        return Pair.of(k2, entries.get("Capital").get(k2).get(k3));
    }

    private static void shouldBe(@Nonnull final MultiStageCache<String, String, String, String> cache, Map<String, Map<String, Map<String, String>>> expectedLayout) {
        Map<String, Map<String, Map<String, String>>> result = new HashMap<>();
        for (String key : cache.getStats().getAllKeys()) {
            result.computeIfAbsent(key, k -> new HashMap<>());
            for (String secondaryKey : cache.getStats().getAllSecondaryKeys(key)) {
                result.get(key).put(secondaryKey, cache.getStats().getAllTertiaryMappings(key, secondaryKey));
            }
        }
        Assertions.assertThat(result).isEqualTo(expectedLayout);
    }

    @Test
    void cacheStoresDataCorrectly() {
        final var builder = MultiStageCache.<String, String, String, String>newMultiStageCacheBuilder();
        final MultiStageCache<String, String, String, String> testCache = builder.setSize(2).setSecondarySize(2).build();
        final var result1 = readCache(testCache, "U.S.", "Animal", "river");
        Assertions.assertThat(result1).isEqualTo("American Alligator");
        final var result2 = readCache(testCache, "U.S.", "Animal", "river");
        Assertions.assertThat(result2).isEqualTo("restored American Alligator from cache");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"))));
    }

    @Test
    void primaryCacheEvictsDataCorrectly() {
        final var builder = MultiStageCache.<String, String, String, String>newMultiStageCacheBuilder();
        final var ticker = new FakeTicker();
        final MultiStageCache<String, String, String, String> testCache = builder
                .setSize(2)
                .setSecondarySize(2)
                .setTertiarySize(2)
                .setTtl(10)
                .setSecondaryTtl(5000) // we don't care about observing state of secondary cache for now, see test <code>secondaryCacheEvictsDataCorrectly</code>.
                .setTertiaryTtl(5000) // we don't care about observing state of tertiary cache for now, see test <code>tertiaryCacheEvictsDataCorrectly</code>.
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTertiaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();

        // warm up cache
        var result = readCache(testCache, "U.S.", "Animal", "river");
        Assertions.assertThat(result).isEqualTo("American Alligator");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"))));

        result = readCache(testCache, "U.S.", "Landform", "river");
        Assertions.assertThat(result).isEqualTo("Mississippi River");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"), "Landform", Map.of("river", "Mississippi River"))));

        ticker.advance(Duration.of(1, ChronoUnit.MILLIS));

        result = readCache(testCache, "E.U.", "Landform", "sea");
        Assertions.assertThat(result).isEqualTo("Mediterranean Sea");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"), "Landform", Map.of("river", "Mississippi River")), "E.U.", Map.of("Landform", Map.of("sea", "Mediterranean Sea"))));

        ticker.advance(Duration.of(8, ChronoUnit.MILLIS));

        // let's refresh U.S. entry by accessing it.

        result = readCache(testCache, "U.S.", "Landform", "river");
        Assertions.assertThat(result).isEqualTo("restored Mississippi River from cache");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"), "Landform", Map.of("river", "Mississippi River")), "E.U.", Map.of("Landform", Map.of("sea", "Mediterranean Sea"))));

        ticker.advance(Duration.of(4, ChronoUnit.MILLIS)); // E.U. entry should've expired ...

        // ... leaving space for the new entry of "China".
        result = readCache(testCache, "China", "Capital", "Anhui");
        Assertions.assertThat(result).isEqualTo("Hefei");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"), "Landform", Map.of("river", "Mississippi River")), "China", Map.of("Capital", Map.of("Anhui", "Hefei"))));
    }

    @Test
    void secondaryCacheEvictsDataCorrectly() {
        final var builder = MultiStageCache.<String, String, String, String>newMultiStageCacheBuilder();
        final var ticker = new FakeTicker();
        final MultiStageCache<String, String, String, String> testCache = builder
                .setSize(2)
                .setSecondarySize(2) // two slots only for secondary cache items (à la plan-family in production).
                .setTertiarySize(2)
                .setTtl(10)
                .setSecondaryTtl(5)
                .setTertiaryTtl(5000) // we don't care about observing state of tertiary cache for now, see test <code>tertiaryCacheEvictsDataCorrectly</code>.
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTertiaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();

        // warm up the cache
        // add U.S. -> Animal -> river -> American Alligator
        var result = readCache(testCache, "U.S.", "Animal", "river");
        Assertions.assertThat(result).isEqualTo("American Alligator");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"))));

        // advance time
        ticker.advance(Duration.of(2, ChronoUnit.MILLIS));

        // let's add another item: U.S. -> Landform -> river -> Mississippi River
        result = readCache(testCache, "U.S.", "Landform", "river");
        Assertions.assertThat(result).isEqualTo("Mississippi River");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"), "Landform", Map.of("river", "Mississippi River"))));

        // advance time
        ticker.advance(Duration.of(4, ChronoUnit.MILLIS)); // U.S. -> Animal -> river -> American Alligator should have expired
        shouldBe(testCache, Map.of("U.S.", Map.of("Landform", Map.of("river", "Mississippi River"))));

        // let's add one more item: U.S. -> Capital -> California -> Sacramento
        result = readCache(testCache, "U.S.", "Capital", "California");
        Assertions.assertThat(result).isEqualTo("Sacramento");

        // we're over the max size limit of secondary -> eviction should take place.
        // i.e. adding capital evicts the _expired_ element, which is U.S. -> Animal -> river -> American Alligator.
        // let's verify that
        shouldBe(testCache, Map.of("U.S.", Map.of("Capital", Map.of("California", "Sacramento"), "Landform", Map.of("river", "Mississippi River"))));
    }

    @Test
    void removeCacheKeyIfSecondaryIsEmptyWorks() {
        final var builder = MultiStageCache.<String, String, String, String>newMultiStageCacheBuilder();
        final var ticker = new FakeTicker();
        final MultiStageCache<String, String, String, String> testCache = builder
                .setSize(2)
                .setSecondarySize(2) // two slots only for secondary cache items (à la plan-family in production).
                .setTertiarySize(2)
                .setTtl(1000) // set to high value because we want to prove that eviction of cache key happens when the secondary cache becomes empty.
                .setSecondaryTtl(5)
                .setTertiaryTtl(1000) // set to high value because we want to prove that eviction of cache key happens when the secondary cache becomes empty
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTertiaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();

        // warm up the cache
        // add U.S. -> Animal -> American Alligator
        var result = readCache(testCache, "U.S.", "Animal", "river");
        Assertions.assertThat(result).isEqualTo("American Alligator");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"))));

        // advance time
        ticker.advance(Duration.of(2, ChronoUnit.MILLIS));

        // let's add another item: U.S. -> Landform -> Colorado River
        result = readCache(testCache, "U.S.", "Landform", "river");
        Assertions.assertThat(result).isEqualTo("Mississippi River");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"), "Landform", Map.of("river", "Mississippi River"))));

        // advance time
        // U.S. -> Animal -> American Alligator, and U.S. -> Landform -> Colorado River should have expired
        ticker.advance(Duration.of(10, ChronoUnit.MILLIS));

        // this is needed because expiration is done passively for better performance.
        // cleanup causes expiration handling to kick in immediately resulting in deterministic behavior which is necessary for testing.
        testCache.cleanUp();

        // let's verify that
        shouldBe(testCache, Map.of());
    }

    @Test
    void tertiaryCacheEvictsDataCorrectly() {
        final var builder = MultiStageCache.<String, String, String, String>newMultiStageCacheBuilder();
        final var ticker = new FakeTicker();
        final MultiStageCache<String, String, String, String> testCache = builder
                .setSize(2)
                .setSecondarySize(2)
                .setTertiarySize(2) // two slots only for tertiary cache items (à la plan-family in production).
                .setTtl(100)
                .setSecondaryTtl(10)
                .setTertiaryTtl(5)
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTertiaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();

        // warm up the cache
        // add U.S. -> Animal -> river -> American Alligator
        var result = readCache(testCache, "U.S.", "Animal", "river");
        Assertions.assertThat(result).isEqualTo("American Alligator");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"))));

        // advance time
        ticker.advance(Duration.of(2, ChronoUnit.MILLIS));

        // let's add another item: U.S. -> Animal-> mountain -> Mountain Goat
        result = readCache(testCache, "U.S.", "Animal", "mountain");
        Assertions.assertThat(result).isEqualTo("Mountain Goat");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator", "mountain", "Mountain Goat"))));

        // advance time
        ticker.advance(Duration.of(4, ChronoUnit.MILLIS)); // U.S. -> Animal -> river -> American Alligator should have expired
        // This is necessary because the cache size may return 2 despite the element having been cleaned, making the assertion fail
        testCache.cleanUp();
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("mountain", "Mountain Goat"))));

        // let's add one more item: U.S. -> Animal -> sea-> California Sea Lion
        result = readCache(testCache, "U.S.", "Animal", "sea");
        Assertions.assertThat(result).isEqualTo("California Sea Lion");

        // This is necessary because the cache size may return 2 despite the element having been cleaned, making the assertion fail
        testCache.cleanUp();
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("mountain", "Mountain Goat", "sea", "California Sea Lion"))));
    }

    @Test
    void removeCacheKeyIfTertiaryIsEmptyWorks() {
        final var builder = MultiStageCache.<String, String, String, String>newMultiStageCacheBuilder();
        final var ticker = new FakeTicker();
        final MultiStageCache<String, String, String, String> testCache = builder
                .setSize(2)
                .setSecondarySize(2) // two slots only for secondary cache items (à la plan-family in production).
                .setTertiarySize(2)
                .setTtl(1000) // set to high value because we want to prove that eviction of cache key happens when the tertiary cache becomes empty.
                .setSecondaryTtl(1000) // set to high value because we want to prove that eviction of cache key happens when the tertiary cache becomes empty
                .setTertiaryTtl(5)
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTertiaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();

        // warm up the cache
        // add U.S. -> Animal -> river -> American Alligator
        var result = readCache(testCache, "U.S.", "Animal", "river");
        Assertions.assertThat(result).isEqualTo("American Alligator");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator"))));

        // advance time
        ticker.advance(Duration.of(2, ChronoUnit.MILLIS));

        // let's add another item: U.S. -> Animal -> mountain -> Mountain Goat
        result = readCache(testCache, "U.S.", "Animal", "mountain");
        Assertions.assertThat(result).isEqualTo("Mountain Goat");
        shouldBe(testCache, Map.of("U.S.", Map.of("Animal", Map.of("river", "American Alligator", "mountain", "Mountain Goat"))));

        // advance time
        // U.S. -> Animal -> river -> American Alligator, and U.S. -> Animal -> mountain -> Mountain Goat should have expired
        ticker.advance(Duration.of(10, ChronoUnit.MILLIS));

        // this is needed because expiration is done passively for better performance.
        // cleanup causes expiration handling to kick in immediately resulting in deterministic behavior which is necessary for testing.
        testCache.cleanUp();

        // let's verify that
        shouldBe(testCache, Map.of());
    }

    private static String readCache(@Nonnull MultiStageCache<String, String, String, String> cache, @Nonnull String key,
                                    @Nonnull String secondaryKey, @Nonnull String tertiaryKey) {
        return cache.reduce(key, secondaryKey, tertiaryKey,
                () -> NonnullPair.of(tertiaryKey, entries.get(key).get(secondaryKey).get(tertiaryKey)),
                MultiStageCacheTests::fetchFromCache,
                MultiStageCacheTests::pickFirst, e -> { });
    }
}
