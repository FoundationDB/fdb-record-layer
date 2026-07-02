/*
 * RandomHelpers.java
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

package com.apple.foundationdb.async.common;

import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

public final class RandomHelpers {
    private RandomHelpers() {
        // nothing
    }

    /**
     * Seed a {@link SplittableRandom} deterministically from a record's primary key, folding the key's full encoded
     * bytes into the seed so distinct keys yield distinct streams. (Seeding from {@link Object#hashCode()} — only
     * 32 bits — risks birthday collisions, which for ids derived from the stream would conflate distinct records.)
     *
     * @param primaryKey the record's primary key
     *
     * @return a new {@link SplittableRandom}
     */
    @Nonnull
    public static SplittableRandom random(@Nonnull final Tuple primaryKey) {
        return new SplittableRandom(seedFromBytes(primaryKey.pack()));
    }

    /**
     * Seed a {@link SplittableRandom} deterministically from a UUID identity (such as a task id), folding all 128
     * bits into the seed.
     *
     * @param identity the UUID identity
     *
     * @return a new {@link SplittableRandom}
     */
    @Nonnull
    public static SplittableRandom random(@Nonnull final UUID identity) {
        return new SplittableRandom(seedFromBytes(bytesOf(identity)));
    }

    /**
     * Returns a good double hash code for the argument of type {@code long}. It uses {@link #splitMixLong(long)}
     * internally and then maps the {@code long} result to a {@code double} between {@code 0} and {@code 1}.
     * @param x a {@code long}
     * @return a high quality hash code of {@code x} as a {@code double} in the range {@code [0.0d, 1.0d)}.
     */
    public static double splitMixDouble(final long x) {
        return (splitMixLong(x) >>> 11) * 0x1.0p-53;
    }

    /**
     * Returns a good long hash code for the argument of type {@code long}. It is an implementation of the
     * output mixing function {@code SplitMix64} (the finalizing mix of the SplitMix algorithm) as employed by many
     * PRNGs such as {@link SplittableRandom}.
     * @param x a {@code long}
     * @return a high quality hash code of {@code x}
     */
    static long splitMixLong(long x) {
        x += 0x9e3779b97f4a7c15L;
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        x = x ^ (x >>> 31);
        return x;
    }

    /**
     * Derives a high-entropy 64-bit seed from {@code bytes} by folding each byte through the SplitMix64 mix, so the
     * full content of an identity (rather than a 32-bit {@link Object#hashCode()}) determines the seed.
     *
     * @param bytes the bytes to fold
     *
     * @return a 64-bit seed
     */
    private static long seedFromBytes(@Nonnull final byte[] bytes) {
        long seed = 0L;
        for (final byte b : bytes) {
            seed = splitMixLong(seed ^ (b & 0xffL));
        }
        return seed;
    }

    /**
     * Returns the 16-byte big-endian representation of {@code uuid} (most-significant bits first).
     *
     * @param uuid the UUID to encode
     *
     * @return its 16-byte representation
     */
    @Nonnull
    private static byte[] bytesOf(@Nonnull final UUID uuid) {
        return ByteBuffer.allocate(Long.BYTES * 2)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    /**
     * Returns a fresh {@link UUID}: a reproducible one derived from {@code random} when deterministic randomness is
     * requested, or a true random {@link UUID#randomUUID()} otherwise.
     *
     * @param random the random source to derive a reproducible UUID from when {@code deterministicRandomness} is set
     * @param deterministicRandomness whether to derive the UUID deterministically from {@code random}
     *
     * @return a new UUID
     */
    @Nonnull
    public static UUID randomUuid(@Nonnull final SplittableRandom random, final boolean deterministicRandomness) {
        return deterministicRandomness ? randomUuid(random) : UUID.randomUUID();
    }

    /**
     * Derives a type-4 (random) {@link UUID} from the given {@link SplittableRandom}, so that UUID generation can be
     * made reproducible by seeding the source.
     *
     * @param random the random source to draw the UUID bits from
     *
     * @return a type-4 UUID derived from {@code random}
     */
    @Nonnull
    public static UUID randomUuid(@Nonnull final SplittableRandom random) {
        long msb = random.nextLong();
        long lsb = random.nextLong();

        // Set version to 4
        msb &= 0xffffffffffff0fffL;
        msb |= 0x0000000000004000L;

        // Set variant to IETF variant
        lsb &= 0x3fffffffffffffffL;
        lsb |= 0x8000000000000000L;

        return new UUID(msb, lsb);
    }

    /**
     * Returns a {@link UUID} for a record: a reproducible, collision-resistant 128-bit hash
     * ({@link UUID#nameUUIDFromBytes}) of the primary key's encoded bytes when deterministic randomness is
     * requested, or a random {@link UUID#randomUUID()} otherwise. Deriving the deterministic id straight from the
     * full primary key (rather than from a low-entropy seeded stream) guarantees distinct records get distinct ids.
     *
     * @param primaryKey the record's primary key
     * @param deterministicRandomness whether to derive the UUID deterministically from {@code primaryKey}
     *
     * @return a new UUID
     */
    @Nonnull
    public static UUID randomUuid(@Nonnull final Tuple primaryKey, final boolean deterministicRandomness) {
        return deterministicRandomness ? UUID.nameUUIDFromBytes(primaryKey.pack()) : UUID.randomUUID();
    }

    /**
     * Returns a {@link UUID}: a reproducible, collision-resistant 128-bit hash ({@link UUID#nameUUIDFromBytes}) of
     * the given UUID identity's bytes when deterministic randomness is requested, or a random
     * {@link UUID#randomUUID()} otherwise.
     *
     * @param identity the UUID identity to derive from
     * @param deterministicRandomness whether to derive the UUID deterministically from {@code identity}
     *
     * @return a new UUID
     */
    @Nonnull
    public static UUID randomUuid(@Nonnull final UUID identity, final boolean deterministicRandomness) {
        return deterministicRandomness ? UUID.nameUUIDFromBytes(bytesOf(identity)) : UUID.randomUUID();
    }

    /**
     * Applies an asynchronous body to each item, giving every invocation its own independent {@link SplittableRandom}
     * split from {@code splittableRandom} so the work stays reproducible under deterministic randomness.
     *
     * @param splittableRandom the random source to split per item
     * @param items the items to process
     * @param body the asynchronous operation to apply to each item together with its own random stream
     * @param parallelism the maximum number of items to process concurrently
     * @param executor the executor to run on
     * @param <T> the type of input item
     * @param <U> the type of result produced per item
     *
     * @return a future completing with the per-item results
     */
    @Nonnull
    public static <T, U> CompletableFuture<List<U>> forEach(@Nonnull final SplittableRandom splittableRandom,
                                                            @Nonnull final Iterable<T> items,
                                                            @Nonnull final BiFunction<T, SplittableRandom, CompletableFuture<U>> body,
                                                            final int parallelism,
                                                            @Nonnull final Executor executor) {
        final Iterable<ItemRandomPair<T>> itemWithRandoms =
                Iterables.transform(items, item -> new ItemRandomPair<>(item, splittableRandom.split()));

        return MoreAsyncUtil.forEach(itemWithRandoms,
                itemWithRandom -> body.apply(itemWithRandom.getItem(), itemWithRandom.getRandom()),
                parallelism, executor);
    }

    /**
     * Pairs an item with its own {@link SplittableRandom} so that each item processed in parallel draws from an
     * independent, deterministically split random stream.
     *
     * @param <T> the type of the paired item
     */
    private static class ItemRandomPair<T> {
        @Nonnull
        private final T item;
        @Nonnull
        private final SplittableRandom random;

        public ItemRandomPair(@Nonnull final T item, @Nonnull final SplittableRandom random) {
            this.item = item;
            this.random = random;
        }

        @Nonnull
        public T getItem() {
            return item;
        }

        @Nonnull
        public SplittableRandom getRandom() {
            return random;
        }
    }

    public static final class GaussianSampler {
        private final SplittableRandom random;
        // Cached second sample (because each call produces 2 normals)
        private boolean hasSpare = false;
        private double spare;

        public GaussianSampler(final SplittableRandom random) {
            this.random = random;
        }

        public double nextGaussian() {
            if (hasSpare) {
                hasSpare = false;
                return spare;
            }

            double u;
            double v;
            double s;
            do {
                u = 2.0 * random.nextDouble() - 1.0; // [-1, 1]
                v = 2.0 * random.nextDouble() - 1.0; // [-1, 1]
                s = u * u + v * v;
            } while (s >= 1.0 || s == 0.0);

            double mul = Math.sqrt(-2.0 * Math.log(s) / s);

            spare = v * mul;
            hasSpare = true;

            return u * mul;
        }
    }
}
