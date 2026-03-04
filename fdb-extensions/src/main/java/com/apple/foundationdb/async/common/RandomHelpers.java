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

import javax.annotation.Nonnull;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public final class RandomHelpers {
    private RandomHelpers() {
        // nothing
    }

    /**
     * Seed a {@link SplittableRandom} in a deterministic way using the primary key of a record.
     * @param someIdentity something fairly unique
     * @return a new {@link SplittableRandom}
     */
    @Nonnull
    public static SplittableRandom random(@Nonnull final Object someIdentity) {
        return new SplittableRandom(splitMixLong(someIdentity.hashCode()));
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
     * output mixing function {@code SplitMix64} as employed by many PRNG such as {@link SplittableRandom}.
     * See <a href="https://en.wikipedia.org/wiki/Linear_congruential_generator">Linear congruential generator</a> for
     * more information.
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

    @Nonnull
    public static UUID nextUuid(@Nonnull final SplittableRandom random, final boolean sequential) {
        return sequential ? SequentialUUID.getNext() : randomUUID(random);
    }

    @Nonnull
    public static UUID randomUUID(@Nonnull final SplittableRandom random) {
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

    private static class SequentialUUID {
        private static final AtomicLong sequenceAtomic = new AtomicLong(0L);

        @Nonnull
        private static UUID getNext() {
            return new UUID(0, sequenceAtomic.getAndIncrement());
        }
    }
}
