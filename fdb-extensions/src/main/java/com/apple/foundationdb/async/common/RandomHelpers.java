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

import java.util.SplittableRandom;

public final class RandomHelpers {
    private RandomHelpers() {
        // nothing
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
