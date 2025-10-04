/*
 * QuantizerTest.java
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

package com.apple.foundationdb.async.rabitq;

import com.apple.foundationdb.async.hnsw.Metrics;
import org.junit.jupiter.api.Test;

import java.util.Random;

public class QuantizerTest {
    @Test
    void basicEncodeTest() {
        final int dims = 768;
        final Random random = new Random(System.nanoTime());
        final double[] v = createRandomVector(random, dims);
        final double[] centroid = new double[dims];
        final Quantizer.Result result =
                Quantizer.exBitsCodeWithFactor(v, centroid, 4, Metrics.EUCLIDEAN_METRIC);
        final double[] v_bar = normalize(v);
        final double[] recentered = new double[dims];
        for (int i = 0; i < dims; i ++) {
            recentered[i] = (double)result.signedCode[i] - 15.5d;
        }
        final double[] recentered_bar = normalize(recentered);
        System.out.println(dot(v_bar, recentered_bar));
    }

    private static double[] createRandomVector(final Random random, final int dims) {
        final double[] components = new double[dims];
        for (int d = 0; d < dims; d ++) {
            components[d] = random.nextDouble() * (random.nextBoolean() ? -1 : 1);
        }
        return components;
    }

    private static double l2(double[] x) {
        double s = 0.0;
        for (double v : x) {
            s += v * v;
        }
        return Math.sqrt(s);
    }

    private static double[] normalize(double[] x) {
        double n = l2(x);
        double[] y = new double[x.length];
        if (n == 0.0 || !Double.isFinite(n)) {
            return y; // all zeros
        }
        double inv = 1.0 / n;
        for (int i = 0; i < x.length; i++) {
            y[i] = x[i] * inv;
        }
        return y;
    }

    private static double dot(double[] a, double[] b) {
        double s = 0.0;
        for (int i = 0; i < a.length; i++) {
            s += a[i] * b[i];
        }
        return s;
    }
}
