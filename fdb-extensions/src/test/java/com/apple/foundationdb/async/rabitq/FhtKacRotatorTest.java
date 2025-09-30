/*
 * FhtKacRotatorTest.java
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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class FhtKacRotatorTest {
    @Test
    void testSimpleTest() {
        int n = 3001;
        final FhtKacRotator rotator = new FhtKacRotator(n);

        double[] x = new double[n];
        for (int i = 0; i < n; i++) {
            x[i] = (i % 7) - 3; // some data
        }

        double[] y = rotator.apply(x, null);
        double[] z = rotator.applyTranspose(y, null);

        // Verify ||x|| ≈ ||y|| and P^T P ≈ I
        double nx = FhtKacRotator.norm2(x);
        double ny = FhtKacRotator.norm2(y);
        double maxErr = FhtKacRotator.maxAbsDiff(x, z);
        System.out.printf("||x|| = %.6f  ||Px|| = %.6f  max|x - P^T P x|=%.3e%n", nx, ny, maxErr);
    }

    @Test
    void testOrthogonality() {
        final int n = 3000;
        long startTs = System.nanoTime();
        final FhtKacRotator rotator = new FhtKacRotator(n);
        double durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTs);
        System.out.println("rotator created in: " +  durationMs + " ms.");
        startTs = System.nanoTime();
        final Matrix p = new RowMajorMatrix(rotator.computeP());
        durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTs);
        System.out.println("P computed in: " +  durationMs + " ms.");
        startTs = System.nanoTime();
        final Matrix product = p.transpose().multiply(p);
        durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTs);
        System.out.println("P^T * P computed in: " +  durationMs + " ms.");

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                double expected = (i == j) ? 1.0 : 0.0;
                Assertions.assertThat(Math.abs(product.getEntry(i, j) - expected))
                        .satisfies(difference -> Assertions.assertThat(difference).isLessThan(10E-9d));
            }
        }
    }
}
