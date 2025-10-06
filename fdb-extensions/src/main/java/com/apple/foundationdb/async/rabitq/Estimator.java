/*
 * Estimator.java
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

public class Estimator {
    /** Estimate metric(queryRot, encodedVector) using ex-bits-only factors. */
    public static double estimate(final double[] queryRot,      // pre-rotated query q
                                  final double[] centroidRot,   // centroid c for this code (same rotated space)
                                  final int[] totalCode,        // packed sign+magnitude per dim
                                  final int exBits,             // B
                                  final double fAddEx,          // from ex_bits_code_with_factor at encode-time
                                  final double fRescaleEx       // from ex_bits_code_with_factor at encode-time
    ) {
        final int D = queryRot.length;
        final double cb = (1 << exBits) - 0.5;

        // dot((q - c), xu_cb), with xu_cb = totalCode - cb
        double gAdd = 0.0;
        double dot = 0.0;
        for (int i = 0; i < D; i++) {
            double qc  = queryRot[i] - centroidRot[i];
            double xuc = totalCode[i] - cb;
            gAdd += qc * qc;
            dot += qc * xuc;
        }
        // Same formula for both metrics; just ensure fAddEx/fRescaleEx were computed for that metric.
        return fAddEx + gAdd + fRescaleEx * dot;
    }

    /** Optional: same estimate but avoids recomputing (q - c) each time. */
    public static double estimateWithResidual(
            double[] residualRot,    // r = q - c (precomputed)
            int[] totalCode, int exBits,
            double fAddEx, double fRescaleEx
    ) {
        final int D = residualRot.length;
        final double cb = (1 << exBits) - 0.5;
        double dot = 0.0;
        for (int i = 0; i < D; i++) {
            dot += residualRot[i] * (totalCode[i] - cb);
        }
        return fAddEx + fRescaleEx * dot;
    }
}

