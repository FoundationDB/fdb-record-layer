/*
 * ScalarBackend.java
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

package com.apple.foundationdb.linear;

import javax.annotation.Nonnull;

/**
 * Scalar (plain {@code for}-loop) implementation of {@link Backend}. Always available; serves as
 * the fallback when the SIMD backend cannot be loaded at runtime.
 */
final class ScalarBackend implements Backend {

    @Nonnull
    @Override
    public String name() {
        return "scalar";
    }

    @Override
    public void addInto(@Nonnull final double[] a, @Nonnull final double[] b, @Nonnull final double[] out) {
        for (int i = 0; i < a.length; i++) {
            out[i] = a[i] + b[i];
        }
    }

    @Override
    public void addInto(@Nonnull final double[] a, final double scalar, @Nonnull final double[] out) {
        for (int i = 0; i < a.length; i++) {
            out[i] = a[i] + scalar;
        }
    }

    @Override
    public void subtractInto(@Nonnull final double[] a, @Nonnull final double[] b, @Nonnull final double[] out) {
        for (int i = 0; i < a.length; i++) {
            out[i] = a[i] - b[i];
        }
    }

    @Override
    public void subtractInto(@Nonnull final double[] a, final double scalar, @Nonnull final double[] out) {
        for (int i = 0; i < a.length; i++) {
            out[i] = a[i] - scalar;
        }
    }

    @Override
    public void multiplyInto(@Nonnull final double[] a, final double scalar, @Nonnull final double[] out) {
        for (int i = 0; i < a.length; i++) {
            out[i] = a[i] * scalar;
        }
    }

    @Override
    public double dot(@Nonnull final double[] a, @Nonnull final double[] b) {
        double sum = 0.0d;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    @Override
    public double l2SquaredNorm(@Nonnull final double[] a) {
        double sum = 0.0d;
        for (int i = 0; i < a.length; i++) {
            sum += a[i] * a[i];
        }
        return sum;
    }

    @Override
    public double euclideanSquared(@Nonnull final double[] a, @Nonnull final double[] b) {
        double sum = 0.0d;
        for (int i = 0; i < a.length; i++) {
            final double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }
}
