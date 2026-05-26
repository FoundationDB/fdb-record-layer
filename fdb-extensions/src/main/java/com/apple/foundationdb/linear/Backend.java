/*
 * Backend.java
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
 * Per-component vector arithmetic primitives on {@code double[]} arrays. Implementations may be a
 * pure-Java scalar loop or a SIMD-using variant; {@link RealVectorPrimitives} dispatches to the
 * implementation it picks at static-init time.
 * <p>
 * Implementations live in {@link com.apple.foundationdb.linear} (scalar) and
 * {@code com.apple.foundationdb.linear.simd} (SIMD). The interface is {@code public} only because
 * the SIMD implementation in the sub-package has to be able to {@code implements} it; this is not
 * a stable extension point for external callers.
 * <p>
 * All array-mutating methods are permitted to write to an output array that aliases an input
 * (e.g. {@code add(a, b, a)}) — the operations are per-element and have no cross-element data
 * dependencies. Reduction methods return a scalar and do not mutate inputs.
 * <p>
 * Callers must ensure array length consistency before invoking these methods; implementations may
 * assume {@code a.length == b.length == out.length} and skip length checks for speed.
 */
public interface Backend {
    /**
     * Returns a short, human-readable identifier of this backend (e.g. {@code "scalar"} or
     * {@code "simd[Species[double, 8, ...]]"}). Used by the dispatch logger and the
     * {@code BackendSelectionTest} to confirm which implementation is active.
     *
     * @return a non-null label that uniquely identifies this backend at runtime
     */
    @Nonnull
    String name();

    /**
     * Computes the element-wise sum {@code out[i] = a[i] + b[i]} for every {@code i}.
     * <p>
     * {@code out} may alias {@code a} or {@code b} — each lane is loaded from its source positions
     * before any value is written back, so in-place updates such as {@code addInto(x, y, x)} are
     * supported.
     *
     * @param a left operand
     * @param b right operand
     * @param out destination array; receives {@code a + b} component-wise. Must have the same
     *        length as {@code a} and {@code b}
     */
    void addInto(@Nonnull double[] a, @Nonnull double[] b, @Nonnull double[] out);

    /**
     * Computes the broadcast sum {@code out[i] = a[i] + scalar} for every {@code i}.
     * <p>
     * {@code out} may alias {@code a}.
     *
     * @param a left operand
     * @param scalar value broadcast across every component of {@code a}
     * @param out destination array; receives {@code a + scalar} component-wise. Must have the
     *        same length as {@code a}
     */
    void addInto(@Nonnull double[] a, double scalar, @Nonnull double[] out);

    /**
     * Computes the element-wise difference {@code out[i] = a[i] - b[i]} for every {@code i}.
     * <p>
     * {@code out} may alias {@code a} or {@code b}.
     *
     * @param a minuend
     * @param b subtrahend
     * @param out destination array; receives {@code a - b} component-wise. Must have the same
     *        length as {@code a} and {@code b}
     */
    void subtractInto(@Nonnull double[] a, @Nonnull double[] b, @Nonnull double[] out);

    /**
     * Computes the broadcast difference {@code out[i] = a[i] - scalar} for every {@code i}.
     * <p>
     * {@code out} may alias {@code a}.
     *
     * @param a minuend
     * @param scalar value broadcast across every component of {@code a}
     * @param out destination array; receives {@code a - scalar} component-wise. Must have the
     *        same length as {@code a}
     */
    void subtractInto(@Nonnull double[] a, double scalar, @Nonnull double[] out);

    /**
     * Computes the broadcast product {@code out[i] = a[i] * scalar} for every {@code i}. Used
     * indirectly by {@link RealVectorPrimitives#normalizeInto} to scale a vector by
     * {@code 1 / l2Norm}.
     * <p>
     * {@code out} may alias {@code a}.
     *
     * @param a operand
     * @param scalar value broadcast across every component of {@code a}
     * @param out destination array; receives {@code a * scalar} component-wise. Must have the
     *        same length as {@code a}
     */
    void multiplyInto(@Nonnull double[] a, double scalar, @Nonnull double[] out);

    /**
     * Returns the dot product {@code Σ a[i] * b[i]} as a single {@code double}.
     * <p>
     * SIMD implementations typically use fused multiply-add and may sum partial lanes in a
     * different order than a strict left-to-right scalar accumulation, so the low-order bits of
     * the result can differ from a scalar reference by a few ULPs. Callers that depend on
     * bit-exact reproducibility should force the scalar backend via
     * {@code -Dfdb.vector.simd=scalar}.
     *
     * @param a left operand
     * @param b right operand; must have the same length as {@code a}
     * @return the dot product of {@code a} and {@code b}
     */
    double dot(@Nonnull double[] a, @Nonnull double[] b);

    /**
     * Returns the squared L2 norm {@code Σ a[i] * a[i]} as a single {@code double}. Equivalent
     * to {@code dot(a, a)} but typically faster because it requires only one array load per
     * lane.
     * <p>
     * Same reduction-order caveat as {@link #dot(double[], double[])}.
     *
     * @param a vector to take the norm of
     * @return the squared L2 norm of {@code a}
     */
    double l2SquaredNorm(@Nonnull double[] a);

    /**
     * Returns the squared Euclidean distance {@code Σ (a[i] - b[i])^2} as a single {@code double}.
     * Equivalent to {@code dot(a - b, a - b)} but fused into a single pass that avoids
     * materializing the difference vector.
     * <p>
     * Same reduction-order caveat as {@link #dot(double[], double[])}.
     *
     * @param a left operand
     * @param b right operand; must have the same length as {@code a}
     * @return the squared Euclidean distance between {@code a} and {@code b}
     */
    double euclideanSquared(@Nonnull double[] a, @Nonnull double[] b);
}
