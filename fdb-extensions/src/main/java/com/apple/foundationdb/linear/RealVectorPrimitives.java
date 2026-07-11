/*
 * RealVectorPrimitives.java
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * Package-private vector primitives that can be shared by all vector implementations (including
 * those not extending from {@link AbstractRealVector}). All methods contained in this class do not
 * assume an immutable or mutable vector they operate on.
 * <p>
 * Loops delegate to a {@link Backend} chosen at static-init time:
 * <ul>
 *   <li>Default ({@code auto}): try {@code com.apple.foundationdb.linear.simd.SimdBackend}; fall
 *       back to {@link ScalarBackend} if it can't be loaded (typically because
 *       {@code --add-modules jdk.incubator.vector} wasn't passed at runtime).</li>
 *   <li>{@code -Dfdb.vector.simd=scalar}: force {@link ScalarBackend} regardless.</li>
 *   <li>{@code -Dfdb.vector.simd=simd}: try the SIMD backend and propagate failure (useful in
 *       tests that want to guarantee SIMD is active).</li>
 * </ul>
 */
public final class RealVectorPrimitives {
    private static final Logger logger = LoggerFactory.getLogger(RealVectorPrimitives.class);
    private static final String simdBackendClassName = "com.apple.foundationdb.linear.simd.SimdBackend";
    private static final String simdPropertyName = "fdb.vector.simd";

    private static final Backend backend = selectBackend();

    /**
     * A permanently-scalar backend backing the {@code …Exact} primitives below. Unlike
     * {@link #backend}, this never resolves to SIMD, so its reductions always use the fixed
     * left-to-right, non-fused accumulation order of {@link ScalarBackend} — the same order
     * {@code -Dfdb.vector.simd=scalar} forces globally, but here scoped to individual calls.
     * <p>
     * SIMD reductions sum partial lanes in a different order (and fuse multiply-adds), so their
     * low-order bits can differ from scalar by a few ULPs and even differ between two SIMD hosts
     * of different vector widths. That is harmless for search-time distance math, but fatal for
     * any value that is persisted and later compared byte-for-byte across machines. The exact
     * variants give such callers — principally RaBitQ encoding, whose calibration constants and
     * quantized codes feed a stored content signature — a machine-independent result while
     * leaving the ambient {@link #backend} free to use SIMD everywhere else.
     */
    private static final Backend scalarBackend = new ScalarBackend();

    private RealVectorPrimitives() {
        // nothing
    }

    @Nonnull
    static Backend backend() {
        return backend;
    }

    /**
     * Functional seam for loading the SIMD backend. Production wiring uses the reflective
     * {@link #loadSimdBackend()}; tests pass a stub so {@link #selectBackend(String, SimdBackendLoader)}
     * can exercise every branch without depending on whether {@code jdk.incubator.vector} is
     * actually on the module path.
     */
    @FunctionalInterface
    interface SimdBackendLoader {
        @Nonnull
        Backend load() throws ReflectiveOperationException;
    }

    @Nonnull
    private static Backend selectBackend() {
        return selectBackend(System.getProperty(simdPropertyName, "auto"), RealVectorPrimitives::loadSimdBackend);
    }

    /**
     * Reflectively loads and instantiates the SIMD backend. Package-private and
     * {@link VisibleForTesting} so an integration test running <em>without</em>
     * {@code --add-modules jdk.incubator.vector} can pass this real loader to
     * {@link #selectBackend(String, SimdBackendLoader)} and verify the genuine module-absent
     * behavior (auto-fallback to scalar, or strict failure).
     *
     * @return a freshly instantiated SIMD backend
     * @throws ReflectiveOperationException if the SIMD backend class cannot be loaded or instantiated
     */
    @Nonnull
    @VisibleForTesting
    @SuppressWarnings("PMD.UseProperClassLoader")
    static Backend loadSimdBackend() throws ReflectiveOperationException {
        final Class<?> cls = Class.forName(
                simdBackendClassName, true, RealVectorPrimitives.class.getClassLoader());
        return (Backend) cls.getDeclaredConstructor().newInstance();
    }

    /**
     * Pure backend-selection logic, separated from the JVM-global system property and module state
     * so every branch is unit-testable. The branches are:
     * <ul>
     *   <li>{@code mode == "scalar"} → {@link ScalarBackend} (the loader is never consulted);</li>
     *   <li>otherwise attempt {@code simdLoader.load()}: on success use it; on failure either throw
     *       (when {@code mode == "simd"}, the strict opt-in) or fall back to {@link ScalarBackend}
     *       (the default {@code auto} mode).</li>
     * </ul>
     *
     * @param modeProperty raw value of the {@code fdb.vector.simd} property ({@code auto} by default)
     * @param simdLoader supplier of the SIMD backend; throws if it cannot be loaded
     * @return the selected backend
     */
    @Nonnull
    @VisibleForTesting
    static Backend selectBackend(@Nonnull final String modeProperty, @Nonnull final SimdBackendLoader simdLoader) {
        final String mode = modeProperty.toLowerCase(Locale.getDefault());
        if ("scalar".equals(mode)) {
            logger.info("RealVectorPrimitives backend forced to scalar via -D{}", simdPropertyName);
            return new ScalarBackend();
        }
        final boolean strict = "simd".equals(mode);
        try {
            final Backend candidate = simdLoader.load();
            if (logger.isInfoEnabled()) {
                logger.info("RealVectorPrimitives backend = {}", candidate.name());
            }
            return candidate;
        } catch (final RuntimeException | ReflectiveOperationException | NoClassDefFoundError e) {
            if (strict) {
                throw new IllegalStateException(
                        "SIMD backend required (-D" + simdPropertyName + "=simd) but not loadable: " + e, e);
            }
            if (logger.isInfoEnabled()) {
                logger.info("SIMD vector backend unavailable, using scalar: {}: {}", e.getClass().getSimpleName(), e.getMessage());
            }
            return new ScalarBackend();
        }
    }

    @Nonnull
    static double[] normalizeInto(@Nonnull final double[] in, @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == in.length);
        final double n = Math.sqrt(backend.l2SquaredNorm(in));
        if (n == 0.0d || !Double.isFinite(n)) {
            throw new IllegalArgumentException("vector has an L2 norm of infinite, not a number, or 0");
        }
        backend.multiplyInto(in, 1.0d / n, target);
        return target;
    }

    @Nonnull
    static double[] addInto(@Nonnull final double[] a,
                            @Nonnull final double[] b,
                            @Nonnull final double[] target) {
        Preconditions.checkArgument(a.length == b.length);
        Preconditions.checkArgument(target.length == a.length);
        backend.addInto(a, b, target);
        return target;
    }

    @Nonnull
    static double[] addInto(@Nonnull final double[] a,
                            final double scalar,
                            @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == a.length);
        backend.addInto(a, scalar, target);
        return target;
    }

    @Nonnull
    static double[] subtractInto(@Nonnull final double[] a,
                                 @Nonnull final double[] b,
                                 @Nonnull final double[] target) {
        Preconditions.checkArgument(a.length == b.length);
        Preconditions.checkArgument(target.length == a.length);
        backend.subtractInto(a, b, target);
        return target;
    }

    @Nonnull
    static double[] subtractInto(@Nonnull final double[] a,
                                 final double scalar,
                                 @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == a.length);
        backend.subtractInto(a, scalar, target);
        return target;
    }

    @Nonnull
    static double[] multiplyInto(@Nonnull final double[] a,
                                 final double scalar,
                                 @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == a.length);
        backend.multiplyInto(a, scalar, target);
        return target;
    }

    static void multiplyAddInto(final double scalar, @Nonnull final double[] x, @Nonnull final double[] y,
                                final int from, final int length) {
        // BLAS-style in-place AXPY: y := scalar * x + y over the slice. Delegates to the
        // backend's general 3-operand form with out aliased to y; the per-lane FMA is the
        // same instruction either way.
        backend.multiplyAddInto(scalar, x, y, y, from, length);
    }

    static void multiplyAddInto(final double scalar, @Nonnull final double[] x, @Nonnull final double[] y,
                                @Nonnull final double[] out, final int from, final int length) {
        backend.multiplyAddInto(scalar, x, y, out, from, length);
    }

    static double dot(@Nonnull final double[] a, @Nonnull final double[] b) {
        Preconditions.checkArgument(a.length == b.length);
        return backend.dot(a, b);
    }

    static double dot(@Nonnull final double[] a, @Nonnull final double[] b, final int from, final int length) {
        return backend.dot(a, b, from, length);
    }

    static double l2SquaredNorm(@Nonnull final double[] a) {
        return backend.l2SquaredNorm(a);
    }

    static double l2SquaredNorm(@Nonnull final double[] a, final int from, final int length) {
        return backend.l2SquaredNorm(a, from, length);
    }

    static double euclideanSquared(@Nonnull final double[] a, @Nonnull final double[] b) {
        Preconditions.checkArgument(a.length == b.length);
        return backend.euclideanSquared(a, b);
    }

    //
    // Scalar-forced ("exact") reduction variants. These bypass the ambient backend and always
    // compute on {@link #scalarBackend}, so the result is bit-reproducible across machines and
    // JVMs. See {@link #scalarBackend} for why this matters. Only reductions (and the
    // reduction-derived {@code normalizeInto}) need exact variants: the element-wise primitives
    // ({@code addInto}, {@code subtractInto}, {@code multiplyInto}) round each lane
    // independently and are already bit-identical on both backends.
    //

    static double dotExact(@Nonnull final double[] a, @Nonnull final double[] b) {
        Preconditions.checkArgument(a.length == b.length);
        return scalarBackend.dot(a, b);
    }

    static double l2SquaredNormExact(@Nonnull final double[] a) {
        return scalarBackend.l2SquaredNorm(a);
    }

    static double euclideanSquaredExact(@Nonnull final double[] a, @Nonnull final double[] b) {
        Preconditions.checkArgument(a.length == b.length);
        return scalarBackend.euclideanSquared(a, b);
    }

    @Nonnull
    static double[] normalizeIntoExact(@Nonnull final double[] in, @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == in.length);
        final double n = Math.sqrt(scalarBackend.l2SquaredNorm(in));
        if (n == 0.0d || !Double.isFinite(n)) {
            throw new IllegalArgumentException("vector has an L2 norm of infinite, not a number, or 0");
        }
        scalarBackend.multiplyInto(in, 1.0d / n, target);
        return target;
    }
}
