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
    private static final Logger LOGGER = LoggerFactory.getLogger(RealVectorPrimitives.class);
    private static final String SIMD_BACKEND_CLASS = "com.apple.foundationdb.linear.simd.SimdBackend";
    private static final String SIMD_PROPERTY = "fdb.vector.simd";

    private static final Backend BACKEND = selectBackend();

    private RealVectorPrimitives() {
        // nothing
    }

    @Nonnull
    static Backend backend() {
        return BACKEND;
    }

    @Nonnull
    @SuppressWarnings("PMD.UseProperClassLoader")
    private static Backend selectBackend() {
        final String mode = System.getProperty(SIMD_PROPERTY, "auto").toLowerCase(Locale.getDefault());
        if ("scalar".equals(mode)) {
            LOGGER.info("RealVectorPrimitives backend forced to scalar via -D{}", SIMD_PROPERTY);
            return new ScalarBackend();
        }
        final boolean strict = "simd".equals(mode);
        try {
            final Class<?> cls = Class.forName(
                    SIMD_BACKEND_CLASS, true, RealVectorPrimitives.class.getClassLoader());
            final Backend candidate = (Backend) cls.getDeclaredConstructor().newInstance();
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("RealVectorPrimitives backend = {}", candidate.name());
            }
            return candidate;
        } catch (final RuntimeException | ReflectiveOperationException e) {
            if (strict) {
                throw new IllegalStateException(
                        "SIMD backend required (-D" + SIMD_PROPERTY + "=simd) but not loadable: " + e, e);
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("SIMD vector backend unavailable, using scalar: {}", e.toString());
            }
            return new ScalarBackend();
        }
    }

    @Nonnull
    static double[] normalizeInto(@Nonnull final double[] in, @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == in.length);
        final double n = Math.sqrt(BACKEND.l2SquaredNorm(in));
        if (n == 0.0d || !Double.isFinite(n)) {
            throw new IllegalArgumentException("vector has an L2 norm of infinite, not a number, or 0");
        }
        BACKEND.multiplyInto(in, 1.0d / n, target);
        return target;
    }

    @Nonnull
    static double[] addInto(@Nonnull final double[] a,
                            @Nonnull final double[] b,
                            @Nonnull final double[] target) {
        Preconditions.checkArgument(a.length == b.length);
        Preconditions.checkArgument(target.length == a.length);
        BACKEND.addInto(a, b, target);
        return target;
    }

    @Nonnull
    static double[] addInto(@Nonnull final double[] a,
                            final double scalar,
                            @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == a.length);
        BACKEND.addInto(a, scalar, target);
        return target;
    }

    @Nonnull
    static double[] subtractInto(@Nonnull final double[] a,
                                 @Nonnull final double[] b,
                                 @Nonnull final double[] target) {
        Preconditions.checkArgument(a.length == b.length);
        Preconditions.checkArgument(target.length == a.length);
        BACKEND.subtractInto(a, b, target);
        return target;
    }

    @Nonnull
    static double[] subtractInto(@Nonnull final double[] a,
                                 final double scalar,
                                 @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == a.length);
        BACKEND.subtractInto(a, scalar, target);
        return target;
    }

    @Nonnull
    static double[] multiplyInto(@Nonnull final double[] a,
                                 final double scalar,
                                 @Nonnull final double[] target) {
        Preconditions.checkArgument(target.length == a.length);
        BACKEND.multiplyInto(a, scalar, target);
        return target;
    }

    static double dot(@Nonnull final double[] a, @Nonnull final double[] b) {
        Preconditions.checkArgument(a.length == b.length);
        return BACKEND.dot(a, b);
    }

    static double l2SquaredNorm(@Nonnull final double[] a) {
        return BACKEND.l2SquaredNorm(a);
    }

    static double euclideanSquared(@Nonnull final double[] a, @Nonnull final double[] b) {
        Preconditions.checkArgument(a.length == b.length);
        return BACKEND.euclideanSquared(a, b);
    }
}
