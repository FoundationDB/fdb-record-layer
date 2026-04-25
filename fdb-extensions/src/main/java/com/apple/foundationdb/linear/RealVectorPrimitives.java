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

import javax.annotation.Nonnull;

/**
 * Package-private vector primitives that can be shared by all vector implementations (including those not extending
 * from {@link AbstractRealVector}. All methods contained in this class do not assume an immutable or mutable vector
 * they operate on.
 */
public final class RealVectorPrimitives {
    private RealVectorPrimitives() {
        // nothing
    }

    @Nonnull
    static double[] normalizeInto(@Nonnull final RealVector in, @Nonnull final double[] target) {
        double n = in.l2Norm();
        final int numDimensions = in.getNumDimensions();
        Preconditions.checkArgument(target.length == numDimensions);

        if (n == 0.0 || !Double.isFinite(n)) {
            throw new IllegalArgumentException("vector has an L2 norm of infinite, not a number, or 0");
        }
        double inv = 1.0 / n;
        for (int i = 0; i < numDimensions; i++) {
            target[i] = in.getComponent(i) * inv;
        }
        return target;
    }

    @Nonnull
    static double[] addInto(@Nonnull final RealVector in,
                            @Nonnull final RealVector other,
                            @Nonnull final double[] target) {
        final int numDimensions = in.getNumDimensions();
        Preconditions.checkArgument(numDimensions == other.getNumDimensions());
        Preconditions.checkArgument(target.length == numDimensions);

        for (int i = 0; i < numDimensions; i ++) {
            target[i] = in.getComponent(i) + other.getComponent(i);
        }
        return target;
    }

    @Nonnull
    static double[] addInto(@Nonnull final RealVector in,
                            final double scalar,
                            @Nonnull final double[] target) {
        final int numDimensions = in.getNumDimensions();
        Preconditions.checkArgument(target.length == numDimensions);

        for (int i = 0; i < numDimensions; i ++) {
            target[i] = in.getComponent(i) + scalar;
        }
        return target;
    }

    @Nonnull
    static double[] subtractInto(@Nonnull final RealVector in,
                                 @Nonnull final RealVector other,
                                 @Nonnull final double[] target) {
        final int numDimensions = in.getNumDimensions();
        Preconditions.checkArgument(numDimensions == other.getNumDimensions());
        Preconditions.checkArgument(target.length == numDimensions);

        for (int i = 0; i < numDimensions; i ++) {
            target[i] = in.getComponent(i) - other.getComponent(i);
        }
        return target;
    }

    @Nonnull
    static double[] subtractInto(@Nonnull final RealVector in,
                                 final double scalar,
                                 @Nonnull final double[] target) {
        final int numDimensions = in.getNumDimensions();
        Preconditions.checkArgument(target.length == numDimensions);

        for (int i = 0; i < numDimensions; i ++) {
            target[i] = in.getComponent(i) - scalar;
        }
        return target;
    }

    @Nonnull
    static double[] multiplyInto(@Nonnull final RealVector in,
                                 final double scalar,
                                 @Nonnull final double[] target) {
        final int numDimensions = in.getNumDimensions();
        Preconditions.checkArgument(target.length == numDimensions);

        for (int i = 0; i < numDimensions; i ++) {
            target[i] = in.getComponent(i) * scalar;
        }
        return target;
    }
}
