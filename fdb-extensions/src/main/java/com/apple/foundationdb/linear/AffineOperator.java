/*
 * AffineOperator.java
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

package com.apple.foundationdb.linear;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Vector operator that applies/unapplies a linear operator and an addition to a vector.
 */
@SpotBugsSuppressWarnings(value = "SING_SINGLETON_HAS_NONPRIVATE_CONSTRUCTOR", justification = "Singleton designation is a false positive")
public class AffineOperator implements VectorOperator {
    private static final AffineOperator IDENTITY_OPERATOR = new AffineOperator(null, null);

    @Nullable
    private final LinearOperator linearOperator;
    @Nullable
    private final RealVector translationVector;

    public AffineOperator(@Nullable final LinearOperator linearOperator, @Nullable final RealVector translationVector) {
        Preconditions.checkArgument(linearOperator == null || translationVector == null ||
                linearOperator.getNumColumnDimensions() == translationVector.getNumDimensions());
        this.linearOperator = linearOperator;
        this.translationVector = translationVector;
    }

    @Override
    public int getNumDimensions() {
        return linearOperator != null
               ? linearOperator.getNumDimensions()
               : (translationVector != null
                  ? translationVector.getNumDimensions()
                  : -1);
    }

    @Nonnull
    @Override
    public RealVector apply(@Nonnull final RealVector vector) {
        if (linearOperator == null && translationVector == null) {
            return vector;
        }
        if (linearOperator == null /* && translationVector != null */) {
            return vector.add(translationVector);
        }
        if (/* linearOperator != null && */ translationVector == null) {
            return linearOperator.apply(vector);
        }
        return linearOperator.apply(vector.add(translationVector));
    }

    @Nonnull
    @Override
    public RealVector applyInvert(@Nonnull final RealVector vector) {
        if (linearOperator == null && translationVector == null) {
            return vector;
        }
        if (linearOperator == null /* && translationVector != null */) {
            return vector.subtract(translationVector);
        }
        if (/* linearOperator != null && */ translationVector == null) {
            return linearOperator.applyTranspose(vector);
        }
        return linearOperator.applyTranspose(vector).subtract(translationVector);
    }

    @Nonnull
    public static AffineOperator identity() {
        return IDENTITY_OPERATOR;
    }
}
