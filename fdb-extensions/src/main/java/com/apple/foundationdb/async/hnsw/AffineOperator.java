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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.linear.LinearOperator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.VectorOperator;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

public class AffineOperator implements VectorOperator {
    @Nonnull
    private final LinearOperator linearOperator;
    @Nonnull
    private final RealVector translationVector;

    public AffineOperator(@Nonnull final LinearOperator linearOperator, @Nonnull final RealVector translationVector) {
        Preconditions.checkArgument(linearOperator.getNumColumnDimensions() == translationVector.getNumDimensions());
        this.linearOperator = linearOperator;
        this.translationVector = translationVector;
    }

    @Override
    public int getNumDimensions() {
        return translationVector.getNumDimensions();
    }

    @Nonnull
    @Override
    public RealVector apply(@Nonnull final RealVector vector) {
        return linearOperator.apply(vector.add(translationVector));
    }

    @Nonnull
    @Override
    public RealVector applyInvert(@Nonnull final RealVector vector) {
        return linearOperator.applyTranspose(vector).subtract(translationVector);
    }
}
