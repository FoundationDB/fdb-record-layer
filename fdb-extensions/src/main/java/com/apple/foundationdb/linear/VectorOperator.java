/*
 * VectorOperator.java
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

import javax.annotation.Nonnull;

public interface VectorOperator {
    /**
     * Returns the numbers of dimensions a vector must have to be able to be applied or apply-inverted.
     * @return the numbers of dimensions this vector operator supports; can be {@code -1} if any number of dimensions
     *         is supported.
     */
    int getNumDimensions();

    @Nonnull
    RealVector apply(@Nonnull RealVector vector);

    @Nonnull
    RealVector applyInvert(@Nonnull RealVector vector);
}
