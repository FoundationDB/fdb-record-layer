/*
 * Transformed.java
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

import com.google.common.base.Objects;

import javax.annotation.Nonnull;

/**
 * This class aims to reduce potential logic problems with respect to coordinate transformations by soliciting help from
 * Java's type system.
 * <p>
 * While implementing complex algorithms that required coordinate transformations, some problems seemed to occur
 * repeatedly and the following observations were made:
 * <ul>
 *     <li>A few algorithms use an API that passes vectors back and forth in a coordinate system given by the user.
 *     Internally, however, the same algorithms transform these vectors into some other coordinate system that is more
 *     advantageous to the algorithm in some way. Therefore, vectors are constantly transformed back and forth between
 *     the respective coordinate systems.</li>
 *     <li>We observed cases where there are mixtures of vectors handled withing the same methods, i.e. some vectors
 *     were expressed using the internal and some vectors were expressed using the external coordinate system.
 *     Problems occur when these vectors are intermingled and the coordinate system mappings of the individual vectors
 *     are lost.
 *     </li>
 *     <li>
 *     We observed cases where a vector is transformed from one coordinate system to another one and then erroneously
 *     transformed a second time.
 *     </li>
 * </ul>
 * <p>
 * The following approach only makes sense for scenarios that deal with exactly two coordinate systems.
 * <p>
 * We would like to express vectors in one system by {@link RealVector} whereas the vectors in the secondary system
 * are expressed using {@link Transformed} of {@link RealVector}. The hope is that Java's compiler can assist in
 * avoiding using the wrong sort of vector in the wrong situation. While it is possible to circumvent these best-effort
 * type system-imposed restrictions, this class is meant to be utilized in a more pragmatic way.
 * <p>
 * Objects of this class wrap some vector of type {@code V} creating a transformed vector. The visibility of
 * this class' constructor is package-private by design. Only operators implementing {@link VectorOperator} can
 * transform an instance of type {@code V} extends {@link RealVector} into a {@code Transformed} object. The same is
 * true for inverse transformations: only operators can transform a {@code Transformed} vector back to the original
 * vector.
 * <p>
 * In other places where {@code Transformed}s are created (and destructed) users should be aware of exactly what happens
 * and why. We tried to restrict visibilities of constructors and accessors, but due to Java's lack in expressiveness
 * when it comes to type system finesse, this is a best-effort approach. If a {@code Transformed} is
 * <i>deconstructed</i> using {@link #getUnderlyingVector()}, the user should ensure that the resulting vector is not
 * further transformed by e.g. another affine operator.
 * In short, we want to avoid users to write code similar to
 * {@code someNewOperator.transform(oldTransformed.getUnderlyingVector()} as the result would be a transformed vector
 * that is in fact transformed a second time. Note that this can make sense in some cases, however, in the described
 * use case it mostly does not.
 * @param <V> the wrapped kind of {@link RealVector}
 */
public final class Transformed<V extends RealVector> {
    @Nonnull
    private final V transformedVector;

    Transformed(@Nonnull final V transformedVector) {
        this.transformedVector = transformedVector;
    }

    @Nonnull
    public V getUnderlyingVector() {
        return transformedVector;
    }

    public Transformed<RealVector> add(@Nonnull Transformed<? extends RealVector> other) {
        return new Transformed<>(transformedVector.add(other.transformedVector));
    }

    public Transformed<RealVector> multiply(double scalarFactor) {
        return new Transformed<>(transformedVector.multiply(scalarFactor));
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Transformed)) {
            return false;
        }
        final Transformed<?> that = (Transformed<?>)o;
        return Objects.equal(transformedVector, that.transformedVector);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(transformedVector);
    }

    @Override
    public String toString() {
        return transformedVector.toString();
    }
}
