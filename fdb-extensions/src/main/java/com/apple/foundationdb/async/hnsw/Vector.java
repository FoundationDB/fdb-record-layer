/*
 * Vector.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.christianheina.langx.half4j.Half;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

/**
 * An abstract base class representing a mathematical vector.
 * <p>
 * This class provides a generic framework for vectors of different numerical types,
 * where {@code R} is a subtype of {@link Number}. It includes common operations and functionalities like size,
 * component access, equality checks, and conversions. Concrete implementations must provide specific logic for
 * data type conversions and raw data representation.
 */
public interface Vector {
    /**
     * Returns the number of elements in the vector, i.e. the number of dimensions.
     * @return the number of dimensions
     */
    int getNumDimensions();

    /**
     * Gets the component of this object at the specified dimension.
     * <p>
     * The dimension is a zero-based index. For a 3D vector, for example, dimension 0 might correspond to the
     * x-component, 1 to the y-component, and 2 to the z-component. This method provides direct access to the
     * underlying data element.
     * @param dimension the zero-based index of the component to retrieve.
     * @return the component at the specified dimension, which is guaranteed to be non-null.
     * @throws IndexOutOfBoundsException if the {@code dimension} is negative or
     *         greater than or equal to the number of dimensions of this object.
     */
    double getComponent(int dimension);

    /**
     * Returns the underlying data array.
     * <p>
     * The returned array is guaranteed to be non-null. Note that this method
     * returns a direct reference to the internal array, not a copy.
     * @return the data array of type {@code R[]}, never {@code null}.
     */
    @Nonnull
    double[] getData();

    @Nonnull
    Vector withData(@Nonnull double[] data);

    /**
     * Gets the raw byte data representation of this object.
     * <p>
     * This method provides a direct, unprocessed view of the object's underlying data. The format of the byte array is
     * implementation-specific and should be documented by the concrete class that implements this method.
     * @return a non-null byte array containing the raw data.
     */
    @Nonnull
    byte[] getRawData();

    /**
     * Converts this object into a {@code Vector} of {@link Half} precision floating-point numbers.
     * <p>
     * As this is an abstract method, implementing classes are responsible for defining the specific conversion logic
     * from their internal representation to a {@code Vector} of {@link Half} objects. If this object already is a
     * {@code HalfVector} this method should return {@code this}.
     * @return a non-null {@link Vector} containing the {@link Half} precision floating-point representation of this
     *         object.
     */
    @Nonnull
    HalfVector toHalfVector();

    /**
     * Converts this vector into a {@link DoubleVector}.
     * <p>
     * This method provides a way to obtain a double-precision floating-point representation of the vector. If the
     * vector is already an instance of {@code DoubleVector}, this method may return the instance itself. Otherwise,
     * it will create a new {@code DoubleVector} containing the same elements, which may involve a conversion of the
     * underlying data type.
     * @return a non-null {@link DoubleVector} representation of this vector.
     */
    @Nonnull
    DoubleVector toDoubleVector();

    default double dot(@Nonnull final Vector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        double sum = 0.0d;
        for (int i = 0; i < getNumDimensions(); i ++) {
            sum += getComponent(i) * other.getComponent(i);
        }
        return sum;
    }

    default double l2Norm() {
        return Math.sqrt(dot(this));
    }

    @Nonnull
    default Vector normalize() {
        double n = l2Norm();
        final int numDimensions = getNumDimensions();
        double[] y = new double[numDimensions];
        if (n == 0.0 || !Double.isFinite(n)) {
            return withData(y); // all zeros
        }
        double inv = 1.0 / n;
        for (int i = 0; i < numDimensions; i++) {
            y[i] = getComponent(i) * inv;
        }
        return withData(y);
    }

    @Nonnull
    default Vector add(@Nonnull final Vector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) + other.getComponent(i);
        }
        return withData(result);
    }

    @Nonnull
    default Vector add(final double scalar) {
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) + scalar;
        }
        return withData(result);
    }

    @Nonnull
    default Vector subtract(@Nonnull final Vector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) - other.getComponent(i);
        }
        return withData(result);
    }

    @Nonnull
    default Vector subtract(final double scalar) {
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) - scalar;
        }
        return withData(result);
    }

    @Nonnull
    default Vector multiply(final double scalar) {
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) * scalar;
        }
        return withData(result);
    }

    /**
     * Calculates the distance between two vectors using a specified metric.
     * <p>
     * This static utility method provides a convenient way to compute the distance by handling the conversion of
     * generic {@code Vector<R>} objects to primitive {@code double} arrays. The actual distance computation is then
     * delegated to the provided {@link Metrics} instance.
     * @param metric the {@link Metrics} to use for the distance calculation.
     * @param vector1 the first vector.
     * @param vector2 the second vector.
     * @return the calculated distance between the two vectors as a {@code double}.
     */
    static double distance(@Nonnull final Metrics metric,
                           @Nonnull final Vector vector1,
                           @Nonnull final Vector vector2) {
        return metric.distance(vector1.getData(), vector2.getData());
    }

    /**
     * Calculates the comparative distance between two vectors using a specified metric.
     * <p>
     * This utility method converts the input vectors, which can contain any {@link Number} type, into primitive double
     * arrays. It then delegates the actual distance computation to the {@code comparativeDistance} method of the
     * provided {@link Metrics} object.
     * @param metric the {@link Metrics} to use for the distance calculation. Must not be null.
     * @param vector1 the first vector for the comparison. Must not be null.
     * @param vector2 the second vector for the comparison. Must not be null.
     * @return the calculated comparative distance as a {@code double}.
     * @throws NullPointerException if {@code metric}, {@code vector1}, or {@code vector2} is null.
     */
    static double comparativeDistance(@Nonnull final Metrics metric,
                                      @Nonnull final Vector vector1,
                                      @Nonnull final Vector vector2) {
        return metric.comparativeDistance(vector1.getData(), vector2.getData());
    }
}
