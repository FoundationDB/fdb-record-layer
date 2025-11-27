/*
 * RealVector.java
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

import com.apple.foundationdb.half.Half;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * An abstract base class representing a mathematical vector.
 * <p>
 * This class provides a generic framework for vectors of different numerical types,
 * where {@code R} is a subtype of {@link Number}. It includes common operations and functionalities like size,
 * component access, equality checks, and conversions. Concrete implementations must provide specific logic for
 * data type conversions and raw data representation.
 */
public interface RealVector {
    ImmutableList<VectorType> VECTOR_TYPES = ImmutableList.copyOf(VectorType.values());

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
    RealVector withData(@Nonnull double[] data);

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
     * Converts this object into a {@code RealVector} of {@link Half} precision floating-point numbers.
     * <p>
     * As this is an abstract method, implementing classes are responsible for defining the specific conversion logic
     * from their internal representation to a {@code RealVector} using {@link Half} objects to serialize and
     * deserialize the vector. If this object already is a {@code HalfRealVector} this method should return {@code this}.
     * @return a non-null {@link HalfRealVector} containing the {@link Half} precision floating-point representation of
     *         this object.
     */
    @Nonnull
    HalfRealVector toHalfRealVector();

    /**
     * Converts this object into a {@code RealVector} of single precision floating-point numbers.
     * <p>
     * As this is an abstract method, implementing classes are responsible for defining the specific conversion logic
     * from their internal representation to a {@code RealVector} using floating point numbers to serialize and
     * deserialize the vector. If this object already is a {@code FloatRealVector} this method should return
     * {@code this}.
     * @return a non-null {@link FloatRealVector} containing the single precision floating-point representation of
     *         this object.
     */
    @Nonnull
    FloatRealVector toFloatRealVector();

    /**
     * Converts this vector into a {@link DoubleRealVector}.
     * <p>
     * This method provides a way to obtain a double-precision floating-point representation of the vector. If the
     * vector is already an instance of {@code DoubleRealVector}, this method may return the instance itself. Otherwise,
     * it will create a new {@code DoubleRealVector} containing the same elements, which may involve a conversion of the
     * underlying data type.
     * @return a non-null {@link DoubleRealVector} representation of this vector.
     */
    @Nonnull
    DoubleRealVector toDoubleRealVector();

    default double dot(@Nonnull final RealVector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        double sum = 0.0d;
        final double[] thisData = getData();
        final double[] otherData = other.getData();
        for (int i = 0; i < thisData.length; i++) {
            sum += thisData[i] * otherData[i];
        }
        return sum;
    }

    default double l2Norm() {
        return Math.sqrt(dot(this));
    }

    @Nonnull
    default RealVector normalize() {
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
    default RealVector add(@Nonnull final RealVector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) + other.getComponent(i);
        }
        return withData(result);
    }

    @Nonnull
    default RealVector add(final double scalar) {
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) + scalar;
        }
        return withData(result);
    }

    @Nonnull
    default RealVector subtract(@Nonnull final RealVector other) {
        Preconditions.checkArgument(getNumDimensions() == other.getNumDimensions());
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) - other.getComponent(i);
        }
        return withData(result);
    }

    @Nonnull
    default RealVector subtract(final double scalar) {
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) - scalar;
        }
        return withData(result);
    }

    @Nonnull
    default RealVector multiply(final double scalarFactor) {
        final double[] result = new double[getNumDimensions()];
        for (int i = 0; i < getNumDimensions(); i ++) {
            result[i] = getComponent(i) * scalarFactor;
        }
        return withData(result);
    }

    @Nonnull
    static VectorType fromVectorTypeOrdinal(final int ordinal) {
        return VECTOR_TYPES.get(ordinal);
    }

    /**
     * Creates a {@link RealVector} from a byte array.
     * <p>
     * This method interprets the input byte array by interpreting the first byte of the array as the type of vector.
     * It then delegates to {@link #fromBytes(VectorType, byte[])} to do the actual deserialization.
     *
     * @param vectorBytes the non-null byte array to convert.
     * @return a new {@link RealVector} instance created from the byte array.
     */
    @Nonnull
    static RealVector fromBytes(@Nonnull final byte[] vectorBytes) {
        final byte vectorTypeOrdinal = vectorBytes[0];
        return fromBytes(fromVectorTypeOrdinal(vectorTypeOrdinal), vectorBytes);
    }

    /**
     * Creates a {@link RealVector} from a byte array.
     * <p>
     * This implementation dispatches to the actual logic that deserialize a byte array to a vector which is located in
     * the respective implementations of {@link RealVector}.
     * @param vectorType the vector type of the serialized vector
     * @param vectorBytes the non-null byte array to convert.
     * @return a new {@link RealVector} instance created from the byte array.
     */
    @Nonnull
    static RealVector fromBytes(@Nonnull final VectorType vectorType, @Nonnull final byte[] vectorBytes) {
        switch (vectorType) {
            case HALF:
                return HalfRealVector.fromBytes(vectorBytes);
            case SINGLE:
                return FloatRealVector.fromBytes(vectorBytes);
            case DOUBLE:
                return DoubleRealVector.fromBytes(vectorBytes);
            default:
                throw new RuntimeException("unable to deserialize vector");
        }
    }
}
