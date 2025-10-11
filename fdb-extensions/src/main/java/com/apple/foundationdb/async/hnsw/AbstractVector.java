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

import com.google.common.base.Suppliers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An abstract base class representing a mathematical vector.
 * <p>
 * This class provides a generic framework for vectors of different numerical types,
 * where {@code R} is a subtype of {@link Number}. It includes common operations and functionalities like size,
 * component access, equality checks, and conversions. Concrete implementations must provide specific logic for
 * data type conversions and raw data representation.
 */
public abstract class AbstractVector implements Vector {
    @Nonnull
    final double[] data;

    @Nonnull
    protected Supplier<Integer> hashCodeSupplier;

    @Nonnull
    private final Supplier<byte[]> toRawDataSupplier;

    /**
     * Constructs a new Vector with the given data.
     * <p>
     * This constructor uses the provided array directly as the backing store for the vector. It does not create a
     * defensive copy. Therefore, any subsequent modifications to the input array will be reflected in this vector's
     * state. The contract of this constructor is that callers do not modify {@code data} after calling the constructor.
     * We do not want to copy the array here for performance reasons.
     * @param data the components of this vector
     * @throws NullPointerException if the provided {@code data} array is null.
     */
    protected AbstractVector(@Nonnull final double[] data) {
        this.data = data;
        this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
        this.toRawDataSupplier = Suppliers.memoize(this::computeRawData);
    }

    /**
     * Returns the number of elements in the vector.
     * @return the number of elements
     */
    public int getNumDimensions() {
        return data.length;
    }

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
    public double getComponent(int dimension) {
        return data[dimension];
    }

    /**
     * Returns the underlying data array.
     * <p>
     * The returned array is guaranteed to be non-null. Note that this method
     * returns a direct reference to the internal array, not a copy.
     * @return the data array of type {@code R[]}, never {@code null}.
     */
    @Nonnull
    public double[] getData() {
        return data;
    }

    /**
     * Gets the raw byte data representation of this object.
     * <p>
     * This method provides a direct, unprocessed view of the object's underlying data. The format of the byte array is
     * implementation-specific and should be documented by the concrete class that implements this method.
     * @return a non-null byte array containing the raw data.
     */
    @Nonnull
    public byte[] getRawData() {
        return toRawDataSupplier.get();
    }

    /**
     * Computes the raw byte data representation of this object.
     * <p>
     * This method provides a direct, unprocessed view of the object's underlying data. The format of the byte array is
     * implementation-specific and should be documented by the concrete class that implements this method.
     * @return a non-null byte array containing the raw data.
     */
    @Nonnull
    protected abstract byte[] computeRawData();

    /**
     * Returns the number of bytes used for the serialization of this vector per component.
     * @return the component size, i.e. the number of bytes used for the serialization of this vector per component.
     */
    public int precision() {
        return (1 << precisionShift());
    }

    /**
     * Returns the number of bits we need to shift {@code 1} to express {@link #precision()} used for the serialization
     * of this vector per component.
     * @return returns the number of bits we need to shift {@code 1} to express {@link #precision()}
     */
    public abstract int precisionShift();

    /**
     * Compares this vector to the specified object for equality.
     * <p>
     * The result is {@code true} if and only if the argument is not {@code null} and is a {@code Vector} object that
     * has the same data elements as this object. This method performs a deep equality check on the underlying data
     * elements using {@link Objects#deepEquals(Object, Object)}.
     * @param o the object to compare with this {@code Vector} for equality.
     * @return {@code true} if the given object is a {@code Vector} equivalent to this vector, {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof AbstractVector)) {
            return false;
        }
        final AbstractVector vector = (AbstractVector)o;
        return Objects.deepEquals(data, vector.data);
    }

    /**
     * Returns a hash code value for this object. The hash code is computed once and memoized.
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    /**
     * Computes a hash code based on the internal {@code data} array.
     * @return the computed hash code for this object.
     */
    private int computeHashCode() {
        return Arrays.hashCode(data);
    }

    /**
     * Returns a string representation of the object.
     * <p>
     * This method provides a default string representation by calling
     * {@link #toString(int)} with a predefined indentation level of 3.
     *
     * @return a string representation of this object with a default indentation.
     */
    @Override
    public String toString() {
        return toString(10);
    }

    /**
     * Generates a string representation of the data array, with an option to limit the number of dimensions shown.
     * <p>
     * If the specified {@code limitDimensions} is less than the actual number of dimensions in the data array,
     * the resulting string will be a truncated view, ending with {@code ", ..."} to indicate that more elements exist.
     * Otherwise, the method returns a complete string representation of the entire array.
     * @param limitDimensions The maximum number of array elements to include in the string. A non-positive
     *        value will cause an {@link com.google.common.base.VerifyException}.
     * @return A string representation of the data array, potentially truncated.
     * @throws com.google.common.base.VerifyException if {@code limitDimensions} is not positive
     */
    public String toString(final int limitDimensions) {
        Verify.verify(limitDimensions > 0);
        if (limitDimensions < data.length) {
            return "[" + Arrays.stream(Arrays.copyOfRange(data, 0, limitDimensions))
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(",")) + ", ...]";
        } else {
            return "[" + Arrays.stream(data)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(",")) + "]";
        }
    }

    @Nonnull
    protected static double[] fromInts(@Nonnull final int[] ints) {
        final double[] result = new double[ints.length];
        for (int i = 0; i < ints.length; i++) {
            result[i] = ints[i];
        }
        return result;
    }

    @Nonnull
    protected static double[] fromLongs(@Nonnull final long[] longs) {
        final double[] result = new double[longs.length];
        for (int i = 0; i < longs.length; i++) {
            result[i] = longs[i];
        }
        return result;
    }
}
