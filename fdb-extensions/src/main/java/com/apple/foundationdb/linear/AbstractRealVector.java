/*
 * AbstractRealVector.java
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

import com.google.common.base.Suppliers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Common implementation skeleton for every {@link RealVector} subtype in this package. Holds
 * the canonical {@code double[]} storage, memoizes the three derived values that are pure
 * functions of the data ({@link #hashCode()}, {@link #getRawData()}, {@link #l2SquaredNorm()}),
 * and delegates the precision-specific bits (raw-byte format, conversions to other vector
 * representations) to concrete subclasses.
 * <p>
 * The class is intentionally precision-agnostic at this level — every accessor returns
 * {@code double}, and subclasses only differ in <em>storage</em> resolution (which they truncate
 * to in their constructors) and in the wire format produced by {@link #computeRawData()}.
 * Mutable storage is the special case modeled by {@link MutableDoubleRealVector}; everything
 * else inheriting from this class is treated as immutable for the lifetime of the instance
 * (per the {@link #AbstractRealVector(double[]) constructor contract}).
 */
public abstract class AbstractRealVector implements RealVector {
    @Nonnull
    protected final double[] data;

    @Nonnull
    @SuppressWarnings("this-escape")
    protected Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<byte[]> toRawDataSupplier = Suppliers.memoize(this::computeRawData);

    @Nonnull
    @SuppressWarnings("this-escape")
    private final Supplier<Double> l2SquaredNormSupplier = Suppliers.memoize(this::computeL2SquaredNorm);

    /**
     * Constructs a new RealVector with the given data.
     * <p>
     * This constructor uses the provided array directly as the backing store for the vector. It does not create a
     * defensive copy. Therefore, any subsequent modifications to the input array will be reflected in this vector's
     * state. The contract of this constructor is that callers do not modify {@code data} after calling the constructor.
     * We do not want to copy the array here for performance reasons.
     * @param data the components of this vector
     * @throws NullPointerException if the provided {@code data} array is null.
     */
    protected AbstractRealVector(@Nonnull final double[] data) {
        this.data = data;
    }

    /**
     * Returns the number of elements in the vector.
     * @return the number of elements
     */
    @Override
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
     * @return the component at the specified dimension as a {@code double}.
     * @throws IndexOutOfBoundsException if the {@code dimension} is negative or
     *         greater than or equal to the number of dimensions of this object.
     */
    @Override
    public double getComponent(int dimension) {
        return data[dimension];
    }

    /**
     * Returns the underlying {@code double[]} data array.
     * <p>
     * The returned array is guaranteed to be non-null. Note that this method
     * returns a direct reference to the internal array, not a copy — callers must not mutate it
     * unless the concrete subtype is {@link MutableDoubleRealVector}.
     * @return the data array, never {@code null}.
     */
    @Nonnull
    @Override
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
    @Override
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
     * Compares this vector to the specified object for equality.
     * <p>
     * The result is {@code true} if and only if the argument is not {@code null} and is a {@code RealVector} object that
     * has the same data elements as this object. This method performs a deep equality check on the underlying data
     * elements using {@link Objects#deepEquals(Object, Object)}.
     * @param o the object to compare with this {@code RealVector} for equality.
     * @return {@code true} if the given object is a {@code RealVector} equivalent to this vector, {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof final AbstractRealVector vector)) {
            return false;
        }
        return Arrays.equals(data, vector.data);
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
    protected int computeHashCode() {
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
                    .collect(Collectors.joining(", ")) + ", ...]";
        } else {
            return "[" + Arrays.stream(data)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(", ")) + "]";
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Memoized via the {@code l2SquaredNormSupplier} so repeated calls — including the ones
     * driving {@link #l2Norm()} — share a single computation.
     */
    @Override
    public double l2SquaredNorm() {
        return l2SquaredNormSupplier.get();
    }

    /**
     * Computes the squared L2 norm from scratch as {@code dot(this)}. Backs the memoizing
     * supplier behind {@link #l2SquaredNorm()}; subclasses normally don't need to call it
     * directly.
     *
     * @return the squared L2 norm of this vector
     */
    protected double computeL2SquaredNorm() {
        return dot(this);
    }

    /**
     * Widens an {@code int[]} to a freshly allocated {@code double[]} suitable for handing to
     * {@link #AbstractRealVector(double[]) the array-by-reference constructor}. Used by
     * subclass int-constructors so the call site does not need to manage the widening copy.
     *
     * @param ints the source components
     * @return a new {@code double[]} of the same length, each element widened from the
     *         corresponding {@code ints[i]}
     */
    @Nonnull
    protected static double[] fromInts(@Nonnull final int[] ints) {
        final double[] result = new double[ints.length];
        for (int i = 0; i < ints.length; i++) {
            result[i] = ints[i];
        }
        return result;
    }

    /**
     * Widens a {@code long[]} to a freshly allocated {@code double[]} suitable for handing to
     * {@link #AbstractRealVector(double[]) the array-by-reference constructor}. Used by
     * subclass long-constructors so the call site does not need to manage the widening copy.
     * Note that {@code long} values outside the 53-bit mantissa of {@code double} may lose
     * precision in the widening.
     *
     * @param longs the source components
     * @return a new {@code double[]} of the same length, each element widened from the
     *         corresponding {@code longs[i]}
     */
    @Nonnull
    protected static double[] fromLongs(@Nonnull final long[] longs) {
        final double[] result = new double[longs.length];
        for (int i = 0; i < longs.length; i++) {
            result[i] = longs[i];
        }
        return result;
    }
}
