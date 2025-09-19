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
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
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
 * @param <R> the type of the numbers stored in this vector, which must extend {@link Number}.
 */
public abstract class Vector<R extends Number> {
    @Nonnull
    protected R[] data;
    @Nonnull
    protected Supplier<Integer> hashCodeSupplier;

    /**
     * Constructs a new Vector with the given data.
     * <p>
     * This constructor uses the provided array directly as the backing store for the vector. It does not create a
     * defensive copy. Therefore, any subsequent modifications to the input array will be reflected in this vector's
     * state. The contract of this constructor is that callers do not modify {@code data} after calling the constructor.
     * We do not want to copy the array here for performance reasons.
     * @param data the array of elements for this vector; must not be {@code null}.
     * @throws NullPointerException if the provided {@code data} array is null.
     */
    public Vector(@Nonnull final R[] data) {
        this.data = data;
        this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
    }

    /**
     * Returns the number of elements in the vector.
     * @return the number of elements
     */
    public int size() {
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
    @Nonnull
    R getComponent(int dimension) {
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
    public R[] getData() {
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
    public abstract byte[] getRawData();

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
    public abstract Vector<Half> toHalfVector();

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
    public abstract DoubleVector toDoubleVector();

    /**
     * Returns the number of digits to the right of the decimal point.
     * @return the precision, which is the number of digits to the right of the decimal point.
     */
    public abstract int precision();

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
        if (!(o instanceof Vector)) {
            return false;
        }
        final Vector<?> vector = (Vector<?>)o;
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
        return toString(3);
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
                    .map(String::valueOf)
                    .collect(Collectors.joining(",")) + ", ...]";
        } else {
            return "[" + Arrays.stream(data)
                    .map(String::valueOf)
                    .collect(Collectors.joining(",")) + "]";
        }
    }

    /**
     * A vector class encoding a vector over half components. Conversion to {@link DoubleVector} is supported and
     * memoized.
     */
    public static class HalfVector extends Vector<Half> {
        @Nonnull
        private final Supplier<DoubleVector> toDoubleVectorSupplier;
        @Nonnull
        private final Supplier<byte[]> toRawDataSupplier;

        public HalfVector(@Nonnull final Half[] data) {
            super(data);
            this.toDoubleVectorSupplier = Suppliers.memoize(this::computeDoubleVector);
            this.toRawDataSupplier = Suppliers.memoize(this::computeRawData);
        }

        @Nonnull
        @Override
        public Vector<Half> toHalfVector() {
            return this;
        }

        @Nonnull
        @Override
        public DoubleVector toDoubleVector() {
            return toDoubleVectorSupplier.get();
        }

        @Override
        public int precision() {
            return 16;
        }

        @Nonnull
        public DoubleVector computeDoubleVector() {
            Double[] result = new Double[data.length];
            for (int i = 0; i < data.length; i ++) {
                result[i] = data[i].doubleValue();
            }
            return new DoubleVector(result);
        }

        @Nonnull
        @Override
        public byte[] getRawData() {
            return toRawDataSupplier.get();
        }

        @Nonnull
        private byte[] computeRawData() {
            return StorageAdapter.bytesFromVector(this);
        }

        @Nonnull
        public static HalfVector halfVectorFromBytes(@Nonnull final byte[] vectorBytes) {
            return StorageAdapter.vectorFromBytes(vectorBytes);
        }
    }

    /**
     * A vector class encoding a vector over double components. Conversion to {@link HalfVector} is supported and
     * memoized.
     */
    public static class DoubleVector extends Vector<Double> {
        @Nonnull
        private final Supplier<HalfVector> toHalfVectorSupplier;

        public DoubleVector(@Nonnull final Double[] data) {
            super(data);
            this.toHalfVectorSupplier = Suppliers.memoize(this::computeHalfVector);
        }

        @Nonnull
        @Override
        public HalfVector toHalfVector() {
            return toHalfVectorSupplier.get();
        }

        @Nonnull
        public HalfVector computeHalfVector() {
            Half[] result = new Half[data.length];
            for (int i = 0; i < data.length; i ++) {
                result[i] = Half.valueOf(data[i]);
            }
            return new HalfVector(result);
        }

        @Nonnull
        @Override
        public DoubleVector toDoubleVector() {
            return this;
        }

        @Override
        public int precision() {
            return 64;
        }

        @Nonnull
        @Override
        public byte[] getRawData() {
            // TODO
            throw new UnsupportedOperationException("not implemented yet");
        }
    }

    /**
     * Calculates the distance between two vectors using a specified metric.
     * <p>
     * This static utility method provides a convenient way to compute the distance by handling the conversion of
     * generic {@code Vector<R>} objects to primitive {@code double} arrays. The actual distance computation is then
     * delegated to the provided {@link Metric} instance.
     * @param <R> the type of the numbers in the vectors, which must extend {@link Number}.
     * @param metric the {@link Metric} to use for the distance calculation.
     * @param vector1 the first vector.
     * @param vector2 the second vector.
     * @return the calculated distance between the two vectors as a {@code double}.
     */
    public static <R extends Number> double distance(@Nonnull Metric metric,
                                                     @Nonnull final Vector<R> vector1,
                                                     @Nonnull final Vector<R> vector2) {
        return metric.distance(vector1.toDoubleVector().getData(), vector2.toDoubleVector().getData());
    }

    /**
     * Calculates the comparative distance between two vectors using a specified metric.
     * <p>
     * This utility method converts the input vectors, which can contain any {@link Number} type, into primitive double
     * arrays. It then delegates the actual distance computation to the {@code comparativeDistance} method of the
     * provided {@link Metric} object.
     * @param <R> the type of the numbers in the vectors, which must extend {@link Number}.
     * @param metric the {@link Metric} to use for the distance calculation. Must not be null.
     * @param vector1 the first vector for the comparison. Must not be null.
     * @param vector2 the second vector for the comparison. Must not be null.
     * @return the calculated comparative distance as a {@code double}.
     * @throws NullPointerException if {@code metric}, {@code vector1}, or {@code vector2} is null.
     */
    static <R extends Number> double comparativeDistance(@Nonnull Metric metric,
                                                         @Nonnull final Vector<R> vector1,
                                                         @Nonnull final Vector<R> vector2) {
        return metric.comparativeDistance(vector1.toDoubleVector().getData(), vector2.toDoubleVector().getData());
    }

    /**
     * Creates a {@code Vector} instance from its byte representation.
     * <p>
     * This method deserializes a byte array into a vector object. The precision parameter is crucial for correctly
     * interpreting the byte data. Currently, this implementation only supports 16-bit precision, which corresponds to a
     * {@code HalfVector}.
     * @param bytes the non-null byte array representing the vector.
     * @param precision the precision of the vector's elements in bits (e.g., 16 for half-precision floats).
     * @return a new {@code Vector} instance created from the byte array.
     * @throws UnsupportedOperationException if the specified {@code precision} is not yet supported.
     */
    public static Vector<?> fromBytes(@Nonnull final byte[] bytes, int precision) {
        if (precision == 16) {
            return HalfVector.halfVectorFromBytes(bytes);
        }
        // TODO
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Abstract iterator implementation to read the IVecs/FVecs data format that is used by publicly available
     * embedding datasets.
     * @param <N> the component type of the vectors which must extends {@link Number}
     * @param <T> the type of object this iterator creates and uses to represent a stored vector in memory
     */
    public abstract static class StoredVecsIterator<N extends Number, T> extends AbstractIterator<T> {
        @Nonnull
        private final FileChannel fileChannel;

        protected StoredVecsIterator(@Nonnull final FileChannel fileChannel) {
            this.fileChannel = fileChannel;
        }

        @Nonnull
        protected abstract N[] newComponentArray(int size);

        @Nonnull
        protected abstract N toComponent(@Nonnull ByteBuffer byteBuffer);

        @Nonnull
        protected abstract T toTarget(@Nonnull N[] components);


        @Nullable
        @Override
        protected T computeNext() {
            try {
                final ByteBuffer headerBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                // allocate a buffer for reading floats later; you may reuse
                headerBuf.clear();
                final int bytesRead = fileChannel.read(headerBuf);
                if (bytesRead < 4) {
                    if (bytesRead == -1) {
                        return endOfData();
                    }
                    throw new IOException("corrupt fvecs file");
                }
                headerBuf.flip();
                final int dims = headerBuf.getInt();
                if (dims <= 0) {
                    throw new IOException("Invalid dimension " + dims + " at position " + (fileChannel.position() - 4));
                }
                final ByteBuffer vecBuf = ByteBuffer.allocate(dims * 4).order(ByteOrder.LITTLE_ENDIAN);
                while (vecBuf.hasRemaining()) {
                    int read = fileChannel.read(vecBuf);
                    if (read < 0) {
                        throw new EOFException("unexpected EOF when reading vector data");
                    }
                }
                vecBuf.flip();
                final N[] rawVecData = newComponentArray(dims);
                for (int i = 0; i < dims; i++) {
                    rawVecData[i] = toComponent(vecBuf);
                }

                return toTarget(rawVecData);
            } catch (final IOException ioE) {
                throw new RuntimeException(ioE);
            }
        }
    }

    /**
     * Iterator to read floating point vectors from a {@link FileChannel} providing an iterator of
     * {@link DoubleVector}s.
     */
    public static class StoredFVecsIterator extends StoredVecsIterator<Double, DoubleVector> {
        public StoredFVecsIterator(@Nonnull final FileChannel fileChannel) {
            super(fileChannel);
        }

        @Nonnull
        @Override
        protected Double[] newComponentArray(final int size) {
            return new Double[size];
        }

        @Nonnull
        @Override
        protected Double toComponent(@Nonnull final ByteBuffer byteBuffer) {
            return (double)byteBuffer.getFloat();
        }

        @Nonnull
        @Override
        protected DoubleVector toTarget(@Nonnull final Double[] components) {
            return new DoubleVector(components);
        }
    }

    /**
     * Iterator to read vectors from a {@link FileChannel} into a list of integers.
     */
    public static class StoredIVecsIterator extends StoredVecsIterator<Integer, List<Integer>> {
        public StoredIVecsIterator(@Nonnull final FileChannel fileChannel) {
            super(fileChannel);
        }

        @Nonnull
        @Override
        protected Integer[] newComponentArray(final int size) {
            return new Integer[size];
        }

        @Nonnull
        @Override
        protected Integer toComponent(@Nonnull final ByteBuffer byteBuffer) {
            return byteBuffer.getInt();
        }

        @Nonnull
        @Override
        protected List<Integer> toTarget(@Nonnull final Integer[] components) {
            return ImmutableList.copyOf(components);
        }
    }
}
