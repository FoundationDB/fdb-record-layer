/*
 * HNSWHelpers.java
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
 * TODO.
 * @param <R> representation type
 */
public abstract class Vector<R extends Number> {
    @Nonnull
    protected R[] data;
    @Nonnull
    protected Supplier<Integer> hashCodeSupplier;

    public Vector(@Nonnull final R[] data) {
        this.data = data;
        this.hashCodeSupplier = Suppliers.memoize(this::computeHashCode);
    }

    public int size() {
        return data.length;
    }

    @Nonnull
    R getComponent(int dimension) {
        return data[dimension];
    }

    @Nonnull
    public R[] getData() {
        return data;
    }

    @Nonnull
    public abstract byte[] getRawData();

    @Nonnull
    public abstract Vector<Half> toHalfVector();

    @Nonnull
    public abstract DoubleVector toDoubleVector();

    public abstract int precision();

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Vector)) {
            return false;
        }
        final Vector<?> vector = (Vector<?>)o;
        return Objects.deepEquals(data, vector.data);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int computeHashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return toString(3);
    }

    public String toString(final int limitDimensions) {
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

    public static <R extends Number> double distance(@Nonnull Metric metric,
                                                     @Nonnull final Vector<R> vector1,
                                                     @Nonnull final Vector<R> vector2) {
        return metric.distance(vector1.toDoubleVector().getData(), vector2.toDoubleVector().getData());
    }

    static <R extends Number> double comparativeDistance(@Nonnull Metric metric,
                                                         @Nonnull final Vector<R> vector1,
                                                         @Nonnull final Vector<R> vector2) {
        return metric.comparativeDistance(vector1.toDoubleVector().getData(), vector2.toDoubleVector().getData());
    }

    public static Vector<?> fromBytes(@Nonnull final byte[] bytes, int precision) {
        if (precision == 16) {
            return HalfVector.halfVectorFromBytes(bytes);
        }
        // TODO
        throw new UnsupportedOperationException("not implemented yet");
    }

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
