/*
 * StoredVecsIterator.java
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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * Abstract iterator implementation to read the IVecs/FVecs data format that is used by publicly available
 * embedding datasets.
 *
 * @param <N> the component type of the vectors which must extends {@link Number}
 * @param <T> the type of object this iterator creates and uses to represent a stored vector in memory
 */
public abstract class StoredVecsIterator<N extends Number, T> extends AbstractIterator<T> {
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

    /**
     * Iterator to read floating point vectors from a {@link FileChannel} providing an iterator of
     * {@link DoubleRealVector}s.
     */
    public static class StoredFVecsIterator extends StoredVecsIterator<Double, DoubleRealVector> {
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
        protected DoubleRealVector toTarget(@Nonnull final Double[] components) {
            return new DoubleRealVector(components);
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
