/*
 * LuceneOptimizedWrappedIndexInput.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.codec;

import com.google.common.base.Suppliers;
import org.apache.lucene.store.IndexInput;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * An {@code IndexInput} optimized for FDB storage.
 */
public class LuceneOptimizedWrappedIndexInput extends IndexInput {
    Supplier<byte[]> value;
    private int position;

    public LuceneOptimizedWrappedIndexInput(@Nonnull String name, @Nonnull Supplier<byte[]> supplier) {
        super(name);
        value = Suppliers.memoize( () -> supplier.get());
        position = 0;
    }

    @Override
    public void close() throws IOException {
        // Simply to override the abstract method of super class. Nothing is needed when closing this.
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public void seek(final long pos) throws IOException {
        position = (int) pos;
    }

    @Override
    public long length() {
        return value.get().length;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public byte readByte() throws IOException {
        return value.get()[position++];
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
        System.arraycopy(value.get(), position, b, offset, len);
        position = position + len;
    }
}
