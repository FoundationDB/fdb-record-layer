/*
 * LuceneOptimizedWrappedIndexOutput.java
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

import org.apache.lucene.store.IndexOutput;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;

/**
 * {@code IndexOutput} optimized for FDB storage.
 */
public abstract class LuceneOptimizedWrappedIndexOutput extends IndexOutput {
    protected final ByteArrayOutputStream outputStream;
    private final CRC32 crc;

    public LuceneOptimizedWrappedIndexOutput(@Nonnull String name) {
        super(name, name);
        this.outputStream = new ByteArrayOutputStream();
        this.crc = new CRC32();
    }

    @Override
    public long getFilePointer() {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public long getChecksum() throws IOException {
        return crc.getValue();
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        outputStream.write(b);
        crc.update(b);
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        outputStream.write(b, offset, length);
        crc.update(b, offset, length);
    }
}
