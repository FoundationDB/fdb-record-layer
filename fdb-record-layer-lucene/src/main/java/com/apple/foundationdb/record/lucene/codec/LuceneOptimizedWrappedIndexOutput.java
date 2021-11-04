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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IndexOutput;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;

public class LuceneOptimizedWrappedIndexOutput extends IndexOutput {
    private final String name;
    private final FDBDirectory directory;
    private final ByteArrayOutputStream outputStream;
    private final CRC32 crc;
    private final boolean isSegmentInfo;

    public LuceneOptimizedWrappedIndexOutput(@Nonnull String name, @Nonnull FDBDirectory directory, boolean isSegmentInfo) {
        super(name, name);
        this.name = name;
        this.directory = (FDBDirectory) FilterDirectory.unwrap(directory);
        this.outputStream = new ByteArrayOutputStream();
        this.crc = new CRC32();
        this.isSegmentInfo = isSegmentInfo;
    }

    @Override
    public void close() throws IOException {
        FDBLuceneFileReference reference = new FDBLuceneFileReference(-1, -1, -1);
        if (isSegmentInfo) {
            reference.setSegmentInfo(outputStream.toByteArray());
        } else {
            reference.setEntries(outputStream.toByteArray());
        }
        directory.writeFDBLuceneFileReference(name, reference);
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
