/*
 * EmptyIndexInput.java
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

package com.apple.foundationdb.record.lucene.directory;

import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;

/**
 * An {@link IndexInput} to go with {@link EmptyIndexOutput}.
 * This input always has length <em>0</em>.
 */
public class EmptyIndexInput extends IndexInput {
    public EmptyIndexInput(final String resourceDescription) {
        super(resourceDescription);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public long getFilePointer() {
        return 0;
    }

    @Override
    public void seek(final long pos) throws IOException {
        throw new EOFException();
    }

    @Override
    public long length() {
        return 0;
    }

    @Override
    public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
        throw new EOFException();
    }

    @Override
    public byte readByte() throws IOException {
        throw new EOFException();
    }

    @Override
    public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
        throw new EOFException();
    }
}
