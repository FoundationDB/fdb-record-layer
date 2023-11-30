/*
 * EmptyIndexOutput.java
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

package com.apple.foundationdb.record.lucene.directory;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * An output that is used to cause file references to exist, but doesn't actually allow writing.
 * <p>
 *     This could just use an {@link FDBIndexOutput} and never actually write anything, but this helps fail earlier in
 *     cases where we never expect to write anything.
 * </p>
 */
public class EmptyIndexOutput extends IndexOutput {
    private final FDBDirectory fdbDirectory;
    private final String resourceDescription;
    private final long id;

    protected EmptyIndexOutput(final String resourceDescription, final String name, final FDBDirectory fdbDirectory) {
        super(resourceDescription, name);
        this.resourceDescription = resourceDescription;
        this.fdbDirectory = fdbDirectory;
        this.id = fdbDirectory.getIncrement();
    }

    @Override
    public void close() throws IOException {
        fdbDirectory.writeFDBLuceneFileReference(resourceDescription,
                new FDBLuceneFileReference(id, 0, 0, 0));
    }

    @Override
    public long getFilePointer() {
        return 0;
    }

    @Override
    public long getChecksum() {
        return 0;
    }

    @Override
    public void writeByte(final byte b) throws IOException {
        throw new IOException("Tried to write to FieldInfos");
    }

    @Override
    public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
        throw new IOException("Tried to write to FieldInfos");
    }
}
