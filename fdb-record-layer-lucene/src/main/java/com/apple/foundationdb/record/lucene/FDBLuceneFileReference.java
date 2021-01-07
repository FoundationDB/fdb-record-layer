/*
 * FDBLuceneFileReference.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.tuple.Tuple;

/**
 * A File Reference record laying out the id, size, and block size.
 */
public class FDBLuceneFileReference {
    private final long id;
    private final long size;
    private final long blockSize;

    public FDBLuceneFileReference(Tuple tuple) {
        this(tuple.getLong(0), tuple.getLong(1), tuple.getLong(2));
    }

    public FDBLuceneFileReference(long id, long size, long blockSize) {
        this.id = id;
        this.size = size;
        this.blockSize = blockSize;
    }

    public long getId() {
        return id;
    }

    public long getSize() {
        return size;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public Tuple getTuple() {
        return Tuple.from(id, size, blockSize);
    }

    @Override
    public String toString() {
        return "Reference [ id=" + id + ", size=" + size + ", blockSize=" + blockSize + "]";
    }
}
