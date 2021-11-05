/*
 * FDBLuceneFileReference.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * A File Reference record laying out the id, size, and block size.
 */
@SpotBugsSuppressWarnings("EI_EXPOSE_REP")
@API(API.Status.EXPERIMENTAL)
public class FDBLuceneFileReference {
    private final long id;
    private final long size;
    private final long blockSize;
    private byte[] segmentInfo;
    private byte[] entries;

    public FDBLuceneFileReference(@Nonnull Tuple tuple) {
        this(tuple.getLong(0), tuple.getLong(1), tuple.getLong(2), tuple.getBytes(3), tuple.getBytes(4));
    }

    public FDBLuceneFileReference(long id, long size, long blockSize) {
        this(id, size, blockSize, null, null);
    }

    public FDBLuceneFileReference(long id, long size, long blockSize, byte[] segmentInfo, byte[] entries) {
        this.id = id;
        this.size = size;
        this.blockSize = blockSize;
        this.segmentInfo = segmentInfo;
        this.entries = entries;
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

    public void setSegmentInfo(byte[] segmentInfo) {
        this.segmentInfo = segmentInfo;
    }

    public void setEntries(byte[] entries) {
        this.entries = entries;
    }

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public byte[] getSegmentInfo() {
        return segmentInfo;
    }

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public byte[] getEntries() {
        return entries;
    }

    public Tuple getTuple() {
        return Tuple.from(id, size, blockSize, segmentInfo, entries);
    }

    @Override
    public String toString() {
        return "Reference [ id=" + id + ", size=" + size + ", blockSize=" + blockSize + ", segmentInfo=" + (getSegmentInfo() == null ? 0 : getSegmentInfo().length) + ", entries=" + (getEntries() == null ? 0 : getEntries().length) + "]";
    }
}
