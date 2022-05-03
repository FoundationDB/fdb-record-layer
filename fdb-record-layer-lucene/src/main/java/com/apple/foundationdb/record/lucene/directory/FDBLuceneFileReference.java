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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneFileSystemProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A File Reference record laying out the id, size, and block size.
 */
@SpotBugsSuppressWarnings("EI_EXPOSE_REP")
@API(API.Status.EXPERIMENTAL)
public class FDBLuceneFileReference {
    private final long id;
    private final long size;
    private final long actualSize;
    private final long blockSize;
    private byte[] segmentInfo;
    private byte[] entries;

    private FDBLuceneFileReference(@Nonnull LuceneFileSystemProto.LuceneFileReference protoMessage) {
        this(protoMessage.getId(), protoMessage.getSize(), protoMessage.getActualSize(), protoMessage.getBlockSize(),
                protoMessage.hasSegmentInfo() ? protoMessage.getSegmentInfo().toByteArray() : null,
                protoMessage.hasEntries() ? protoMessage.getEntries().toByteArray() : null);
    }

    public FDBLuceneFileReference(long id, long size, long actualSize, long blockSize) {
        this(id, size, actualSize, blockSize, null, null);
    }

    private FDBLuceneFileReference(long id, long size, long actualSize, long blockSize, byte[] segmentInfo, byte[] entries) {
        this.id = id;
        this.size = size;
        this.actualSize = actualSize;
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

    public long getActualSize() {
        return actualSize;
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

    @Nonnull
    public byte[] getBytes() {
        final LuceneFileSystemProto.LuceneFileReference.Builder builder = LuceneFileSystemProto.LuceneFileReference.newBuilder();
        builder.setId(this.id);
        builder.setSize(this.size);
        builder.setBlockSize(this.blockSize);
        builder.setActualSize(this.actualSize);
        if (this.segmentInfo != null) {
            builder.setSegmentInfo(ByteString.copyFrom(this.segmentInfo));
        }
        if (this.entries != null) {
            builder.setEntries(ByteString.copyFrom(this.entries));
        }
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return "Reference [ id=" + id + ", size=" + size + ", actualSize=" + actualSize + ", blockSize=" + blockSize + ", segmentInfo=" + (getSegmentInfo() == null ? 0 : getSegmentInfo().length) + ", entries=" + (getEntries() == null ? 0 : getEntries().length) + "]";
    }

    @Nullable
    public static FDBLuceneFileReference parseFromBytes(@Nullable byte[] value) {
        try {
            return value == null ? null : new FDBLuceneFileReference(LuceneFileSystemProto.LuceneFileReference.parseFrom(value));
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordCoreException("Invalid bytes for parsing of lucene file reference", ex);
        }
    }
}
