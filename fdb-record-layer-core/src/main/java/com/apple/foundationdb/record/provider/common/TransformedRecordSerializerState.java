/*
 * TransformedRecordSerializerState.java
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * The internal state of serialization / deserialization, pointing to a portion of a byte array.
 */
@API(API.Status.INTERNAL)
@SpotBugsSuppressWarnings("EI_EXPOSE_REP")
class TransformedRecordSerializerState extends CompressedAndEncryptedSerializerState {
    @Nonnull
    private byte[] data;
    private int offset;
    private int length;

    public TransformedRecordSerializerState(@Nonnull byte[] data) {
        this(data, 0, data.length);
    }

    public TransformedRecordSerializerState(@Nonnull byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    @Nonnull
    public byte[] getData() {
        return data;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    @Nonnull
    public byte[] getDataArray() {
        if (getOffset() == 0 && getLength() == getData().length) {
            return getData();
        } else {
            byte[] newData = Arrays.copyOfRange(getData(), getOffset(), getOffset() + getLength());
            offset = 0;
            length = newData.length;
            data = newData;
            return newData;
        }
    }

    public void setDataArray(@Nonnull byte[] data) {
        setDataArray(data, 0, data.length);
    }

    public void setDataArray(@Nonnull byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }
}
