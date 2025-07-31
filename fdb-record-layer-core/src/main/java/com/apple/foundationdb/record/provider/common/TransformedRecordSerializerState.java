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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import java.util.Arrays;

@SpotBugsSuppressWarnings("EI_EXPOSE_REP")
class TransformedRecordSerializerState {
    public boolean compressed;
    public boolean encrypted;
    public int keyNumber;

    @Nonnull
    public byte[] data;
    public int offset;
    public int length;

    public TransformedRecordSerializerState(@Nonnull byte[] data) {
        this(data, 0, data.length);
    }

    public TransformedRecordSerializerState(@Nonnull byte[] data, int offset, int length) {
        this.compressed = false;
        this.encrypted = false;
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    @Nonnull
    public byte[] getDataArray() {
        if (offset == 0 && length == data.length) {
            return data;
        } else {
            byte[] newData = Arrays.copyOfRange(data, offset, offset + length);
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
