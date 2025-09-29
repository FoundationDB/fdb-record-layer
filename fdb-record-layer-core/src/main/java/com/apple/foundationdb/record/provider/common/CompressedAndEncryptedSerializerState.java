/*
 * CompressedAndEncryptedSerializerState.java
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

/**
 * Information on intended / found serialization format: compressed and/or encrypted.
 */
@API(API.Status.INTERNAL)
public class CompressedAndEncryptedSerializerState {
    private boolean compressed;
    private boolean encrypted;
    private int keyNumber;

    public CompressedAndEncryptedSerializerState() {
        this.compressed = false;
        this.encrypted = false;
    }

    public boolean isCompressed() {
        return compressed;
    }

    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    public boolean isEncrypted() {
        return encrypted;
    }

    public void setEncrypted(boolean encrypted) {
        this.encrypted = encrypted;
    }

    public int getKeyNumber() {
        return keyNumber;
    }

    public void setKeyNumber(final int keyNumber) {
        this.keyNumber = keyNumber;
    }
}
