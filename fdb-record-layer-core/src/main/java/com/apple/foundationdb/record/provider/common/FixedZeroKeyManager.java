/*
 * FixedZeroKeyManager.java
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.Key;
import java.security.SecureRandom;
import java.util.Random;

/**
 * A {@link SerializationKeyManager} that always returns the same fixed {@link Key}.
 */
public class FixedZeroKeyManager implements SerializationKeyManager {
    private final Key encryptionKey;
    private final String cipherName;
    private final Random random;

    public FixedZeroKeyManager(@Nonnull Key encryptionKey, @Nullable String cipherName, @Nullable Random random) {
        if (cipherName == null) {
            cipherName = CipherPool.DEFAULT_CIPHER;
        }
        if (random == null) {
            random = new SecureRandom();
        }
        this.encryptionKey = encryptionKey;
        this.cipherName = cipherName;
        this.random = random;
    }

    @Override
    public int getSerializationKey() {
        return 0;
    }

    @Override
    public Key getKey(int keyNumber) {
        if (keyNumber != 0) {
            throw new RecordSerializationException("only provide key number 0");
        }
        return encryptionKey;
    }

    @Override
    public String getCipher(int keyNumber) {
        if (keyNumber != 0) {
            throw new RecordSerializationException("only provide key number 0");
        }
        return cipherName;
    }

    @Override
    public Random getRandom(int keyNumber) {
        if (keyNumber != 0) {
            throw new RecordSerializationException("only provide key number 0");
        }
        return random;
    }
}
