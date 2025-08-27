/*
 * RollingTestKeyManager.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * A {@link SerializationKeyManager} that gives out lots of different keys.
 */
public class RollingTestKeyManager implements SerializationKeyManager {
    private final KeyGenerator keyGenerator;
    private final Map<Integer, SecretKey> keys;
    private final Random random;

    public RollingTestKeyManager() throws NoSuchAlgorithmException {
        keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(128);
        keys = new HashMap<>();
        random = new SecureRandom();
    }

    @Override
    public int getSerializationKey() {
        int newKey = random.nextInt();
        if (!keys.containsKey(newKey)) {
            keys.put(newKey, keyGenerator.generateKey());
        }
        return newKey;
    }

    @Override
    public Key getKey(final int keyNumber) {
        if (!keys.containsKey(keyNumber)) {
            throw new RecordCoreArgumentException("invalid key number");
        }
        return keys.get(keyNumber);
    }

    @Override
    public String getCipher(final int keyNumber) {
        return CipherPool.DEFAULT_CIPHER;
    }

    @Override
    public Random getRandom(final int keyNumber) {
        return random;
    }

    public int numberOfKeys() {
        return keys.size();
    }
}
