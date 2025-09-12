/*
 * CipherPool.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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
import javax.crypto.Cipher;
import java.security.GeneralSecurityException;

/**
 * Helper class for pooling {@link Cipher}.
 */
public class CipherPool {
    // AES with 128 bits key
    public static final String DEFAULT_CIPHER = "AES/CBC/PKCS5Padding";
    public static final int IV_SIZE = 16;
    public static final MappedPool<String, Cipher, GeneralSecurityException> MAPPED_POOL = new MappedPool<>(Cipher::getInstance);

    private CipherPool() {
    }

    public static Cipher borrowCipher() throws GeneralSecurityException {
        return borrowCipher(DEFAULT_CIPHER);
    }

    public static Cipher borrowCipher(@Nonnull String cipherName) throws GeneralSecurityException {
        return MAPPED_POOL.poll(cipherName);
    }

    public static void returnCipher(@Nonnull Cipher cipher) {
        MAPPED_POOL.offer(cipher.getAlgorithm(), cipher);
    }

    public static void invalidateAll() {
        MAPPED_POOL.invalidateAll();
    }
}
