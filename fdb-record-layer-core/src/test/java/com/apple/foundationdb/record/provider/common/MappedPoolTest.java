/*
 * MappedPoolTest.java
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

package com.apple.foundationdb.record.provider.common;

import org.junit.jupiter.api.Test;
import javax.crypto.Cipher;
import java.security.GeneralSecurityException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests of the {@link MappedPool}.
 *
 */
public class MappedPoolTest {

    public static String CIPHER = "DES/ECB/PKCS5Padding";
    public static MappedPool<String, Cipher, GeneralSecurityException> MAPPED_POOL = new MappedPool<>(Cipher::getInstance);

    @Test
    public void testCipherPool() throws Exception {
        Cipher lastCipher = null;
        for (int i = 0; i < 100; i++) {
            Cipher cipher = MAPPED_POOL.poll(CIPHER);
            if (lastCipher != null) {
                assertSame(cipher, lastCipher);
                lastCipher = cipher;
            }
            assertNotNull(cipher);
            assertTrue(MAPPED_POOL.offer(CIPHER, cipher));
        }
        assertEquals(1, MAPPED_POOL.getPoolSize(CIPHER));
        assertThat(MAPPED_POOL.getKeys(), hasItem(CIPHER));
    }

    @Test
    public void testMaxPoolSize() throws Exception {
        Cipher[] ciphers = new Cipher[1000];
        for (int i = 0; i < 1000; i++) {
            ciphers[i] = MAPPED_POOL.poll(CIPHER);
        }
        for (int i = 0; i < MappedPool.DEFAULT_POOL_SIZE; i++) {
            assertTrue(MAPPED_POOL.offer(CIPHER, ciphers[i]));
        }
        for (int i = MappedPool.DEFAULT_POOL_SIZE; i < 1000; i++) {
            assertFalse(MAPPED_POOL.offer(CIPHER, ciphers[i]));
        }
        assertEquals(64, MAPPED_POOL.getPoolSize(CIPHER));
    }

}
