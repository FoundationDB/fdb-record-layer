/*
 * ByteArrayUtil2Test.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.tuple;

import com.apple.test.RandomSeedSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ByteArrayUtil}.
 */
public class ByteArrayUtil2Test {

    @Test
    void testLoggable() {
        String hexRegex = "^\\\\x[0-9a-f][0-9a-f]$";
        for (int i = Byte.MIN_VALUE; i < (byte)' '; i++) {
            String l = ByteArrayUtil2.loggable(new byte[]{(byte)i});
            assertTrue(l.matches(hexRegex), l + " matches /" + hexRegex + "/");
        }
        for (int i = (byte)' '; i < (byte)'"'; i++) {
            assertEquals(Character.toString((char) i), ByteArrayUtil2.loggable(new byte[]{(byte)i}));
        }
        assertEquals("\\x22", ByteArrayUtil2.loggable(new byte[]{(byte)'"'}));
        for (int i = (byte)'"' + 1; i < (byte)'='; i++) {
            assertEquals(Character.toString((char) i), ByteArrayUtil2.loggable(new byte[]{(byte)i}));
        }
        assertEquals("\\x3d", ByteArrayUtil2.loggable(new byte[]{(byte)'='}));
        for (int i = (byte)'=' + 1; i < (byte)'\\'; i++) {
            assertEquals(Character.toString((char) i), ByteArrayUtil2.loggable(new byte[]{(byte)i}));
        }
        assertEquals("\\\\", ByteArrayUtil2.loggable(new byte[]{'\\'}));
        for (int i = (byte)'\\' + 1; i < (byte)127; i++) {
            assertEquals(Character.toString((char) i), ByteArrayUtil2.loggable(new byte[]{(byte)i}));
        }
        assertEquals("\\x7f", ByteArrayUtil2.loggable(new byte[]{127}));
    }

    @Test
    void testUnprint() {
        byte[] allBytes = new byte[Math.abs((int)Byte.MIN_VALUE) + Byte.MAX_VALUE];
        for (byte b = Byte.MIN_VALUE; b < Byte.MAX_VALUE; b++) {
            allBytes[b - Byte.MIN_VALUE] = b;
        }
        assertArrayEquals(allBytes, ByteArrayUtil2.unprint(ByteArrayUtil2.loggable(allBytes)));
        assertArrayEquals(allBytes, ByteArrayUtil2.unprint(ByteArrayUtil.printable(allBytes)));
    }

    @ParameterizedTest
    // Nothing particular about these seeds, except that though some of them create random bytes that are
    // encoded identically by ByteArrayUtil.printable and ByteArrayUtil2.loggable, whereas others create
    // byte arrays that are encoded differently
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testRandomBytes(long seed) {
        Random r = new Random(seed);
        int length = r.nextInt(100);
        byte[] bytes = new byte[length];
        r.nextBytes(bytes);

        final String printable = ByteArrayUtil.printable(bytes);
        byte[] unprinted = ByteArrayUtil2.unprint(printable);
        assertArrayEquals(bytes, unprinted, "Unprinting printable bytes should reconstruct original array");

        final String loggable = ByteArrayUtil2.loggable(bytes);
        byte[] unlogged = ByteArrayUtil2.unprint(loggable);
        assertArrayEquals(bytes, unlogged, "Unprinting loggable bytes should reconstruct original array");
        assertFalse(loggable.contains("="), "loggable string should not contain equals sign");
        assertFalse(loggable.contains("\""), "loggable string should not contain quote");

        if (!printable.contains("=") && !printable.contains("\"")) {
            assertEquals(printable, loggable);
        }
    }
}
