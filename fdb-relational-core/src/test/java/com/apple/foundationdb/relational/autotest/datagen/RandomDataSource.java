/*
 * RandomDataSource.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.autotest.datagen;

import java.nio.charset.StandardCharsets;

/**
 * Replacement for java.util.Random so that we can generate different data distributions if we need to.
 */
public interface RandomDataSource {

    int nextInt();

    default int nextInt(int max) {
        return nextInt(0, max);
    }

    int nextInt(int min, int max);

    long nextLong();

    float nextFloat();

    double nextDouble();

    String nextUtf8();

    byte[] nextBytes();

    byte[] nextBytes(int byteLength);

    default byte nextByte(int lowByte, int highByte) {
        //make the bounds unsigned, so that we can do the math properly
        return (byte) (nextInt(lowByte & 0xFF, highByte & 0xFF));
    }

    String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    default String nextAlphaNumeric(int length) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < length) {
            int pos = nextInt(0, AB.length());
            sb.append(AB.charAt(pos));
        }
        return sb.toString();
    }

    /**
     *  Generates a random string consisting only of valid UTF-8 characters. Why? Because I like strings without
     *  weird "symbol missing" errors, that's why. Don't Judge me.
     */
    default String nextUtf8(int stringLength) {
        StringBuilder sb = new StringBuilder();
        byte[] data = new byte[4];
        int size;
        while (sb.length() < stringLength) {
            size = -1;
            int fb = nextInt(0, 256);
            if (fb < 0x00000080) {
                data[0] = (byte) fb;
                size = 1;
            } else if (fb >= 0x000000C2 && fb < 0x000000E0) {
                size = 2;
                data[0] = (byte) fb;
                data[1] = nextByte(0x80, 0xC0);
            } else if (fb == 0x000000E0) {
                size = 3;
                data[0] = (byte) fb;
                data[1] = nextByte(0xA0, 0xC0);
                data[2] = nextByte(0x80, 0xC0);
            } else if (fb >= 0x000000E1 && fb < 0x000000ED) {
                size = 3;
                data[0] = (byte) fb;
                data[1] = nextByte(0x80, 0xC0);
                data[2] = nextByte(0x80, 0xC0);
            } else if (fb == 0xED) {
                size = 3;
                data[0] = (byte) fb;
                data[1] = nextByte(0x80, 0xA0);
                data[2] = nextByte(0x80, 0xC0);
            } else if (fb >= 0xEE && fb < 0xF0) {
                size = 3;
                data[0] = (byte) fb;
                data[1] = nextByte(0x80, 0xA0);
                data[2] = nextByte(0x80, 0xC0);
            } else if (fb == 0xF0) {
                size = 4;
                data[0] = (byte) fb;
                data[1] = nextByte(0x90, 0xC0);
                data[2] = nextByte(0x80, 0xC0);
                data[3] = nextByte(0x80, 0xC0);
            } else if (fb >= 0xF1 && fb < 0xF4) {
                size = 4;
                data[0] = (byte) fb;
                data[1] = nextByte(0x80, 0xC0);
                data[2] = nextByte(0x80, 0xC0);
                data[3] = nextByte(0x80, 0xC0);
            } else if (fb == 0xF4) {
                size = 4;
                data[0] = (byte) fb;
                data[1] = nextByte(0x80, 0x9F);
                data[2] = nextByte(0x80, 0xC0);
                data[3] = nextByte(0x80, 0xC0);
            }

            if (size > 0) {
                sb.append(new String(data, 0, size, StandardCharsets.UTF_8));
            }
        }
        return sb.toString();
    }

    boolean nextBoolean();

}
