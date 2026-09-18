/*
 * RandomUtil.java
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

package com.apple.foundationdb.record.util;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyByteString;

import java.util.Random;
import java.util.function.Function;

/**
 * Utilities for working with randomness within tests.
 */
public class RandomUtil {
    private RandomUtil() {
    }

    public static byte[] randomBytes(Random r, int length) {
        byte[] bytes = new byte[length];
        r.nextBytes(bytes);
        return bytes;
    }

    public static ByteString randomByteString(Random r, int length) {
        return ZeroCopyByteString.wrap(randomBytes(r, length));
    }

    private static char randomAlphabeticChar(Random r) {
        char start = r.nextBoolean() ? 'A' : 'a';
        return (char)(start + r.nextInt(26));
    }

    private static char randomNumericChar(Random r) {
        return (char)('0' + r.nextInt(10));
    }

    private static char randomAlphanumericChar(Random r) {
        if (r.nextDouble() * 62 <= 10) {
            return randomNumericChar(r);
        } else {
            return randomAlphabeticChar(r);
        }
    }

    public static String randomString(Random r, int length, Function<Random, Character> charGenerator) {
        if (length <= 0) {
            return "";
        }
        char[] arr = new char[length];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = charGenerator.apply(r);
        }
        return new String(arr);
    }

    public static String randomAlphanumericString(Random r, int length) {
        return randomString(r, length, RandomUtil::randomAlphanumericChar);
    }
}
