/*
 * HNSWHelpersTest.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.half.Half;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class HNSWHelpersTest {
    @Test
    public void bytesToHex_MultipleBytesWithLeadingZeros_ReturnsTrimmedHexTest() {
        final byte[] bytes = new byte[] {0, 1, 16, (byte)255}; // Represents 000110FF
        final String result = HNSWHelpers.bytesToHex(bytes);
        assertEquals("0x110FF", result);
    }

    @Test
    public void bytesToHex_NegativeByteValues_ReturnsCorrectUnsignedHexTest() {
        final byte[] bytes = new byte[] {-1, -2}; // 0xFFFE
        final String result = HNSWHelpers.bytesToHex(bytes);
        assertEquals("0xFFFE", result);
    }

    @Test
    public void halfValueOf_NegativeFloat_ReturnsCorrectHalfValue_Test() {
        final float inputValue = -56.75f;
        final Half expected = Half.valueOf(inputValue);
        final Half result = HNSWHelpers.halfValueOf(inputValue);
        assertEquals(expected, result);
    }

    @Test
    public void halfValueOf_PositiveFloat_ReturnsCorrectHalfValue_Test() {
        final float inputValue = 123.4375f;
        Half expected = Half.valueOf(inputValue);
        Half result = HNSWHelpers.halfValueOf(inputValue);
        assertEquals(expected, result);
    }

    @Test
    public void halfValueOf_NegativeDouble_ReturnsCorrectHalfValue_Test() {
        final double inputValue = -56.75d;
        final Half expected = Half.valueOf(inputValue);
        final Half result = HNSWHelpers.halfValueOf(inputValue);
        assertEquals(expected, result);
    }

    @Test
    public void halfValueOf_PositiveDouble_ReturnsCorrectHalfValue_Test() {
        final double inputValue = 123.4375d;
        Half expected = Half.valueOf(inputValue);
        Half result = HNSWHelpers.halfValueOf(inputValue);
        assertEquals(expected, result);
    }
}
