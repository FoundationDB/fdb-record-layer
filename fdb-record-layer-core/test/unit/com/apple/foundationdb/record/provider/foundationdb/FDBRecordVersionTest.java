/*
 * FDBRecordVersionTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Versionstamp;
import org.junit.jupiter.api.Test;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link FDBRecordVersion}.
 */
public class FDBRecordVersionTest {
    private static final byte[] VERSION_BYTES_ONE = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN)
            .putLong(1066L).putShort((short)1).array();
    private static final byte[] VERSION_BYTES_TWO = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN)
            .putLong(1066L).putShort((short)2).array();
    private static final byte[] VERSION_BYTES_THREE = ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN)
            .putLong(1776L).putShort((short)1).array();
    private static final byte[] VERSION_BYTES_FOUR = ByteBuffer.allocate(12).order(ByteOrder.BIG_ENDIAN)
            .putLong(1776L).putShort((short)2).putShort((short)0).array();
    private static final byte[] VERSION_BYTES_FIVE = ByteBuffer.allocate(12).order(ByteOrder.BIG_ENDIAN)
            .putLong(1776L).putShort((short)2).putShort((short)0xffff).array();

    @Test
    public void complete() {
        FDBRecordVersion v1 = FDBRecordVersion.complete(VERSION_BYTES_ONE, 5);
        assertTrue(v1.isComplete());
        assertArrayEquals(VERSION_BYTES_ONE, v1.getGlobalVersion());
        assertEquals(5, v1.getLocalVersion());

        FDBRecordVersion v2 = FDBRecordVersion.complete(VERSION_BYTES_FOUR);
        assertTrue(v2.isComplete());
        assertArrayEquals(Arrays.copyOfRange(VERSION_BYTES_FOUR, 0, 10), v2.getGlobalVersion());
        assertEquals(0, v2.getLocalVersion());

        FDBRecordVersion v3 = FDBRecordVersion.complete(VERSION_BYTES_FIVE);
        assertTrue(v3.isComplete());
        assertArrayEquals(Arrays.copyOfRange(VERSION_BYTES_FIVE, 0, 10), v3.getGlobalVersion());
        assertEquals(0xffff, v3.getLocalVersion());
    }

    @Test
    public void completeBadGlobalVersion() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.complete(new byte[]{0x01, 0x02}, 99));
    }

    @Test
    public void incompleteBadLocalVersionLow() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.incomplete(-1));
    }

    @Test
    public void incompleteBadLocalVersionHigh() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.incomplete(0x10000));
    }

    @Test
    public void completeBadLocalVersionLow() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.complete(VERSION_BYTES_ONE, -1));
    }

    @Test
    public void completeBadLocalVersionHigh() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.complete(VERSION_BYTES_TWO, 0x10000));
    }

    @Test
    public void incomplete() {
        FDBRecordVersion v2 = FDBRecordVersion.incomplete(4);
        assertFalse(v2.isComplete());
        try {
            v2.getGlobalVersion();
            fail("did not throw on incomplete call for global version");
        } catch (FDBRecordVersion.IncompleteRecordVersionException e) {
            // caught
        }
        assertEquals(4, v2.getLocalVersion());
    }

    @Test
    public void fromVersionStamp() {
        Versionstamp stamp1 = Versionstamp.complete(VERSION_BYTES_TWO, 100);
        assertEquals(FDBRecordVersion.complete(VERSION_BYTES_TWO, 100), FDBRecordVersion.fromVersionstamp(stamp1));

        Versionstamp stamp2 = Versionstamp.incomplete(42);
        assertEquals(FDBRecordVersion.incomplete(42), FDBRecordVersion.fromVersionstamp(stamp2));
    }

    @Test
    public void fromBytes() {
        FDBRecordVersion version1 = FDBRecordVersion.incomplete(1);
        FDBRecordVersion version2 = FDBRecordVersion.fromBytes(version1.toBytes());
        assertFalse(version2.isComplete());
        assertEquals(1, version2.getLocalVersion());
        assertEquals(version1, version2);
        assertTrue(version2.toBytes(false) != version1.toBytes(false));

        FDBRecordVersion version3 = FDBRecordVersion.complete(VERSION_BYTES_THREE, 2);
        FDBRecordVersion version4 = FDBRecordVersion.fromBytes(version3.toBytes());
        assertTrue(version4.isComplete());
        assertEquals(2, version4.getLocalVersion());
        assertArrayEquals(VERSION_BYTES_THREE, version4.getGlobalVersion());
        assertEquals(version3, version4);
        assertTrue(version3.toBytes(false) != version4.toBytes(false));

        FDBRecordVersion version5 = FDBRecordVersion.fromBytes(version3.toBytes(false), false);
        assertTrue(version5.isComplete());
        assertEquals(2, version5.getLocalVersion());
        assertArrayEquals(VERSION_BYTES_THREE, version5.getGlobalVersion());
        assertEquals(version3, version5);
        assertEquals(version4, version5);
        assertTrue(version3.toBytes(false) == version5.toBytes(false));

        FDBRecordVersion version6 = FDBRecordVersion.fromBytes(VERSION_BYTES_FOUR, false);
        assertTrue(version6.toBytes(false) == VERSION_BYTES_FOUR);
        assertEquals(0, version6.getLocalVersion());
        assertTrue(version6.isComplete());

        FDBRecordVersion version7 = FDBRecordVersion.fromBytes(VERSION_BYTES_FIVE);
        assertTrue(version7.toBytes(false) != VERSION_BYTES_FIVE);
        assertArrayEquals(VERSION_BYTES_FIVE, version7.toBytes(false));
        assertEquals(0xffff, version7.getLocalVersion());
        assertThat(version7.getLocalVersion(), greaterThan(0));
    }

    @Test
    public void comparisons() {
        assertFalse(FDBRecordVersion.incomplete(3).equals(null));

        assertEquals(FDBRecordVersion.incomplete(5), FDBRecordVersion.incomplete(5));
        assertEquals(FDBRecordVersion.incomplete(5).hashCode(), FDBRecordVersion.incomplete(5).hashCode());
        assertEquals(0, FDBRecordVersion.incomplete(5).compareTo(FDBRecordVersion.incomplete(5)));

        assertNotEquals(FDBRecordVersion.incomplete(5), FDBRecordVersion.incomplete(6));
        assertNotEquals(FDBRecordVersion.incomplete(5).hashCode(), FDBRecordVersion.incomplete(6).hashCode());
        assertThat(FDBRecordVersion.incomplete(5).compareTo(FDBRecordVersion.incomplete(6)), lessThan(0));
        assertThat(FDBRecordVersion.incomplete(6).compareTo(FDBRecordVersion.incomplete(5)), greaterThan(0));

        assertEquals(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10), FDBRecordVersion.complete(VERSION_BYTES_TWO, 10));
        assertEquals(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).hashCode(), FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).hashCode());
        assertEquals(0, FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).compareTo(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10)));

        assertNotEquals(FDBRecordVersion.complete(VERSION_BYTES_THREE, 10), FDBRecordVersion.complete(VERSION_BYTES_TWO, 10));
        assertNotEquals(FDBRecordVersion.complete(VERSION_BYTES_THREE, 10).hashCode(), FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).hashCode());
        assertThat(FDBRecordVersion.complete(VERSION_BYTES_THREE, 10).compareTo(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10)), greaterThan(0));
        assertThat(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).compareTo(FDBRecordVersion.complete(VERSION_BYTES_THREE, 10)), lessThan(0));

        assertNotEquals(FDBRecordVersion.complete(VERSION_BYTES_ONE, 10), FDBRecordVersion.complete(VERSION_BYTES_TWO, 10));
        assertNotEquals(FDBRecordVersion.complete(VERSION_BYTES_ONE, 10).hashCode(), FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).hashCode());
        assertThat(FDBRecordVersion.complete(VERSION_BYTES_ONE, 10).compareTo(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10)), lessThan(0));
        assertThat(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).compareTo(FDBRecordVersion.complete(VERSION_BYTES_ONE, 10)), greaterThan(0));

        assertNotEquals(FDBRecordVersion.complete(VERSION_BYTES_TWO, 15), FDBRecordVersion.complete(VERSION_BYTES_TWO, 10));
        assertNotEquals(FDBRecordVersion.complete(VERSION_BYTES_TWO, 15).hashCode(), FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).hashCode());
        assertThat(FDBRecordVersion.complete(VERSION_BYTES_TWO, 15).compareTo(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10)), greaterThan(0));
        assertThat(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).compareTo(FDBRecordVersion.complete(VERSION_BYTES_TWO, 15)), lessThan(0));

        assertNotEquals(FDBRecordVersion.incomplete(10), FDBRecordVersion.complete(VERSION_BYTES_ONE, 10));
        assertNotEquals(FDBRecordVersion.incomplete(10).hashCode(), FDBRecordVersion.complete(VERSION_BYTES_ONE, 10).hashCode());
        assertThat(FDBRecordVersion.incomplete(10).compareTo(FDBRecordVersion.complete(VERSION_BYTES_ONE, 10)), greaterThan(0));
        assertThat(FDBRecordVersion.complete(VERSION_BYTES_ONE, 10).compareTo(FDBRecordVersion.incomplete(10)), lessThan(0));

        assertThat(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).compareTo(FDBRecordVersion.MAX_VERSION), lessThan(0));
        assertThat(FDBRecordVersion.complete(VERSION_BYTES_TWO, 10).compareTo(FDBRecordVersion.MIN_VERSION), greaterThan(0));

        FDBRecordVersion version = FDBRecordVersion.incomplete(3);
        assertEquals(version, version);
    }

    @Test
    public void min() {
        byte[] minBytes = FDBRecordVersion.MIN_VERSION.toBytes();
        assertEquals(FDBRecordVersion.VERSION_LENGTH, minBytes.length);
        for (byte b : minBytes) {
            assertEquals(0x00, b);
        }
        assertEquals(FDBRecordVersion.MIN_VERSION, FDBRecordVersion.fromBytes(minBytes));
    }

    @Test
    public void max() {
        byte[] maxBytes = FDBRecordVersion.MAX_VERSION.toBytes();
        assertEquals(FDBRecordVersion.VERSION_LENGTH, maxBytes.length);
        for (int i = 0; i < maxBytes.length; i++) {
            assertEquals(i == FDBRecordVersion.GLOBAL_VERSION_LENGTH - 1 ? (byte)0xfe : (byte)0xff, maxBytes[i]);
        }
        assertEquals(FDBRecordVersion.MAX_VERSION, FDBRecordVersion.fromBytes(maxBytes));
    }

    @Test
    public void nextAndPrev() {
        List<Integer> localVersions = Arrays.asList(0, 0xff, 0xf0ff);
        for (int localVersion : localVersions) {
            FDBRecordVersion version1 = FDBRecordVersion.complete(VERSION_BYTES_ONE, localVersion);
            FDBRecordVersion version2 = version1.next();
            assertThat(version1.compareTo(version2), lessThan(0));
            assertTrue(version2.isComplete());
            assertArrayEquals(version1.getGlobalVersion(), version2.getGlobalVersion());
            assertEquals(localVersion + 1, version2.getLocalVersion());
            assertEquals(version1, version2.prev());

            FDBRecordVersion version3 = FDBRecordVersion.incomplete(localVersion);
            FDBRecordVersion version4 = version3.next();
            assertThat(version3.compareTo(version4), lessThan(0));
            assertFalse(version4.isComplete());
            assertEquals(localVersion + 1, version4.getLocalVersion());
            assertEquals(version3, version4.prev());
        }

        // Roll over from local to global version.
        FDBRecordVersion lastBytesOne = FDBRecordVersion.complete(VERSION_BYTES_ONE, 0xffff);
        FDBRecordVersion firstBytesTwo = lastBytesOne.next();
        assertThat(lastBytesOne.compareTo(firstBytesTwo), lessThan(0));
        assertTrue(firstBytesTwo.isComplete());
        assertArrayEquals(VERSION_BYTES_TWO, firstBytesTwo.getGlobalVersion());
        assertEquals(0, firstBytesTwo.getLocalVersion());
        assertEquals(lastBytesOne, firstBytesTwo.prev());
    }

    @Test
    public void nextAfterMax() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.MAX_VERSION.next());
    }

    @Test
    public void prevBeforeMin() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.MIN_VERSION.prev());
    }

    @Test
    public void nextAfterMaxIncomplete() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.incomplete(0xffff).next());
    }

    @Test
    public void prevBeforeMinIncomplete() {
        assertThrows(RecordCoreException.class, () -> FDBRecordVersion.incomplete(0).prev());
    }

    @Test
    public void firstAndLastInDBVersion() {
        FDBRecordVersion firstInVersion = FDBRecordVersion.firstInDBVersion(1066L);
        assertArrayEquals(ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN).putLong(1066L).putShort((short)0).array(),
                firstInVersion.getGlobalVersion());
        assertEquals(0, firstInVersion.getLocalVersion());

        FDBRecordVersion lastInVersion = FDBRecordVersion.lastInDBVersion(1065L);
        assertArrayEquals(ByteBuffer.allocate(10).order(ByteOrder.BIG_ENDIAN).putLong(1065L).putShort((short)0xffff).array(),
                lastInVersion.getGlobalVersion());
        assertEquals(0xffff, lastInVersion.getLocalVersion());

        assertThat(lastInVersion.compareTo(firstInVersion), lessThan(0));
        assertEquals(firstInVersion, lastInVersion.next());
        assertEquals(lastInVersion, firstInVersion.prev());
    }

    @Test
    public void firstAndLastInGlobalVersion() {
        FDBRecordVersion firstInVersion = FDBRecordVersion.firstInGlobalVersion(VERSION_BYTES_TWO);
        assertTrue(firstInVersion.isComplete());
        assertArrayEquals(VERSION_BYTES_TWO, firstInVersion.getGlobalVersion());
        assertEquals(0, firstInVersion.getLocalVersion());

        FDBRecordVersion lastInVersion = FDBRecordVersion.lastInGlobalVersion(VERSION_BYTES_ONE);
        assertTrue(lastInVersion.isComplete());
        assertArrayEquals(VERSION_BYTES_ONE, lastInVersion.getGlobalVersion());
        assertEquals(0xffff, lastInVersion.getLocalVersion());

        assertThat(lastInVersion.compareTo(firstInVersion), lessThan(0));
        assertEquals(firstInVersion, lastInVersion.next());
        assertEquals(lastInVersion, firstInVersion.prev());
    }

    @Test
    public void writeToByteBuffer() {
        FDBRecordVersion version = FDBRecordVersion.complete(VERSION_BYTES_ONE, 10);
        byte[] versionBytes =  version.writeTo(ByteBuffer.allocate(FDBRecordVersion.VERSION_LENGTH)).array();
        assertArrayEquals(version.toBytes(), versionBytes);

        byte[] versionWithPrefixAndSuffix = version.writeTo(
                ByteBuffer.allocate(FDBRecordVersion.VERSION_LENGTH * 3)
                    .put(VERSION_BYTES_FOUR))
                .put(VERSION_BYTES_FIVE).array();
        assertArrayEquals(ByteArrayUtil.join(VERSION_BYTES_FOUR, versionBytes, VERSION_BYTES_FIVE), versionWithPrefixAndSuffix);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(1066);
        byte[] origArray = Arrays.copyOf(buffer.array(), Integer.BYTES);
        buffer.position(0);
        assertThrows(BufferOverflowException.class, () -> version.writeTo(buffer));
        assertArrayEquals(origArray, buffer.array());
    }

    @Test
    public void writeToByteArray() {
        FDBRecordVersion version = FDBRecordVersion.complete(VERSION_BYTES_ONE, 20);
        byte[] destArray = new byte[3 * FDBRecordVersion.VERSION_LENGTH - 1];
        int nextPos = version.writeTo(destArray);
        assertEquals(FDBRecordVersion.VERSION_LENGTH, nextPos);
        assertArrayEquals(version.toBytes(), Arrays.copyOfRange(destArray, 0, nextPos));

        nextPos = FDBRecordVersion.fromBytes(VERSION_BYTES_FIVE).writeTo(destArray, nextPos);
        assertEquals(2 * FDBRecordVersion.VERSION_LENGTH, nextPos);
        assertArrayEquals(version.toBytes(), Arrays.copyOfRange(destArray, 0, FDBRecordVersion.VERSION_LENGTH));
        assertArrayEquals(VERSION_BYTES_FIVE, Arrays.copyOfRange(destArray, FDBRecordVersion.VERSION_LENGTH, nextPos));

        assertThrows(RecordCoreArgumentException.class, () -> FDBRecordVersion.fromBytes(VERSION_BYTES_FOUR).writeTo(destArray, 2 * FDBRecordVersion.VERSION_LENGTH));
        assertArrayEquals(version.toBytes(), Arrays.copyOfRange(destArray, 0, FDBRecordVersion.VERSION_LENGTH));
        assertArrayEquals(VERSION_BYTES_FIVE, Arrays.copyOfRange(destArray, FDBRecordVersion.VERSION_LENGTH, nextPos));
        assertArrayEquals(new byte[FDBRecordVersion.VERSION_LENGTH - 1], Arrays.copyOfRange(destArray, nextPos, destArray.length));
    }
}
