/*
 * FDBIndexInputTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test to FDBIndexInput functionality.
 *
 */
@Tag(Tags.RequiresFDB)
public class FDBIndexInputTest extends FDBDirectoryBaseTest {
    private static final String FILE_NAME = "y";

    @Test
    public void testWriteReadBytes() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        byte[] expected = new byte[randomInt(1)];
        random.nextBytes(expected);
        output.writeBytes(expected, expected.length);
        output.close();
        FDBIndexInput input = new FDBIndexInput(FILE_NAME, directory);
        byte[] actual = new byte[expected.length];
        input.readBytes(actual, 0, expected.length);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testSeek() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        byte[] expected = new byte[randomInt(512)];
        random.nextBytes(expected);
        output.writeBytes(expected, expected.length);
        output.close();
        FDBIndexInput input = new FDBIndexInput(FILE_NAME, directory);
        input.seek(256);
        byte[] actual = new byte[expected.length - 256];
        input.readBytes(actual, 0, expected.length - 256);
        byte[] expectedAfterSeek = new byte[expected.length - 256];
        ByteBuffer.wrap(expected, 256, expected.length - 256).get(expectedAfterSeek);
        assertArrayEquals(expectedAfterSeek, actual);
    }

    @Test
    void testWriteReadWithOffset() throws Exception {
        FDBIndexOutput output = new FDBIndexOutput(FILE_NAME, directory);
        byte[] expected = new byte[randomInt(0) * 7 + randomInt(10)];
        int offset = random.nextInt(expected.length - 1);
        random.nextBytes(expected);
        output.writeBytes(expected, offset, expected.length - offset);
        output.close();
        FDBIndexInput input = new FDBIndexInput(FILE_NAME, directory);
        byte[] actual = new byte[expected.length - offset];
        input.readBytes(actual, 0, actual.length);
        assertEquals(actual.length, expected.length - offset);
    }

}
