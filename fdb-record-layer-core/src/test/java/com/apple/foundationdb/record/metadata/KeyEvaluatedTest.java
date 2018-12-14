/*
 * KeyEvaluatedTest.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link Key.Evaluated}.
 */
public class KeyEvaluatedTest {
    private final byte[] deadcode = new byte[]{(byte)0xde, (byte)0xad, (byte)0xc0, (byte)0xde};
    private final byte[] baseball = new byte[]{(byte)0xba, (byte)0x5e, (byte)0xba, (byte)0x11};
    private final byte[] scalable = new byte[]{(byte)0x5c, (byte)0xa1, (byte)0xab, (byte)0x1e};

    @Test
    public void testScalarBytesToTuple() {
        Key.Evaluated eval = new Key.Evaluated(Arrays.asList(
                ByteString.copyFrom(deadcode),
                ByteString.copyFrom(baseball),
                ByteString.copyFrom(scalable)));

        Tuple tuple = eval.toTuple();
        assertEquals(3, tuple.size());

        assertArrayEquals(deadcode, (byte[])tuple.get(0));
        assertArrayEquals(baseball, (byte[])tuple.get(1));
        assertArrayEquals(scalable, (byte[])tuple.get(2));

        // This should not throw an error.
        tuple.pack();
    }

    @Test
    public void testConcatenatedBytesToTuple() {
        Key.Evaluated eval = Key.Evaluated.scalar(Arrays.asList(
                ByteString.copyFrom(deadcode),
                ByteString.copyFrom(baseball),
                ByteString.copyFrom(scalable)
        ));

        Tuple tuple = eval.toTuple();
        assertEquals(1, tuple.size());

        @SuppressWarnings("unchecked")
        List<byte[]> list = (List<byte[]>)tuple.get(0);
        assertEquals(3, list.size());

        assertArrayEquals(deadcode, list.get(0));
        assertArrayEquals(baseball, list.get(1));
        assertArrayEquals(scalable, list.get(2));

        // This should also not throw an error.
        tuple.pack();
    }
}
