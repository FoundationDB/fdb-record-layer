/*
 * TextIndexBunchedSerializerTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.map.BunchedSerializationException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.tuple.Versionstamp;
import com.apple.foundationdb.util.UUIDUtils;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link TextIndexBunchedSerializer}.
 */
public class TextIndexBunchedSerializerTest {
    private TextIndexBunchedSerializer serializer = TextIndexBunchedSerializer.instance();

    @Test
    public void serializeEntry() {
        Tuple key = Tuple.from(1066L);
        List<Integer> value = Arrays.asList(0, 1, 127 + 1, 128 + 1 + 127);
        byte[] serialized = serializer.serializeEntry(key, value);
        assertArrayEquals(ByteArrayUtil.join(
                    new byte[]{0x03}, key.pack(),
                    new byte[]{0x05}, new byte[]{0x00, 0x01, 0x7f, (byte)0x81, 0x00}
                ), serialized);
    }

    static <K, V> Map.Entry<K, V> entryOf(@Nonnull K key, @Nonnull V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    @Test
    public void example() {
        // Make sure the example is actually correct
        final List<Map.Entry<Tuple, List<Integer>>> entries = Arrays.asList(
                entryOf(Tuple.from(1066L), Arrays.asList(1, 3, 5, 8)),
                entryOf(Tuple.from(1415L), Arrays.asList(0, 600, 605))
        );
        final byte[] expectedBytes = new byte[]{0x20, 0x04, 0x01, 0x02, 0x02, 0x03, 0x03, 0x16, 0x05, (byte)0x87, 0x04, 0x00, (byte)0x84, 0x58, 0x05};
        assertArrayEquals(expectedBytes, serializer.serializeEntries(entries));
    }

    @Test
    public void serializeEntryList() {
        final List<Map.Entry<Tuple, List<Integer>>> entries = Arrays.asList(
                entryOf(Tuple.from(0L), Collections.emptyList()),
                entryOf(Tuple.from(), Arrays.asList(0, 1)),
                entryOf(Tuple.from("hello"), Arrays.asList(0, 1, 1)),
                entryOf(Tuple.from(UUIDUtils.random()), Arrays.asList(12345, 67890)),
                entryOf(Tuple.from(Tuple.from("i'm nested", null), "and i'm not", null), Arrays.asList(0, 127, 127 + 128))
        );
        byte[] serialized = serializer.serializeEntries(entries);
        List<Map.Entry<Tuple, List<Integer>>> deserializedEntries = serializer.deserializeEntries(Tuple.from(0L), serialized);
        assertEquals(entries, deserializedEntries);
        List<Tuple> deserializedKeys = serializer.deserializeKeys(Tuple.from(0L), serialized);
        assertEquals(entries.stream().map(Map.Entry::getKey).collect(Collectors.toList()), deserializedKeys);
    }

    @Test
    public void ratherLargeEntry() {
        Random r = new Random(0x5ca1ab1e);
        Tuple t = Tuple.fromStream(LongStream.generate(r::nextLong).limit(100).boxed());
        int last = 0;
        List<Integer> list = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            int bytes = r.nextInt(4);
            int bound = 1 << (7 * bytes);
            int next = last + r.nextInt(bound);
            list.add(next);
            last = next;
        }
        byte[] serialized = serializer.serializeEntries(Arrays.asList(entryOf(TupleHelpers.EMPTY, Collections.emptyList()), entryOf(t, list)));
        byte[] serializedTuple = t.pack();
        int tupleStart = (Integer.SIZE - Integer.numberOfLeadingZeros(serializedTuple.length) + 6) / 7 + 2;
        assertArrayEquals(serializedTuple, Arrays.copyOfRange(serialized, tupleStart, tupleStart + serializedTuple.length));
        List<Map.Entry<Tuple, List<Integer>>> deserializedEntries = serializer.deserializeEntries(TupleHelpers.EMPTY, serialized);
        Map.Entry<Tuple, List<Integer>> entry = deserializedEntries.get(1);
        assertEquals(t, entry.getKey());
        assertEquals(list, entry.getValue());
    }

    @Test
    public void allIntegers() {
        // Try serializing a list with every integer between 1 and 2^15 in batches of 100.
        Map.Entry<Tuple, List<Integer>> emptyEntry = entryOf(TupleHelpers.EMPTY, Collections.emptyList());
        for (int i = 0; i < 1 << 15; i += 100) {
            List<Integer> value = new ArrayList<>(100);
            int last = 0;
            for (int j = i; j < i + 100; j++) {
                value.add(last + j);
                last += j;
            }
            byte[] serialized = serializer.serializeEntries(Arrays.asList(emptyEntry, entryOf(Tuple.from(1066L), value)));
            List<Integer> deserializedValue = serializer.deserializeEntries(TupleHelpers.EMPTY, serialized).get(1).getValue();
            assertEquals(value, deserializedValue);
        }
    }

    @Test
    public void invalidSerialization() {
        final List<Map.Entry<Tuple, List<Integer>>> invalidToSerialize = Arrays.asList(
                entryOf(Tuple.from(1066L), Arrays.asList(0, 1, 0)),
                entryOf(Tuple.from(1415L), Arrays.asList(-1, 1, 2)),
                entryOf(Tuple.from(1707L), Arrays.asList(1, 3, -1)),
                entryOf(Tuple.from(Versionstamp.incomplete()), Arrays.asList(0, 1, 2)),
                entryOf(Tuple.from(this), Arrays.asList(0, 1, 2))
        );
        for (Map.Entry<Tuple, List<Integer>> entry : invalidToSerialize) {
            assertThrows(BunchedSerializationException.class, () -> serializer.serializeEntry(entry));
        }
        for (int i = 0; i < invalidToSerialize.size(); i++) {
            final int idx = i;
            assertThrows(BunchedSerializationException.class, () ->
                    serializer.serializeEntries(Arrays.asList(entryOf(TupleHelpers.EMPTY, Collections.emptyList()), invalidToSerialize.get(idx)))
            );
        }
        assertThrows(BunchedSerializationException.class, () -> serializer.serializeEntries(Collections.emptyList()));
    }

    @Test
    public void invalidDeserialization() {
        final Tuple key = Tuple.from(1066L);
        final List<byte[]> invalidDataArrays = Arrays.asList(
                // FIXME: Broken until FoundationDB issue #672 is resolved: https://github.com/apple/foundationdb/issues/672
                // ByteArrayUtil.join(new byte[]{0x20, 0x00, 0x02}, Arrays.copyOfRange(Tuple.from(1415L).pack(), 0, 2), new byte[]{0x00}) // Tuple missing final byte
                new byte[]{0x21, 0x04, 0x01, 0x02, 0x03, 0x04}, // invalid prefix
                new byte[]{0x20, 0x04, 0x01, 0x02, 0x03}, // too few items in entry list
                ByteArrayUtil.join(new byte[]{0x20, 0x00, 0x03}, Tuple.from(1415L).pack()) // no final entry list
        );
        for (byte[] data : invalidDataArrays) {
            assertThrows(BunchedSerializationException.class, () -> serializer.deserializeEntries(key, data));
            assertThrows(BunchedSerializationException.class, () -> serializer.deserializeKeys(key, data));
        }
    }
}
