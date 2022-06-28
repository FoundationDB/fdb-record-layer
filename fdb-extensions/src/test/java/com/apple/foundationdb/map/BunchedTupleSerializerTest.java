/*
 * BunchedTupleSerializerTest.java
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

package com.apple.foundationdb.map;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for serialization in {@link BunchedMap}.
 */
public class BunchedTupleSerializerTest {
    private static final List<Tuple> TEST_TUPLES = Arrays.asList(
            Tuple.from(),
            Tuple.from((Object)null),
            Tuple.from(1066L),
            Tuple.from(1066L, null),
            Tuple.from(1066L, "hello"),
            Tuple.from(1415L),
            Tuple.from(Tuple.from(1066L)),
            Tuple.from(Tuple.from(1066L), "hello"),
            Tuple.from(Tuple.from(1066L, null), "hello"),
            Tuple.from(Tuple.from(1066L, 1415L), "hello"),
            Tuple.from(Tuple.from(1066L, "hello")),
            Tuple.from(Tuple.from(1066L, "hello"), null)
    );
    private static final BunchedTupleSerializer serializer = BunchedTupleSerializer.instance();

    @Test
    public void serializeKey() {
        List<Tuple> sortedTuples = TEST_TUPLES.stream().sorted().collect(Collectors.toList());
        List<Tuple> sortedKeys = TEST_TUPLES.stream()
                .map(serializer::serializeKey)
                .sorted(ByteArrayUtil::compareUnsigned)
                .map(serializer::deserializeKey)
                .collect(Collectors.toList());
        assertEquals(sortedTuples, sortedKeys);

        // Add a subspace and make sure unpacking by length works.
        Subspace subspace = new Subspace(Tuple.from("fake", "subspace"));
        List<Tuple> prefixedTuples = TEST_TUPLES.stream()
                .map(serializer::serializeKey)
                .map(b -> ByteArrayUtil.join(subspace.getKey(), b))
                .map(data -> serializer.deserializeKey(data, subspace.getKey().length))
                .collect(Collectors.toList());
        assertEquals(TEST_TUPLES, prefixedTuples);
    }

    @Test
    public void serializeNonTupleType() {
        assertThrows(BunchedSerializationException.class,
                () -> serializer.serializeKey(Tuple.from(new Object())));
        assertThrows(BunchedSerializationException.class,
                () -> serializer.serializeEntry(Tuple.from(new Object()), Tuple.from(1066L)));
        assertThrows(BunchedSerializationException.class,
                () -> serializer.serializeEntry(Tuple.from(Tuple.from(1066L)), Tuple.from(new Object())));
    }

    @Test
    public void deserializeInvalidBytes() {
        // Not a valid tuple type
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeKey(new byte[]{0x04}));

        // Invalid offsets or length
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeKey(Tuple.from("hello", "world").pack(), 100, 0));
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeKey(Tuple.from("hello", "world").pack(), 100, 1));
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeKey(Tuple.from("hello", "world").pack(), 0, 100));
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeKey(Tuple.from("hello", "world").pack(), 0, -1));
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeKey(Tuple.from("hello").pack(), 0, 8));

        final Tuple key = Tuple.from(1066L);

        // Invalid prefix.
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeEntries(key, ByteArrayUtil.join(new byte[]{0x20}, Tuple.from(Tuple.from(1415L)).pack())));

        // Non-Tuples in the entry list.
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeEntries(key, ByteArrayUtil.join(BunchedTupleSerializer.PREFIX, Tuple.from(1066L).pack())));
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeEntries(key, ByteArrayUtil.join(BunchedTupleSerializer.PREFIX, Tuple.from(Tuple.from(1066L), 1415L).pack())));

        // Invalid size
        assertThrows(BunchedSerializationException.class,
                () -> serializer.deserializeEntries(key, ByteArrayUtil.join(BunchedTupleSerializer.PREFIX, Tuple.from(Tuple.from(1066L), Tuple.from(800L), Tuple.from(1415L), Tuple.from(1763L)).pack())));
    }

    @Test
    public void serializeEntries() {
        List<Map.Entry<Tuple, Tuple>> entries = TEST_TUPLES.stream()
                .map(t -> new AbstractMap.SimpleImmutableEntry<>(t, t))
                .collect(Collectors.toList());
        List<Map.Entry<Tuple, Tuple>> deserializedEntries = serializer.deserializeEntries(TEST_TUPLES.get(0), serializer.serializeEntries(entries));
        assertEquals(entries, deserializedEntries);
    }

    @Test
    public void serializeEmptyList() {
        assertThrows(BunchedSerializationException.class, () -> serializer.serializeEntries(Collections.emptyList()));
    }

    @Test
    public void serializeAndAppend() {
        List<Map.Entry<Tuple, Tuple>> entries = new ArrayList<>(TEST_TUPLES.size());
        entries.add(new AbstractMap.SimpleImmutableEntry<>(TEST_TUPLES.get(0), TEST_TUPLES.get(1)));
        byte[] data = serializer.serializeEntries(entries);
        for (int i = 1; i < TEST_TUPLES.size(); i++) {
            Map.Entry<Tuple, Tuple> entry = new AbstractMap.SimpleEntry<>(TEST_TUPLES.get(i), TEST_TUPLES.get((i + 1) % TEST_TUPLES.size()));
            entries.add(entry);
            byte[] nextData = ByteArrayUtil.join(data, serializer.serializeEntry(entry));
            assertArrayEquals(serializer.serializeEntries(entries), nextData);
            data = nextData;
        }
    }
}
