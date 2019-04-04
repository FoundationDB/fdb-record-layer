/*
 * BunchedTupleSerializer.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * A {@link BunchedSerializer} that uses {@link Tuple}s as both the expected key
 * and value type. This uses the ability of {@link Tuple} classes to pack themselves
 * in a fairly straightforward way. This will do the right thing, but it can be somewhat
 * space inefficient when serializing an entry as the {@link Tuple} encoding is
 * designed to preserve order, which is unnecessary for values.
 */
@API(API.Status.EXPERIMENTAL)
public class BunchedTupleSerializer implements BunchedSerializer<Tuple, Tuple> {
    @Nonnull
    static final byte[] PREFIX = new byte[]{0x10};
    private static final BunchedTupleSerializer INSTANCE = new BunchedTupleSerializer();

    /**
     * Get the serializer singleton.
     *
     * @return the <code>BunchedTupleSerializer</code> singleton
     */
    public static BunchedTupleSerializer instance() {
        return INSTANCE;
    }

    private BunchedTupleSerializer() {
    }

    /**
     * Serialize a {@link Tuple} to bytes. This uses the standard Tuple
     * packing function to do so.
     *
     * @param key key to serialize to bytes
     * @return the serialized key
     */
    @Nonnull
    @Override
    public byte[] serializeKey(@Nonnull Tuple key) {
        try {
            return key.pack();
        } catch (IllegalArgumentException e) {
            throw new BunchedSerializationException("unable so serialize key", e)
                    .setValue(key);
        }
    }

    /**
     * Serialize a pair of {@link Tuple}s to bytes. This packs each {@link Tuple}
     * as nested {@link Tuple}s and then returns the concatenated bytes. As
     * {@link #canAppend()} returns <code>true</code>, this can be used to
     * append an element to the end of an entry list.
     *
     * @param key the key of the map entry
     * @param value the value of the map entry
     * @return the serialized entry
     */
    @Nonnull
    @Override
    public byte[] serializeEntry(@Nonnull Tuple key, @Nonnull Tuple value) {
        try {
            return Tuple.from(key, value).pack();
        } catch (IllegalArgumentException e) {
            throw new BunchedSerializationException("unable to serialize entry", e)
                    .setValue(Tuple.from(key, value));
        }
    }

    /**
     * Serialize an entry list to bytes. This will place a prefix at the beginning
     * of the list in order to support versioning of the entry list data structure
     * in the future. Otherwise, it concatenates serialized entries together.
     *
     * @param entries the list of entries to serialize
     * @return the serialized entry list
     */
    @Nonnull
    @Override
    public byte[] serializeEntries(@Nonnull List<Map.Entry<Tuple, Tuple>> entries) {
        if (entries.isEmpty()) {
            throw new BunchedSerializationException("cannot serialize empty entry list");
        }
        List<byte[]> serializedEntries = new ArrayList<>(1 + entries.size());
        serializedEntries.add(PREFIX);
        if (!entries.isEmpty()) {
            Iterator<Map.Entry<Tuple, Tuple>> iterator = entries.iterator();
            Map.Entry<Tuple, Tuple> firstEntry = iterator.next();
            serializedEntries.add(Tuple.from(firstEntry.getValue()).pack());
            while (iterator.hasNext()) {
                Map.Entry<Tuple, Tuple> entry = iterator.next();
                serializedEntries.add(serializeEntry(entry));
            }
        }
        return ByteArrayUtil.join(null, serializedEntries);
    }

    @Nonnull
    @Override
    public Tuple deserializeKey(@Nonnull byte[] data, int offset, int length) {
        // It seems that bounds checking should be done by the Tuple layer rather
        // than here, but it apparently is not.
        if (offset < 0 || offset > data.length || length < 0 || offset + length > data.length) {
            throw new BunchedSerializationException("offset (" + offset + ") or length " + length + " out of range (" + data.length + ")").setData(data);
        }
        try {
            return Tuple.fromBytes(data, offset, length);
        } catch (RuntimeException e) {
            throw new BunchedSerializationException("unable to deserialize key", e)
                    .setData(Arrays.copyOfRange(data, offset, offset + length));
        }
    }

    @Nonnull
    @SuppressWarnings("unchecked") // we catch the ClassCastException in the calling method
    private static Tuple toTuple(@Nonnull Object o) {
        if (o instanceof Tuple) {
            return (Tuple)o;
        } else {
            return Tuple.fromItems((Iterable)o);
        }
    }

    @Nonnull
    @Override
    public List<Map.Entry<Tuple, Tuple>> deserializeEntries(@Nonnull Tuple key, @Nonnull byte[] data) {
        if (!ByteArrayUtil.startsWith(data, PREFIX)) {
            throw new BunchedSerializationException("data did not begin with expected prefix")
                    .setData(data);
        }
        Tuple entriesTuple = Tuple.fromBytes(data, PREFIX.length, data.length - PREFIX.length);
        if (entriesTuple.size() % 2 == 0) {
            throw new BunchedSerializationException("deserialized entry list had incorrect number of elements");
        }
        Iterator<Object> tupleIterator = entriesTuple.iterator();
        List<Map.Entry<Tuple, Tuple>> entryList = new ArrayList<>(entriesTuple.size() / 2);
        boolean first = true;
        while (tupleIterator.hasNext()) {
            Tuple entryKey = null;
            Tuple entryValue = null;
            try {
                if (first) {
                    entryKey = key;
                    first = false;
                } else {
                    entryKey = toTuple(tupleIterator.next());
                }
                entryValue = toTuple(tupleIterator.next());
                entryList.add(new AbstractMap.SimpleImmutableEntry<>(entryKey, entryValue));
            } catch (ClassCastException e) {
                throw new BunchedSerializationException("deserialized entry list did not consist only of tuples", e);
            }
        }
        return entryList;
    }

    /**
     * Returns <code>true</code>. This format supports appending serialized entries to
     * the end of a serialized entry list to create the serialized bytes of
     * the original list with the new entry at the end.
     *
     * @return <code>true</code> as this serialization format supports appending
     */
    @Override
    public boolean canAppend() {
        return true;
    }
}
