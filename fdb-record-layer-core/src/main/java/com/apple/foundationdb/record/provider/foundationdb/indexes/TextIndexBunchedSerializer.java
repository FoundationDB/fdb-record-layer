/*
 * TextIndexBunchedSerializer.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.map.BunchedSerializationException;
import com.apple.foundationdb.map.BunchedSerializer;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Serializer used by the {@link TextIndexMaintainer} to write entries into a
 * {@link com.apple.foundationdb.map.BunchedMap BunchedMap}. This is specifically
 * designed for writing out the mapping from document ID to position list. As a result,
 * it requires that the lists it serializes be monotonically increasing non-negative
 * integers (which is true for position lists). This allows it to delta compress the integers
 * in its list, which can be a significant space savings.
 *
 * <p>
 * Keys are serialized using default {@link Tuple} packing. Bunches are serialized as follows:
 * each bunch begins with a prefix (that can be used to version the serialization format), and
 * then each entry in the bunch is serialized by writing the length of the key (using a base-128
 * variable length integer encoding) followed by the serialized key bytes followed by the length
 * of the serialized position list followed by the (delta compressed) entries of each position
 * list. Additionally, the key of the first entry in the bunch is omitted as that can be determined
 * by using the sign-post key within the <code>BunchedMap</code>.
 * </p>
 *
 * <p>
 * For example, suppose one attempts to serialize two entries into a single bunch, one
 * with key <code>(1066,)</code> and value <code>[1, 3, 5, 8]</code> and another with key
 * <code>(1415,)</code> and value <code>[0, 600, 605]</code>. The tuple <code>(1066,)</code>
 * serializes to <code>16 04 2A</code> (in hex), and the tuple <code>(1415,)</code>
 * serializes to <code>16 05 87</code>. Most of the deltas are small, but <code>600</code>
 * is encoded by taking its binary representation, <code>1001011000</code>, and separating
 * the lower order groups of 7 bits into their own bytes and then using the most significant
 * bit as a continuation flag, so it becomes <code>10000100 01011000 = 84 58</code>.
 * So, the full entry is (with <code>20</code> as the prefix):
 * </p>
 * <pre><code>
 *     20 (04 (01 02 02 03)) (03 (16 05 87) 04 (00 (84 58) 05))
 * </code></pre>
 * <p>
 * The parentheses are added for clarity and separate each entry as well as grouping variable
 * length integers together. Note that to add a new entry to the end of a serialized list,
 * one can take the serialized entry and append it to the end of that list rather than
 * deserializing the entry list, appending the new entry, and then serializing
 * the new list.
 * </p>
 *
 * @see TextIndexMaintainer
 * @see com.apple.foundationdb.map.BunchedMap BunchedMap
 */
@API(API.Status.EXPERIMENTAL)
public class TextIndexBunchedSerializer implements BunchedSerializer<Tuple, List<Integer>> {
    private static final byte[] PREFIX = new byte[]{0x20};
    private static final TextIndexBunchedSerializer INSTANCE = new TextIndexBunchedSerializer();

    /**
     * Get the serializer singleton. This serializer maintains no state between serializing
     * different values, so it is safe to maintain as a singleton.
     *
     * @return the <code>TextIndexBunchedSerializer</code> singleton
     */
    public static TextIndexBunchedSerializer instance() {
        return INSTANCE;
    }

    private TextIndexBunchedSerializer() {
    }

    // Get the size (in bytes) of storing an integer using base 128 var-int encoding.
    private static int getVarIntSize(int val) {
        if (val == 0) {
            return 1;
        } else {
            return (Integer.SIZE - Integer.numberOfLeadingZeros(val) + 6) / 7;
        }
    }

    // Get the size (in bytes) of storing the list. It does *not* include the size
    // of the list (which is serialized at its start). It calculates the sizes
    // taking into account the fact that they will be delta compressed.
    private static int getListSize(@Nonnull List<Integer> list) {
        int sum = 0;
        int last = 0;
        for (int val : list) {
            if (val < 0 || val < last) {
                throw new BunchedSerializationException("list is not monotonically increasing non-negative integers")
                        .setValue(list);
            }
            sum += getVarIntSize(val - last);
            last = val;
        }
        return sum;
    }

    // Serializes the integer using a base-128 variable length encoding.
    private static void serializeVarInt(@Nonnull ByteBuffer buffer, int val) {
        if (val == 0) {
            buffer.put((byte)0x00);
        } else {
            int numBytes = getVarIntSize(val);
            for (int i = numBytes - 1; i >= 0; i--) {
                // Shift over the value and take the last 7 bits, then set
                // the most significant bit to 1 if this is not the last byte.
                byte nextByte = (byte)(((val >> (7 * i)) & 0x7f) | (i == 0 ? 0x00 : 0x80));
                buffer.put(nextByte);
            }
        }
    }

    private static int deserializeVarInt(@Nonnull ByteBuffer buffer) {
        int val = 0;
        boolean done;
        do {
            // Keep adding the lower 7 bits of each byte until reaching a
            // byte with a 0 in its most significant bit.
            byte nextByte = buffer.get();
            val = (val << 7) + (nextByte & 0x7f);
            done = (nextByte & 0x80) == 0;
        } while (!done);
        return val;
    }

    // Write the serialized size of the list and then write each entry of the list
    // to the buffer. It will delta compress the entries as it serializes.
    private static void serializeList(@Nonnull ByteBuffer buffer, @Nonnull List<Integer> list, int serializedSize) {
        serializeVarInt(buffer, serializedSize);
        int last = 0;
        for (int val : list) {
            serializeVarInt(buffer, val - last);
            last = val;
        }
    }

    @Nonnull
    private static List<Integer> deserializeList(@Nonnull ByteBuffer buffer) {
        // Determine the serialized size of the list to use as an estimate for the number
        // of elements within it. This is an upper bound, so it reduces allocations to
        // only 1, and it will be exactly the right size if every var-int fits in a
        // single byte.
        int serializedSize = deserializeVarInt(buffer);
        if (serializedSize == 0) {
            return Collections.emptyList();
        }
        List<Integer> endList = new ArrayList<>(serializedSize);
        int curPos = buffer.position();
        int last = 0;
        while (buffer.position() < curPos + serializedSize) {
            int next = deserializeVarInt(buffer);
            endList.add(last + next);
            last = last + next;
        }
        return endList;
    }

    /**
     * Packs a key using standard {@link Tuple} encoding. Note that <code>Tuple</code>s pack
     * in a way that preserves order, which is a requirement of {@link BunchedSerializer}s.
     *
     * @param key key to serialize to bytes
     * @return the key packed to bytes
     * @throws BunchedSerializationException if packing the tuple fails
     */
    @Nonnull
    @Override
    public byte[] serializeKey(@Nonnull Tuple key) {
        try {
            return key.pack();
        } catch (IllegalArgumentException e) {
            throw new BunchedSerializationException("unable to serialize key", e)
                    .setValue(key);
        }
    }

    /**
     * Packs a key and value into a byte array. This will write out the tuple and
     * position list in a way consistent with the way each entry is serialized by
     * {@link #serializeEntries(List)}. Because this serializer supports appending,
     * one can take the output of this function and append it to the end of an
     * already serialized entry list to produce the serialized form of that list
     * with this entry appended to the end.
     *
     * @param key the key of the map entry
     * @param value the value of the map entry
     * @return the serialized map entry
     * @throws BunchedSerializationException if the value is not monotonically increasing
     *                                       non-negative integers or if packing the tuple fails
     */
    @Nonnull
    @Override
    public byte[] serializeEntry(@Nonnull Tuple key, @Nonnull List<Integer> value) {
        try {
            byte[] serializedKey = key.pack();
            int listSize = getListSize(value);
            int size = getVarIntSize(serializedKey.length) + serializedKey.length + getVarIntSize(listSize) + listSize;
            ByteBuffer buffer = ByteBuffer.allocate(size);
            serializeVarInt(buffer, serializedKey.length);
            buffer.put(serializedKey);
            serializeList(buffer, value, listSize);
            return buffer.array();
        } catch (RuntimeException e) {
            throw new BunchedSerializationException("unable to serialize entry", e)
                    .setValue(new AbstractMap.SimpleImmutableEntry<>(key, value));
        }
    }

    /**
     * Packs an entry list into a single byte array. This does so by combining the
     * serialized forms of each key and value in the entry list with their
     * lengths. Their is a more in-depth explanation of the serialization format
     * in the class-level documentation.
     *
     * @param entries the list of entries to serialize
     * @return the serialized entry list
     * @throws BunchedSerializationException if the entries are invalid such as if the list is empty
     *                                       or contains a list that is not monotonically increasing
     */
    @Nonnull
    @Override
    public byte[] serializeEntries(@Nonnull List<Map.Entry<Tuple, List<Integer>>> entries) {
        if (entries.isEmpty()) {
            throw new BunchedSerializationException("cannot serialize empty entry list");
        }
        // Calculate size (to avoid unnecessary allocations)
        try {
            int size = PREFIX.length;
            List<byte[]> serializedKeys = new ArrayList<>(entries.size() - 1);
            int[] listSizes = new int[entries.size()];
            for (int i = 0; i < entries.size(); i++) {
                Map.Entry<Tuple, List<Integer>> entry = entries.get(i);
                if (i != 0) {
                    byte[] serializedKey = entry.getKey().pack();
                    size += getVarIntSize(serializedKey.length) + serializedKey.length;
                    serializedKeys.add(serializedKey);
                }
                int listSize = getListSize(entry.getValue());
                listSizes[i] = listSize;
                size += getVarIntSize(listSize) + listSize;
            }
            // Allocate one byte buffer to rule them all and serialize everything to that buffer
            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffer.put(PREFIX);
            for (int i = 0; i < entries.size(); i++) {
                if (i != 0) {
                    byte[] key = serializedKeys.get(i - 1);
                    serializeVarInt(buffer, key.length);
                    buffer.put(key);
                }
                serializeList(buffer, entries.get(i).getValue(), listSizes[i]);
            }
            return buffer.array();
        } catch (RuntimeException e) {
            throw new BunchedSerializationException("could not serialize entry list", e).setValue(entries);
        }
    }

    /**
     * Deserializes a key using standard {@link Tuple} unpacking.
     *
     * @param data source data to deserialize
     * @param offset beginning offset of serialized key (indexed from 0)
     * @param length length of serialized key
     * @return the deserialized key
     * @throws BunchedSerializationException if the byte array is malformed
     */
    @Nonnull
    @Override
    public Tuple deserializeKey(@Nonnull byte[] data, int offset, int length) {
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

    private void checkPrefix(@Nonnull byte[] data) {
        if (!ByteArrayUtil.startsWith(data, PREFIX)) {
            throw new BunchedSerializationException("serialized data begins with incorrect prefix").setData(data);
        }
    }

    @Nonnull
    private <T> List<T> deserializeBunch(@Nonnull Tuple key, @Nonnull byte[] data, boolean deserializeValues, @Nonnull BiFunction<Tuple, List<Integer>, T> itemCreator) {
        checkPrefix(data);
        try {
            List<T> list = new ArrayList<>();
            ByteBuffer buffer = ByteBuffer.wrap(data);
            buffer.position(PREFIX.length);
            boolean first = true;
            while (buffer.hasRemaining()) {
                Tuple entryKey;
                if (!first) {
                    int tupleSize = deserializeVarInt(buffer);
                    if (tupleSize == 0) {
                        entryKey = TupleHelpers.EMPTY;
                    } else {
                        entryKey = Tuple.fromBytes(data, buffer.position(), tupleSize);
                    }
                    buffer.position(buffer.position() + tupleSize);
                } else {
                    entryKey = key;
                    first = false;
                }
                List<Integer> entryValue;
                if (deserializeValues) {
                    entryValue = deserializeList(buffer);
                } else {
                    // Don't deserialize the value but advance the pointer as if we had.
                    entryValue = Collections.emptyList();
                    int listSize = deserializeVarInt(buffer);
                    buffer.position(buffer.position() + listSize);
                }
                list.add(itemCreator.apply(entryKey, entryValue));
            }
            return Collections.unmodifiableList(list);
        } catch (RuntimeException e) {
            throw new BunchedSerializationException("unable to deserialize entries", e).setData(data);
        }
    }

    /**
     * Deserializes an entry list from bytes.
     *
     * @param key key under which the serialized entry list was stored
     * @param data source list to deserialize
     * @return the deserialized entry list
     * @throws BunchedSerializationException if the byte array is malformed
     */
    @Nonnull
    @Override
    public List<Map.Entry<Tuple, List<Integer>>> deserializeEntries(@Nonnull Tuple key, @Nonnull byte[] data) {
        return deserializeBunch(key, data, true, AbstractMap.SimpleImmutableEntry::new);
    }

    /**
     * Deserializes the keys from a serialized entry list. Because the serialization format
     * contains markers with the length of the entries, it can skip the position list while
     * reading through the data, so it is more efficient (in terms of memory and space)
     * to call this method than {@link #deserializeEntries(Tuple, byte[]) deserializeEntries()}
     * if one only needs to know the keys.
     *
     * @param key key under which the serialized entry list was stored
     * @param data source data to deserialize
     * @return the list of keys in the serialized data array
     * @throws BunchedSerializationException if the byte array is malformed
     */
    @Nonnull
    @Override
    public List<Tuple> deserializeKeys(@Nonnull Tuple key, @Nonnull byte[] data) {
        return deserializeBunch(key, data, false, (t, ignore) -> t);
    }

    /**
     * Return <code>true</code> as this serialization format supports appending.
     *
     * @return <code>true</code>
     */
    @Override
    public boolean canAppend() {
        return true;
    }
}
