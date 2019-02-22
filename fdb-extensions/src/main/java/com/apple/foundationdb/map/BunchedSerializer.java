/*
 * BunchedSerializer.java
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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * A class to serialize and deserialize entries of a {@link BunchedMap}. This
 * is fairly standard in that it serializes and deserializes keys and values
 * to and from byte arrays.
 *
 * @param <K> type of the keys in the {@link BunchedMap}
 * @param <V> type of the values in the {@link BunchedMap}
 */
@API(API.Status.EXPERIMENTAL)
public interface BunchedSerializer<K,V> {

    /**
     * Serialize a key to bytes. These bytes will be used by the {@link BunchedMap}
     * to write a FoundationDB key. As a result, the sort order of the serialized
     * bytes should match the sort order of the underlying keys of the
     * map. (The comparison of bytes is done using unsigned byte comparison.)
     *
     * @param key key to serialize to bytes
     * @return the serialized key
     * @throws BunchedSerializationException if serializing the key fails
     */
    @Nonnull
    byte[] serializeKey(@Nonnull K key);

    /**
     * Serialize a single entry to bytes. This serializes a single key and
     * value to bytes. This function will be used in the case that
     * {@link #canAppend()} returns {@code true} and the {@link BunchedMap}
     * can optimize what gets written by appending an entry to the
     * end of an existing entry.
     *
     * @param key the key of the map entry
     * @param value the value of the map entry
     * @return the serialized entry
     * @throws BunchedSerializationException if serializing the entry fails
     */
    @Nonnull
    byte[] serializeEntry(@Nonnull K key, @Nonnull V value);

    /**
     * Serialize a single entry to bytes. This has the same semantics
     * as calling {@link #serializeEntry(Object, Object)} with the key and
     * value contained within the entry provided.
     *
     * @param entry the map entry to serialize
     * @return the serialized entry
     * @throws BunchedSerializationException if serializing the entry fails
     */
    @Nonnull
    default byte[] serializeEntry(@Nonnull Map.Entry<K, V> entry) {
        return serializeEntry(entry.getKey(), entry.getValue());
    }

    /**
     * Serialize a list of entries. This will be used when multiple keys and
     * values are serialized together in a single key/value pair in the
     * underlying FoundationDB cluster. The {@link BunchedMap} class guarantees
     * that when it serializes the entry list, it will store it under a key
     * corresponding to the first entry in the list, so implementations can
     * choose to omit the first entry's key to save space. That key will
     * then be passed back to the serializer by
     * {@link #deserializeEntries(Object, byte[]) deserializeEntries()}.
     *
     * @param entries the list of entries to serialize
     * @return the serialized list
     * @throws BunchedSerializationException if serializing the entries fails
     */
    @Nonnull
    byte[] serializeEntries(@Nonnull List<Map.Entry<K,V>> entries);

    /**
     * Deserialize a byte array into a key. This assumes that the entire
     * array is being used to store the data for the key. This
     * function should be the inverse of {@link #serializeKey(Object) serializeKey}.
     *
     * @param data source data to deserialize
     * @return key deserialized from reading <code>data</code>
     * @throws BunchedSerializationException if deserializing the key fails
     */
    @Nonnull
    default K deserializeKey(@Nonnull byte[] data) {
        return deserializeKey(data, 0, data.length);
    }

    /**
     * Deserialize a slice of a byte array into a key. This will only
     * deserialize the portion of the array starting at
     * <code>offset</code> and going to the end.
     *
     * @param data source data to deserialize
     * @param offset beginning offset of serialized key (indexed from 0)
     * @return key deserialized from reading <code>data</code>
     * @throws BunchedSerializationException if deserializing the key fails
     */
    @Nonnull
    default K deserializeKey(@Nonnull byte[] data, int offset) {
        return deserializeKey(data, offset, data.length - offset);
    }

    /**
     * Deserialize a slice of a byte array into a key. This will
     * only deserialize the portion of the array starting
     * at <code>offset</code> and going for <code>length</code>
     * bytes.
     *
     * @param data source data to deserialize
     * @param offset beginning offset of serialized key (indexed from 0)
     * @param length length of serialized key
     * @return key deserialized from reading <code>data</code>
     * @throws BunchedSerializationException if deserializing the key fails
     */
    @Nonnull
    K deserializeKey(@Nonnull byte[] data, int offset, int length);

    /**
     * Deserialize raw data to a list of entries. This should be
     * the inverse of {@link #serializeEntries(List) serializeEntries}.
     * Note that the order of elements returned within the list
     * should be the same as their sort order. The key that this entry
     * list is stored under is passed in so that implementations that
     * wish to can choose to omit the first key of the entry list from
     * the serialized value.
     *
     * @param key key under which the serialized entry list was stored
     * @param data source list to deserialize
     * @return entry list deserialized from reading <code>data</code>
     * @throws BunchedSerializationException if deserializing the entries fails
     */
    @Nonnull
    List<Map.Entry<K,V>> deserializeEntries(@Nonnull K key, @Nonnull byte[] data);

    /**
     * Deserialize raw data to a list of keys. This expects that <code>data</code>
     * contains both the keys and values for the various entries within.
     * However, this function will only return the keys. By default,
     * this will deserialize both the keys and the values contained within
     * and just throw away the values, but implementations may decide
     * to do this more efficiently if there is a way to do so.
     *
     * @param key key under which the serialized entry list was stored
     * @param data source data to deserialize
     * @return key list deserialized from reading <code>data</code>
     * @throws BunchedSerializationException if deserializing the keys fails
     */
    @Nonnull
    default List<K> deserializeKeys(@Nonnull K key, @Nonnull byte[] data) {
        return deserializeEntries(key, data).stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    /**
     * Whether the output from {@link #serializeEntry(Object, Object) serializeEntry}
     * can be appended to an existing serialized entry list to produce a new bunched value.
     * That is, if this function returns <code>true</code>, then if some entry list
     * <code>l1</code> is a prefix of another entry list <code>l2</code>, then
     * the serialization of <code>l1</code> is also a serialization of the list
     * <code>l2</code>. If this is the case, than the {@link BunchedMap} class
     * can make certain optimizations during value insertion.
     *
     * @return whether this serializer will serialize entry lists in a way that
     *         allows entries to be appended to existing entry lists
     */
    default boolean canAppend() {
        return false;
    }
}
