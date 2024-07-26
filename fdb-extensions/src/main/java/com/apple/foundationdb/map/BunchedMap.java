/*
 * BunchedMap.java
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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionContext;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncPeekCallbackIterator;
import com.apple.foundationdb.async.AsyncPeekIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.util.LogMessageKeys;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * An implementation of a FoundationDB-backed map that bunches close keys together to minimize the
 * overhead of storing keys with a common prefix. The most straight-forward way to store a map in
 * FoundationDB is to store one key-value pair in the some subspace of the database for each key
 * and value of the map. However, this can lead to problems if there are either too many keys or
 * if the subspace prefix is too large as that prefix will be repeated many times (once for each key
 * in the map).
 *
 * <p>
 * This structure "bunches" adjacent keys together so that one key in the database is responsible
 * for storing multiple entries in the map, which effectively amortizes the cost of this subspace prefix
 * across multiple map entries. In particular, the map will choose "signpost" keys in the map. For each
 * signpost, a key in the database is constructed that is the subspace prefix concatenated with the
 * serialized key. This key is then responsible for storing every entry in the map for which the key
 * is greater than or equal to the signpost key but less than the next signpost key. The signposts are
 * chosen dynamically as keys are added and removed from the map. In particular, there is a target
 * "bunch size" that is a parameter to the map, and upon inserting, the map will see if there is a
 * bunch that the given key can be placed in without exceeding the bunch size. If not, it will create
 * one by adding a new signpost key.
 * </p>
 *
 * <p>
 * The cost for bunching entries this way is that it requires that the client perform additional database
 * reads while inserting, so mutations have a higher latency than under the simpler scheme, and two clients
 * attempting to modify the same map are likely to experience contention. It is also more expensive to read
 * a single key from the map (as the read now will also read the data for keys in the same bunch as the desired
 * key). A full scan of the map requires less data be transferred over the wire as the subspace prefix can
 * be sent fewer times, so scan-heavy use-cases might not experience much of an overhead at all.
 * </p>
 *
 * <p>
 * Most methods of this class take a subspace. For the most part, these methods assume that there is one
 * durable instance of a <code>BunchedMap</code> within the bounds of the subspace provided. The exception
 * to this is the
 * {@link #scanMulti(ReadTransaction, Subspace, SubspaceSplitter, byte[], byte[], byte[], int, boolean)} scanMulti()}
 * family of methods. See the documentation on those methods for more information.
 * </p>
 *
 * <p>
 * This class is not thread-safe in the general case. Assuming that the serializer and key-comparator are
 * both thread-safe, this class is safe to use from multiple transactions at once or with multiple subspaces
 * concurrently within a single transaction. However, it is unsafe to modify two keys within the same subspace
 * in the same transaction from multiple threads.
 * </p>
 *
 * @param <K> type of keys in the map
 * @param <V> type of values in the map
 */
@API(API.Status.EXPERIMENTAL)
public class BunchedMap<K, V> {
    private static final int MAX_VALUE_SIZE = 10_000; // The actual max value size is 100_000, but let's stay clear of that
    private static final byte[] ZERO_ARRAY = { 0x00 };

    @Nonnull
    private final Comparator<K> keyComparator;
    @Nonnull
    private final BunchedSerializer<K, V> serializer;
    private final int bunchSize;

    /**
     * Create a bunched map with the given serializer, key comparator, and bunch size. The provided serializer
     * is used to serialize keys and values when writing to the database and to deserialize them when
     * reading. The comparator is used to maintain keys in a sorted order. The sorted order of keys, however,
     * should be consistent with the byte order of serialized keys (when using unsigned lexicographic comparison),
     * as that comparison method is used by the map when it is more efficient. The bunch size is the maximum number
     * of map keys within any bunch of keys within the database. This value is not stored durably in the database,
     * and it is safe to change this value over time (and to have different writers using different values for the
     * bunch size concurrently), though one writer might undo the work of another writer or make different decisions
     * when splitting up values or adding to bunches.
     *
     * @param serializer serialize to use when reading or writing data
     * @param keyComparator comparator used to order keys
     * @param bunchSize maximum size of bunch within the database
     */
    public BunchedMap(@Nonnull BunchedSerializer<K, V> serializer, @Nonnull Comparator<K> keyComparator, int bunchSize) {
        this.serializer = serializer;
        this.keyComparator = keyComparator;
        this.bunchSize = bunchSize;
    }

    /**
     * Copy constructor for {@link BunchedMap}s. This will create a new {@link BunchedMap} with the same internal
     * state as the original one. Because the {@link BunchedMap} has entirely immutable state, the primary use
     * case for this is for extending subclasses to ensure that the class gets initialized correctly, which is
     * why this is not a {@code public} method.
     *
     * @param model original {@link BunchedMap} to base the new one on
     */
    protected BunchedMap(@Nonnull BunchedMap<K, V> model) {
        this(model.serializer, model.keyComparator, model.bunchSize);
    }

    private static <T> List<T> makeMutable(@Nonnull List<T> list) {
        if (list instanceof ArrayList<?>) {
            return list;
        } else {
            return new ArrayList<>(list);
        }
    }

    /**
     * Instrument a range read. The base implementation only returns the original future, but extenders are encouraged
     * to override this method with their own implementations that, for example, records the total numbers
     * of keys read and their sizes.
     *
     * @param readFuture a future that will complete to a list of keys and values
     * @return an instrumented future that returns the same values as the original future
     */
    @Nonnull
    protected CompletableFuture<List<KeyValue>> instrumentRangeRead(@Nonnull CompletableFuture<List<KeyValue>> readFuture) {
        return readFuture;
    }

    /**
     * Instrument a write. The default implementation does nothing, but extenders are encouraged to override this
     * method with their own implementations that, for example, counts how many bytes have been added
     * or removed from the database. Implementations should <em>not</em> modify the values in any
     * of the arrays, and they should also be aware that the {@code oldValue} parameter may be {@code null}.
     *
     * @param key the key being written
     * @param value the new value being written to the key
     * @param oldValue the previous value being overwritten or {@code null} if the key is new or the previous value unknown
     */
    protected void instrumentWrite(@Nonnull byte[] key, @Nonnull byte[] value, @Nullable byte[] oldValue) {

    }

    /**
     * Instrument a delete. The default implementation does nothing, but extenders are encouraged to override
     * this method with their own implementation that, for example, counts how many bytes have been
     * removed from the database. Implementations should <em>not</em> modify the values in any of the arrays,
     * and they should also be aware that the {@code oldValue} parameter may be {@code null}.
     *
     * @param key the key being deleted
     * @param oldValue the previous value being delete or {@code null} if the previous value is unknown
     */
    protected void instrumentDelete(@Nonnull byte[] key, @Nullable byte[] oldValue) {

    }

    private CompletableFuture<Optional<KeyValue>> entryForKey(@Nonnull Transaction tr, @Nonnull byte[] subspaceKey, @Nonnull K key) {
        byte[] keyBytes = ByteArrayUtil.join(subspaceKey, serializer.serializeKey(key));
        tr.addReadConflictKey(keyBytes);
        // We need to use a range read rather than a getKey with a single key selector
        // because we need to return the key back as well as the value.
        // In practice, this range request should always return a single element, but
        // in rare cases, concurrent updates near and around the endpoints might
        // result in additional elements being returned.
        return instrumentRangeRead(tr.snapshot().getRange(
                KeySelector.lastLessOrEqual(keyBytes),
                KeySelector.firstGreaterThan(keyBytes),
                ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL
        ).asList()).thenApply(keyValues -> {
            if (keyValues.isEmpty()) {
                // There aren't any entries before this key in the database.
                return Optional.empty();
            } else {
                // The last (and probably only) result of the range read should be
                // the greatest key that is less than or equal to keyBytes.
                KeyValue kv = keyValues.get(keyValues.size() - 1);
                if (ByteArrayUtil.compareUnsigned(kv.getKey(), keyBytes) > 0) {
                    throw new BunchedMapException("signpost key found for key is greater than original key")
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspaceKey))
                            .addLogInfo("key", ByteArrayUtil2.loggable(keyBytes))
                            .addLogInfo("signpostKey", ByteArrayUtil2.loggable(kv.getKey()));
                }
                if (ByteArrayUtil.startsWith(kv.getKey(), subspaceKey)) {
                    // The candidate key is in the correct subspace, so this is the signpost key for the given key
                    return Optional.of(kv);
                } else {
                    // The candidate key is not in the correct subspace, so we must be looking for a
                    // key that is smaller than the smallest key currently in the map (which is
                    // vacuously the case if the map is empty).
                    return Optional.empty();
                }
            }
        });
    }

    // Grand Theory of Conflict Ranges
    //
    // Because the map must do range scans that can potentially touch much larger ranges than
    // is necessary, all reads are done at snapshot isolation level and then conflict ranges are
    // added as needed to try and decrease contention. This logic is a little complicated, so here
    // is an attempt to explain the reasoning behind it. The main goal is to enforce the following
    // invariants:
    //
    // 1. For any given DB key, all map keys greater than or equal to that key but strictly less
    //    than the next DB key are in the DB value associated with that key.
    // 2. Any map read that depends on the exact value of a map key being read that is changed
    //    by a concurrent transaction will trigger a conflict at commit time.
    // 3. After a user has written a value to the map, subsequent operations should preserve that
    //    value.
    //
    // In some sense, condition 1 is that the integrity of the data structure should be preserved,
    // condition 2 is somewhat analogous to serializability, and condition 3 is analogous to
    // linearizability and durability. This leads to the following set of conflict ranges, specified
    // here in an order that is supposed to reflect how straightforward each added conflict range is:
    //
    // a. When reading a map key, add a read conflict key to the corresponding key in the
    //    DB regardless of DB keys actually read. When modifying a map key, add a write
    //    conflict key to that same key. This gets us (2).
    // b. When modifying a DB key, we will end up issuing a write that re-writes all
    //    values in its range, so add a read conflict range over those keys so that any
    //    modifications to those keys that happen between read time and commit time
    //    are not overwritten by the re-write. This gets us (3).
    // c. When adding a map key to the end of a DB key's range or merging an existing
    //    DB key's range into a new key, a write conflict range must be added for the "gaps"
    //    that existed between the key ranges. This is necessary because without it,
    //    a concurrent modification can read the range that the DB key is now responsible
    //    for and write to it in a way that violates (1).
    //
    // There exist semi-formal proofs as to why these conflict ranges are sufficient to
    // guarantee the three invariants proposed, but they are too large to fit into this
    // comment. But in addition to proving them correct, a fair amount of testing has gone into
    // trying to verify that they work as intended through randomized testing.

    private void addEntryListReadConflictRange(@Nonnull Transaction tr, @Nonnull byte[] subspaceKey, @Nonnull byte[] keyBytes, @Nonnull List<Map.Entry<K, V>> entryList) {
        byte[] end = ByteArrayUtil.join(subspaceKey, serializer.serializeKey(entryList.get(entryList.size() - 1).getKey()), ZERO_ARRAY);
        tr.addReadConflictRange(keyBytes, end);
    }

    private void insertAlone(@Nonnull Transaction tr, @Nonnull byte[] keyBytes, @Nonnull Map.Entry<K, V> entry) {
        tr.addReadConflictKey(keyBytes);
        byte[] valueBytes = serializer.serializeEntries(Collections.singletonList(entry));
        tr.set(keyBytes, valueBytes);
        instrumentWrite(keyBytes, valueBytes, null);
    }

    private void writeEntryListWithoutChecking(@Nonnull Transaction tr, @Nonnull byte[] subspaceKey, @Nonnull byte[] keyBytes,
                                               @Nullable KeyValue oldKv, @Nonnull byte[] newKey, @Nonnull List<Map.Entry<K, V>> entryList,
                                               @Nonnull byte[] serializedBytes) {
        // The order of these operations is fairly important as, it turns out, adding an explicit
        // read conflict range does will skip over values that have already been written. This
        // means that we will miss the value that is the actual key we are writing if we
        // do these in the wrong order.
        // TODO: Adding an explicit read conflict range skips the keys in write cache (https://github.com/apple/foundationdb/issues/126)
        addEntryListReadConflictRange(tr, subspaceKey, newKey, entryList);
        final byte[] oldKey = oldKv == null ? null : oldKv.getKey();
        final byte[] oldValue;
        if (oldKey != null && !Arrays.equals(oldKey, newKey)) {
            tr.clear(oldKey);
            instrumentDelete(oldKey, oldKv.getValue());
            oldValue = null; // set the old value to null so that the instrumentation of the write doesn't double-count the delete
        } else {
            oldValue = oldKv == null ? null : oldKv.getValue();
        }
        tr.set(newKey, serializedBytes);
        instrumentWrite(newKey, serializedBytes, oldValue);
        if (!Arrays.equals(keyBytes, newKey)) {
            tr.addWriteConflictKey(keyBytes);
        }
    }

    private void writeEntryList(@Nonnull Transaction tr, @Nonnull byte[] subspaceKey, @Nonnull byte[] keyBytes,
                                @Nullable KeyValue oldKv, @Nonnull byte[] newKey, @Nonnull List<Map.Entry<K, V>> entryList,
                                @Nullable KeyValue kvAfter, boolean isFirst, boolean isLast) {
        byte[] serializedBytes = serializer.serializeEntries(entryList);
        if (serializedBytes.length > MAX_VALUE_SIZE) {
            if (isFirst || entryList.size() == 1) {
                insertAlone(tr, keyBytes, entryList.get(0));
            } else if (isLast) {
                insertAfter(tr, subspaceKey, keyBytes, kvAfter, entryList.get(entryList.size() - 1));
            } else {
                // Splits the keys down the middle. In principle, this might result in keys that exceed
                // the maximum value size (if there are weird non-linearities in the serializer--for example,
                // if it compresses). However, in practice, this will almost always produce two keys that
                // are under the maximum value size. If one or more of the two keys exceed the correct size, but
                // each value is still less than the FDB maximum value size (which is likely given that
                // MAX_VALUE_SIZE is much less than the actual FDB maximum value size), then everything will just
                // work. If either one exceeds that maximum value size, then the insertion fails and an error
                // bubbles up to the user. This is worse than there not being an error, but it is safe.
                int splitPoint = entryList.size() / 2;
                List<Map.Entry<K, V>> firstEntries = entryList.subList(0, splitPoint);
                byte[] firstSerialized = serializer.serializeEntries(firstEntries);
                List<Map.Entry<K, V>> secondEntries = entryList.subList(splitPoint, entryList.size());
                byte[] secondSerialized = serializer.serializeEntries(secondEntries);
                writeEntryListWithoutChecking(tr, subspaceKey, keyBytes, oldKv, newKey, firstEntries, firstSerialized);
                byte[] secondKey = ByteArrayUtil.join(subspaceKey, serializer.serializeKey(secondEntries.get(0).getKey()));
                writeEntryListWithoutChecking(tr, subspaceKey, keyBytes, null, secondKey, secondEntries, secondSerialized);
            }
        } else {
            if (serializer.canAppend() && isLast && entryList.size() > 1 && oldKv != null && Arrays.equals(oldKv.getKey(), newKey)) {
                // Note: APPEND_IF_FITS will silently fail if the size of the value is greater than the maximum
                // value size. It is therefore *very* important that we check what the size will be before
                // calling this method to make sure that the total size is not too large. Otherwise, we might
                // lose data.
                addEntryListReadConflictRange(tr, subspaceKey, newKey, entryList);
                byte[] appendBytes = serializer.serializeEntry(entryList.get(entryList.size() - 1));
                tr.mutate(MutationType.APPEND_IF_FITS, newKey, appendBytes);
                instrumentWrite(newKey, appendBytes, null); // do not include old value in instrumentation as we are incrementing the total size
                tr.addWriteConflictKey(keyBytes);
            } else {
                writeEntryListWithoutChecking(tr, subspaceKey, keyBytes, oldKv, newKey, entryList, serializedBytes);
            }
            // When appending before the beginning or writing after the end, we are essentially asserting
            // that this key will be responsible for an additional range of map keys. Concurrent transactions
            // might also claim this section of the logical key range in incompatible ways if we do not
            // declare write conflict ranges here.
            if (isFirst && entryList.size() >= 2) {
                tr.addWriteConflictRange(keyBytes, ByteArrayUtil.join(subspaceKey, serializer.serializeKey(entryList.get(1).getKey())));
            }
            if (isLast && entryList.size() >= 2) {
                tr.addWriteConflictRange(ByteArrayUtil.join(subspaceKey, serializer.serializeKey(entryList.get(entryList.size() - 2).getKey())), keyBytes);
            }
        }
    }

    private void insertAfter(@Nonnull Transaction tr, @Nonnull byte[] subspaceKey, @Nonnull byte[] keyBytes,
                             @Nullable KeyValue kvAfter, @Nonnull Map.Entry<K, V> entry) {
        if (kvAfter == null) {
            insertAlone(tr, keyBytes, entry);
        } else {
            K afterKey = serializer.deserializeKey(kvAfter.getKey(), subspaceKey.length);
            List<Map.Entry<K, V>> afterEntryList = serializer.deserializeEntries(afterKey, kvAfter.getValue());
            if (afterEntryList.size() >= bunchSize) {
                // The next list of entries is too large. Write to a separate KV pair.
                insertAlone(tr, keyBytes, entry);
            } else {
                // Bunch this entry with the next one.
                List<Map.Entry<K, V>> newEntryList = new ArrayList<>(afterEntryList.size() + 1);
                newEntryList.add(entry);
                newEntryList.addAll(afterEntryList);
                writeEntryList(tr, subspaceKey, keyBytes, kvAfter, keyBytes, newEntryList, null, true, false);
            }
        }
    }

    @Nonnull
    private Optional<V> insertEntry(@Nonnull Transaction tr, @Nonnull byte[] subspaceKey, @Nonnull byte[] keyBytes,
                                    @Nonnull K key, @Nonnull V value, @Nullable KeyValue kvBefore, @Nullable KeyValue kvAfter,
                                    @Nonnull Map.Entry<K, V> entry) {
        if (kvBefore == null) {
            insertAfter(tr, subspaceKey, keyBytes, kvAfter, entry);
            return Optional.empty();
        } else {
            K beforeKey = serializer.deserializeKey(kvBefore.getKey(), subspaceKey.length);
            List<Map.Entry<K, V>> beforeEntryList = serializer.deserializeEntries(beforeKey, kvBefore.getValue());
            int insertIndex = 0;
            while (insertIndex < beforeEntryList.size() && keyComparator.compare(key, beforeEntryList.get(insertIndex).getKey()) > 0) {
                insertIndex++;
            }
            if (insertIndex < beforeEntryList.size() && keyComparator.compare(key, beforeEntryList.get(insertIndex).getKey()) == 0) {
                // This key is already in the map, so we are going to end up re-writing it iff the value is different.
                Map.Entry<K, V> oldEntry = beforeEntryList.get(insertIndex);
                V oldValue = oldEntry.getValue();
                if (!oldEntry.getValue().equals(value)) {
                    beforeEntryList = makeMutable(beforeEntryList);
                    beforeEntryList.set(insertIndex, entry);
                    writeEntryList(tr, subspaceKey, keyBytes, kvBefore, kvBefore.getKey(),
                            beforeEntryList, kvAfter, false, false);
                } else {
                    // We are choosing to not re-write the key because it
                    // is already the value we wanted any way. Add a
                    // read conflict key to it so if something else
                    // changes it, this transaction will need to be retried
                    // to set it back.
                    tr.addReadConflictKey(keyBytes);
                }
                return Optional.of(oldValue);
            } else if (insertIndex < beforeEntryList.size()) {
                // This key is going to be inserted somewhere in the middle
                beforeEntryList = makeMutable(beforeEntryList);
                beforeEntryList.add(insertIndex, entry);
                if (beforeEntryList.size() <= bunchSize) {
                    // Insert the entry in the middle and serialize.
                    writeEntryList(tr, subspaceKey, keyBytes, kvBefore, kvBefore.getKey(),
                            beforeEntryList, kvAfter, false, false);
                } else {
                    // Split this entry in half (roughly) and insert both halves
                    int splitPoint = beforeEntryList.size() / 2;
                    writeEntryList(tr, subspaceKey, keyBytes, kvBefore, kvBefore.getKey(),
                            beforeEntryList.subList(0, splitPoint), null, false, false);
                    List<Map.Entry<K, V>> secondEntries = beforeEntryList.subList(splitPoint, beforeEntryList.size());
                    byte[] secondKey = ByteArrayUtil.join(subspaceKey, serializer.serializeKey(secondEntries.get(0).getKey()));
                    writeEntryList(tr, subspaceKey, keyBytes, null, secondKey, secondEntries, kvAfter, false, false);
                }
                return Optional.empty();
            } else {
                // This key is going to be inserted after all of the keys in the before entry.
                if (beforeEntryList.size() < bunchSize) {
                    // Append to the end of the current list.
                    List<Map.Entry<K, V>> newEntryList = new ArrayList<>(beforeEntryList.size() + 1);
                    newEntryList.addAll(beforeEntryList);
                    newEntryList.add(entry);
                    writeEntryList(tr, subspaceKey, keyBytes, kvBefore, kvBefore.getKey(), newEntryList, kvAfter, false, true);
                } else {
                    // This key would make the bunch too large. Insert it into the next one.
                    insertAfter(tr, subspaceKey, keyBytes, kvAfter, entry);
                }
                return Optional.empty();
            }
        }
    }

    /**
     * Inserts or updates a key into a map with a new value. This will find an appropriate
     * bunch to insert the key into (or create one if one doesn't exist or if all of the candidates
     * are full). It will do work to make sure that the placement is locally optimal (that is, it
     * will choose between the one or two bunches closest to the key when performing its bunching).
     * It makes no attempt to fix suboptimal bunches elsewhere within the map. If the map already
     * contains <code>key</code>, it will overwrite the existing key with the new value. This will
     * return the old value if one is present.
     *
     * <p>
     * Note that this method is <b>not</b> thread-safe if multiple threads call it with the same
     * transaction and subspace. (Multiple calls with different transactions or subspaces are safe.)
     * </p>
     *
     * <p>
     * Note that this call is asynchronous. It will return a {@link CompletableFuture} that will be
     * completed when this task has completed.
     * </p>
     *
     * @param tcx database or transaction to use when performing the insertion
     * @param subspace subspace within which the map's data are located
     * @param key key of the map entry to insert
     * @param value value of the map entry to insert
     * @return a future that will complete with an optional that will either contain the previous value
     *         associated with the key or be empty if there was not a previous value
     */
    @Nonnull
    public CompletableFuture<Optional<V>> put(@Nonnull TransactionContext tcx, @Nonnull Subspace subspace, @Nonnull K key, @Nonnull V value) {
        return tcx.runAsync(tr -> {
            byte[] subspaceKey = subspace.pack();
            byte[] keyBytes = ByteArrayUtil.join(subspaceKey, serializer.serializeKey(key));

            // We need to know the key (and value) that is less than or equal to
            // the key we are trying to insert in our map as well as the key (and value)
            // that is greater than our key in the map. Many insertions will not actually
            // require both of these, but it is better to grab them both at once using
            // a single range read that (with a very high likelihood) will hit a single
            // storage server than to do the two reads separately.
            //
            // Note that we do this read at snapshot isolation level, so if we read too much,
            // that won't be a problem in terms of conflict ranges (we will just add the
            // correct conflict ranges later).
            //
            // In practice, the range read will almost always return at most 2 results. Because
            // of how range reads with key selectors are implemented, there is a slight
            // possibility that there will be more than two if, for example, additional
            // keys are added (within this transaction) to the RYW cache.
            return instrumentRangeRead(tr.snapshot().getRange(
                    KeySelector.lastLessOrEqual(keyBytes),
                    KeySelector.firstGreaterThan(keyBytes).add(1),
                    ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL
            ).asList()).thenApply(keyValues -> {
                KeyValue kvBefore = null;
                KeyValue kvAfter = null;
                for (KeyValue next : keyValues) {
                    if (ByteArrayUtil.startsWith(next.getKey(), subspaceKey)) {
                        if (ByteArrayUtil.compareUnsigned(keyBytes, next.getKey()) < 0) {
                            kvAfter = next;
                            break; // no need to continue after kvAfter is set
                        }
                        if (ByteArrayUtil.compareUnsigned(next.getKey(), keyBytes) <= 0) {
                            kvBefore = next;
                        }
                    }
                }

                // If either of these trigger, than it means that I screwed up the logic here in
                // picking the correct keys and values.
                if (kvBefore != null && (ByteArrayUtil.compareUnsigned(keyBytes, kvBefore.getKey()) < 0 || !ByteArrayUtil.startsWith(kvBefore.getKey(), subspaceKey))) {
                    throw new BunchedMapException("database key before map key compared incorrectly")
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspaceKey))
                            .addLogInfo("key", ByteArrayUtil2.loggable(keyBytes))
                            .addLogInfo("kvBefore", ByteArrayUtil2.loggable(kvBefore.getKey()));
                }
                if (kvAfter != null && (ByteArrayUtil.compareUnsigned(keyBytes, kvAfter.getKey()) >= 0 || !ByteArrayUtil.startsWith(kvAfter.getKey(), subspaceKey))) {
                    throw new BunchedMapException("database key after map key compared incorrectly")
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspaceKey))
                            .addLogInfo("key", ByteArrayUtil2.loggable(keyBytes))
                            .addLogInfo("kvAfter", ByteArrayUtil2.loggable(kvAfter.getKey()));
                }

                Map.Entry<K, V> newEntry = new AbstractMap.SimpleImmutableEntry<>(key, value);
                return insertEntry(tr, subspaceKey, keyBytes, key, value, kvBefore, kvAfter, newEntry);
            });
        });
    }

    /**
     * Determines whether a key is contained within the map. This method is safe to run concurrently
     * with other map operations in other threads. However, if there are concurrent
     * {@link #put(TransactionContext, Subspace, Object, Object) put}
     * or {@link #remove(TransactionContext, Subspace, Object) remove}
     * calls, then there are no guarantees as to whether this will return <code>true</code> or <code>false</code>.
     *
     * @param tcx database or transaction to use when performing reads
     * @param subspace subspace within which the map's data are located
     * @param key the key to check for membership within the map
     * @return a future that will be completed to <code>true</code> if the map contains <code>key</code>
     *         and <code>false</code> otherwise
     */
    @Nonnull
    public CompletableFuture<Boolean> containsKey(@Nonnull TransactionContext tcx, @Nonnull Subspace subspace, @Nonnull K key) {
        final byte[] subspaceKey = subspace.getKey();
        return tcx.runAsync(tr -> entryForKey(tr, subspaceKey, key)
            .thenApply(optionalEntry -> optionalEntry
                .map(kv -> {
                    K mapKey = serializer.deserializeKey(kv.getKey(), subspaceKey.length);
                    return serializer.deserializeKeys(mapKey, kv.getValue()).contains(key);
                })
                .orElse(false)
            )
        );
    }

    /**
     * Retrieves the value associated with a key from the map. This method is safe to run concurrently
     * with other map operations. However, if there are concurrent
     * {@link #put(TransactionContext, Subspace, Object, Object) put}
     * or {@link #remove(TransactionContext, Subspace, Object) remove}
     * operations, then there are no guarantees as to whether this operation will see the result of the
     * concurrent operation or not.
     *
     * @param tcx database or transaction to use when performing reads
     * @param subspace subspace within which the map's data are located
     * @param key the key within the map to retrieve the value of
     * @return a future that will be completed with an optional that will be present with the value
     *         associated with the key in the database or empty if the key is not contained within the map
     */
    @Nonnull
    public CompletableFuture<Optional<V>> get(@Nonnull TransactionContext tcx, @Nonnull Subspace subspace, @Nonnull K key) {
        final byte[] subspaceKey = subspace.getKey();
        return tcx.runAsync(tr -> entryForKey(tr, subspaceKey, key)
            .thenApply(optionalEntry -> optionalEntry
                .flatMap(kv -> {
                    K mapKey = serializer.deserializeKey(kv.getKey(), subspaceKey.length);
                    final List<Map.Entry<K, V>> entryList = serializer.deserializeEntries(mapKey, kv.getValue());
                    return entryList.stream()
                            .filter(entry -> entry.getKey().equals(key))
                            .findAny()
                            .map(Map.Entry::getValue);
                })
            )
        );
    }

    /**
     * Removes a key from the map. This returns a future that will contain an optional with the
     * old value associated with the key within the map (prior to deletion) if one is present or
     * will be empty if the key was not contained within the map.
     *
     * <p>
     * Note that this method is <b>not</b> thread-safe if multiple threads call it with the same
     * transaction and subspace. (Multiple calls with different transactions or subspaces are safe.)
     * </p>
     *
     * <p>
     * Note that this call is asynchronous. It will return a {@link CompletableFuture} that will be completed
     * when this task has completed.
     * </p>
     *
     * @param tcx database or transaction to use when removing the key
     * @param subspace subspace within which the map's data are located
     * @param key the key to remove from the map
     * @return a future that will be completed with an optional that will be present with the value associated
     *         with the key in the database (prior to removal) or will be empty if the key was not present
     */
    @Nonnull
    public CompletableFuture<Optional<V>> remove(@Nonnull TransactionContext tcx, @Nonnull Subspace subspace, @Nonnull K key) {
        final byte[] subspaceKey = subspace.getKey();
        return tcx.runAsync(tr -> entryForKey(tr, subspaceKey, key).thenApply(optionalEntry -> optionalEntry.flatMap((KeyValue kv) -> {
            K mapKey = serializer.deserializeKey(kv.getKey(), subspaceKey.length);
            List<Map.Entry<K, V>> entryList = serializer.deserializeEntries(mapKey, kv.getValue());
            int foundIndex = -1;
            for (int i = 0; i < entryList.size(); i++) {
                if (entryList.get(i).getKey().equals(key)) {
                    foundIndex = i;
                    break;
                }
            }
            if (foundIndex != -1) {
                final Map.Entry<K, V> oldEntry = entryList.get(foundIndex);
                // The value that gets written is based on the contents of the entries read, so
                // we need to add a read range with all of the values we are currently writing.
                addEntryListReadConflictRange(tr, subspaceKey, kv.getKey(), entryList);
                tr.addWriteConflictKey(ByteArrayUtil.join(subspaceKey, serializer.serializeKey(key)));

                if (entryList.size() == 1) {
                    // The only key that was in the range was the key that
                    // we are currently removing, so just remove it.
                    tr.clear(kv.getKey());
                    instrumentDelete(kv.getKey(), null);
                } else {
                    // We have other items in the entry. Remove the entry
                    // we actually care about and serialize the rest.
                    entryList = makeMutable(entryList);
                    entryList.remove(foundIndex);
                    final byte[] newKey;
                    final byte[] oldValue;
                    if (foundIndex == 0) {
                        tr.clear(kv.getKey());
                        instrumentDelete(kv.getKey(), kv.getValue());
                        newKey = ByteArrayUtil.join(subspaceKey, serializer.serializeKey(entryList.get(0).getKey()));
                        oldValue = null;
                    } else {
                        newKey = kv.getKey();
                        oldValue = kv.getValue();
                    }
                    final byte[] newValue = serializer.serializeEntries(entryList);
                    tr.set(newKey, newValue);
                    instrumentWrite(newKey, newValue, oldValue);
                }
                return Optional.of(oldEntry.getValue());
            } else {
                return Optional.empty();
            }
        })));
    }

    /**
     * Verify the integrity of the bunched map. This will read through all of the database keys associated
     * with the map and verify that all of the keys are in order. If it encounters an error, it will
     * complete with an exception. Otherwise, the returned future will complete normally.
     *
     * @param tcx database or transaction to use when removing the key
     * @param subspace subspace within which the map's data are located
     * @return a future that will complete when the integrity check has finished
     */
    @Nonnull
    public CompletableFuture<Void> verifyIntegrity(@Nonnull TransactionContext tcx, @Nonnull Subspace subspace) {
        return tcx.runAsync(tr -> {
            AtomicReference<K> lastKey = new AtomicReference<>(null);
            byte[] subspaceKey = subspace.getKey();
            AsyncIterable<KeyValue> iterable = tr.getRange(subspace.range());
            return AsyncUtil.forEach(iterable, kv -> {
                K boundaryKey = serializer.deserializeKey(kv.getKey(), subspaceKey.length);
                if (lastKey.get() != null && keyComparator.compare(boundaryKey, lastKey.get()) < 0) {
                    throw new BunchedMapException("boundary key out of order")
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspaceKey))
                            .addLogInfo("lastKey", lastKey.get())
                            .addLogInfo("boundaryKey", boundaryKey);
                }
                lastKey.set(boundaryKey);
                List<K> keys = serializer.deserializeKeys(boundaryKey, kv.getValue());
                for (K key : keys) {
                    if (keyComparator.compare(key, lastKey.get()) < 0) {
                        throw new BunchedMapException("keys within bunch out of order")
                                .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(subspaceKey))
                                .addLogInfo("lastKey", lastKey.get())
                                .addLogInfo("nextKey", key);
                    }
                    lastKey.set(key);
                }
            }, tr.getExecutor());
        });
    }

    private void flushEntryList(@Nonnull Transaction tr, @Nonnull byte[] subspaceKey,
                                @Nonnull List<Map.Entry<K, V>> currentEntryList,
                                @Nonnull AtomicReference<K> lastKey) {
        byte[] keyBytes = ByteArrayUtil.join(subspaceKey, serializer.serializeKey(currentEntryList.get(0).getKey()));
        writeEntryListWithoutChecking(tr, subspaceKey, keyBytes, null, keyBytes, currentEntryList,
                serializer.serializeEntries(currentEntryList));
        lastKey.set(currentEntryList.get(currentEntryList.size() - 1).getKey());
        currentEntryList.clear();
    }

    /**
     * Compact the values within the map into as few keys as possible. This will scan through and re-write
     * the keys to be optimal. This feature is experimental at the moment, but it should be used to better
     * pack entries if needed.
     *
     * @param tcx database or transaction to use when compacting data
     * @param subspace subspace within which the map's data are located
     * @param keyLimit maximum number of database keys to read in a single transaction
     * @param continuation the continuation returned from a previous call or <code>null</code>
     *                     to start from the beginning of the subspace
     * @return future that will complete with a continuation that can be used to complete
     *         the compaction across multiple transactions (<code>null</code> if finished)
     */
    @Nonnull
    protected CompletableFuture<byte[]> compact(@Nonnull TransactionContext tcx, @Nonnull Subspace subspace,
                                              int keyLimit, @Nullable byte[] continuation) {
        return tcx.runAsync(tr -> {
            byte[] subspaceKey = subspace.getKey();
            byte[] begin = (continuation == null) ? subspaceKey : continuation;
            byte[] end = subspace.range().end;
            final AsyncIterable<KeyValue> iterable = tr.snapshot().getRange(begin, end, keyLimit);
            List<Map.Entry<K, V>> currentEntryList = new ArrayList<>(bunchSize);
            // The estimated size can be off (and will be off for many implementations of BunchedSerializer),
            // but it is just a heuristic to know when to split, so that's fine (I claim).
            AtomicInteger currentEntrySize = new AtomicInteger(0);
            AtomicInteger readKeys = new AtomicInteger(0);
            AtomicReference<byte[]> lastReadKeyBytes = new AtomicReference<>(null);
            AtomicReference<K> lastKey = new AtomicReference<>(null);
            return AsyncUtil.forEach(iterable, kv -> {
                final K boundaryKey = serializer.deserializeKey(kv.getKey(), subspaceKey.length);
                final List<Map.Entry<K,  V>> entriesFromKey = serializer.deserializeEntries(boundaryKey, kv.getValue());
                readKeys.incrementAndGet();
                if (entriesFromKey.size() >= bunchSize && currentEntryList.isEmpty()) {
                    // Nothing can be done. Just move on.
                    lastReadKeyBytes.set(null);
                    return;
                }
                if (lastReadKeyBytes.get() == null) {
                    lastReadKeyBytes.set(kv.getKey());
                }
                final byte[] endKeyBytes = ByteArrayUtil.join(subspaceKey, serializer.serializeKey(entriesFromKey.get(entriesFromKey.size() - 1).getKey()), ZERO_ARRAY);
                tr.addReadConflictRange(lastReadKeyBytes.get(), endKeyBytes);
                tr.addWriteConflictRange(lastReadKeyBytes.get(), kv.getKey());
                lastReadKeyBytes.set(endKeyBytes);
                tr.clear(kv.getKey());
                instrumentDelete(kv.getKey(), kv.getValue());
                for (Map.Entry<K, V> entry : entriesFromKey) {
                    byte[] serializedEntry = serializer.serializeEntry(entry);
                    if (currentEntrySize.get() + serializedEntry.length > MAX_VALUE_SIZE && !currentEntryList.isEmpty()) {
                        flushEntryList(tr, subspaceKey, currentEntryList, lastKey);
                        currentEntrySize.set(0);
                    }
                    currentEntryList.add(entry);
                    currentEntrySize.addAndGet(serializedEntry.length);
                    if (currentEntryList.size() == bunchSize) {
                        flushEntryList(tr, subspaceKey, currentEntryList, lastKey);
                        currentEntrySize.set(0);
                    }
                }
            }, tr.getExecutor()).thenApply(vignore -> {
                if (!currentEntryList.isEmpty()) {
                    flushEntryList(tr, subspaceKey, currentEntryList, lastKey);
                }
                // Return a valid continuation if there might be more keys
                if (lastKey.get() != null && keyLimit != ReadTransaction.ROW_LIMIT_UNLIMITED && readKeys.get() == keyLimit) {
                    return ByteArrayUtil.join(subspaceKey, serializer.serializeKey(lastKey.get()), ZERO_ARRAY);
                } else {
                    return null;
                }
            });
        });
    }

    /**
     * Get the serializer used to encode keys and values.
     *
     * @return this map's serializer
     * @see BunchedSerializer
     */
    @Nonnull
    public BunchedSerializer<K, V> getSerializer() {
        return serializer;
    }

    /**
     * Get the comparator used to order keys of the map.
     *
     * @return this map's key comparator
     */
    @Nonnull
    public Comparator<K> getKeyComparator() {
        return keyComparator;
    }

    /**
     * Get the maximum number of map entries to encode in a single database key. This property
     * controls how "bunched" the map is. A higher value will use less space at the cost of
     * additional I/O at update time (and at read time in the case of
     * {@link #get(TransactionContext, Subspace, Object)} operations).
     *
     * @return the maximum number of map entries to encode in a single database key
     */
    public int getBunchSize() {
        return bunchSize;
    }

    /**
     * Scans the map and returns an iterator over all entries. This has the same behavior as the
     * {@link #scan(ReadTransaction, Subspace, byte[], int, boolean) scan()} method that takes more
     * parameters, but it will return an iterator that has no limit and always returns entries in ascending
     * order by key.
     *
     * @param tr transaction to use when scanning the map
     * @param subspace subspace in which the map's data are located
     * @return an iterator over the entries in the map
     */
    @Nonnull
    public BunchedMapIterator<K, V> scan(@Nonnull ReadTransaction tr, @Nonnull Subspace subspace) {
        return scan(tr, subspace, null, Transaction.ROW_LIMIT_UNLIMITED, false);
    }

    /**
     * Scans the maps and returns an iterator over all entries. This has the same behavior as the
     * {@link #scan(ReadTransaction, Subspace, byte[], int, boolean) scan()} method that takes more
     * parameters, but it will return an iterator that has no limit and always returns entries in ascending
     * order by key.
     *
     * @param tr transaction to use when scanning the map
     * @param subspace subspace in which the map's data are located
     * @param continuation continuation from a previous scan (or <code>null</code> to start from the beginning)
     * @return an iterator over the entries in the map
     */
    @Nonnull
    public BunchedMapIterator<K, V> scan(@Nonnull ReadTransaction tr, @Nonnull Subspace subspace, @Nullable byte[] continuation) {
        return scan(tr, subspace, continuation, ReadTransaction.ROW_LIMIT_UNLIMITED, false);
    }

    /**
     * Scans the map and returns an iterator over all entries. All entries will be returned sorted by
     * key. If <code>reverse</code> is <code>true</code>, it will return keys in descending order instead of in
     * ascending order. It will return at most <code>limit</code> keys from the map. Note that because of
     * bunching, this will probably require reading fewer keys from the database. Scans that require
     * multiple transactions can provide a <code>continuation</code> from a previous scan. The returned iterator
     * has a {@link BunchedMapIterator#getContinuation() getContinuation()} method that can be used to get an
     * appropriate value for that parameter.
     *
     * @param tr transaction to use when scanning the map
     * @param subspace subspace in which the map's data are located
     * @param continuation continuation from a previous scan (or <code>null</code> to start from the beginning)
     * @param limit maximum number of keys to return or {@link ReadTransaction#ROW_LIMIT_UNLIMITED} if no limit
     * @param reverse <code>true</code> if keys are wanted in descending instead of ascending order
     * @return an iterator over the entries in the map
     */
    @Nonnull
    public BunchedMapIterator<K, V> scan(@Nonnull ReadTransaction tr, @Nonnull Subspace subspace, @Nullable byte[] continuation, int limit, boolean reverse) {
        byte[] subspaceKey = subspace.getKey();
        AsyncIterable<KeyValue> rangeReadIterable;
        K continuationKey;
        if (continuation == null) {
            continuationKey = null;
            rangeReadIterable = tr.getRange(subspace.range(), ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
        } else {
            continuationKey = serializer.deserializeKey(continuation);
            byte[] continuationKeyBytes = ByteArrayUtil.join(subspaceKey, continuation);
            if (reverse) {
                rangeReadIterable = tr.getRange(subspaceKey, continuationKeyBytes, ReadTransaction.ROW_LIMIT_UNLIMITED, true);
            } else {
                rangeReadIterable = tr.getRange(KeySelector.lastLessOrEqual(continuationKeyBytes), KeySelector.firstGreaterOrEqual(subspace.range().end), ReadTransaction.ROW_LIMIT_UNLIMITED, false);
            }
        }
        return new BunchedMapIterator<>(
                AsyncPeekIterator.wrap(rangeReadIterable.iterator()),
                tr,
                subspace,
                subspace.getKey(),
                this,
                continuationKey,
                limit,
                reverse
        );
    }

    /**
     * Scans multiple maps and returns an iterator over all of them. This behaves the same was as the
     * {@link #scanMulti(ReadTransaction, Subspace, SubspaceSplitter, byte[], byte[], byte[], int, boolean) scanMulti()}
     * method that takes additional <code>subspaceStart</code> and <code>subspaceEnd</code> parameters,
     * but this will always scan from the beginning of <code>subspace</code> to the end, assumes that
     * there is no limit to the number of entries to return, and always returns items in ascending order.
     *
     * @param tr transaction to use when scanning the maps
     * @param subspace subspace containing one or more maps
     * @param splitter object to determine which map a given key is in
     * @param <T> type of the tag of each map subspace
     * @return an iterator over the entries in multiple maps
     */
    @Nonnull
    public <T> BunchedMapMultiIterator<K, V, T> scanMulti(@Nonnull ReadTransaction tr, @Nonnull Subspace subspace, @Nonnull SubspaceSplitter<T> splitter) {
        return scanMulti(tr, subspace, splitter, null, ReadTransaction.ROW_LIMIT_UNLIMITED, false);
    }

    /**
     * Scans multiple maps and returns an iterator over all of them. This behaves the same was as the
     * {@link #scanMulti(ReadTransaction, Subspace, SubspaceSplitter, byte[], byte[], byte[], int, boolean) scanMulti()}
     * method that takes additional <code>subspaceStart</code> and <code>subspaceEnd</code> parameters,
     * but this will always scan from the beginning of <code>subspace</code> to the end.
     *
     * @param tr transaction to use when scanning the maps
     * @param subspace subspace containing one or more maps
     * @param splitter object to determine which map a given key is in
     * @param continuation continuation from previous scan (or <code>null</code> to start from the beginning)
     * @param limit maximum number of entries to return
     * @param reverse <code>true</code> if the entries should be returned in descending order or <code>false</code> otherwise
     * @param <T> type of the tag of each map subspace
     * @return an iterator over the entries in multiple maps
     */
    @Nonnull
    public <T> BunchedMapMultiIterator<K, V, T> scanMulti(@Nonnull ReadTransaction tr, @Nonnull Subspace subspace, @Nonnull SubspaceSplitter<T> splitter,
                                                        @Nullable byte[] continuation, int limit, boolean reverse) {
        return scanMulti(tr, subspace, splitter, null, null, continuation, limit, reverse);
    }

    /**
     * Scans multiple maps and returns an iterator over all of them. The returned iterator
     * will produce one item for each entry in each map. To do so, the maps must be backed by contiguous
     * subspaces within <code>subspace</code>. The scan will start at the first subspace greater
     * than or equal to the subspace formed by concatenating the raw prefix of <code>subspace</code>
     * with <code>subspaceStart</code> (or with nothing if <code>subspaceStart</code> is
     * <code>null</code>) and will end with the last subspace less than the subspace formed
     * by concatenating the raw prefix of <code>subspace</code> with <code>subspaceEnd</code>
     * (or with nothing if <code>subspaceEnd</code> is <code>null</code>). The provided
     * {@link SubspaceSplitter} should be able to determine the subspace of the map that
     * a given key contains and (optionally) provide some tag that can be used to associate
     * that map with some value that might be used to distinguish one map from the other (if
     * the application needs it).
     *
     * <p>
     * For example, suppose there are ten <code>BunchedMaps</code> that all begin with a raw prefix
     * <code>prefix</code> followed by a {@link com.apple.foundationdb.tuple.Tuple Tuple}-encoded
     * integer (0 through 10). If one wanted to scan the three maps starting with map 3 and ending
     * with map 6, one would supply the following parameters:
     * </p>
     * <ul>
     *     <li><code>subspace</code> should be set to <code>new Subspace(prefix)</code></li>
     *     <li>
     *         <code>splitter</code> should be implemented so that if given <code>key</code> within map <code>n</code>,
     *         {@link SubspaceSplitter#subspaceOf(byte[]) subspaceOf(key)} returns <code>subspace.subspace(Tuple.from(n))</code>
     *         and {@link SubspaceSplitter#subspaceTag(Subspace) subspaceTag(subspaceOf(key))} returns <code>n</code>.
     *     </li>
     *     <li><code>subspaceStart</code> should be set to <code>Tuple.pack(3)</code></li>
     *     <li><code>subspaceEnd</code> should be set to <code>Tuple.pack(7)</code> (or <code>Tuple.pack(6)</code> concatenated with a 0-byte)</li>
     * </ul>
     *
     * <p>
     * Furthermore, this method can be used across multiple calls or transactions by setting the
     * <code>continuation</code> parameter to the result of {@link BunchedMapMultiIterator#getContinuation() getContinuation()}
     * from a previous scan. (One should pass <code>null</code> to restart the scan from the beginning.)
     * The iterator will return at most <code>limit</code> entries. The entries will ordered first
     * by subspace and within a subspace by key. If <code>reverse</code> is <code>true</code>, the
     * items will be returned in descending order. Otherwise, they will be returned in ascending order.
     * </p>
     *
     * @param tr transaction to use when scanning the maps
     * @param subspace subspace containing one or more maps
     * @param splitter object to determine which map a given key is in
     * @param subspaceStart inclusive starting suffix of <code>subspace</code> to start the scan
     * @param subspaceEnd exclusive ending suffix of <code>subspace</code> to end the scan
     * @param continuation continuation from previous scan (or <code>null</code> to start from the beginning)
     * @param limit maximum number of entries to return
     * @param reverse <code>true</code> if the entries should be returned in descending order or <code>false</code> otherwise
     * @param <T> type of the tag of each map subspace
     * @return an iterator over the entries in multiple maps
     */
    @Nonnull
    public <T> BunchedMapMultiIterator<K, V, T> scanMulti(@Nonnull ReadTransaction tr, @Nonnull Subspace subspace, @Nonnull SubspaceSplitter<T> splitter,
                                                          @Nullable byte[] subspaceStart, @Nullable byte[] subspaceEnd,
                                                          @Nullable byte[] continuation, int limit, boolean reverse) {
        return scanMulti(tr, subspace, splitter, subspaceStart, subspaceEnd, continuation, limit, null, reverse);
    }


    /**
     * Overload of {@link #scanMulti(ReadTransaction, Subspace, SubspaceSplitter, byte[], byte[], byte[], int, boolean) scanMulti()}
     * that provides a callback to run after each key is read. This hook can be used to interface with metrics collection.
     *
     * @param tr transaction to use when scanning the maps
     * @param subspace subspace containing one or more maps
     * @param splitter object to determine which map a given key is in
     * @param subspaceStart inclusive starting suffix of <code>subspace</code> to start the scan
     * @param subspaceEnd exclusive ending suffix of <code>subspace</code> to end the scan
     * @param continuation continuation from previous scan (or <code>null</code> to start from the beginning)
     * @param postReadCallback callback to execute after reading each key
     * @param limit maximum number of entries to return
     * @param reverse <code>true</code> if the entries should be returned in descending order or <code>false</code> otherwise
     * @param <T> type of the tag of each map subspace
     * @return an iterator over the entries in multiple maps
     * @see #scanMulti(ReadTransaction, Subspace, SubspaceSplitter, byte[], byte[], byte[], int, Consumer, boolean)
     */
    @Nonnull
    public <T> BunchedMapMultiIterator<K, V, T> scanMulti(@Nonnull ReadTransaction tr, @Nonnull Subspace subspace, @Nonnull SubspaceSplitter<T> splitter,
                                                          @Nullable byte[] subspaceStart, @Nullable byte[] subspaceEnd,
                                                          @Nullable byte[] continuation, int limit, @Nullable Consumer<KeyValue> postReadCallback, boolean reverse) {
        byte[] subspaceKey = subspace.getKey();
        byte[] startBytes = (subspaceStart == null ? subspaceKey : ByteArrayUtil.join(subspaceKey, subspaceStart));
        byte[] endBytes = (subspaceEnd == null ? ByteArrayUtil.strinc(subspaceKey) : ByteArrayUtil.join(subspaceKey, subspaceEnd));
        AsyncIterable<KeyValue> rangeReadIterable;
        if (continuation == null) {
            rangeReadIterable = tr.getRange(startBytes, endBytes, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
        } else {
            byte[] continuationEndpoint = ByteArrayUtil.join(subspaceKey, continuation);
            if (reverse) {
                if (ByteArrayUtil.compareUnsigned(continuationEndpoint, endBytes) < 0) {
                    rangeReadIterable = tr.getRange(startBytes, continuationEndpoint, ReadTransaction.ROW_LIMIT_UNLIMITED, true);
                } else {
                    rangeReadIterable = tr.getRange(startBytes, endBytes, ReadTransaction.ROW_LIMIT_UNLIMITED, true);
                }
            } else {
                if (ByteArrayUtil.compareUnsigned(continuationEndpoint, startBytes) < 0) {
                    rangeReadIterable = tr.getRange(startBytes, endBytes, ReadTransaction.ROW_LIMIT_UNLIMITED, false);
                } else {
                    rangeReadIterable = tr.getRange(KeySelector.lastLessThan(continuationEndpoint), KeySelector.firstGreaterOrEqual(endBytes), ReadTransaction.ROW_LIMIT_UNLIMITED, false);
                }
            }
        }
        final AsyncPeekIterator<KeyValue> wrappedIterator;
        if (postReadCallback == null) {
            wrappedIterator = AsyncPeekIterator.wrap(rangeReadIterable.iterator());
        } else {
            wrappedIterator = AsyncPeekCallbackIterator.wrap(rangeReadIterable.iterator(), postReadCallback);
        }
        return new BunchedMapMultiIterator<>(
                wrappedIterator,
                tr,
                subspace,
                subspaceKey,
                splitter,
                this,
                continuation,
                limit,
                reverse
        );
    }
}
