/*
 * HighContentionAllocatorTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.layers.interning;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.HighContentionAllocator.AllocationWindow;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(Tags.RequiresFDB)
class HighContentionAllocatorTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    private FDBDatabase database;
    private KeySpacePath path;

    @BeforeEach
    void setup() {
        database = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RAW_DATA);
    }

    @Test
    void testAllocationsUnique() {
        final Map<Long, String> allocated = new HashMap<>();
        try (FDBRecordContext context = database.openContext()) {
            HighContentionAllocator hca = new HighContentionAllocator(context, path);

            for (int i = 0; i < 50; i++) {
                String storedValue = "allocate-" + i;
                Long thisAllocation = hca.allocate(storedValue).join();
                assertThat("allocations are unique within a transaction", allocated, not(hasKey(thisAllocation)));
                allocated.put(thisAllocation, storedValue);
            }
            validateAllocation(context, hca, allocated);
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            HighContentionAllocator hca = new HighContentionAllocator(context, path);

            validateAllocation(context, hca, allocated);
            for (int i = 0; i < 10; i++) {
                String storedValue = "new-allocate-" + i;
                Long newAllocation = hca.allocate(storedValue).join();
                assertThat("allocations are unique across transactions", allocated, not(hasKey(newAllocation)));
            }
        }
    }

    @Test
    void testAllocationsParallelSameTransaction() {
        final Map<Long, String> allocated;
        try (FDBRecordContext context = database.openContext()) {
            HighContentionAllocator hca = new HighContentionAllocator(context, path);

            List<CompletableFuture<Pair<Long, String>>> allocationOperations = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                String storedValue = "allocate-" + i;
                allocationOperations.add(hca.allocate(storedValue).thenApply(id -> Pair.of(id, storedValue)));
            }

            allocated = AsyncUtil.getAll(allocationOperations).join().stream()
                    .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
            assertThat("every allocation operation has a distinct value", allocated.entrySet(), hasSize(50));
            validateAllocation(context, hca, allocated);
            context.commit();
        }
    }

    @Test
    void testAllocationsParallelSeparateTransactions() {
        List<CompletableFuture<Pair<Long, String>>> allocationOperations = new ArrayList<>();
        AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < 50; i++) {
            CompletableFuture<Pair<Long, String>> allocation = database.runAsync(context -> {
                HighContentionAllocator hca = new HighContentionAllocator(context, path);
                String storedValue = "allocate-" + count.getAndIncrement();
                return hca.allocate(storedValue).thenApply(id -> Pair.of(id, storedValue));
            });
            allocationOperations.add(allocation);
        }

        Map<Long, String> allocated = AsyncUtil.getAll(allocationOperations).join().stream()
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        assertThat("all values are allocated", allocated.entrySet(), hasSize(50));

        try (FDBRecordContext context = database.openContext()) {
            HighContentionAllocator hca = new HighContentionAllocator(context, path);
            validateAllocation(context, hca, allocated);
        }
    }

    @Test
    @Tag(Tags.WipesFDB)
    void testCheckForRootConflicts() {
        Range everything = new Range(new byte[]{(byte)0x00}, new byte[]{(byte)0xFF});
        database.run(context -> {
            context.ensureActive().clear(everything);
            return null;
        });

        try (FDBRecordContext context = database.openContext()) {
            HighContentionAllocator hca = HighContentionAllocator.forRoot(context, path);
            // write conflicting keys
            // this unfortunately requires some knowledge of the implementation details of the allocator
            // starting from a clean slate the first allocation will be an integer in the range [0, 64)
            // so write something for all of those keys
            for (int i = 0; i < 64; i++) {
                byte[] key = Tuple.from(i, "string-" + i).pack();
                byte[] value = new byte[0];
                context.ensureActive().set(key, value);
            }

            try {
                hca.allocate("some-string").join();
                fail("allocate should fail in the same transaction");
            } catch (Exception e) {
                assertThat("a", e.getCause().getMessage(), is("database already has keys in allocation range"));
            }

            // check that the hca marks these keys as invalid
            // the thing here is that when the post allocation hook fails the allocator still writes a key,
            // that's actually a good thing since it will prevent us from trying to allocate that key again
            // but we need to make sure we have a way to exclude from the reverse lookup
            Subspace allocationSubspace = hca.getAllocationSubspace();
            Range initialwindow = new Range(allocationSubspace.getKey(), allocationSubspace.pack(64));
            List<KeyValue> allocatedValues = context.ensureActive().getRange(initialwindow).asList().join();
            byte[] valueBytes = allocatedValues.get(0).getValue();
            assertThat("there's exactly one allocation key", allocatedValues, hasSize(1));
            assertArrayEquals(valueBytes, new byte[]{(byte) 0xFD}, "the value is set to the magic byte");

            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            HighContentionAllocator hca = HighContentionAllocator.forRoot(context, path);

            try {
                hca.allocate("some-string").join();
                fail("allocate should fail in new transaction");
            } catch (Exception e) {
                assertThat("a", e.getCause().getMessage(), is("database already has keys in allocation range"));
            }
        }

        database.run(context -> {
            context.ensureActive().clear(everything);
            return null;
        });
    }

    @Test
    void testAllocationWindow() {
        validateAllocationWindow(AllocationWindow.startingFrom(0));
        validateAllocationWindow(AllocationWindow.startingFrom(1000));
        validateAllocationWindow(AllocationWindow.startingFrom(2345));
    }

    private void validateAllocation(FDBRecordContext context, HighContentionAllocator hca, Map<Long, String> allocations) {
        Subspace allocationSubspace = hca.getAllocationSubspace();
        Transaction transaction = context.ensureActive();
        List<KeyValue> keyValueList = transaction.getRange(allocationSubspace.range()).asList().join();
        Map<Long, String> storedAllocations = keyValueList.stream()
                .collect(Collectors.toMap(kv -> extractKey(allocationSubspace, kv), this::extractValue));
        assertThat("we see the allocated keys in the subspace", allocations.entrySet(),
                containsInAnyOrder(storedAllocations.entrySet().toArray()));
    }

    private String extractValue(KeyValue kv) {
        return Tuple.fromBytes(kv.getValue()).getString(0);
    }

    private Long extractKey(Subspace allocationSubspace, KeyValue kv) {
        return allocationSubspace.unpack(kv.getKey()).getLong(0);
    }

    private void validateAllocationWindow(AllocationWindow window) {
        long start = window.getStart();
        long end = window.getEnd();

        assertThat("window size is accurate", window.size(), is(Math.toIntExact(end - start)));
        List<Long> randoms = IntStream.range(0, 100).mapToObj(ignored -> window.random()).collect(Collectors.toList());
        assertThat("generated values are all in the window", randoms, everyItem(allOf(
                greaterThanOrEqualTo(start), lessThan(end))));
    }
}
