/*
 * StringInterningLayerTest.java
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.TestHelpers.ExceptionMessageMatcher.hasMessageContaining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag(Tags.RequiresFDB)
class StringInterningLayerTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    private FDBDatabase database;
    private Subspace testSubspace;

    @BeforeEach
    void setup() {
        database = dbExtension.getDatabase();
        KeySpacePath path = pathManager.createPath(TestKeySpace.RAW_DATA);
        testSubspace = database.run(path::toSubspace);
    }

    @Test
    void testInterning() {
        final ResolverResult internedValue;
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
            internedValue = interningLayer.intern(context, "a-string").join();

            for (int i = 0; i < 5; i++) {
                ResolverResult currentCall = interningLayer.intern(context, "a-string").join();
                assertThat("we see the same interned value for subsequent calls in the transaction",
                        currentCall, is(internedValue));
            }
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);

            for (int i = 0; i < 5; i++) {
                ResolverResult currentCall = interningLayer.intern(context, "a-string").join();
                assertThat("we see the same interned value for subsequent transactions", currentCall, is(internedValue));
            }
        }
    }

    @Test
    void testExists() {
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
            interningLayer.intern(context, "string-1").join();
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
            interningLayer.intern(context, "string-2").join();

            assertTrue(interningLayer.exists(context, "string-1").join(), "we see previously committed values");
            assertTrue(interningLayer.exists(context, "string-2").join(), "we see values added in the current transaction");
            assertFalse(interningLayer.exists(context, "string-3").join(), "we don't see values that haven't been added");
        }
    }

    @Test
    void testCreate() {
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
            String toIntern = "some-string";
            byte[] metadata = Tuple.from("some-metadata").pack();
            ResolverResult interned = interningLayer.create(context, toIntern, metadata).join();
            assertArrayEquals(interned.getMetadata(), metadata, "we see the metadata with the interned value");

            Optional<ResolverResult> maybeRead = interningLayer.read(context, toIntern).join();
            assertIsPresentWithValue("we read the value", maybeRead, interned);

            Optional<String> maybeReverseRead = interningLayer.readReverse(context, interned.getValue()).join();
            assertIsPresentWithValue("the reverse lookup works", maybeReverseRead, toIntern);

            try {
                interningLayer.create(context, toIntern, null).join();
                fail("should throw exception");
            } catch (CompletionException ex) {
                assertThat(ex.getCause(), is(instanceOf(RecordCoreException.class)));
                assertThat(ex.getCause().getMessage(), containsString("value already exists in interning layer"));
                RecordCoreException cause = (RecordCoreException) ex.getCause();
                assertThat("exception log info has the key", cause.getLogInfo(), hasEntry("value", toIntern));
            }
        }
    }

    @Test
    void testReadValue() {
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);

            ResolverResult internedValue = interningLayer.intern(context, "string-a").join();

            Optional<ResolverResult> maybereadValueA = interningLayer.read(context, "string-a").join();
            Optional<ResolverResult> maybereadValueB = interningLayer.read(context, "string-b").join();

            assertThat("we read strings that are there", maybereadValueA.get(), is(internedValue));
            assertThat("we get empty result for values not interned", maybereadValueB, is(Optional.empty()));
        }
    }

    @Test
    void testTransactional() {
        final String toInternWithCommit;
        final ResolverResult committedValue;
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);

            toInternWithCommit = "with-commit";
            committedValue = interningLayer.intern(context, toInternWithCommit).join();
            context.commit();
        }

        final String toInternNoCommit;
        final ResolverResult uncommittedValue;
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);

            toInternNoCommit = "no-commit";
            uncommittedValue = interningLayer.intern(context, toInternNoCommit).join();
        }

        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);

            assertTrue(interningLayer.exists(context, toInternWithCommit).join(), "committed value exists");
            assertIsPresentWithValue("read value", interningLayer.read(context, toInternWithCommit).join(), committedValue);
            assertIsPresentWithValue("reverse lookup", interningLayer.readReverse(context, committedValue.getValue()).join(), toInternWithCommit);
            assertFalse(interningLayer.exists(context, toInternNoCommit).join(), "uncommitted value doesn't exist");
            assertFalse(interningLayer.read(context, toInternNoCommit).join().isPresent(), "read value is empty");
            assertFalse(interningLayer.readReverse(context, uncommittedValue.getValue()).join().isPresent(), "read reverse is empty");
        }
    }

    @Test
    @Tag(Tags.WipesFDB)
    void testFilterInvalidAllocationValues() {
        Range everything = new Range(new byte[]{(byte) 0x00}, new byte[]{(byte) 0xFF});
        database.run(context -> {
            context.ensureActive().clear(everything);
            return null;
        });

        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace, true);

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
                interningLayer.intern(context, "a-string").join();
                fail("intern should throw exception");
            } catch (Exception e) {
                assertThat("a", e.getCause().getMessage(), is("database already has keys in allocation range"));
            }

            Optional<ResolverResult> maybeRead = interningLayer.read(context, "a-string").join();
            assertThat("we don't read anything", maybeRead, is(Optional.empty()));
        }
    }

    @Test
    void testInterningConcurrent() {
        Map<String, Long> allocations;
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
            List<CompletableFuture<NonnullPair<String, Long>>> futures = IntStream.range(0, 50)
                    .mapToObj(i -> "intern-" + i)
                    .map(value -> interningLayer.intern(context, value).thenApply(interned -> NonnullPair.of(value, interned.getValue())))
                    .collect(Collectors.toList());
            allocations = AsyncUtil.getAll(futures).join().stream()
                    .collect(Collectors.toMap(NonnullPair::getLeft, NonnullPair::getRight));

            for (int i = 0; i < 50; i++) {
                String toIntern = "intern-" + i;
                Long value = interningLayer.intern(context, toIntern).join().getValue();
                assertThat("value is in mapping", allocations.get(toIntern), is(value));
            }
        }
    }

    @Test
    void testConcurrentAddSameValue() {
        List<CompletableFuture<NonnullPair<String, ResolverResult>>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            futures.add(
                    database.runAsync(context -> {
                        StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
                        return interningLayer.intern(context, "same-string");
                    }).thenApply(value -> NonnullPair.of("same-string", value))
            );
        }
        final Set<NonnullPair<String, ResolverResult>> allocationSet = new HashSet<>(AsyncUtil.getAll(futures).join());

        assertThat("only one value is allocated", allocationSet, hasSize(1));
        ResolverResult allocated = allocationSet.stream().findFirst().get().getRight();
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
            assertIsPresentWithValue("forward mapping is consistent",
                    interningLayer.read(context, "same-string").join(), allocated);
            assertIsPresentWithValue("reverse mapping is consistent",
                    interningLayer.readReverse(context, allocated.getValue()).join(), "same-string");
        }
    }

    @Test
    void testReverseLookup() {
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
            ResolverResult internedValue = interningLayer.intern(context, "a-string").join();

            Optional<String> maybeReverseLookup = interningLayer.readReverse(context, internedValue.getValue()).join();
            assertThat("we can lookup the string by the interned value", maybeReverseLookup.get(), is("a-string"));

            Optional<String> maybeNotThere = interningLayer.readReverse(context, internedValue.getValue() + 1).join();
            assertThat("lookups for missing values return nothing", maybeNotThere.isPresent(), is(false));
        }
    }

    @Test
    void testUpdateMetadata() {
        byte[] oldMetadata = Tuple.from("old-metadata").pack();
        byte[] newMetadata = Tuple.from("new-metadata").pack();
        StringInterningLayer interningLayer;
        ResolverResult initial;
        ResolverResult updated;
        final String key = "update-metadata";
        try (FDBRecordContext context = database.openContext()) {
            interningLayer = new StringInterningLayer(testSubspace);
            initial = interningLayer.create(context, key, oldMetadata).join();
            interningLayer.updateMetadata(context, key, newMetadata).join();
            updated = interningLayer.read(context, key).join().orElseThrow(() -> new AssertionError("should be present"));
            assertThat("we see the new metadata", updated, is(new ResolverResult(initial.getValue(), newMetadata)));
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            ResolverResult newTransactionRead = interningLayer.read(context, key).join().orElseThrow(() -> new AssertionError("should be present"));
            assertThat("we see the committed metadata in a new transaction", newTransactionRead, is(updated));
        }
    }

    @Test
    void testUpdateForMissingKey() {
        byte[] metadata = Tuple.from("some-metadata").pack();
        try (FDBRecordContext context = database.openContext()) {
            StringInterningLayer interningLayer = new StringInterningLayer(testSubspace);
            interningLayer.updateMetadata(context, "a-missing-key", metadata).join();
            fail("should throw NoSuchElementException");
        } catch (CompletionException ex) {
            assertThat(ex.getCause(), is(instanceOf(NoSuchElementException.class)));
            assertThat(ex.getCause(), hasMessageContaining("updateMetadata must reference key that already exists"));
        }
    }

    private <T> void assertIsPresentWithValue(String message, Optional<T> maybeContains, T value) {
        assertThat(message, maybeContains.orElseThrow(() -> new AssertionError("should contain value")), is(value));
    }

}
