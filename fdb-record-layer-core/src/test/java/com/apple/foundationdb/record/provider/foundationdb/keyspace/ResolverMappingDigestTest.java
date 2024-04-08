/*
 * ResolverMappingDigestTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.MetadataHook;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSortedMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.security.MessageDigest;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.DEFAULT_CHECK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link ResolverMappingDigest}.
 */
@Tag(Tags.RequiresFDB)
public class ResolverMappingDigestTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    private FDBDatabase database;
    private Random random = new Random();
    private KeySpacePath basePath;

    @BeforeEach
    public void setup() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setDirectoryCacheSize(100);
        database = dbExtension.getDatabase();
        basePath = pathManager.createPath(TestKeySpace.RESOLVER_MAPPING_REPLICATOR);
    }

    @Test
    public void testDirectoryLayerAndInterningLayer() throws Exception {
        LocatableResolver primary;
        LocatableResolver replica;
        try (FDBRecordContext context = database.openContext()) {
            primary = new ScopedDirectoryLayer(database, basePath.add("to").add("primary").toResolvedPath(context));
            replica = new ScopedInterningLayer(database, basePath.add("to").add("replica").toResolvedPath(context));
        }

        testComputeDigest(primary, replica, false);
    }

    @Test
    public void testInterningLayerAndInterningLayer() throws Exception {
        LocatableResolver primary;
        LocatableResolver replica;
        try (FDBRecordContext context = database.openContext()) {
            primary = new ScopedInterningLayer(database, basePath.add("to").add("primary").toResolvedPath(context));
            replica = new ScopedInterningLayer(database, basePath.add("to").add("replica").toResolvedPath(context));
        }

        testComputeDigest(primary, replica, false);
    }

    @Test
    public void testInterningLayerAndInterningLayerWithMetadata() throws Exception {
        LocatableResolver primary;
        LocatableResolver replica;
        try (FDBRecordContext context = database.openContext()) {
            primary = new ScopedInterningLayer(database, basePath.add("to").add("primary").toResolvedPath(context));
            replica = new ScopedInterningLayer(database, basePath.add("to").add("replica").toResolvedPath(context));
        }

        byte[] metadata = Tuple.from("some-metadata").pack();
        testComputeDigest(primary, replica, true);
    }

    @Test
    public void testInterningLayerAndExtendedLayer() throws Exception {
        LocatableResolver primary;
        LocatableResolver replica;
        try (FDBRecordContext context = database.openContext()) {
            primary = new ScopedInterningLayer(database, basePath.add("to").add("primary").toResolvedPath(context));
            replica = new ExtendedDirectoryLayer(database, basePath.add("to").add("replica").toResolvedPath(context));
        }

        testComputeDigest(primary, replica, false);
    }

    @Test
    public void testInterningLayerAndExtendedLayerWithMetadata() throws Exception {
        LocatableResolver primary;
        LocatableResolver replica;
        try (FDBRecordContext context = database.openContext()) {
            primary = new ScopedInterningLayer(database, basePath.add("to").add("primary").toResolvedPath(context));
            replica = new ExtendedDirectoryLayer(database, basePath.add("to").add("replica").toResolvedPath(context));
        }

        testComputeDigest(primary, replica, true);
    }

    private void testComputeDigest(LocatableResolver primary, LocatableResolver replica, boolean allowMetadata) throws Exception {
        SortedMap<String, ResolverResult> mappings = new TreeMap<>();

        ResolverResult result;
        for (int i = 0; i < 10; i++) {
            String key = "some-key-" + i;
            // if we allow metadata, set the metadata ~1/2 the time
            boolean metadataForThisKey = allowMetadata && random.nextBoolean();
            byte[] metadata = metadataForThisKey ? Tuple.from("some metadata for key: " + key).pack() : null;
            MetadataHook hook = ignore -> metadata;
            result = primary.resolveWithMetadata(key, new ResolverCreateHooks(DEFAULT_CHECK, hook)).join();

            mappings.put(key, result);
        }

        ResolverMappingReplicator replicator = new ResolverMappingReplicator(primary);
        replicator.copyTo(replica);

        final byte[] expectedDigest = expectedDigest(mappings);
        final byte[] wrongKeyDigest = wrongKeyDigest(mappings);
        final byte[] wrongValueDigest = wrongValueDigest(mappings);
        final byte[] wrongMetadataDigest = wrongMetadata(mappings);
        final byte[] extraEntryDigest = extraEntry(mappings);

        ResolverMappingDigest primaryResolverMappingDigest = new ResolverMappingDigest(primary);
        ResolverMappingDigest replicaResolverMappingDigest = new ResolverMappingDigest(replica);
        assertThat("digests match only if key, value and metadata are the same (independent of scope)",
                primaryResolverMappingDigest.computeDigest().join(),
                allOf(
                        is(expectedDigest),
                        is(replicaResolverMappingDigest.computeDigest().join()),
                        is(not(wrongKeyDigest)),
                        is(not(wrongValueDigest)),
                        is(not(wrongMetadataDigest)),
                        is(not(extraEntryDigest))
                ));
    }

    private byte[] expectedDigest(SortedMap<String, ResolverResult> mappings) throws Exception {
        // the modification is just the normal digest update, should always produce the correct digest for the mappings
        return computeModifiedDigest(mappings, (md, entry) -> md.update(Tuple.from(entry.getKey(), entry.getValue().getValue(), entry.getValue().getMetadata()).pack()));
    }

    private byte[] wrongKeyDigest(SortedMap<String, ResolverResult> mappings) throws Exception {
        return computeModifiedDigest(mappings, (md, entry) -> md.update(Tuple.from("wrong-key", entry.getValue().getValue(), entry.getValue().getMetadata()).pack()));
    }

    private byte[] wrongValueDigest(SortedMap<String, ResolverResult> mappings) throws Exception {
        return computeModifiedDigest(mappings, (md, entry) -> md.update(Tuple.from(entry.getKey(), "wrongValue", entry.getValue().getMetadata()).pack()));
    }

    private byte[] wrongMetadata(SortedMap<String, ResolverResult> mappings) throws Exception {
        return computeModifiedDigest(mappings, (md, entry) -> {
            byte[] wrongMetadata = new byte[] {(byte)0xAB, (byte)0xCD, (byte)0xEF};
            md.update(Tuple.from(entry.getKey(), entry.getValue().getValue(), wrongMetadata).pack());
        });
    }

    private byte[] computeModifiedDigest(SortedMap<String, ResolverResult> mappings,
                                         BiConsumer<MessageDigest, Map.Entry<String, ResolverResult>> modification) throws Exception {
        // compute the digest for the mappings, chose one element at random and instead update the digest using the
        // modification function
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        int indexToModify = random.nextInt(mappings.entrySet().size());
        int index = 0;
        for (Map.Entry<String, ResolverResult> entry : mappings.entrySet()) {
            if (indexToModify == index++) {
                modification.accept(md, entry);
            } else {
                md.update(Tuple.from(entry.getKey(), entry.getValue().getValue(), entry.getValue().getMetadata()).pack());
            }
        }
        return md.digest();
    }

    private byte[] extraEntry(SortedMap<String, ResolverResult> mappings) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        SortedMap<String, ResolverResult> wrongMappings = ImmutableSortedMap.<String, ResolverResult>naturalOrder()
                .putAll(mappings)
                .put("an-extra-key", new ResolverResult(1, null))
                .build();
        for (Map.Entry<String, ResolverResult> entry : wrongMappings.entrySet()) {
            md.update(Tuple.from(entry.getKey(), entry.getValue().getValue(), entry.getValue().getMetadata()).pack());
        }
        return md.digest();
    }

}
