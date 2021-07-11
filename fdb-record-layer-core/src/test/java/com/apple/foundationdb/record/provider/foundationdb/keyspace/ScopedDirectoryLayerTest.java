/*
 * ScopedDirectoryLayerTest.java
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

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.MetadataHook;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link ScopedDirectoryLayer}.
 */
@Tag(Tags.WipesFDB)
@Tag(Tags.RequiresFDB)
public class ScopedDirectoryLayerTest extends LocatableResolverTest {
    public ScopedDirectoryLayerTest() {
        super(TestingResolverFactory.ResolverType.SCOPED_DIRECTORY_LAYER);
    }

    @Test
    public void testDefaultDirectoryResolver() {
        LocatableResolver resolver = globalScope;

        try (FDBRecordContext context = database.openContext()) {
            Long value = resolver.resolve(context.getTimer(), "foo").join();

            DirectoryLayer directoryLayer = DirectoryLayer.getDefault();
            validate(context, resolver, directoryLayer, "foo", value);
        }
    }

    @Test
    public void testDefaultResolverSeesPreviousDefaultDirectoryLayerEntries() {
        final DirectoryLayer directoryLayer = DirectoryLayer.getDefault();

        final List<String> names = IntStream.range(0, 5)
                .mapToObj(number -> String.format("name-%d", number))
                .collect(Collectors.toList());
        Map<String, Long> values = new HashMap<>();
        try (FDBRecordContext context = database.openContext()) {
            for (String name : names) {
                values.put(name, directoryLayer.createOrOpen(context.ensureActive(), ImmutableList.of(name))
                        .thenApply(subspace -> Tuple.fromBytes(subspace.getKey()).getLong(0)).join());
            }
            context.commit();
        }

        try (FDBRecordContext context = database.openContext()) {
            for (String name : names) {
                Long resolvedValue = globalScope.resolve(context.getTimer(), name).join();
                assertThat("resolver sees all mappings in directory layer", values.get(name), is(resolvedValue));
            }
        }
    }

    @Test
    public void testLocatableDirectoryResolver() {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("path", KeyType.STRING, "path")
                        .addSubdirectory(new KeySpaceDirectory("to", KeyType.STRING, "to")
                                .addSubdirectory(new KeySpaceDirectory("dirLayer", KeyType.STRING, "dirLayer"))
                        )
        );

        ResolvedKeySpacePath path;
        try (FDBRecordContext context = database.openContext()) {
            path = keySpace.resolveFromKey(context, Tuple.from("path", "to", "dirLayer"));
        }

        LocatableResolver resolver = resolverFactory.create(path);
        Long value = resolver.resolve("foo").join();

        DirectoryLayer directoryLayer = new DirectoryLayer(
                new Subspace(Bytes.concat(path.toTuple().pack(), DirectoryLayer.DEFAULT_NODE_SUBSPACE.getKey())),
                path.toSubspace());

        try (FDBRecordContext context = database.openContext()) {
            validate(context, resolver, directoryLayer, "foo", value);

            DirectoryLayer defaultDirectoryLayer = DirectoryLayer.getDefault();
            List<String> defaultDirectories = defaultDirectoryLayer.list(context.ensureActive()).join();
            assertThat("entry is not in the default directory layer", defaultDirectories, not(hasItem("foo")));
        }
    }

    @Test
    @Override
    public void testResolveWithMetadata() {
        ResolverCreateHooks noMetadata = ResolverCreateHooks.getDefault();
        String key1 = "key1";
        assertThat(noMetadata.getMetadataHook().apply(key1), is(nullValue()));
        // works as long as the metadatahook returns null
        globalScope.resolveWithMetadata(key1, noMetadata).join();

        String key2 = "key2";
        MetadataHook hook = name -> Tuple.from(name).pack();
        ResolverCreateHooks withMetadata = new ResolverCreateHooks(ResolverCreateHooks.DEFAULT_CHECK, hook);
        assertThat(withMetadata.getMetadataHook().apply(key2), is(not(nullValue())));
        assertThrows(CompletionException.class,
                () -> globalScope.resolveWithMetadata(key2, withMetadata).join());
    }

    private void validate(FDBRecordContext context, LocatableResolver resolver, DirectoryLayer directoryLayer, String key, Long value) {
        List<String> directories = directoryLayer.list(context.ensureActive()).join();
        assertThat("entry was added to the appropriate directory layer", directories, hasItem(key));

        Subspace resultSubpsace = directoryLayer.open(context.ensureActive(), ImmutableList.of(key)).join();
        Long directoryValue = resolver.deserializeValue(resultSubpsace.getKey()).getValue();
        assertThat("resolver returned the value of the subspace prefix", directoryValue, is(value));

        Long newValue = resolver.resolve(context.getTimer(), key).join();
        assertThat("repeated calls to resolve return the same value", newValue, is(value));
    }


    // Unsupported operations

    @Test
    @Override
    public void testSetWindow() {
        // not supported
        assertThrows(UnsupportedOperationException.class, () -> globalScope.setWindow(123L).join());
    }

    @Test
    @Override
    @Disabled("Not implemented for ScopedDirectoryLayer")
    public void testSetMappingWithConflicts() {
        super.testSetMappingWithConflicts();
    }


    @Test
    @Override
    public void testSetMapping() {
        // not supported
        assertThrows(UnsupportedOperationException.class, () -> {
            try (FDBRecordContext context = database.openContext()) {
                globalScope.setMapping(context, "foo", new ResolverResult(23L, null)).join();
            }
        });
    }

    @Test
    @Override
    public void testUpdateMetadata() {
        // not supported
        assertThrows(UnsupportedOperationException.class, () -> {
            try (FDBRecordContext context = database.openContext()) {
                globalScope.updateMetadata(context, "foo", null).join();
            }
        });
    }

}
