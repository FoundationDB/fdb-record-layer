/*
 * ResolverCreateHooksTest.java
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
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.PreWriteCheck;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
class ResolverCreateHooksTest {
    private FDBDatabase database;
    private KeySpace keySpace;
    private final Random random = new Random();

    @BeforeEach
    public void setup() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        database = factory.getDatabase();
        database.clearCaches();

        keySpace = new KeySpace(
                new KeySpaceDirectory("test-root", KeySpaceDirectory.KeyType.STRING, "test-" + random.nextLong())
                        .addSubdirectory(new KeySpaceDirectory("resolvers", KeySpaceDirectory.KeyType.STRING, "resolvers")
                                .addSubdirectory(new KeySpaceDirectory("resolverNode", KeySpaceDirectory.KeyType.STRING)))
                        .addSubdirectory(new KeySpaceDirectory("should-use-A", KeySpaceDirectory.KeyType.STRING, "should-use-A")));
    }

    @AfterEach
    public void teardown() {
        database.run(context -> root(context).toTupleAsync().thenAccept(tuple -> context.ensureActive().clear(tuple.range())));
    }

    @Test
    void testPreWriteChecks() {
        // reads the key, and chooses the resolver based on the value
        final PreWriteCheck check = (context, providedResolver) -> {
            CompletableFuture<LocatableResolver> expectedResolverFuture = root(context).add("should-use-A").toTupleAsync()
                    .thenCompose(keyTuple -> context.ensureActive().get(keyTuple.pack()))
                    .thenApply(value -> {
                        boolean useA = Tuple.fromBytes(value).getBoolean(0);
                        return new ScopedInterningLayer(resolverPath(context, useA ? "A" : "B"));
                    });
            return expectedResolverFuture.thenApply(expectedResolver -> expectedResolver.equals(providedResolver));
        };

        final ResolverCreateHooks hooks = new ResolverCreateHooks(check, ResolverCreateHooks.DEFAULT_HOOK);

        // use resolver A
        database.run(context ->
                root(context).add("should-use-A").toTupleAsync()
                        .thenAccept(tuple -> context.ensureActive().set(tuple.pack(), Tuple.from(true).pack())));

        try (FDBRecordContext context = database.openContext()) {
            LocatableResolver resolverA = new ScopedInterningLayer(resolverPath(context, "A"));
            LocatableResolver resolverB = new ScopedInterningLayer(resolverPath(context, "B"));

            assertChecks(context, resolverA, hooks, true);
            assertChecks(context, resolverB, hooks, false);
        }

        // use resolver B
        database.run(context ->
                root(context).add("should-use-A").toTupleAsync()
                        .thenAccept(tuple -> context.ensureActive().set(tuple.pack(), Tuple.from(false).pack())));

        // after migration
        try (FDBRecordContext context = database.openContext()) {
            LocatableResolver resolverA = new ScopedInterningLayer(resolverPath(context, "A"));
            LocatableResolver resolverB = new ScopedInterningLayer(resolverPath(context, "B"));

            assertChecks(context, resolverA, hooks, false);
            assertChecks(context, resolverB, hooks, true);
        }
    }

    private KeySpacePath root(FDBRecordContext context) {
        return keySpace.path(context, "test-root");
    }

    private KeySpacePath resolverPath(FDBRecordContext context, String value) {
        return root(context).add("resolvers").add("resolverNode", value);
    }

    private static void assertChecks(FDBRecordContext context, LocatableResolver resolver, ResolverCreateHooks hooks, boolean shouldPass) {
        List<Boolean> checks = hooks.getPreWriteChecks().stream()
                .map(check -> check.apply(context, resolver).join())
                .collect(Collectors.toList());

        if (shouldPass) {
            assertFalse(checks.contains(false), "all checks should return true");
        } else {
            assertTrue(checks.contains(false), "at least one check must fail");
        }
    }
}
