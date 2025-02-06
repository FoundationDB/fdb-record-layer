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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks.PreWriteCheck;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
class ResolverCreateHooksTest {
    @RegisterExtension
    @Order(0)
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    @Order(1)
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase database;
    private KeySpacePath rootPath;
    private final Random random = new Random();

    @BeforeEach
    public void setup() {
        database = dbExtension.getDatabase();
        database.clearCaches();
        rootPath = pathManager.createPath(TestKeySpace.RESOLVER_HOOKS);
    }

    @Test
    void testPreWriteChecks() {
        // reads the key, and chooses the resolver based on the value
        final PreWriteCheck check = (context, providedResolver) -> {
            CompletableFuture<LocatableResolver> expectedResolverFuture = rootPath.add("should-use-A").toTupleAsync(context)
                    .thenCompose(keyTuple -> context.ensureActive().get(keyTuple.pack()))
                    .thenApply(value -> {
                        boolean useA = Tuple.fromBytes(value).getBoolean(0);
                        return new ScopedInterningLayer(database, resolverPath(context, useA ? "A" : "B"));
                    });
            return expectedResolverFuture.thenApply(expectedResolver -> expectedResolver.equals(providedResolver));
        };

        final ResolverCreateHooks hooks = new ResolverCreateHooks(check, ResolverCreateHooks.DEFAULT_HOOK);

        // use resolver A
        database.runAsync(context ->
                rootPath.add("should-use-A").toTupleAsync(context)
                        .thenAccept(tuple -> context.ensureActive().set(tuple.pack(), Tuple.from(true).pack()))).join();

        try (FDBRecordContext context = database.openContext()) {
            LocatableResolver resolverA = new ScopedInterningLayer(database, resolverPath(context, "A"));
            LocatableResolver resolverB = new ScopedInterningLayer(database, resolverPath(context, "B"));

            assertChecks(context, resolverA, hooks, true);
            assertChecks(context, resolverB, hooks, false);
        }

        // use resolver B
        database.run(context ->
                rootPath.add("should-use-A").toTupleAsync(context)
                        .thenAccept(tuple -> context.ensureActive().set(tuple.pack(), Tuple.from(false).pack())));

        // after migration
        try (FDBRecordContext context = database.openContext()) {
            LocatableResolver resolverA = new ScopedInterningLayer(database, resolverPath(context, "A"));
            LocatableResolver resolverB = new ScopedInterningLayer(database, resolverPath(context, "B"));

            assertChecks(context, resolverA, hooks, false);
            assertChecks(context, resolverB, hooks, true);
        }
    }

    private ResolvedKeySpacePath resolverPath(FDBRecordContext context, String value) {
        return rootPath.add("resolvers").add("resolverNode", value).toResolvedPath(context);
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
