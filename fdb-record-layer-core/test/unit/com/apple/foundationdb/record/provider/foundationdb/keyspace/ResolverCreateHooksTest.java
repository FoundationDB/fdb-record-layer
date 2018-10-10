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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.test.Tags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.WipesFDB)
@Tag(Tags.RequiresFDB)
class ResolverCreateHooksTest {
    private FDBDatabase database;

    @BeforeEach
    public void setup() {
        FDBDatabaseFactory factory = FDBDatabaseFactory.instance();
        database = factory.getDatabase();
        database.clearCaches();
        wipeFDB();
    }

    @AfterEach
    public void teardown() {
        // tests that modify migration state of global directory layer need to wipe FDB to prevent pollution of other suites
        wipeFDB();
    }

    private void wipeFDB() {
        database.run((context -> {
            context.ensureActive().clear(new Range(new byte[] {(byte)0x00}, new byte[] {(byte)0xFF}));
            return null;
        }));
    }

    @Test
    void testGetMigratableGlobal() {
        final ResolverCreateHooks hooks = ResolverCreateHooks.getMigratableGlobal();

        // before migration
        try (FDBRecordContext context = database.openContext()) {
            assertChecks(context, ScopedDirectoryLayer.global(database), hooks, true);
            assertChecks(context, ScopedInterningLayer.global(database), hooks, false);
        }

        // migrate
        ScopedDirectoryLayer.global(database).retireLayer().join();

        // after migration
        try (FDBRecordContext context = database.openContext()) {
            assertChecks(context, ScopedDirectoryLayer.global(database), hooks, false);
            assertChecks(context, ScopedInterningLayer.global(database), hooks, true);
        }
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
