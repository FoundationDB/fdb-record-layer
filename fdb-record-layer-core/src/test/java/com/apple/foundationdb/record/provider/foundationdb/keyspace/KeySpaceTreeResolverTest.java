/*
 * KeySpaceTreeResolverTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeySpaceTreeResolver}.
 *
 * <p>If a new per-index value is added to {@link FDBRecordStoreKeyspace} without being added
 * to the switch in {@link KeySpaceTreeResolver#resolveNonDirectory}, the resolver returns
 * {@code UNRESOLVED} for paths under that keyspace and tooling that visualizes the keyspace
 * tree (e.g. {@code KeySpaceCountTree}) loses the ability to label keys by their owning
 * index. {@link #resolverHandlesPerIndexKeyspace} catches that gap; the
 * {@link #keyspaceIsCategorized} guard fails the build outright if a future enum value
 * isn't categorized in the test, forcing the author to consider it.</p>
 */
class KeySpaceTreeResolverTest {

    /**
     * Keyspaces that are NOT keyed by {@code index.getSubspaceTupleKey()} and so are
     * intentionally not handled by the per-index switch in
     * {@link KeySpaceTreeResolver#resolveNonDirectory}. New entries must be justified.
     */
    private static final Set<FDBRecordStoreKeyspace> NON_PER_INDEX_KEYSPACES = ImmutableSet.of(
            FDBRecordStoreKeyspace.STORE_INFO,             // store header
            FDBRecordStoreKeyspace.RECORD,                 // record data — has its own resolution branch
            FDBRecordStoreKeyspace.RECORD_COUNT,           // global record counts
            FDBRecordStoreKeyspace.RECORD_VERSION_SPACE,   // per-record versions
            FDBRecordStoreKeyspace.INDEX_STATE_SPACE       // keyed by index NAME, not subspace key (see TODO in resolveNonDirectory)
    );

    static Stream<FDBRecordStoreKeyspace> perIndexKeyspaces() {
        return Arrays.stream(FDBRecordStoreKeyspace.values())
                .filter(k -> !NON_PER_INDEX_KEYSPACES.contains(k));
    }

    static Stream<FDBRecordStoreKeyspace> nonPerIndexKeyspaces() {
        return NON_PER_INDEX_KEYSPACES.stream();
    }

    @ParameterizedTest
    @EnumSource(FDBRecordStoreKeyspace.class)
    void keyspaceIsCategorized(FDBRecordStoreKeyspace keyspace) {
        // Forward-looking guard: every value must either be in NON_PER_INDEX_KEYSPACES (and
        // therefore intentionally not resolved as an index keyspace), or be handled by the
        // per-index switch in KeySpaceTreeResolver.resolveNonDirectory.
        final boolean nonPerIndex = NON_PER_INDEX_KEYSPACES.contains(keyspace);
        assertTrue(nonPerIndex || isPerIndex(keyspace),
                keyspace + " is not categorized. Either add it to NON_PER_INDEX_KEYSPACES (with a "
                        + "justification) or to the per-index switch in "
                        + "KeySpaceTreeResolver.resolveNonDirectory and ensure "
                        + "resolverHandlesPerIndexKeyspace passes.");
    }

    @ParameterizedTest
    @MethodSource("perIndexKeyspaces")
    void resolverHandlesPerIndexKeyspace(FDBRecordStoreKeyspace keyspace) {
        final RecordMetaData metaData = buildMetaData();
        final Index index = metaData.getIndex("MySimpleRecord$str_value_indexed");
        final KeySpaceTreeResolver resolver = new KeySpaceTreeResolver();
        final KeySpace dummyKeySpace = new KeySpace(
                new KeySpaceDirectory("root", KeySpaceDirectory.KeyType.STRING, "test"));
        final KeySpaceTreeResolver.Resolved root = new KeySpaceTreeResolver.ResolvedRoot(dummyKeySpace);

        final KeySpaceTreeResolver.ResolvedRecordStoreKeyspace parent =
                new KeySpaceTreeResolver.ResolvedRecordStoreKeyspace(root, keyspace, metaData, keyspace.key());

        final KeySpaceTreeResolver.Resolved resolved =
                resolver.resolve(parent, index.getSubspaceTupleKey()).join();

        assertNotNull(resolved,
                "KeySpaceTreeResolver returned UNRESOLVED for " + keyspace + " under an index subspace key. "
                        + "The per-index switch in KeySpaceTreeResolver.resolveNonDirectory probably needs a "
                        + "case for this keyspace.");
        assertTrue(resolved instanceof KeySpaceTreeResolver.ResolvedIndexKeyspace,
                "KeySpaceTreeResolver did not produce a ResolvedIndexKeyspace for " + keyspace
                        + " — got " + resolved.getClass().getSimpleName() + " instead.");
        final KeySpaceTreeResolver.ResolvedIndexKeyspace asIndex =
                (KeySpaceTreeResolver.ResolvedIndexKeyspace) resolved;
        assertSame(index, asIndex.getIndex(),
                "Resolved index does not match the index whose subspace key was looked up for " + keyspace);
    }

    @ParameterizedTest
    @MethodSource("nonPerIndexKeyspaces")
    void resolverDoesNotResolveNonPerIndexKeyspaceAsIndex(FDBRecordStoreKeyspace keyspace) {
        // Pin down current behavior: for keyspaces NOT in the per-index switch, the resolver
        // must NOT return a ResolvedIndexKeyspace — even if the object happens to match a
        // valid index subspace key. This keeps NON_PER_INDEX_KEYSPACES honest.
        final RecordMetaData metaData = buildMetaData();
        final Index index = metaData.getIndex("MySimpleRecord$str_value_indexed");
        final KeySpaceTreeResolver resolver = new KeySpaceTreeResolver();
        final KeySpace dummyKeySpace = new KeySpace(
                new KeySpaceDirectory("root", KeySpaceDirectory.KeyType.STRING, "test"));
        final KeySpaceTreeResolver.Resolved root = new KeySpaceTreeResolver.ResolvedRoot(dummyKeySpace);

        final KeySpaceTreeResolver.ResolvedRecordStoreKeyspace parent =
                new KeySpaceTreeResolver.ResolvedRecordStoreKeyspace(root, keyspace, metaData, keyspace.key());

        final KeySpaceTreeResolver.Resolved resolved =
                resolver.resolve(parent, index.getSubspaceTupleKey()).join();

        assertFalse(resolved instanceof KeySpaceTreeResolver.ResolvedIndexKeyspace,
                "KeySpaceTreeResolver should not resolve " + keyspace
                        + " as a ResolvedIndexKeyspace, but did.");
    }

    private static boolean isPerIndex(FDBRecordStoreKeyspace keyspace) {
        // Mirrors the per-index switch in KeySpaceTreeResolver.resolveNonDirectory.
        switch (keyspace) {
            case INDEX:
            case INDEX_SECONDARY_SPACE:
            case INDEX_RANGE_SPACE:
            case INDEX_UNIQUENESS_VIOLATIONS_SPACE:
            case INDEX_BUILD_SPACE:
            case INDEX_SLIDING_WINDOW_SPACE:
                return true;
            default:
                return false;
        }
    }

    private static RecordMetaData buildMetaData() {
        // Use the simple test meta-data, which has an index named MySimpleRecord$str_value_indexed.
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor());
        return builder.getRecordMetaData();
    }
}
