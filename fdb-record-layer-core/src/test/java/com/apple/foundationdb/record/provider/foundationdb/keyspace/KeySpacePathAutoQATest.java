/*
 * KeySpacePathAutoQATest.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Additional tests for {@link KeySpacePath} written by auto-qa.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class KeySpacePathAutoQATest {

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    private final String rootUuid = UUID.randomUUID().toString();

    private KeySpace createKeySpace(KeySpaceDirectory.KeyType leafType) {
        KeySpaceDirectory rootDir = new KeySpaceDirectory("test_root", KeySpaceDirectory.KeyType.STRING, rootUuid);
        rootDir.addSubdirectory(new KeySpaceDirectory("leaf", leafType));
        return new KeySpace(rootDir);
    }

    @Test
    void equalPathsWithNegativeIntegerAndLongHaveEqualHashCodes() {
        // BUG AQA-21: KeySpaceDirectory.areEqual treats Integer -1 and Long -1L as equal, but valueHashCode uses
        // Objects.hashCode, so equal KeySpacePaths have different hash codes. Spec: Object#hashCode contract.
        // paths with Integer -1 and Long -1L are equal per equals(), so their hash codes must match too
        final KeySpace keySpace = createKeySpace(KeySpaceDirectory.KeyType.LONG);
        final KeySpacePath intPath = keySpace.path("test_root").add("leaf", -1);
        final KeySpacePath longPath = keySpace.path("test_root").add("leaf", -1L);

        assertEquals(intPath, longPath);
        assertEquals(intPath.hashCode(), longPath.hashCode());
    }

    @Test
    void bytesPathRoundTripsThroughTupleWithEqualHashCode() {
        // a BYTES path resolved back from its own tuple is equal to the original and has the same hash code
        final FDBDatabase database = dbExtension.getDatabase();
        final KeySpace keySpace = createKeySpace(KeySpaceDirectory.KeyType.BYTES);
        final KeySpacePath path = keySpace.path("test_root").add("leaf", new byte[] {0x01, 0x00, (byte) 0xff});

        try (FDBRecordContext context = database.openContext()) {
            final Tuple tuple = path.toTuple(context);
            final KeySpacePath roundTripped = keySpace.resolveFromKey(context, tuple).toPath();

            assertEquals(path, roundTripped);
            assertEquals(path.hashCode(), roundTripped.hashCode());
            assertArrayEquals((byte[]) path.getValue(), (byte[]) roundTripped.getValue());
        }
    }

    @Test
    void deleteAllDataDoesNotDeleteSiblingWithNulSuffix() {
        // BUG AQA-22: KeySpacePathImpl uses [pack(), strinc(pack())) instead of a tuple subspace range, so the path for
        // "a" also covers the sibling "a\u0000b". Spec: KeySpacePath deleteAllDataAsync / hasDataAsync javadoc.
        // deleting path "a" must not delete data stored under the sibling path "a\u0000b"
        final FDBDatabase database = dbExtension.getDatabase();
        final KeySpace keySpace = createKeySpace(KeySpaceDirectory.KeyType.STRING);
        final KeySpacePath path = keySpace.path("test_root").add("leaf", "a");
        final KeySpacePath sibling = keySpace.path("test_root").add("leaf", "a\u0000b");

        try (FDBRecordContext context = database.openContext()) {
            context.ensureActive().set(sibling.toSubspace(context).pack(Tuple.from(1L)), Tuple.from("x").pack());
            path.deleteAllData(context);
            assertTrue(sibling.hasData(context));
        }
    }

    @Test
    void hasDataIgnoresSiblingWithNulSuffix() {
        // BUG AQA-22: KeySpacePathImpl uses [pack(), strinc(pack())) instead of a tuple subspace range, so the path for
        // "a" also covers the sibling "a\u0000b". Spec: KeySpacePath deleteAllDataAsync / hasDataAsync javadoc.
        // hasData for path "a" must be false when only the sibling path "a\u0000b" holds data
        final FDBDatabase database = dbExtension.getDatabase();
        final KeySpace keySpace = createKeySpace(KeySpaceDirectory.KeyType.STRING);
        final KeySpacePath path = keySpace.path("test_root").add("leaf", "a");
        final KeySpacePath sibling = keySpace.path("test_root").add("leaf", "a\u0000b");

        try (FDBRecordContext context = database.openContext()) {
            context.ensureActive().set(sibling.toSubspace(context).pack(Tuple.from(1L)), Tuple.from("x").pack());
            assertTrue(sibling.hasData(context));
            assertFalse(path.hasData(context));
        }
    }
}
