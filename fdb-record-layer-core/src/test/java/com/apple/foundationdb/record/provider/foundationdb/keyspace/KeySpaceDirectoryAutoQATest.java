/*
 * KeySpaceDirectoryAutoQATest.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Additional tests for {@link KeySpaceDirectory}.
 */
@Tag(Tags.RequiresFDB)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class KeySpaceDirectoryAutoQATest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @Test
    void addSubdirectoryEqualByteConstantsThrows() {
        // BUG AQA-7: addSubdirectory compares constants with Objects.equals, so equal byte[] contents are not detected.
        // Spec: KeySpaceDirectory.addSubdirectory javadoc, throws if "a subdirectory of the same type already exists
        // with the same constant value".
        // two BYTES constants with the same content (different array instances) overlap and must be rejected
        final KeySpaceDirectory parent = new KeySpaceDirectory("parent", KeyType.STRING, "p");
        parent.addSubdirectory(new KeySpaceDirectory("a", KeyType.BYTES, new byte[] {0x01, 0x02}));
        assertThatThrownBy(() -> parent.addSubdirectory(new KeySpaceDirectory("b", KeyType.BYTES, new byte[] {0x01, 0x02})))
                .isInstanceOf(RecordCoreArgumentException.class);
    }

    @Test
    void addSubdirectoryIntegerAndLongSameConstantThrows() {
        // BUG AQA-8: addSubdirectory compares constants with Objects.equals, so LONG constants 5L and Integer 5 are not
        // detected as the same value. Spec: KeySpaceDirectory.addSubdirectory javadoc.
        // an Integer and a Long constant with the same numeric value encode to the same tuple element and must be rejected
        final KeySpaceDirectory parent = new KeySpaceDirectory("parent", KeyType.STRING, "p");
        parent.addSubdirectory(new KeySpaceDirectory("a", KeyType.LONG, 5L));
        assertThatThrownBy(() -> parent.addSubdirectory(new KeySpaceDirectory("b", KeyType.LONG, 5)))
                .isInstanceOf(RecordCoreArgumentException.class);
    }

    @Test
    void isValueValidIntegerForLongConstantIsTrue() {
        // BUG AQA-9: isValueValid compares with Objects.equals instead of areEqual, so Integer 5 is rejected for LONG
        // constant 5L although the path API accepts it. Spec: KeySpaceDirectory.isValueValid / validateValue javadoc.
        // Integer 5 is accepted by path add for a LONG constant 5L, so isValueValid must agree
        final KeySpace root = new KeySpace(new KeySpaceDirectory("a", KeyType.LONG, 5L));
        final KeySpaceDirectory dir = root.getDirectory("a");
        try (FDBRecordContext context = dbExtension.getDatabase().openContext()) {
            assertThat(root.path("a", 5).toTuple(context).getLong(0)).isEqualTo(5L);
        }
        assertThat(dir.isValueValid(5)).isTrue();
    }

}
