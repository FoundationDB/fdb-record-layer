/*
 * PrimaryScanMatchCandidateTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Tests for {@link PrimaryScanMatchCandidate}.
 */
class PrimaryScanMatchCandidateTest {

    private PrimaryScanMatchCandidate createCandidate(KeyExpression primaryKey) {
        return new PrimaryScanMatchCandidate(
                Traversal.withRoot(Reference.empty()),
                ImmutableList.of(),
                Collections.emptyList(),
                Collections.emptyList(),
                primaryKey,
                Type.Record.fromFields(ImmutableList.of()));
    }

    @Test
    void testHasAndOrderedByRecordTypeKeyWithRecordTypePrefix() {
        final KeyExpression primaryKey = concat(Key.Expressions.recordType(), field("id"));
        final PrimaryScanMatchCandidate candidate = createCandidate(primaryKey);

        Assertions.assertTrue(candidate.hasAndOrderedByRecordTypeKey(),
                "isSortedByRecordTypeKey() should return true when primary key starts with record type");
    }

    @Test
    void testHasAndOrderedByRecordTypeKeyWithoutRecordTypePrefix() {
        final KeyExpression primaryKey = field("id");
        final PrimaryScanMatchCandidate candidate = createCandidate(primaryKey);

        Assertions.assertFalse(candidate.hasAndOrderedByRecordTypeKey(),
                "isSortedByRecordTypeKey() should return false when primary key does not start with record type");
    }

    @Test
    void testHasAndOrderedByRecordTypeKeyRecordTypeNotPrefix() {
        final KeyExpression primaryKey = concat(field("id"), Key.Expressions.recordType());
        final PrimaryScanMatchCandidate candidate = createCandidate(primaryKey);

        Assertions.assertFalse(candidate.hasAndOrderedByRecordTypeKey(),
                "isSortedByRecordTypeKey() should return false when record type is not a prefix of the primary key");
    }
}
