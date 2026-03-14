/*
 * VectorIndexScanMatchCandidateTest.java
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

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Tests for {@link VectorIndexScanMatchCandidate}.
 */
class VectorIndexScanMatchCandidateTest {

    private VectorIndexScanMatchCandidate createVectorIndexScanMatchCandidate(final String indexName,
                                                                                final KeyExpression keyExpression,
                                                                                final String indexType) {
        final Index index = new Index(indexName, keyExpression, indexType);
        final List<RecordType> queriedRecordTypes = Collections.emptyList();
        final Traversal traversal = Traversal.withRoot(Reference.empty());
        final List<CorrelationIdentifier> parameters = ImmutableList.of();
        final List<CorrelationIdentifier> orderingAliases = ImmutableList.of();
        final Set<CorrelationIdentifier> parametersRequiredForBinding = ImmutableSet.of();
        final Type.Record baseType = Type.Record.fromFields(ImmutableList.of());
        final CorrelationIdentifier baseAlias = CorrelationIdentifier.of("base");
        final KeyExpression fullKeyExpression = keyExpression;
        final KeyExpression primaryKey = null;

        return new VectorIndexScanMatchCandidate(
                index,
                queriedRecordTypes,
                traversal,
                parameters,
                orderingAliases,
                parametersRequiredForBinding,
                baseType,
                baseAlias,
                fullKeyExpression,
                primaryKey
        );
    }

    @Test
    void testGetName() {
        final String indexName = "my_vector_index";
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                indexName,
                field("vector_field"),
                "vector"
        );

        Assertions.assertEquals(indexName, candidate.getName(),
                "getName() should return the index name");
    }

    @Test
    void testGetNameWithDifferentIndexName() {
        final String indexName = "another_index";
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                indexName,
                field("embedding"),
                "vector"
        );

        Assertions.assertEquals(indexName, candidate.getName(),
                "getName() should return the correct index name");
    }

    @Test
    void testToString() {
        final String indexName = "my_vector_index";
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                indexName,
                field("vector_field"),
                "vector"
        );

        final String expectedToString = "vector[" + indexName + "]";
        Assertions.assertEquals(expectedToString, candidate.toString(),
                "toString() should return 'vector[' + index name + ']'");
    }

    @Test
    void testToStringWithDifferentIndexName() {
        final String indexName = "embeddings_idx";
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                indexName,
                field("embedding"),
                "vector"
        );

        final String expectedToString = "vector[" + indexName + "]";
        Assertions.assertEquals(expectedToString, candidate.toString(),
                "toString() should format correctly with different index names");
    }

    @Test
    void testToStringFormat() {
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                "test_index",
                field("vec"),
                "vector"
        );

        final String toString = candidate.toString();
        Assertions.assertTrue(toString.startsWith("vector["),
                "toString() should start with 'vector['");
        Assertions.assertTrue(toString.endsWith("]"),
                "toString() should end with ']'");
        Assertions.assertTrue(toString.contains("test_index"),
                "toString() should contain the index name");
    }

    @Test
    void testGetColumnSize() {
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                "my_vector_index",
                field("vector_field"),
                "vector"
        );

        final int columnSize = candidate.getColumnSize();
        Assertions.assertTrue(columnSize >= 0,
                "getColumnSize() should return a non-negative value");
    }

    @Test
    void testGetColumnSizeWithSingleField() {
        final KeyExpression singleField = field("embedding");
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                "single_field_index",
                singleField,
                "vector"
        );

        final int columnSize = candidate.getColumnSize();
        Assertions.assertEquals(1, columnSize,
                "getColumnSize() should return 1 for a single field index");
    }

    @Test
    void testGetColumnSizeConsistency() {
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                "my_vector_index",
                field("vector_field"),
                "vector"
        );

        final int columnSize1 = candidate.getColumnSize();
        final int columnSize2 = candidate.getColumnSize();

        Assertions.assertEquals(columnSize1, columnSize2,
                "getColumnSize() should return consistent values across multiple calls");
    }

    @Test
    void testGetNameIsNotNull() {
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                "test_index",
                field("vector"),
                "vector"
        );

        Assertions.assertNotNull(candidate.getName(),
                "getName() should never return null");
    }

    @Test
    void testToStringIsNotNull() {
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                "test_index",
                field("vector"),
                "vector"
        );

        Assertions.assertNotNull(candidate.toString(),
                "toString() should never return null");
    }

    @Test
    void testToStringNotEmpty() {
        final VectorIndexScanMatchCandidate candidate = createVectorIndexScanMatchCandidate(
                "test_index",
                field("vector"),
                "vector"
        );

        Assertions.assertFalse(candidate.toString().isEmpty(),
                "toString() should never return an empty string");
    }

    @Test
    void testGetNameMatchesConstructorIndexName() {
        final String indexName = "specific_name_123";
        final Index index = new Index(indexName, field("vector"), "vector");
        final VectorIndexScanMatchCandidate candidate = new VectorIndexScanMatchCandidate(
                index,
                Collections.emptyList(),
                Traversal.withRoot(Reference.empty()),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Type.Record.fromFields(ImmutableList.of()),
                CorrelationIdentifier.of("base"),
                field("vector"),
                null
        );

        Assertions.assertEquals(indexName, candidate.getName(),
                "getName() should match the index name provided in constructor");
    }

    @Test
    void testToStringIncludesIndexNameFromConstructor() {
        final String indexName = "constructor_test_index";
        final Index index = new Index(indexName, field("vector"), "vector");
        final VectorIndexScanMatchCandidate candidate = new VectorIndexScanMatchCandidate(
                index,
                Collections.emptyList(),
                Traversal.withRoot(Reference.empty()),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Type.Record.fromFields(ImmutableList.of()),
                CorrelationIdentifier.of("base"),
                field("vector"),
                null
        );

        Assertions.assertEquals("vector[" + indexName + "]", candidate.toString(),
                "toString() should include the index name from constructor");
    }
}
