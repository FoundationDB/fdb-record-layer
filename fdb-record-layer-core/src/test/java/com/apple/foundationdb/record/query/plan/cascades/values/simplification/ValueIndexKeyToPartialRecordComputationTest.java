/*
 * ValueIndexKeyToPartialRecordComputationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FromOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexEntryObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.tuple.TupleOrdering;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.ImmutableIntArray;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Test cases that test logic around the simplification of {@link Value} trees.
 */
class ValueIndexKeyToPartialRecordComputationTest {
    private static final CorrelationIdentifier ALIAS = CorrelationIdentifier.of("_");

    @Test
    void testDeriveIndexKeyToPartialRecordValue1() {
        final var qov = QuantifiedObjectValue.of(ALIAS, someRecordType());

        // _.a.aa.aaa
        final var _a_aa_aaa = FieldValue.ofFieldNames(qov, ImmutableList.of("a", "aa", "aaa"));

        final var resultOptional =
                _a_aa_aaa.extractFromIndexEntryMaybe(qov, AliasMap.emptyMap(), ImmutableSet.of(),
                        IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(1));
        Assertions.assertTrue(resultOptional.isPresent());
        final var expectedResult = new IndexEntryObjectValue(Quantifier.current(), IndexKeyValueToPartialRecord.TupleSource.KEY,
                ImmutableIntArray.of(1), Type.primitiveType(Type.TypeCode.STRING));
        Assertions.assertEquals(_a_aa_aaa, resultOptional.get().getKey());
        Assertions.assertEquals(expectedResult, resultOptional.get().getValue());
    }

    @Test
    void testDeriveIndexKeyToPartialRecordValue2() {
        final var qov = QuantifiedObjectValue.of(ALIAS, someRecordType());

        // _.a.aa.aaa
        final var _a_aa_aaa = FieldValue.ofFieldNames(qov, ImmutableList.of("a", "aa", "aaa"));
        final var toOrderingBytesValue = new ToOrderedBytesValue(_a_aa_aaa, TupleOrdering.Direction.DESC_NULLS_FIRST);

        final var resultOptional =
                toOrderingBytesValue.extractFromIndexEntryMaybe(qov, AliasMap.emptyMap(), ImmutableSet.of(),
                        IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(1));
        final var expectedResult =
                new FromOrderedBytesValue(new IndexEntryObjectValue(Quantifier.current(),
                        IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(1),
                        Type.primitiveType(Type.TypeCode.STRING)),
                TupleOrdering.Direction.DESC_NULLS_FIRST, Type.primitiveType(Type.TypeCode.STRING));
        Assertions.assertTrue(resultOptional.isPresent());
        Assertions.assertEquals(_a_aa_aaa, resultOptional.get().getKey());
        Assertions.assertEquals(expectedResult, resultOptional.get().getValue());
    }

    @Test
    void testDeriveIndexKeyToPartialRecordValue3() {
        final var qov = QuantifiedObjectValue.of(ALIAS, someRecordType());

        // _.a.aa
        final var _a_aa = FieldValue.ofFieldNames(qov, ImmutableList.of("a", "aa"));
        // _.a.aa.aaa
        final var _a_aa__aaa = FieldValue.ofFieldName(_a_aa, "aaa");
        final var toOrderingBytesValue = new ToOrderedBytesValue(_a_aa__aaa, TupleOrdering.Direction.DESC_NULLS_FIRST);

        final var resultOptional =
                toOrderingBytesValue.extractFromIndexEntryMaybe(qov, AliasMap.emptyMap(), ImmutableSet.of(),
                        IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(1));
        final var expectedResult =
                new FromOrderedBytesValue(new IndexEntryObjectValue(Quantifier.current(),
                        IndexKeyValueToPartialRecord.TupleSource.KEY, ImmutableIntArray.of(1),
                        Type.primitiveType(Type.TypeCode.STRING)),
                        TupleOrdering.Direction.DESC_NULLS_FIRST, Type.primitiveType(Type.TypeCode.STRING));
        Assertions.assertTrue(resultOptional.isPresent());
        final var _a_aa_aaa = FieldValue.ofFieldNames(qov, ImmutableList.of("a", "aa", "aaa"));
        Assertions.assertEquals(_a_aa_aaa, resultOptional.get().getKey());
        Assertions.assertEquals(expectedResult, resultOptional.get().getValue());
    }

    @Nonnull
    private static Type.Record someRecordType() {
        final var aaType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aaa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("aab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("aac"))));

        final var aType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aaType, Optional.of("aa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("ab")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("ac"))));

        final var xType = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xa")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("xb")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("xc"))));

        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(aType, Optional.of("a")),
                Type.Record.Field.of(xType, Optional.of("x")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("z"))));
    }
}
