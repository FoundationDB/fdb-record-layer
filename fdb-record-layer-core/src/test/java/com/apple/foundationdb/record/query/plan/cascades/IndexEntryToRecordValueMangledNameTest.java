/*
 * IndexEntryToRecordValueMangledNameTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord.TupleSource;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.ImmutableIntArray;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * {@link IndexEntryToRecordValueHelper} is keyed by field name, so it must be filled with the SQL spelling that
 * {@link IndexEntryToRecordValueHelper#toRecordValue} looks up, not the descriptor spelling an escaped identifier
 * (e.g. {@code foo.tableA.A1} vs {@code foo__2tableA__2A1}) has in storage (issue #4758).
 */
class IndexEntryToRecordValueMangledNameTest {

    @Nonnull
    private static final Type LONG = Type.primitiveType(Type.TypeCode.LONG, false);

    /** The base record as the planner carries it: fields named with the SQL identifiers. */
    @Nonnull
    private static Type.Record baseType(@Nonnull final List<String> fieldNames) {
        return Type.Record.fromFields(false, fieldNames.stream()
                .map(name -> Type.Record.Field.of(LONG, Optional.of(name)))
                .collect(ImmutableList.toImmutableList()));
    }

    /** What an index key expression expands to for the column at {@code ordinal}; its storage name is the mangled one. */
    @Nonnull
    private static FieldValue keyValueNamed(@Nonnull final Value baseObjectValue,
                                            @Nonnull final String sqlName,
                                            final int ordinal) {
        return FieldValue.ofFields(baseObjectValue,
                new FieldValue.FieldPath(ImmutableList.of(
                        FieldValue.ResolvedAccessor.of(Type.Record.Field.of(LONG, Optional.of(sqlName)), ordinal))));
    }

    /**
     * Fills the trie exactly as {@code ScanWithFetchMatchCandidate.recordCoveredField} does, from the pair that
     * {@code extractFromIndexEntryMaybe} returns, then drains it against {@code baseType}.
     */
    @Nonnull
    private static Value translate(@Nonnull final Type.Record baseType,
                                   @Nonnull final List<String> indexColumnNames) {
        final var baseObjectValue = QuantifiedObjectValue.of(CorrelationIdentifier.of("base"), baseType);
        final var helper = new IndexEntryToRecordValueHelper();
        for (int ordinal = 0; ordinal < indexColumnNames.size(); ordinal++) {
            final var keyValue = keyValueNamed(baseObjectValue, indexColumnNames.get(ordinal), ordinal);
            final NonnullPair<FieldValue, Value> extraction =
                    keyValue.extractFromIndexEntryMaybe(baseObjectValue, EvaluationContext.empty(),
                                    AliasMap.emptyMap(), ImmutableSet.of(), TupleSource.KEY,
                                    ImmutableIntArray.of(ordinal))
                            .orElseThrow(() -> new IllegalStateException("extraction failed"));
            helper.withChild(extraction.getKey().getLastFieldName().orElseThrow())
                    .cover(extraction.getValue());
        }
        return helper.toRecordValue(baseType);
    }

    @Test
    void plainIdentifiersTranslate() {
        assertReadsEntry(ImmutableList.of("A1", "A2"));
    }

    @Test
    void escapedIdentifiersTranslate() {
        assertReadsEntry(ImmutableList.of("foo.tableA.A1", "foo.tableA.A2"));
    }

    private static void assertReadsEntry(@Nonnull final List<String> names) {
        final var recordValue = translate(baseType(names), names);
        final var columns = assertInstanceOf(RecordConstructorValue.class, recordValue).getColumns();
        assertEquals(names.size(), columns.size());
        for (final var column : columns) {
            assertFalse(column.getValue() instanceof NullValue,
                    () -> "column " + column.getField().getFieldName() + " should read the entry");
        }
    }

    @Test
    void spellingsDifferWhileValuesAreEqual() {
        final var sqlType = baseType(ImmutableList.of("foo.tableA.A1", "foo.tableA.A2"));
        final var baseObjectValue = QuantifiedObjectValue.of(CorrelationIdentifier.of("base"), sqlType);

        final var keyValue = keyValueNamed(baseObjectValue, "foo.tableA.A1", 0);
        final var bySqlName = FieldValue.ofFieldName(baseObjectValue, "foo.tableA.A1");

        assertEquals("foo__2tableA__2A1", keyValue.getLastFieldStorageName().orElseThrow());
        assertEquals("foo.tableA.A1", keyValue.getLastFieldName().orElseThrow());
        assertEquals(bySqlName.getFieldOrdinals(), keyValue.getFieldOrdinals());
        // equality rests on ordinals, so only a name-keyed lookup can tell the spellings apart
        assertEquals(bySqlName, keyValue);
        assertEquals(bySqlName.hashCode(), keyValue.hashCode());
    }
}
