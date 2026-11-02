/*
 * IndexEntryToRecordValueHelper.java
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
import com.apple.foundationdb.record.util.TrieNode;
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractArrayConstructorValue.LightArrayConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.ImmutableIntArray;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * What an index covers, as a trie over field names: a node's value reads the entry for that field, its children cover
 * fields inside it. {@link #toRecordValue} turns that into the value a plan decodes entries with, in place of
 * {@link com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord}'s copiers. See
 * <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2907">issue 2907</a>.
 */
public final class IndexEntryToRecordValueHelper
        extends TrieNode.AbstractTrieNodeBuilder<String, Value, IndexEntryToRecordValueHelper> {

    public IndexEntryToRecordValueHelper() {
        super(null, null);
    }

    @Nonnull
    @Override
    public IndexEntryToRecordValueHelper getThis() {
        return this;
    }

    /**
     * Reads this node's field through {@code value}, keeping the first of an index's repeated columns for one field, as
     * the copiers do.
     *
     * @param value the value reading the entry
     */
    public void cover(@Nonnull final Value value) {
        if (getValue() == null) {
            setValue(value);
        }
    }

    /**
     * The node for {@code fieldName} beneath this one, created if absent.
     *
     * @param fieldName the field's name
     *
     * @return that node
     */
    @Nonnull
    public IndexEntryToRecordValueHelper withChild(@Nonnull final String fieldName) {
        return computeIfAbsent(fieldName, ignored -> new IndexEntryToRecordValueHelper());
    }

    /**
     * A column per field of {@code targetType}, reading the entry for what this node covers and absent for the rest, one
     * value per message level as {@code MessageCopier} is one converter per level.
     *
     * @param targetType the record an entry is read into, which this node stands for
     *
     * @return the value computing that record from an entry
     */
    @Nonnull
    public RecordConstructorValue toRecordValue(@Nonnull final Type.Record targetType) {
        final var children = getChildrenMap();
        final var columns = ImmutableList.<Column<? extends Value>>builder();
        for (final var field : targetType.getFields()) {
            final var child = children == null ? null : children.get(field.getFieldName());
            final Value column;
            if (child == null) {
                column = absent(field.getFieldType());
            } else if (child.getValue() != null) {
                column = child.getValue();
            } else if (field.getFieldType() instanceof Type.Record) {
                column = child.toRecordValue((Type.Record)field.getFieldType());
            } else {
                column = absent(field.getFieldType());
            }
            columns.add(Column.of(field, column));
        }
        return RecordConstructorValue.ofColumns(columns.build());
    }

    /**
     * What a field the entry does not carry reads: null, which leaves it unset as the copiers do, except a non-nullable
     * array, which cannot be null and whose emptiness is indistinguishable from being unset.
     */
    @Nonnull
    private static Value absent(@Nonnull final Type fieldType) {
        if (fieldType.getTypeCode() == Type.TypeCode.ARRAY && !fieldType.isNullable()) {
            return LightArrayConstructorValue.emptyArray(
                    Objects.requireNonNull(((Type.Array)fieldType).getElementType()));
        }
        return new NullValue(fieldType);
    }

    /**
     * The value reading position {@code ordinal} of an entry's {@code source} tuple, shaped for the named field.
     *
     * @param baseObjectValue a value standing for the record the entry is read into
     * @param fieldName the field the position feeds
     * @param source whether the position is in the entry's key or its value
     * @param ordinal the position
     *
     * @return the value reading it
     */
    @Nonnull
    public static Value entryColumn(@Nonnull final Value baseObjectValue,
                                    @Nonnull final String fieldName,
                                    @Nonnull final TupleSource source,
                                    final int ordinal) {
        return FieldValue.ofFieldName(baseObjectValue, fieldName)
                .extractFromIndexEntryMaybe(baseObjectValue, EvaluationContext.empty(), AliasMap.emptyMap(),
                        ImmutableSet.of(), source, ImmutableIntArray.of(ordinal))
                .orElseThrow(() -> new IllegalStateException("cannot read " + fieldName + " from an index entry"))
                .getRight();
    }
}
