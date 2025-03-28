/*
 * TypeUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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


package com.apple.foundationdb.relational.recordlayer.util;

import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;

public final class TypeUtils {

    @Nonnull
    public static Type setFieldNames(@Nonnull final Type input,
                                     @Nonnull final CompatibleTypeEvolutionPredicate.FieldAccessTrieNode fieldAccessTrieNode) {
        return setFieldNamesInternal(input, fieldAccessTrieNode);
    }

    @Nonnull
    // PMD incorrectly thinks that comparing array sizes it deemed to be object reference comparison requiring equals() instead.
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static Type setFieldNamesInternal(@Nonnull final Type input,
                                              @Nonnull final CompatibleTypeEvolutionPredicate.FieldAccessTrieNode trie) {
        if (input.isPrimitive()) {
            return input;
        }
        if (trie.getChildrenMap() != null && trie.getChildrenMap().isEmpty()) {
            return input;
        }
        if (input.isArray()) {
            final var array = (Type.Array)input;
            return array.withElementType(setFieldNamesInternal(Assert.notNullUnchecked(array.getElementType()), trie));
        }
        Assert.thatUnchecked(input.isRecord(), ErrorCode.INCOMPATIBLE_TABLE_ALIAS,
                () -> "incompatible type found while renaming. Expected " + Type.Record.class.getSimpleName()
                        + " got " + input.getJavaClass().getSimpleName());
        final var record = (Type.Record)input;
        final var recordFields = record.getFields();
        final var newlyNamedFields = ImmutableList.<Type.Record.Field>builder();
        final var fieldAliases = new ArrayList<>(trie.getChildrenMap().keySet());
        Assert.thatUnchecked(fieldAliases.size() == recordFields.size(), ErrorCode.INCOMPATIBLE_TABLE_ALIAS,
                () -> "number of record fields mismatch");
        fieldAliases.sort(Comparator.comparingInt(FieldValue.ResolvedAccessor::getOrdinal));
        for (int i = 0; i < recordFields.size(); i++) {
            final var fieldAlias = fieldAliases.get(i);
            final var recordField = recordFields.get(i);
            final var fieldTrie = trie.getChildrenMap().get(fieldAlias);
            final var renamedFieldType = setFieldNamesInternal(recordField.getFieldType(), fieldTrie);
            final var newField = Type.Record.Field.of(renamedFieldType, Optional.ofNullable(fieldAlias.getName()),
                    Optional.of(recordField.getFieldIndex()));
            newlyNamedFields.add(newField);
        }
        return record.getName() == null ? Type.Record.fromFieldsWithName(record.getName(), record.isNullable(), newlyNamedFields.build())
                : Type.Record.fromFields(record.isNullable(), newlyNamedFields.build());
    }

}
