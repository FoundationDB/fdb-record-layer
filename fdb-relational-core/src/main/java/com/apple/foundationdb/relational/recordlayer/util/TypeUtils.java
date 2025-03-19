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
