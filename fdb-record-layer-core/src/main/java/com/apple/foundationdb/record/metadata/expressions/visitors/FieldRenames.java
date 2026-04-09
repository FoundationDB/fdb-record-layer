/*
 * FieldRenaming.java
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

package com.apple.foundationdb.record.metadata.expressions.visitors;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Object encapsulating a set of renamed fields between pairs of Protobuf {@link Descriptors.Descriptor}s.
 * This class can be used to compare two {@linkplain RecordMetaData#getRecordsDescriptor() records descriptors}
 * taken from two different versions of the same meta-data in order to track the set of fields that were
 * renamed from one version to another. It can then be paired with the {@link RenameFieldsVisitor} in order
 * to rewrite any expressions so that they access the same data.
 *
 * <p>
 * In this class (as elsewhere), it is assumed that two fields should be considered the same if they share
 * the same field number. This is because Protobuf records only store the field number when data is serialized.
 * So, data serialized with one descriptor can be deserialized with another descriptor even if the fields have
 * changed. But in order for Record Layer {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression}s
 * to evaluate to the same values when evaluated against deserialized data, they must be rewritten to use
 * the new names.
 * </p>
 */
public final class FieldRenames {
    @Nonnull
    private static final FieldRenames IDENTITY = new FieldRenames(Collections.emptyMap());

    @Nonnull
    private final Map<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>, Map<String, String>> renamingMap;

    private FieldRenames(@Nonnull Map<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>, Map<String, String>> renamingMap) {
        this.renamingMap = renamingMap;
    }

    /**
     * Whether this contains only identity transformations. If this is {@code true}, then
     * there are no field renames that need to be adjusted.
     *
     * @return whether this {@link FieldRenames} contains only identity transformations
     */
    public boolean isIdentity() {
        return renamingMap.isEmpty();
    }

    /**
     * Get a collection of fields that have changed names between two descriptors. If the given source and target
     * descriptors were compared while constructing the {@link FieldRenames}, then this will return a map
     * linking every field name in {@code sourceDescriptor} where the same field in
     * {@code targetDescriptor} (as specified by the field number) has a different name. Any key expression
     * written for the source descriptor can then be rewritten so it applies to the target descriptor
     * by looking up the source field names in the map to get the target field name. Note that as the map
     * only contains changed fields, if a field is missing, it can be assumed to be unchanged.
     *
     * @param sourceDescriptor a descriptor that is the source of renames
     * @param targetDescriptor a descriptor that is being targeted for renaming
     * @return a map for transforming fields in the {@code sourceDescriptor} to fields in {@code targetDescriptor}
     */
    @Nonnull
    public Map<String, String> getRenamingForTypes(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor) {
        return renamingMap.getOrDefault(NonnullPair.of(sourceDescriptor, targetDescriptor), Collections.emptyMap());
    }

    @Override
    public int hashCode() {
        return renamingMap.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FieldRenames that = (FieldRenames)o;
        return Objects.equals(renamingMap, that.renamingMap);
    }

    @Override
    public String toString() {
        return renamingMap.entrySet().stream()
                .map(entry -> "(" + entry.getKey().getLeft().getFullName() + ", " + entry.getKey().getRight().getFullName() + "): " + entry.getValue())
                .collect(Collectors.joining(", ", "FieldRenaming[", "]"));
    }

    /**
     * Returns an identity instance of {@link FieldRenames} that contains no actually renamed fields.
     * @return a {@link FieldRenames} that encodes no field renames
     */
    @Nonnull
    public static FieldRenames identity() {
        return IDENTITY;
    }

    /**
     * Create a new {@link FieldRenames.Builder} that can be used to register any field renames.
     * Basic usage is that the user should construct a new builder, then walk through two records
     * descriptors in order to find fields that have been renamed, and then call {@link Builder#build()}
     * to create a final immutable set of renames.
     *
     * <p>
     * This should be preferred to {@link #constructFor(Descriptors.Descriptor, Descriptors.Descriptor)} if
     * the caller is already iterating through the type definitions or if the caller wants to combine multiple
     * different correspondences between source and target descriptors into a single {@link FieldRenames}. For example, in the
     * {@link com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator}, all pairs of record types
     * in the old and new record types must be compared, and the logic is already traversing the type
     * definitions to perform additional validations.
     * </p>
     *
     * @return a new builder for registering {@link FieldRenames}
     * @see com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create a {@link FieldRenames} that enumerates the fields that have been renamed between
     * a source and target descriptor. This will recursively traverse the fields defined in each
     * type to produce a complete set of renames. The returned {@link FieldRenames} should be suitable
     * for rewriting key expressions that were originally written for the {@code sourceDescriptor} on
     * the {@code targetDescriptor} using a {@link RenameFieldsVisitor}.
     *
     * <p>
     * Fields between the {@code sourceDescriptor} and {@code targetDescriptor} will be compared by
     * field number. If a field in {@code sourceDescriptor} does not have a corresponding field in
     * {@code targetDescriptor}, then this will throw a {@link MetaDataException}. Likewise, this
     * class has to traverse down any message-valued fields in order to account for any field renames
     * occurring on nested types, and this will similarly throw a {@link MetaDataException} if a field
     * that previously referred to a nested message now refers to a scalar type. Other kinds of
     * transformations (e.g., field type changes or fields going from {@code optional} to {@code required})
     * are not validated by this class as they are not material to field renaming, but they are validated
     * by the {@link com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator}.
     * </p>
     *
     * @param sourceDescriptor the source descriptor to traverse
     * @param targetDescriptor the target descriptor to traverse
     * @return a {@link FieldRenames} containing renames generated by traversing the various fields of the type
     * @see com.apple.foundationdb.record.metadata.MetaDataEvolutionValidator for a more comprehensive validation of changes
     */
    @Nonnull
    public static FieldRenames constructFor(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor) {
        final Builder builder = newBuilder();
        constructRenaming(builder, sourceDescriptor, targetDescriptor, new HashSet<>());
        return builder.build();
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static void constructRenaming(@Nonnull Builder builder, @Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull Set<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>> seen) {
        if (sourceDescriptor == targetDescriptor) {
            // Identical source and target descriptors. Do not bother recursing as all fields will be the same
            return;
        }
        final NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor> descriptors = NonnullPair.of(sourceDescriptor, targetDescriptor);
        if (!seen.add(descriptors)) {
            // Already seen. Do not add again
            return;
        }

        for (Descriptors.FieldDescriptor sourceField : sourceDescriptor.getFields()) {
            @Nullable
            final Descriptors.FieldDescriptor targetField = targetDescriptor.findFieldByNumber(sourceField.getNumber());
            if (targetField == null) {
                throw new MetaDataException("target descriptor missing field")
                        .addLogInfo(LogMessageKeys.MESSAGE, targetDescriptor.getFullName())
                        .addLogInfo(LogMessageKeys.FIELD_NAME, sourceField.getName())
                        .addLogInfo(LogMessageKeys.EXPECTED, sourceField.getNumber());
            }
            if (!sourceField.getName().equals(targetField.getName())) {
                builder.putRenamedField(sourceDescriptor, targetDescriptor, sourceField.getName(), targetField.getName());
            }

            // Recurse down nested fields so that we have any renames that are necessary for nested types.
            if (sourceField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                final Descriptors.Descriptor childSourceDescriptor = sourceField.getMessageType();
                if (targetField.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    throw new MetaDataException("target descriptor field is not a message type")
                            .addLogInfo(LogMessageKeys.MESSAGE, targetDescriptor.getFullName())
                            .addLogInfo(LogMessageKeys.FIELD_NAME, targetField.getName());
                }
                final Descriptors.Descriptor targetSourceDescriptor = targetField.getMessageType();
                constructRenaming(builder, childSourceDescriptor, targetSourceDescriptor, seen);
            }
        }
    }

    /**
     * Builder class for {@link FieldRenames}. This allows for renamed fields to be collected incrementally
     * while iterating over the fields from pairs of {@link Descriptors.Descriptor}s.
     */
    public static final class Builder {
        private final Map<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>, Map<String, String>> renamingMap;

        private Builder() {
            renamingMap = new HashMap<>();
        }

        /**
         * Record one field that has been renamed. The invariant that should be maintained here
         * is that:
         *
         * <pre>{@code
         * sourceDescriptor.findFieldByName(sourceName).getNumber() == targetDescriptor.findFieldByName(targetName).getNumber()
         *  && !sourceName.equals(targetName)
         * }</pre>
         *
         * <p>
         * That is, this should identify a case where the field in the source and target descriptors
         * have the same number but different names. It is the caller's responsibility to ensure this
         * invariant holds.
         * </p>
         *
         * @param sourceDescriptor the source descriptor for the renamed field
         * @param targetDescriptor the target descriptor for the renamed field
         * @param sourceName the name of the field in the source descriptor
         * @param targetName the name of the field in the target descriptor
         * @return this {@link Builder}
         */
        @CanIgnoreReturnValue
        @Nonnull
        public Builder putRenamedField(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull String sourceName, @Nonnull String targetName) {
            final Map<String, String> renamingForTypes = renamingMap.computeIfAbsent(NonnullPair.of(sourceDescriptor, targetDescriptor), ignore -> new HashMap<>());
            renamingForTypes.put(sourceName, targetName);
            return this;
        }

        /**
         * Construct a {@link FieldRenames} that encapsulates all the renames that were noted
         * by calls to {@link #putRenamedField(Descriptors.Descriptor, Descriptors.Descriptor, String, String)}.
         *
         * @return a {@link FieldRenames} with all the recorded field renames
         */
        @Nonnull
        public FieldRenames build() {
            if (renamingMap.isEmpty()) {
                return identity();
            }
            final ImmutableMap.Builder<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>, Map<String, String>> immutableMapBuilder = ImmutableMap.builderWithExpectedSize(renamingMap.size());
            renamingMap.forEach((descriptors, renamingForTypes) -> {
                if (renamingForTypes.isEmpty()) {
                    return;
                }
                immutableMapBuilder.put(descriptors, ImmutableMap.copyOf(renamingForTypes));
            });
            return new FieldRenames(immutableMapBuilder.build());
        }
    }
}
