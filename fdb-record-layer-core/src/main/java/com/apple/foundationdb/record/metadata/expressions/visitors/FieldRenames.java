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
     * there are no field renames that we need to keep track of.
     *
     * @return whether this {@link FieldRenames} contains only identity transformations
     */
    public boolean isIdentity() {
        return renamingMap.isEmpty();
    }

    /**
     * Get a set of fields that have changed names between two descriptors. If the {@link FieldRenames} has
     * been prepared with the source and target descriptor pair given, then this will return a map
     * containing every field name in {@code sourceDescriptor} where the equivalent field in
     * {@code targetDescriptor} has a different name.
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
     * Access an identity instance of {@link FieldRenames} that contains no actually renamed fields.
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
     * @return a new builder for registering {@link FieldRenames}
     */
    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create a {@link FieldRenames} that enumerates the fields that have been renamed between
     * a source and target descriptor. This will recursively traverse the fields defined in each
     * type to produce a complete set of renames. Once this is built, this should be suitable
     * for re-writing key expressions on {@code sourceDescriptor} onto {@code targetDescriptor}
     * using a {@link RenameFieldsVisitor}.
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
         * have the same number but different names.
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
