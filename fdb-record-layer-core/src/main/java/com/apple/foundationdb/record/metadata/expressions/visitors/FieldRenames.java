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

import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.collect.ImmutableMap;
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

public final class FieldRenames {
    @Nonnull
    private static final FieldRenames IDENTITY = new FieldRenames(Collections.emptyMap());

    @Nonnull
    private final Map<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>, Map<String, String>> renamingMap;

    private FieldRenames(@Nonnull Map<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>, Map<String, String>> renamingMap) {
        this.renamingMap = renamingMap;
    }

    public boolean isIdentity() {
        return renamingMap.isEmpty();
    }

    @Nonnull
    public String getRenamedField(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull String fieldName) {
        final Map<String, String> renamingForTypes = getRenamingForTypes(sourceDescriptor, targetDescriptor);
        return renamingForTypes.getOrDefault(fieldName, fieldName);
    }

    @Nonnull
    public Map<String, String> getRenamingForTypes(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor) {
        return renamingMap.getOrDefault(NonnullPair.of(sourceDescriptor, targetDescriptor), Collections.emptyMap());
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    @Nonnull
    public static FieldRenames identity() {
        return IDENTITY;
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

    @Nonnull
    public static FieldRenames constructFor(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor) {
        final Builder builder = newBuilder();
        constructRenaming(builder, sourceDescriptor, targetDescriptor, new HashSet<>());
        return builder.build();
    }

    private static void constructRenaming(@Nonnull Builder builder, @Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull Set<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>> seen) {
        final NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor> descriptors = NonnullPair.of(sourceDescriptor, targetDescriptor);
        if (!seen.add(descriptors)) {
            // Already seen. Do not add again
            return;
        }

        for (Descriptors.FieldDescriptor sourceField : sourceDescriptor.getFields()) {
            @Nullable
            final Descriptors.FieldDescriptor targetField = targetDescriptor.findFieldByNumber(sourceField.getNumber());
            if (targetField == null) {
                throw new MetaDataException("Target descriptor missing field number")
                        .addLogInfo(LogMessageKeys.MESSAGE, targetDescriptor.getFullName())
                        .addLogInfo(LogMessageKeys.FIELD_NAME, sourceField.getName())
                        .addLogInfo(LogMessageKeys.EXPECTED, sourceField.getNumber());
            }
            if (!sourceField.getName().equals(targetField.getName())) {
                builder.putRenamedField(sourceDescriptor, targetDescriptor, sourceField.getName(), targetField.getName());
            }

            // Recurse down
            if (targetField.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                final Descriptors.Descriptor childSourceDescriptor = sourceField.getMessageType();
                final Descriptors.Descriptor targetSourceDescriptor = targetField.getMessageType();
                constructRenaming(builder, childSourceDescriptor, targetSourceDescriptor, seen);
            }
        }
    }

    public static final class Builder {
        private final Map<NonnullPair<Descriptors.Descriptor, Descriptors.Descriptor>, Map<String, String>> renamingMap;

        private Builder() {
            renamingMap = new HashMap<>();
        }

        @Nonnull
        public Builder putRenamedField(@Nonnull Descriptors.Descriptor sourceDescriptor, @Nonnull Descriptors.Descriptor targetDescriptor, @Nonnull String sourceName, @Nonnull String targetName) {
            final Map<String, String> renamingForTypes = renamingMap.computeIfAbsent(NonnullPair.of(sourceDescriptor, targetDescriptor), ignore -> new HashMap<>());
            renamingForTypes.put(sourceName, targetName);
            return this;
        }

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
