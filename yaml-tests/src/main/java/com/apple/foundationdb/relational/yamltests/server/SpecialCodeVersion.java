/*
 * SpecialCodeVersion.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.server;

import com.apple.foundationdb.relational.yamltests.block.FileOptions;

import javax.annotation.Nonnull;
import java.util.EnumMap;
import java.util.Objects;

public class SpecialCodeVersion implements CodeVersion {
    public enum SpecialCodeVersionType {
        MIN("!min_version", false),
        CURRENT(FileOptions.CurrentVersion.TEXT, true),
        MAX("!max_version", true),
        ;

        private final String text;
        private final boolean greaterThanAllSemanticVersions;

        SpecialCodeVersionType(String text, boolean greaterThanAllSemanticVersions) {
            this.text = text;
            this.greaterThanAllSemanticVersions = greaterThanAllSemanticVersions;
        }

        public String getText() {
            return text;
        }

        public boolean isGreaterThanAllSemanticVersions() {
            return greaterThanAllSemanticVersions;
        }
    }

    private static final EnumMap<SpecialCodeVersionType, SpecialCodeVersion> INSTANCES = new EnumMap<>(SpecialCodeVersionType.class);

    static {
        for (SpecialCodeVersionType type : SpecialCodeVersionType.values()) {
            INSTANCES.put(type, new SpecialCodeVersion(type));
        }
    }

    private final SpecialCodeVersionType type;

    private SpecialCodeVersion(SpecialCodeVersionType type) {
        this.type = type;
    }

    @Nonnull
    public SpecialCodeVersionType getType() {
        return type;
    }

    @Override
    public String toString() {
        return type.getText();
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        final SpecialCodeVersion that = (SpecialCodeVersion)object;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type);
    }

    public int compareToSpecialVersion(SpecialCodeVersion other) {
        return type.compareTo(other.type);
    }

    @Nonnull
    public static SpecialCodeVersion of(SpecialCodeVersionType type) {
        return INSTANCES.get(type);
    }

    @Nonnull
    public static SpecialCodeVersion current() {
        return of(SpecialCodeVersionType.CURRENT);
    }

    @Nonnull
    public static SpecialCodeVersion min() {
        return of(SpecialCodeVersionType.MIN);
    }

    @Nonnull
    public static SpecialCodeVersion max() {
        return of(SpecialCodeVersionType.MAX);
    }
}
