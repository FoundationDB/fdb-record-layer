/*
 * CodeVersion.java
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

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Marker interface representing a code version.
 */
public interface CodeVersion extends Comparable<CodeVersion> {
    @Override
    default int compareTo(@Nonnull CodeVersion other) {
        return CodeVersionComparator.instance().compare(this, other);
    }

    @Nonnull
    default List<CodeVersion> lesserVersions(@Nonnull Collection<CodeVersion> rawVersions) {
        return rawVersions.stream()
                .filter(other -> other.compareTo(this) < 0)
                .collect(Collectors.toList());
    }

    @Nonnull
    static CodeVersion parse(@Nonnull String string) {
        for (SpecialCodeVersion.SpecialCodeVersionType type : SpecialCodeVersion.SpecialCodeVersionType.values()) {
            if (type.getText().equals(string)) {
                return SpecialCodeVersion.of(type);
            }
        }
        // If it's none of the special versions, parse as a semantic version
        return SemanticVersion.parse(string);
    }
}
