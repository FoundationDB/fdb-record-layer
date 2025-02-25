/*
 * CodeVersionComparator.java
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
import java.util.Comparator;

public class CodeVersionComparator implements Comparator<CodeVersion> {
    private static final CodeVersionComparator INSTANCE = new CodeVersionComparator();

    private CodeVersionComparator() {
    }

    @Override
    public int compare(final CodeVersion v1, final CodeVersion v2) {
        if (v1 instanceof SemanticVersion) {
            if (v2 instanceof SemanticVersion) {
                return ((SemanticVersion)v1).compareSemanticVersion((SemanticVersion)v2);
            } else {
                return -1 * compare(v2, v1);
            }
        } else if (v1 instanceof SpecialCodeVersion) {
            if (v2 instanceof SemanticVersion) {
                return ((SpecialCodeVersion)v1).getType().isGreaterThanAllSemanticVersions() ? 1 : -1;
            } else if (v2 instanceof SpecialCodeVersion) {
                return ((SpecialCodeVersion)v1).compareToSpecialVersion((SpecialCodeVersion)v2);
            } else {
                throw new IllegalArgumentException("Unknown version class " + v2.getClass() + " for version " + v2);
            }
        } else {
            throw new IllegalArgumentException("Unknown version class " + v1.getClass() + " for version " + v1);
        }
    }

    @Nonnull
    public static CodeVersionComparator instance() {
        return INSTANCE;
    }
}
