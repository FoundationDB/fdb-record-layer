/*
 * AccessHints.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a set of AccessHint a query or a match candidate has.
 */
public class AccessHints {
    @Nonnull
    private final Set<AccessHint> accessHintSet = new HashSet<>();
    private final boolean noAccessHint;

    public AccessHints(AccessHint... accessHints) {
        this.accessHintSet.addAll(Arrays.asList(accessHints));
        this.noAccessHint = this.accessHintSet.isEmpty();
    }

    @Nonnull
    public Set<AccessHint> getAccessHintSet() {
        return accessHintSet;
    }

    public boolean containsAll(AccessHints other) {
        // if accessHint is not set, it's considered to include all possible hints
        if (noAccessHint) {
            return true;
        }
        // check that all hints in other exist in this
        for (AccessHint hint: other.accessHintSet) {
            if (!contains(hint)) {
                return false;
            }
        }
        return true;
    }

    private boolean contains(AccessHint hint) {
        for (AccessHint accessHint: accessHintSet) {
            if (accessHint.equals(hint)) {
                return true;
            }
        }
        return false;
    }
}
