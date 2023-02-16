/*
 * Proposition.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Represents a three-valued logic operation.
 */
public enum Proposition {
    TRUE(Boolean.TRUE),
    FALSE(Boolean.FALSE),
    UNKNOWN(null) {
        @Override
        public Proposition and(@Nonnull final Proposition other) {
            return this;
        }

        @Override
        public Proposition or(@Nonnull final Proposition other) {
            return this;
        }
    };

    @Nullable
    private final Boolean value;

    Proposition(@Nullable final Boolean value) {
        this.value = value;
    }

    public Proposition and(@Nonnull final Proposition other) {
        if (UNKNOWN == other) {
            return UNKNOWN;
        }
        return of(Objects.requireNonNull(this.value) && Objects.requireNonNull(other.value));
    }

    public Proposition or(@Nonnull final Proposition other) {
        if (UNKNOWN == other) {
            return UNKNOWN;
        }
        return of(Objects.requireNonNull(this.value) || Objects.requireNonNull(other.value));
    }

    @Nonnull
    public static Proposition of(@Nullable final Boolean bool) {
        return null == bool ? UNKNOWN : (bool ? TRUE : FALSE);
    }

    /**
     * Collapses the ternary logic into binary logic.
     * <br> 
     * It has the following semantics.
     * <br>
     * <ul>
     *     <li>{@code TRUE} maps to {@code TRUE}</li>
     *     <li>{@code FALSE} maps to {@code FALSE}</li>
     *     <li>{@code UNKNOWN} maps to {@code FALSE}</li>
     * </ul>
     * @return a coalesced boolean value.
     */
    boolean coalesce() {
        return this == TRUE;
    }
}
