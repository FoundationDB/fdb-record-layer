/*
 * PrimaryAccessHint.java
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents a primary key hint.
 */
public class PrimaryAccessHint extends AccessHint {
    @Nonnull
    private final KeyExpression primaryKey;

    public PrimaryAccessHint(@Nonnull final KeyExpression primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        return primaryKey.equals(((PrimaryAccessHint)other).getPrimaryKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryKey);
    }

    @Nonnull
    public KeyExpression getPrimaryKey() {
        return primaryKey;
    }
}
