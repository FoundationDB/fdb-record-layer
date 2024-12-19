/*
 * CatalogKey.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.query.plan.cascades.AccessHint;
import com.apple.foundationdb.record.query.plan.cascades.IndexAccessHint;

import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a entry key in the {@link LogicalOperatorCatalog} that is used to lookup a {@link LogicalOperator}.
 * Note that {@link com.apple.foundationdb.record.query.plan.cascades.AccessHints} are part of the key, this is to avoid
 * accidentally reusing a hinted {@link LogicalOperator} somewhere else in the plan where it hinted differently (or not
 * hinted at all).
 */
public final class CatalogKey {
    @Nonnull
    private final Identifier identifier;

    @Nonnull
    private final Set<AccessHint> hints;

    private CatalogKey(@Nonnull Identifier identifier,
                       @Nonnull Set<AccessHint> hints) {
        this.identifier = identifier;
        this.hints = hints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CatalogKey that = (CatalogKey) o;
        return Objects.equals(getIdentifier(), that.getIdentifier()) && Objects.equals(getHints(), that.getHints());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getIdentifier(), getHints());
    }

    @Nonnull
    public static CatalogKey of(@Nonnull Identifier identifier,
                                @Nonnull Set<String> requestedIndexes) {
        return new CatalogKey(identifier, requestedIndexes.stream().map(IndexAccessHint::new).collect(ImmutableSet.toImmutableSet()));
    }

    @Nonnull
    public static CatalogKey of(@Nonnull Identifier identifier) {
        return new CatalogKey(identifier, ImmutableSet.of());
    }

    @Nonnull
    public Identifier getIdentifier() {
        return identifier;
    }

    @Nonnull
    public Set<AccessHint> getHints() {
        return hints;
    }
}
