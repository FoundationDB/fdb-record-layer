/*
 * LiteralElement.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.view;

import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.predicates.ElementPredicate;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A value representing a single, static value of type {@code T}.
 * @param <T> the type of the static value
 */
public class LiteralElement<T> implements Element {
    @Nullable
    private final T value;

    public LiteralElement(@Nullable T value) {
        this.value = value;
    }

    @Nonnull
    @Override
    public Set<Source> getAncestralSources() {
        return Collections.emptySet();
    }

    @Nonnull
    @Override
    public Optional<ComparisonRange> matchWith(@Nonnull ComparisonRange existingComparisons, @Nonnull ElementPredicate predicate) {
        if (equals(predicate.getElement())) {
            return existingComparisons.tryToAdd(predicate.getComparison());
        }
        return Optional.empty();
    }

    @Nonnull
    @Override
    public Optional<ViewExpressionComparisons> matchSourcesWith(@Nonnull ViewExpressionComparisons viewExpressionComparisons, @Nonnull Element element) {
        return Optional.empty(); // no source to match
    }

    @Nonnull
    @Override
    public Element withSourceMappedInto(@Nonnull Source originalSource, @Nonnull Source duplicateSource) {
        return this;
    }

    @Nullable
    @Override
    public Object eval(@Nonnull SourceEntry sourceEntry) {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LiteralElement<?> that = (LiteralElement<?>)o;
        return Objects.equals(value, that.value);
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        return equals(other); // TODO this should be adapted
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(value);
    }

    @Override
    public int planHash(PlanHashKind hashKind) {
        switch (hashKind) {
            case CONTINUATION:
                return hashCode();
            default:
                throw new UnsupportedOperationException("Hash Kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of(); // TODO we may want to claim that literals are correlated to an expression containing all literals in the graph
    }

    @Nonnull
    @Override
    public Element rebase(@Nonnull final AliasMap translationMap) {
        // TODO we may want to claim that literals are correlated to an expression containing all literals in the graph
        // TODO in which case we may want to be able to rewrite this
        return this;

    }
}

