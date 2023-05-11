/*
 * AndOrPredicate.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.google.common.base.Equivalence;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Common base class for predicates with many children, such as {@link AndPredicate} and {@link OrPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class AndOrPredicate extends AbstractQueryPredicate {
    @Nonnull
    private final List<QueryPredicate> children;

    private final Supplier<Set<Equivalence.Wrapper<QueryPredicate>>> childrenAsSetSupplier;

    protected AndOrPredicate(@Nonnull final List<QueryPredicate> children, final boolean isAtomic) {
        super(isAtomic);
        if (children.size() < 2) {
            throw new RecordCoreException(getClass().getSimpleName() + " must have at least two children");
        }

        this.children = children;
        this.childrenAsSetSupplier = Suppliers.memoize(() -> computeChildrenAsSet(children, AliasMap.identitiesFor(getCorrelatedTo())));
    }

    @Nonnull
    @Override
    public List<? extends QueryPredicate> getChildren() {
        return children;
    }

    @Nonnull
    private Set<Equivalence.Wrapper<QueryPredicate>> getChildrenAsSet() {
        return childrenAsSetSupplier.get();
    }

    @Override
    @SuppressWarnings({"squid:S1206", "EqualsWhichDoesntCheckParameterClass"})
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public boolean equalsForChildren(@Nonnull final QueryPredicate otherPred, @Nonnull final AliasMap aliasMap) {
        final var andOrPredicateOptional = otherPred.narrowMaybe(AndOrPredicate.class);
        if (andOrPredicateOptional.isEmpty()) {
            return false;
        }
        final var andOrPredicate = andOrPredicateOptional.get();

        if (aliasMap.definesOnlyIdentities() && getCorrelatedTo().containsAll(aliasMap.sources())) {
            return getChildrenAsSet().equals(andOrPredicate.getChildrenAsSet());
        }
        return super.equalsForChildren(otherPred, aliasMap);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeSemanticHashCode() {
        return Objects.hash(hashCodeWithoutChildren(), ImmutableSet.copyOf(getChildren()));
    }
    
    protected static List<? extends QueryPredicate> toList(@Nonnull QueryPredicate first, @Nonnull QueryPredicate second,
                                                           @Nonnull QueryPredicate... operands) {
        List<QueryPredicate> children = new ArrayList<>(operands.length + 2);
        children.add(first);
        children.add(second);
        Collections.addAll(children, operands);
        return children;
    }

    @Nonnull
    private static Set<Equivalence.Wrapper<QueryPredicate>> computeChildrenAsSet(@Nonnull final List<QueryPredicate> children, @Nonnull final AliasMap aliasMap)  {
        final var equivalence = new BoundEquivalence<QueryPredicate>(aliasMap);
        final var resultBuilder = ImmutableSet.<Equivalence.Wrapper<QueryPredicate>>builder();
        children.forEach(child -> {
            resultBuilder.add(equivalence.wrap(child));
        });
        return resultBuilder.build();
    }
}
