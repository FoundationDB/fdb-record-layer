/*
 * Scopes.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A stack of {@link Scope}s.
 */
public class Scopes {
    @Nullable
    private Scope currentScope;

    public Scopes() {
        this(null);
    }

    public Scopes(@Nullable Scope currentScope) {
        this.currentScope = currentScope;
    }

    @Nullable
    public Scope getCurrentScope() {
        return currentScope;
    }

    @Nonnull
    public Scope push() {
        this.currentScope = Scope.withParent(currentScope);
        return currentScope;
    }

    @Nonnull
    public Scope pop() {
        try {
            return Objects.requireNonNull(currentScope);
        } finally {
            currentScope = Objects.requireNonNull(currentScope).getParent();
        }
    }

    @Nonnull
    public Optional<Quantifier> resolveQuantifier(@Nonnull final String identifier) {
        return resolveQuantifier(CorrelationIdentifier.of(identifier));
    }

    @Nonnull
    public Optional<Quantifier> resolveQuantifier(@Nonnull final CorrelationIdentifier identifier) {
        Scope scope = currentScope;
        while (scope != null)  {
            final var maybeQuantifier = scope.getQuantifier(identifier);
            if (maybeQuantifier.isPresent()) {
                return maybeQuantifier;
            }
            scope = scope.getParent();
        }
        return Optional.empty();
    }

    /**
     * A frame of the scopes stack, binding names to quantifiers.
     */
    public static class Scope {

        /**
         * Set of flags to control the behavior of parser.
         */
        public enum Flag { GENERATE_AGGREGATION }

        @Nullable
        private final Scope parent;

        @Nonnull
        private final Map<CorrelationIdentifier, Quantifier> quantifiers;

        @Nonnull
        private final List<Column<? extends Value>> projectionList;

        @Nullable
        private QueryPredicate predicate;

        private int groupingColumnOffset;

        @Nonnull
        private Set<Flag> flags;

        private Scope(@Nullable final Scope parent, @Nonnull final Map<CorrelationIdentifier, Quantifier> quantifiers, @Nonnull final List<Column<? extends Value>> projectionList, @Nullable final QueryPredicate predicate) {
            this.parent = parent;
            this.quantifiers = quantifiers;
            this.projectionList = projectionList;
            this.predicate = predicate;
            this.groupingColumnOffset = -1;
            this.flags = new HashSet<>();
        }

        @Nonnull
        public SelectExpression convertToSelectExpression() {
            GraphExpansion.Builder builder = GraphExpansion.builder();
            builder.addAllQuantifiers(new ArrayList<>(quantifiers.values()))
                    .addAllResultColumns(projectionList);
            if (predicate != null) {
                builder.addPredicate(predicate);
            }
            return builder.build().buildSelect();
        }

        public void addQuantifier(@Nonnull final Quantifier quantifier) {
            if (hasQuantifier(quantifier.getAlias())) {
                // TODO we should use error codes for proper dispatch in caller.
                throw new SemanticException(String.format("quantifier with name '%s' already exists in scope", quantifier.getAlias()));
            }
            quantifiers.put(quantifier.getAlias(), quantifier);
        }

        @Nonnull
        public Optional<Quantifier> getQuantifier(@Nonnull final String alias) {
            return getQuantifier(CorrelationIdentifier.of(alias));
        }

        @Nonnull
        public Optional<Quantifier> getQuantifier(@Nonnull final CorrelationIdentifier alias) {
            return Optional.ofNullable(this.quantifiers.get(alias));
        }

        @Nonnull
        public List<Quantifier> getAllQuantifiers() {
            return quantifiers.values().stream().collect(Collectors.toUnmodifiableList());
        }

        public boolean hasQuantifier(@Nonnull final String alias) {
            return hasQuantifier(CorrelationIdentifier.of(alias));
        }

        public boolean hasQuantifier(@Nonnull final CorrelationIdentifier alias) {
            return quantifiers.containsKey(alias);
        }

        public void setPredicate(@Nonnull final QueryPredicate predicate) {
            this.predicate = predicate;
        }

        @Nonnull
        public QueryPredicate getPredicate() {
            if (!hasPredicate()) {
                throw new RecordCoreException("attempt to retrieve non-existing predicate");
            } else {
                Verify.verify(predicate != null);
                return predicate;
            }
        }

        public boolean hasPredicate() {
            return predicate != null;
        }

        public void addProjectionColumn(@Nonnull final Column<? extends Value> column) {
            projectionList.add(column);
        }

        @Nonnull
        public List<Column<? extends Value>> getProjectList() {
            return projectionList;
        }

        public void markGroupingColumnOffset(final int value) {
            this.groupingColumnOffset = value;
        }

        public int getGroupingColumnOffset() {
            return groupingColumnOffset;
        }

        public void setFlag(@Nonnull Flag flag) {
            this.flags.add(flag);
        }

        public boolean isFlagSet(@Nonnull final Flag flag) {
            return this.flags.contains(flag);
        }

        @Nullable
        public Scope getParent() {
            return parent;
        }

        public static Scope withParent(@Nullable final Scope parent) {
            return new Scope(parent, new HashMap<>(), new ArrayList<>(), null);
        }

    }
}
