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

import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A stack of {@link Scope}s.
 */
public class Scopes {
    @Nullable
    private Scope.Builder currentScope;

    public Scopes() {
        this(null);
    }

    public Scopes(@Nullable Scope.Builder currentScope) {
        this.currentScope = currentScope;
    }

    @Nullable
    public Scope.Builder getCurrentScope() {
        return currentScope;
    }

    @Nonnull
    public Scope.Builder push() {
        this.currentScope = Scope.Builder.builder().withParentScope(currentScope);
        return currentScope;
    }

    @Nonnull
    public Scope pop() {
        try {
            return Objects.requireNonNull(currentScope).build();
        } finally {
            currentScope = Objects.requireNonNull(currentScope).getParent();
        }
    }

    @Nonnull
    public Quantifier resolveQuantifier(@Nonnull final String identifier) {
        return resolveQuantifier(CorrelationIdentifier.of(identifier));
    }

    @Nonnull
    public Quantifier resolveQuantifier(@Nonnull final CorrelationIdentifier identifier) {
        Scope.Builder scope = currentScope;
        while (scope != null)  {
            final var maybeQuantifier = scope.getQuantifier(identifier);
            if (maybeQuantifier.isPresent()) {
                return maybeQuantifier.get();
            }
            scope = scope.getParent();
        }
        throw new SemanticException("unresolved identifier " + identifier);
    }

    /**
     * A frame of the scopes stack, binding names to quantifiers.
     */
    public static class Scope {
        @Nonnull
        private final ImmutableMap<CorrelationIdentifier, Quantifier> quantifiers;

        @Nonnull
        private final ImmutableList<Column<? extends Value>> projectionList;

        @Nullable
        private final QueryPredicate predicate;

        @Nonnull
        private final ImmutableList<QuantifiedValue> quantifiedValues;

        Scope(@Nonnull final ImmutableMap<CorrelationIdentifier, Quantifier> quantifiers, @Nonnull final ImmutableList<Column<? extends Value>> projectionList, @Nonnull ImmutableList<QuantifiedValue> quantifiedValues, @Nullable final QueryPredicate predicate) {
            this.quantifiers = quantifiers;
            this.projectionList = projectionList;
            this.predicate = predicate;
            this.quantifiedValues = quantifiedValues;
        }

        @Nonnull
        public SelectExpression convertToSelectExpression() {
            GraphExpansion.Builder builder = GraphExpansion.builder();
            builder.addAllQuantifiers(quantifiers.values().asList())
                    .addAllResultColumns(projectionList)
                    .addAllResultValues(quantifiedValues);
            if (predicate != null) {
                builder.addPredicate(predicate);
            }
            return builder.build().seal().buildSelect();
        }

        @Nonnull
        public Optional<Quantifier> getQuantifier(@Nonnull final String alias) {
            return getQuantifier(CorrelationIdentifier.of(alias));
        }

        @Nonnull
        public Optional<Quantifier> getQuantifier(@Nonnull final CorrelationIdentifier alias) {
            return Optional.ofNullable(this.quantifiers.get(alias));
        }

        public boolean hasQuantifier(@Nonnull final String alias) {
            return hasQuantifier(CorrelationIdentifier.of(alias));
        }

        public boolean hasQuantifier(@Nonnull final CorrelationIdentifier alias) {
            return quantifiers.containsKey(alias);
        }

        /**
         * a builder of a {@link Scope}.
         */
        public static class Builder {

            @Nullable
            private Builder parent;

            @Nonnull
            private final ImmutableMap.Builder<CorrelationIdentifier, Quantifier> quantifiers;

            @Nullable
            private QueryPredicate predicate;

            @Nonnull
            private ImmutableList.Builder<QuantifiedValue> quantifiedValues;

            @Nonnull
            private ImmutableList.Builder<Column<? extends Value>> projectionList;

            private Builder() {
                this.quantifiers = ImmutableMap.builder();
                this.projectionList = ImmutableList.builder();
                this.quantifiedValues = ImmutableList.builder();
                this.predicate = null;
            }

            @Nonnull
            public Builder withParentScope(@Nullable final Builder parent) {
                this.parent = parent;
                return this;
            }

            @Nonnull
            public Builder addProjectionColumn(@Nonnull final Column<? extends Value> column) {
                projectionList.add(column);
                return this;
            }

            @Nonnull
            public Builder addPredicate(@Nonnull final QueryPredicate predicate) {
                this.predicate = predicate;
                return this;
            }

            @Nonnull
            public Builder addResultValue(@Nonnull final QuantifiedValue quantifiedValue) {
                this.quantifiedValues.add(quantifiedValue);
                return this;
            }

            @Nonnull
            public Optional<Column<? extends Value>> getProjectColumn(@Nonnull final String columnName) {
                return projectionList.build().stream().filter(c -> c.getField().getFieldName().equals(columnName)).findFirst();
            }

            @Nonnull
            public Builder renameColumn(@Nonnull final String oldName, @Nonnull final String newName) {
                if (oldName.equals(newName)) {
                    return this;
                }
                projectionList = ImmutableList.<Column<? extends Value>>builder().addAll(projectionList.build().stream().map(
                        col -> col.getField().getFieldName().equals(oldName)
                               ? Column.of(Type.Record.Field.of(col.getField().getFieldType(),Optional.of(newName)), col.getValue())
                               : col)
                        .collect(Collectors.toList()));
                return this;
            }

            @Nonnull
            Builder withPredicate(@Nonnull final QueryPredicate predicate) {
                this.predicate = predicate;
                return this;
            }

            @Nullable
            public Builder getParent() {
                return parent;
            }

            @Nonnull
            public Optional<Quantifier> getQuantifier(@Nonnull final String alias) {
                return getQuantifier(CorrelationIdentifier.of(alias));
            }

            @Nonnull
            public Optional<Quantifier> getQuantifier(@Nonnull final CorrelationIdentifier alias) {
                return Optional.ofNullable(this.quantifiers.build().get(alias));
            }

            @Nonnull
            public ImmutableList<Quantifier> getAllQuantifiers() {
                return quantifiers.build().values().asList();
            }

            public boolean hasQuantifier(@Nonnull final String alias) {
                return hasQuantifier(CorrelationIdentifier.of(alias));
            }

            public boolean hasQuantifier(@Nonnull final CorrelationIdentifier alias) {
                return quantifiers.build().containsKey(alias);
            }

            @Nonnull
            public Builder addQuantifier(@Nonnull final Quantifier quantifier) {
                if (hasQuantifier(quantifier.getAlias())) {
                    // TODO we should use error codes for proper dispatch in caller.
                    throw new SemanticException(String.format("quantifier with name '%s' already exists in scope", quantifier.getAlias()));
                }
                quantifiers.put(quantifier.getAlias(),quantifier);
                return this;
            }

            @Nonnull
            public Scope build() {
                return new Scope(quantifiers.build(), projectionList.build(), quantifiedValues.build(), predicate);
            }

            @Nonnull
            public static Builder builder() {
                return new Builder();
            }
        }
    }
}
