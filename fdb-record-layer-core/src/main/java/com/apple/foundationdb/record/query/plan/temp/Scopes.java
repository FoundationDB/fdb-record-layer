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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.predicates.QuantifiedValue;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

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

    public Scopes push(@Nonnull final Map<CorrelationIdentifier, QuantifiedValue> boundIdentifiers) {
        this.currentScope = new Scope(this.currentScope, boundIdentifiers, GraphExpansion.builder());
        return this;
    }

    public Scope pop() {
        try {
            return currentScope;
        } finally {
            currentScope = Objects.requireNonNull(currentScope).getParentScope();
        }
    }

    @Nonnull
    public QuantifiedValue resolveIdentifier(@Nonnull final String identifier) {
        Scope scope = currentScope;
        final CorrelationIdentifier needle = CorrelationIdentifier.of(identifier);
        while (scope != null)  {
            final Map<CorrelationIdentifier, QuantifiedValue> boundIdentifiers = scope.getBoundQuantifiers();
            if (boundIdentifiers.containsKey(needle)) {
                return boundIdentifiers.get(needle);
            }
            scope = scope.getParentScope();
        }
        throw new SemanticException("unresolved identifier " + identifier);
    }

    public static class Scope {
        @Nullable
        private final Scope parentScope;
        @Nonnull
        private final Map<CorrelationIdentifier, QuantifiedValue> boundQuantifiers;
        @Nonnull
        private final GraphExpansion.Builder graphExpansionBuilder;

        public Scope(@Nullable Scope parentScope,
                     @Nonnull final Map<CorrelationIdentifier, QuantifiedValue> boundQuantifiers,
                     @Nonnull final GraphExpansion.Builder graphExpansionBuilder) {
            this.parentScope = parentScope;
            this.boundQuantifiers = ImmutableMap.copyOf(boundQuantifiers);
            this.graphExpansionBuilder = graphExpansionBuilder;
        }

        @Nullable
        public Scope getParentScope() {
            return parentScope;
        }

        public Map<CorrelationIdentifier, QuantifiedValue> getBoundQuantifiers() {
            return boundQuantifiers;
        }

        @Nonnull
        public GraphExpansion.Builder getGraphExpansionBuilder() {
            return graphExpansionBuilder;
        }
    }
}
