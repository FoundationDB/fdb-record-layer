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

import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

    public Scopes push(@Nonnull final Set<CorrelationIdentifier> visibleAliases,
                       @Nonnull final Map<String, Value> boundIdentifiers) {
        this.currentScope = new Scope(this.currentScope, visibleAliases, boundIdentifiers, GraphExpansion.builder());
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
    public Value resolveIdentifier(@Nonnull final String identifier) {
        Scope scope = currentScope;
        while (scope != null)  {
            final Map<String, Value> boundIdentifiers = scope.getBoundIdentifiers();
            if (boundIdentifiers.containsKey(identifier)) {
                return boundIdentifiers.get(identifier);
            }
            scope = scope.getParentScope();
        }
        throw new SemanticException("unresolved identifier " + identifier);
    }

    public static class Scope {
        @Nullable
        private final Scope parentScope;
        @Nonnull
        private final Set<CorrelationIdentifier> visibleAliases;
        @Nonnull
        private final Map<String, Value> boundIdentifiers;
        @Nonnull
        private final GraphExpansion.Builder graphExpansionBuilder;

        public Scope(@Nullable Scope parentScope,
                     @Nonnull final Set<CorrelationIdentifier> visibleAliases,
                     @Nonnull final Map<String, Value> boundIdentifiers,
                     @Nonnull final GraphExpansion.Builder graphExpansionBuilder) {
            this.parentScope = parentScope;
            this.visibleAliases = ImmutableSet.copyOf(visibleAliases);
            this.boundIdentifiers = ImmutableMap.copyOf(boundIdentifiers);
            this.graphExpansionBuilder = graphExpansionBuilder;
        }

        @Nullable
        public Scope getParentScope() {
            return parentScope;
        }

        public Set<CorrelationIdentifier> getVisibleAliases() {
            return visibleAliases;
        }

        public Map<String, Value> getBoundIdentifiers() {
            return boundIdentifiers;
        }

        @Nonnull
        public GraphExpansion.Builder getGraphExpansionBuilder() {
            return graphExpansionBuilder;
        }
    }
}
