/*
 * DefaultExplainFormatter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.explain;

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A formatter for tokens.
 */
public class DefaultExplainFormatter implements ExplainFormatter {
    private final Supplier<ExplainSymbolMap> symbolMapSupplier;

    private final Deque<ExplainSymbolMap> scopes;

    protected DefaultExplainFormatter(final Supplier<ExplainSymbolMap> symbolMapSupplier) {
        this.symbolMapSupplier = symbolMapSupplier;
        this.scopes = new ArrayDeque<>();
    }

    protected void register() {
        pushScope();
        registerAliasExplicitly(Quantifier.current(), "_");
    }

    public static DefaultExplainFormatter create(final Supplier<ExplainSymbolMap> symbolMapSupplier) {
        final DefaultExplainFormatter formatter = new DefaultExplainFormatter(symbolMapSupplier);
        formatter.register();
        return formatter;
    }

    @Override
    public void registerAlias(final CorrelationIdentifier alias) {
        Objects.requireNonNull(scopes.peek()).registerAlias(alias);
    }

    @Override
    public void registerAliasExplicitly(final CorrelationIdentifier alias, final String symbol) {
        Objects.requireNonNull(scopes.peek()).registerAliasWithExplicitSymbol(alias, symbol);
    }

    @Override
    public Optional<String> getSymbolForAliasMaybe(final CorrelationIdentifier alias) {
        for (final var scope : scopes) {
            final var resolvedSymbol = scope.getSymbolForAlias(alias);
            if (resolvedSymbol != null) {
                return Optional.of(resolvedSymbol);
            }
        }
        return Optional.empty();
    }

    @Override
    public void pushScope() {
        scopes.push(symbolMapSupplier.get());
    }

    @Override
    public void popScope() {
        scopes.pop();
    }

    @Override
    public CharSequence visitLineBreakOrSpace(final ExplainTokens.LineBreakOrSpaceToken lineBreakOrSpaceToken,
                                              final CharSequence stringedToken) {
        return " ";
    }

    @Override
    public CharSequence visitError(final ExplainTokens.Token token,
                                   final CharSequence stringedToken) {
        return "?" + stringedToken + "?";
    }

    public static DefaultExplainFormatter forDebugging() {
        return DefaultExplainFormatter.create(DefaultExplainSymbolMap::new);
    }
}
