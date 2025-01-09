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

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A formatter for tokens.
 */
public class DefaultExplainFormatter implements ExplainFormatter {
    @Nonnull
    private final Supplier<ExplainSymbolMap> symbolMapSupplier;

    @Nonnull
    private final Deque<ExplainSymbolMap> scopes;

    public DefaultExplainFormatter(@Nonnull final Supplier<ExplainSymbolMap> symbolMapSupplier) {
        this.symbolMapSupplier = symbolMapSupplier;
        this.scopes = new ArrayDeque<>();
        this.scopes.push(this.symbolMapSupplier.get());
        registerAliasExplicitly(Quantifier.current(), "_");
    }

    @Override
    public void registerAlias(@Nonnull final CorrelationIdentifier alias) {
        Objects.requireNonNull(scopes.peek()).registerAlias(alias);
    }

    @Override
    public void registerAliasExplicitly(@Nonnull final CorrelationIdentifier alias, @Nonnull final String symbol) {
        Objects.requireNonNull(scopes.peek()).registerAliasWithExplicitSymbol(alias, symbol);
    }

    @Nonnull
    @Override
    public Optional<String> getSymbolForAliasMaybe(@Nonnull final CorrelationIdentifier alias) {
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

    @Nonnull
    @Override
    public CharSequence visitLineBreakOrSpace(@Nonnull final ExplainTokens.LineBreakOrSpaceToken lineBreakOrSpaceToken,
                                              @Nonnull final CharSequence stringedToken) {
        return " ";
    }

    @Nonnull
    @Override
    public CharSequence visitError(@Nonnull final ExplainTokens.Token token,
                                   @Nonnull final CharSequence stringedToken) {
        return "?" + stringedToken + "?";
    }

    @Nonnull
    public static DefaultExplainFormatter forDebugging() {
        return new DefaultExplainFormatter(DefaultExplainSymbolMap::new);
    }
}
