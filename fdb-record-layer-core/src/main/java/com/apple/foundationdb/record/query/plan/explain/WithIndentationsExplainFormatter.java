/*
 * PrettyExplainFormatter.java
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

import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.ToStringToken;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A formatter for tokens.
 */
public class WithIndentationsExplainFormatter extends DefaultExplainFormatter {
    private final int initialIndentation;
    private final int maxWidth;
    private final int tabSize;
    private int indentationLevel;
    private int width;

    public WithIndentationsExplainFormatter(@Nonnull final Supplier<ExplainSymbolMap> symbolMapSupplier,
                                            final int initialIndentation,
                                            final int maxWidth,
                                            final int tabSize) {
        super(symbolMapSupplier);
        this.initialIndentation = initialIndentation;
        this.maxWidth = maxWidth;
        this.tabSize = tabSize;
        this.indentationLevel = 0;
        this.width = initialIndentation;
    }

    @Nonnull
    @Override
    public CharSequence visitWhitespace(@Nonnull final ExplainTokens.WhitespaceToken whiteSpaceToken, @Nonnull final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitOptionalWhitespace(@Nonnull final ExplainTokens.OptionalWhitespaceToken optionalWhiteSpaceToken, @Nonnull final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitLineBreakOrSpace(@Nonnull final ExplainTokens.LineBreakOrSpaceToken lineBreakOrSpaceToken,
                                              @Nonnull final CharSequence stringedToken) {
        return wrapOrSpace(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitIdentifier(@Nonnull final ExplainTokens.IdentifierToken identifierToken,
                                        @Nonnull final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitKeyword(@Nonnull final ExplainTokens.KeywordToken keywordToken,
                                     @Nonnull final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitAliasDefinition(@Nonnull final ExplainTokens.AliasDefinitionToken aliasDefinitionToken,
                                  @Nonnull final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitAliasReference(@Nonnull final ExplainTokens.AliasReferenceToken aliasReferenceToken,
                                            @Nonnull final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitBracketLike(@Nonnull final ExplainTokens.BracketLikeToken bracketLikeToken,
                                         @Nonnull final CharSequence stringedToken) {
        if (bracketLikeToken.isOpen()) {
            indentationLevel ++;
        } else {
            indentationLevel --;
        }
        return increaseWidth(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitToString(@Nonnull final ToStringToken toStringToken,
                                      @Nonnull final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Nonnull
    @Override
    public CharSequence visitError(@Nonnull final ExplainTokens.Token token, @Nonnull final CharSequence stringedToken) {
        return increaseWidth(new StringBuilder().append("!").append(stringedToken).append("!"));
    }

    @Nonnull
    private CharSequence wrapOrSpace(@Nonnull final CharSequence stringedToken) {
        return shouldWrap(stringedToken) ? wrap(stringedToken) : increaseWidth(" " + stringedToken);
    }

    private boolean shouldWrap(@Nonnull final CharSequence stringedToken) {
        return (width + stringedToken.length() > maxWidth);
    }

    @CanIgnoreReturnValue
    @Nonnull
    private CharSequence wrap(@Nonnull final CharSequence stringedToken) {
        this.width = initialIndentation + stringedToken.length();
        return "\n" + " ".repeat(initialIndentation + indentationLevel * tabSize) + stringedToken;
    }

    @Nonnull
    private CharSequence increaseWidth(@Nonnull final CharSequence stringedToken) {
        this.width += stringedToken.length();
        return stringedToken;
    }

    @Nonnull
    public static WithIndentationsExplainFormatter forDot(final int initialIndentation) {
        return new WithIndentationsExplainFormatter(DefaultExplainSymbolMap::new, initialIndentation,
                80, 4);
    }
}
