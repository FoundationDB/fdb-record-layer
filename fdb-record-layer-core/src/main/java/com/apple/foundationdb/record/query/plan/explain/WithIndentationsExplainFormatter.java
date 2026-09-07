/*
 * WithIndentationsExplainFormatter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

    protected WithIndentationsExplainFormatter(final Supplier<ExplainSymbolMap> symbolMapSupplier,
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

    @Override
    public CharSequence visitWhitespace(final ExplainTokens.WhitespaceToken whiteSpaceToken, final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Override
    public CharSequence visitOptionalWhitespace(final ExplainTokens.OptionalWhitespaceToken optionalWhiteSpaceToken, final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Override
    public CharSequence visitLineBreakOrSpace(final ExplainTokens.LineBreakOrSpaceToken lineBreakOrSpaceToken,
                                              final CharSequence stringedToken) {
        return wrapOrSpace(stringedToken);
    }

    @Override
    public CharSequence visitIdentifier(final ExplainTokens.IdentifierToken identifierToken,
                                        final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Override
    public CharSequence visitKeyword(final ExplainTokens.KeywordToken keywordToken,
                                     final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Override
    public CharSequence visitAliasDefinition(final ExplainTokens.AliasDefinitionToken aliasDefinitionToken,
                                             final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Override
    public CharSequence visitAliasReference(final ExplainTokens.AliasReferenceToken aliasReferenceToken,
                                            final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Override
    public CharSequence visitBracketLike(final ExplainTokens.BracketLikeToken bracketLikeToken,
                                         final CharSequence stringedToken) {
        if (bracketLikeToken.isOpen()) {
            indentationLevel ++;
        } else {
            indentationLevel --;
        }
        return increaseWidth(stringedToken);
    }

    @Override
    public CharSequence visitToString(final ToStringToken toStringToken,
                                      final CharSequence stringedToken) {
        return increaseWidth(stringedToken);
    }

    @Override
    public CharSequence visitError(final ExplainTokens.Token token, final CharSequence stringedToken) {
        return increaseWidth(new StringBuilder().append("?").append(stringedToken).append("?"));
    }

    private CharSequence wrapOrSpace(final CharSequence stringedToken) {
        return shouldWrap(stringedToken) ? wrap(stringedToken) : increaseWidth(" " + stringedToken);
    }

    private boolean shouldWrap(final CharSequence stringedToken) {
        return (width + stringedToken.length() > maxWidth);
    }

    @CanIgnoreReturnValue
    private CharSequence wrap(final CharSequence stringedToken) {
        this.width = initialIndentation + stringedToken.length();
        return "\n" + " ".repeat(initialIndentation + indentationLevel * tabSize) + stringedToken;
    }

    private CharSequence increaseWidth(final CharSequence stringedToken) {
        this.width += stringedToken.length();
        return stringedToken;
    }

    public static WithIndentationsExplainFormatter forDot(final int initialIndentation) {
        final WithIndentationsExplainFormatter formatter = new WithIndentationsExplainFormatter(DefaultExplainSymbolMap::new, initialIndentation,
                50, 4);
        formatter.register();
        return formatter;
    }
}
