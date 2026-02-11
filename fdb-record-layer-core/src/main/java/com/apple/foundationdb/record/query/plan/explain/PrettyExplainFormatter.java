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

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A formatter for tokens.
 */
public class PrettyExplainFormatter extends DefaultExplainFormatter {
    private static final int TAB_SIZE = 4;

    private final boolean useOptionalLineBreaks;
    private int indentationLevel;

    private PrettyExplainFormatter(@Nonnull final Supplier<ExplainSymbolMap> symbolMapSupplier,
                                  final boolean useOptionalLineBreaks) {
        super(symbolMapSupplier);
        this.useOptionalLineBreaks = useOptionalLineBreaks;
        this.indentationLevel = 0;
    }

    @Nonnull
    @Override
    public CharSequence visitLineBreakOrSpace(@Nonnull final ExplainTokens.LineBreakOrSpaceToken lineBreakOrSpaceToken,
                                              @Nonnull final CharSequence stringedToken) {
        if (useOptionalLineBreaks) {
            return "\n" + " ".repeat(indentationLevel * TAB_SIZE);
        }
        return " ";
    }

    @Nonnull
    @Override
    public CharSequence visitIdentifier(@Nonnull final ExplainTokens.IdentifierToken identifierToken,
                                  @Nonnull final CharSequence stringedToken) {
        return new StringBuilder().append(Color.BRIGHT_YELLOW).append(stringedToken).append(Color.RESET);
    }

    @Nonnull
    @Override
    public CharSequence visitKeyword(@Nonnull final ExplainTokens.KeywordToken keywordToken,
                                     @Nonnull final CharSequence stringedToken) {
        return new StringBuilder().append(Color.BRIGHT_WHITE).append(stringedToken).append(Color.RESET);
    }

    @Nonnull
    @Override
    public CharSequence visitAliasDefinition(@Nonnull final ExplainTokens.AliasDefinitionToken aliasDefinitionToken,
                                  @Nonnull final CharSequence stringedToken) {
        return new StringBuilder().append(Color.BRIGHT_GREEN).append(stringedToken).append(Color.RESET);
    }

    @Nonnull
    @Override
    public CharSequence visitAliasReference(@Nonnull final ExplainTokens.AliasReferenceToken aliasReferenceToken,
                                            @Nonnull final CharSequence stringedToken) {
        return new StringBuilder().append(Color.GREEN).append(stringedToken).append(Color.RESET);
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
        return new StringBuilder().append(Color.BRIGHT_WHITE).append(stringedToken).append(Color.RESET);
    }

    @Nonnull
    @Override
    public CharSequence visitToString(@Nonnull final ToStringToken toStringToken,
                                      @Nonnull final CharSequence stringedToken) {
        return new StringBuilder().append(Color.WHITE).append(stringedToken).append(Color.RESET);
    }

    @Nonnull
    @Override
    public CharSequence visitError(@Nonnull final ExplainTokens.Token token, @Nonnull final CharSequence stringedToken) {
        return new StringBuilder().append(Color.BRIGHT_RED).append(stringedToken).append(Color.RESET);
    }

    @Nonnull
    public static PrettyExplainFormatter forExplainPlan() {
        final PrettyExplainFormatter formatter = new PrettyExplainFormatter(ExplainSelfContainedSymbolMap::new, true);
        formatter.register();
        return formatter;
    }

    @Nonnull
    public static PrettyExplainFormatter forDebugging() {
        final PrettyExplainFormatter formatter = new PrettyExplainFormatter(DefaultExplainSymbolMap::new, false);
        formatter.register();
        return formatter;
    }

    /**
     * Utility class that encapsulates <a href="https://en.wikipedia.org/wiki/ANSI_escape_code">ANSI escape sequences</a>
     * for colors.
     */
    public enum Color {
        RESET("\u001B[0m"),
        BLACK("\u001B[30m"),
        RED("\u001B[31m"),
        GREEN("\u001B[32m"),
        YELLOW("\u001B[33m"),
        BLUE("\u001B[34m"),
        MAGENTA("\u001B[35m"),
        CYAN("\u001B[36m"),
        WHITE("\u001B[37m"),

        // Bright text colors
        BRIGHT_BLACK("\u001B[90;1m"),
        BRIGHT_RED("\u001B[91;1m"),
        BRIGHT_GREEN("\u001B[92;1m"),
        BRIGHT_YELLOW("\u001B[93;1m"),
        BRIGHT_BLUE("\u001B[94;1m"),
        BRIGHT_MAGENTA("\u001B[95;1m"),
        BRIGHT_CYAN("\u001B[96;1m"),
        BRIGHT_WHITE("\u001B[97;1m");

        @Nonnull
        private final String ansi;

        Color(@Nonnull final String ansi) {
            this.ansi = ansi;
        }

        @Override
        public String toString() {
            return ansi;
        }
    }
}
