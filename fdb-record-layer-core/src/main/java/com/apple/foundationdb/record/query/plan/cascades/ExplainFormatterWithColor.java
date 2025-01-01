/*
 * ExplainFormatter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.function.Supplier;

/**
 * A formatter for tokens.
 */
public class ExplainFormatterWithColor extends DefaultExplainFormatter {
    private static final ExplainFormatterWithColor FOR_DEBUGGING = new ExplainFormatterWithColor(DefaultExplainSymbolMap::new);

    public ExplainFormatterWithColor(@Nonnull final Supplier<ExplainSymbolMap> symbolMapSupplier) {
        super(symbolMapSupplier);
    }

    @Nonnull
    @Override
    public String enterIdentifier(@Nonnull final ExplainTokens.IdentifierToken identifierToken) {
        return Color.BRIGHT_YELLOW.toString();
    }

    @Nonnull
    @Override
    public String leaveIdentifier(@Nonnull final ExplainTokens.IdentifierToken identifierToken) {
        return Color.RESET.toString();
    }

    @Nonnull
    @Override
    public String enterBrackets(@Nonnull final ExplainTokens.BracketsToken bracketsToken) {
        return Color.BRIGHT_WHITE.toString();
    }

    @Nonnull
    @Override
    public String leaveBrackets(@Nonnull final ExplainTokens.BracketsToken bracketsToken) {
        return Color.RESET.toString();
    }

    @Nonnull
    public static ExplainFormatterWithColor forDebugging() {
        return FOR_DEBUGGING;
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
        PURPLE("\u001B[35m"),
        CYAN("\u001B[36m"),
        WHITE("\u001B[37m"),

        // Bright text colors
        BRIGHT_BLACK("\u001B[30;1m"),
        BRIGHT_RED("\u001B[31;1m"),
        BRIGHT_GREEN("\u001B[32;1m"),
        BRIGHT_YELLOW("\u001B[33;1m"),
        BRIGHT_BLUE("\u001B[34;1m"),
        BRIGHT_MAGENTA("\u001B[35;1m"),
        BRIGHT_CYAN("\u001B[36;1m"),
        BRIGHT_WHITE("\u001B[37;1m");

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
