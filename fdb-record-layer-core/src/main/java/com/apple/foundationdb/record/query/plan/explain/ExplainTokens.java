/*
 * ExplainTokens.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.IntBinaryOperator;
import java.util.function.Supplier;

/**
 * Class to hold information about a list of tokens. Tokens are created during the explain walk and rendered afterward.
 * Tokens can be simple, or they can be nested (via {@link NestedToken}). If a token is nested it contains an instance
 * of this class making this general structure more of a tree than a list.
 * <br>
 * Most tokens render into some {@link CharSequence} using an {@link ExplainFormatter}. Some tokens also interact with
 * the formatter in way that changes the internal state of the formatter. Those tokens when rendered exert a side effect
 * on the formatter (e.g. declaring an alias, etc.)
 * <br>
 * Tokens may render differently for different {@link ExplainLevel}s. For instance, a token that renders some
 * information in {@link ExplainLevel#ALL_DETAILS} may only render the string {@code ...} in
 * {@link ExplainLevel#STRUCTURE}.
 * <br>
 * It is important to keep track of the number of characters the renderer would produce if rendered for each individual
 * explain level. Unfortunately, this is not an exact science! While most tokens render to exactly the same character
 * sequence on each explain level (for e.g. a keyword), for some tokens (e.g. aliases) we just do not know what they
 * will render into as not all information is available when the token is created during the explain phase. Staying
 * with the example, we do not know the length of an alias rendering until we know the symbol table to resolve the
 * alias.
 * <br>
 * While we do not know the exact number of characters a token will render into <i>a priori</i>, we can estimate the
 * bounds pretty well (even without knowing all the information to render the token yet (e.g. the symbol table)). To
 * that end, rather than keeping just a length, we keep a {@code minLength} and a {@code maxLength} (both inclusive) for
 * each explain level.
 * <br>
 * This class is mutable and not thread-safe.
 */
public class ExplainTokens {
    private final List<Token> tokens;
    private int[] tokenSizes;
    private int[] minLengths;
    private int[] maxLengths;

    public ExplainTokens() {
        this.tokens = Lists.newArrayList();
        this.tokenSizes = new int[0];
        this.minLengths = new int[0];
        this.maxLengths = new int[0];
    }

    public boolean isEmpty() {
        return tokens.isEmpty();
    }

    public List<Token> getTokens() {
        return tokens;
    }

    protected int[] getTokenSizes() {
        return tokenSizes;
    }

    public int getTokenSize(final int explainLevel) {
        return explainLevel < tokenSizes.length ? tokenSizes[explainLevel] : 0;
    }

    protected int[] getMinLengths() {
        return minLengths;
    }

    public int getMinLength(final int explainLevel) {
        return explainLevel < minLengths.length ? minLengths[explainLevel] : 0;
    }

    protected int[] getMaxLengths() {
        return maxLengths;
    }

    public int getMaxLength(final int explainLevel) {
        return explainLevel < maxLengths.length ? maxLengths[explainLevel] : 0;
    }

    public CharSequence render(final ExplainFormatter formatter) {
        return render(ExplainLevel.ALL_DETAILS, formatter, Integer.MAX_VALUE);
    }

    /**
     * Render all tokens on the given explain level, using the given formatter, and a given budget of number of
     * characters. If the budget is limited (if {@code remainingCharacterBudget < Integer.MAX_VALUE}), this method will
     * at worst render into a string of that budget (plus three characters {@code ...} if the rendered char sequence has
     * to be cut off).
     * @param renderingExplainLevel the desired {@link ExplainLevel}
     * @param formatter the formatter
     * @param remainingCharacterBudget the budget of the number of remaining characters
     * @return a {@link CharSequence} representing the rendered explain string which is potentially cut off if
     *         depending on the budget. The returned character sequence is at most of length
     *         {@code remainingCharacterBudget + 3} to accommodate for the {@code ...}. Note that using
     *         {@link CharSequence} instead of {@link String} is preferable as {@link StringBuilder} is a
     *         {@link CharSequence} allowing us to return un-built string builders without materializing them to
     *         strings.
     */
    public CharSequence render(final int renderingExplainLevel,
                               final ExplainFormatter formatter,
                               int remainingCharacterBudget) {
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            final var token = tokens.get(i);
            final var stringedToken =
                    token.render(renderingExplainLevel, formatter, remainingCharacterBudget);
            stringBuilder.append(stringedToken);
            remainingCharacterBudget -= (remainingCharacterBudget == Integer.MAX_VALUE ? 0 : stringedToken.length());
            if (remainingCharacterBudget < 1) {
                if (remainingCharacterBudget == 0 && i + 1 < tokens.size()) {
                    // if any of the remaining tokens render to more than an empty string we have to append ... instead
                    for (int j = i + 1; j < tokens.size(); j++) {
                        final var remainingToken = tokens.get(j);
                        final var remainingStringedToken =
                                remainingToken.render(renderingExplainLevel, formatter, 0);
                        if (!remainingStringedToken.toString().isEmpty()) {
                            return stringBuilder.append(remainingStringedToken);
                        }
                    }
                }
                return stringBuilder;
            }
        }
        return stringBuilder;
    }

    public ExplainTokens add(final Token token) {
        tokens.add(token);
        this.tokenSizes = vectorAdd(this.tokenSizes, token.getTokenSizes());
        this.minLengths = vectorAdd(this.minLengths, token.getMinLengths());
        this.maxLengths = vectorAdd(this.maxLengths, token.getMaxLengths());
        return this;
    }

    public ExplainTokens addNested(final ExplainTokens additionalExplainTokens) {
        return addNested(Token.DEFAULT_EXPLAIN_LEVEL, additionalExplainTokens);
    }

    public ExplainTokens addNested(final int explainLevel,
                                   final ExplainTokens additionalExplainTokens) {
        return add(new NestedToken(explainLevel, additionalExplainTokens,
                new OptionalWhitespaceToken(Token.DEFAULT_EXPLAIN_LEVEL)));
    }

    public ExplainTokens addNested(final int explainLevel,
                                   final ExplainTokens additionalExplainTokens,
                                   final String replacementTokenString) {
        return addNested(explainLevel, additionalExplainTokens,
                new ToStringToken(Token.DEFAULT_EXPLAIN_LEVEL, replacementTokenString));
    }

    public ExplainTokens addNested(final int explainLevel,
                                   final ExplainTokens additionalExplainTokens,
                                   final Token replacementToken) {
        return add(new NestedToken(explainLevel, additionalExplainTokens, replacementToken));
    }

    public ExplainTokens addAll(final Collection<? extends Token> additionalTokens) {
        additionalTokens.forEach(this::add);
        return this;
    }

    public ExplainTokens addPush() {
        return addPush(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addPush(final int explainLevel) {
        return add(new PushToken(explainLevel));
    }

    public ExplainTokens addPop() {
        return addPop(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addPop(final int explainLevel) {
        return add(new PopToken(explainLevel));
    }

    public ExplainTokens addWhitespace() {
        return addWhitespace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addWhitespace(final int explainLevel) {
        return add(new WhitespaceToken(explainLevel));
    }

    public ExplainTokens addOptionalWhitespace() {
        return addOptionalWhitespace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addOptionalWhitespace(final int explainLevel) {
        return add(new OptionalWhitespaceToken(explainLevel));
    }

    public ExplainTokens addLinebreakOrWhitespace() {
        return addLinebreakOrWhitespace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addLinebreakOrWhitespace(final int explainLevel) {
        return add(new LineBreakOrSpaceToken(explainLevel));
    }

    public ExplainTokens addIdentifier(final String identifier) {
        return addIdentifier(Token.DEFAULT_EXPLAIN_LEVEL, identifier);
    }

    public ExplainTokens addIdentifier(final int explainLevel, final String identifier) {
        return add(new IdentifierToken(explainLevel, identifier));
    }

    public ExplainTokens addKeyword(final String keyword) {
        return addKeyword(Token.DEFAULT_EXPLAIN_LEVEL, keyword);
    }

    public ExplainTokens addKeyword(final int explainLevel, final String keyword) {
        return add(new KeywordToken(explainLevel, keyword));
    }

    public ExplainTokens addComma() {
        return addComma(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addComma(final int explainLevel) {
        return add(new CommaLikeToken(explainLevel, ","));
    }

    public ExplainTokens addCommaAndWhiteSpace() {
        return addCommaAndWhiteSpace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addCommaAndWhiteSpace(final int explainLevel) {
        return addComma(explainLevel).addWhitespace(explainLevel);
    }

    public ExplainTokens addAliasDefinition(final CorrelationIdentifier alias) {
        return addAliasDefinition(Token.DEFAULT_EXPLAIN_LEVEL, alias);
    }

    public ExplainTokens addAliasDefinition(final int explainLevel,
                                            final CorrelationIdentifier alias) {
        return add(new AliasDefinitionToken(explainLevel, alias));
    }

    public ExplainTokens addCurrentAliasDefinition(final CorrelationIdentifier alias) {
        return addCurrentAliasDefinition(Token.DEFAULT_EXPLAIN_LEVEL, alias);
    }

    public ExplainTokens addCurrentAliasDefinition(final int explainLevel,
                                                   final CorrelationIdentifier alias) {
        return add(new CurrentAliasDefinitionToken(explainLevel, alias));
    }

    public ExplainTokens addAliasReference(final CorrelationIdentifier alias) {
        return addAliasReference(Token.DEFAULT_EXPLAIN_LEVEL, alias);
    }

    public ExplainTokens addAliasReference(final int explainLevel,
                                           final CorrelationIdentifier alias) {
        return add(new AliasReferenceToken(explainLevel, alias));
    }

    public ExplainTokens addOpeningParen() {
        return addOpeningParen(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addOpeningParen(final int explainLevel) {
        return add(new BracketLikeToken(explainLevel, true, "("));
    }

    public ExplainTokens addClosingParen() {
        return addClosingParen(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addClosingParen(final int explainLevel) {
        return add(new BracketLikeToken(explainLevel, false, ")"));
    }

    public ExplainTokens addOpeningSquareBracket() {
        return addOpeningSquareBracket(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addOpeningSquareBracket(final int explainLevel) {
        return add(new BracketLikeToken(explainLevel, true, "["));
    }

    public ExplainTokens addClosingSquareBracket() {
        return addClosingSquareBracket(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addClosingSquareBracket(final int explainLevel) {
        return add(new BracketLikeToken(explainLevel, false, "]"));
    }

    public ExplainTokens addOpeningBrace() {
        return addOpeningBrace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addOpeningBrace(final int explainLevel) {
        return add(new BracketLikeToken(explainLevel, true, "{"));
    }

    public ExplainTokens addClosingBrace() {
        return addClosingBrace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addClosingBrace(final int explainLevel) {
        return add(new BracketLikeToken(explainLevel, false, "}"));
    }

    public ExplainTokens addOpeningAngledBracket() {
        return addOpeningAngledBracket(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addOpeningAngledBracket(final int explainLevel) {
        return add(new BracketLikeToken(explainLevel, true, "<"));
    }

    public ExplainTokens addClosingAngledBracket() {
        return addClosingAngledBracket(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    public ExplainTokens addClosingAngledBracket(final int explainLevel) {
        return add(new BracketLikeToken(explainLevel, false, ">"));
    }

    public ExplainTokens addToString(@Nullable final Object object) {
        return addToString(Token.DEFAULT_EXPLAIN_LEVEL, object);
    }

    public ExplainTokens addToString(final int explainLevel, @Nullable final Object object) {
        return add(new ToStringToken(explainLevel, object));
    }

    public ExplainTokens addToStrings(final Iterable<?> objects) {
        return addToStrings(Token.DEFAULT_EXPLAIN_LEVEL, objects);
    }

    public ExplainTokens addToStrings(final int explainLevel, final Iterable<?> objects) {
        for (final var iterator = objects.iterator(); iterator.hasNext(); ) {
            final var object = iterator.next();
            addToString(explainLevel, object);
            if (iterator.hasNext()) {
                addCommaAndWhiteSpace(explainLevel);
            }
        }

        return this;
    }

    public ExplainTokens addFunctionCall(final String identifier) {
        return addFunctionCall(Token.DEFAULT_EXPLAIN_LEVEL, identifier);
    }

    public ExplainTokens addFunctionCall(final int explainLevel, final String identifier) {
        return addFunctionCall(explainLevel, identifier, null);
    }

    public ExplainTokens addFunctionCall(final String identifier,
                                         @Nullable final ExplainTokens childExplainTokens) {
        return addFunctionCall(Token.DEFAULT_EXPLAIN_LEVEL, identifier, childExplainTokens);
    }

    public ExplainTokens addFunctionCall(final int explainLevel,
                                         final String identifier,
                                         @Nullable final ExplainTokens childExplainTokens) {
        if (childExplainTokens == null || childExplainTokens.isEmpty()) {
            return addIdentifier(explainLevel, identifier).addOptionalWhitespace(explainLevel)
                    .addOpeningParen(explainLevel).addClosingParen(explainLevel);
        }
        return addIdentifier(explainLevel, identifier).addOptionalWhitespace(explainLevel).addOpeningParen(explainLevel)
                .addOptionalWhitespace(explainLevel).addNested(childExplainTokens).addOptionalWhitespace(explainLevel)
                .addClosingParen(explainLevel);
    }

    public ExplainTokens addSequence(final Supplier<ExplainTokens> delimiterSupplier,
                                     final ExplainTokens... childrenExplainTokens) {
        return addSequence(delimiterSupplier, Arrays.asList(childrenExplainTokens));
    }

    public ExplainTokens addSequence(final Supplier<ExplainTokens> delimiterSupplier,
                                     final Iterable<ExplainTokens> childrenExplainTokens) {
        for (final var iterator = childrenExplainTokens.iterator(); iterator.hasNext(); ) {
            final var childExplainTokens = iterator.next();
            addNested(childExplainTokens);
            if (iterator.hasNext()) {
                addAll(delimiterSupplier.get().getTokens());
            }
        }
        return this;
    }

    private static int[] vectorAdd(final int[] left, final int[] right) {
        return vectorOp(left, right, Integer::sum);
    }

    private static int[] vectorCoalesce(final int[] left, final int[] right) {
        return vectorOp(left, right, (a, b) -> {
            if (a == 0) {
                return b;
            }
            if (b == 0) {
                return a;
            }
            throw new RecordCoreException("incorrect use of coalesce");
        });
    }

    private static int[] vectorOp(final int[] left, final int[] right, IntBinaryOperator intBinaryOperator) {
        final var maxSize = Math.max(left.length, right.length);
        final var result = new int[maxSize];

        for (int i = 0; i < maxSize; i ++) {
            result[i] = intBinaryOperator.applyAsInt(i < left.length ? left[i] : 0, i < right.length ? right[i] : 0);
        }
        return result;
    }

    /**
     * A token enumeration that is used intermittently when something is explained.
     */
    public enum TokenKind {
        PUSH,
        POP,
        NESTED,
        WHITESPACE,
        OPTIONAL_WHITESPACE,
        LINE_BREAK_OR_SPACE,
        IDENTIFIER,
        KEYWORD,
        COMMA_LIKE,
        ALIAS_DEFINITION,
        ALIAS_REFERENCE,
        BRACKETS_OPEN,
        BRACKETS_CLOSE,
        TO_STRING
    }

    /**
     * Generic token structure.
     */
    public abstract static class Token {
        /**
         * Highest Explain level that all tokens get created at that do not override their explain level.
         */
        public static final int DEFAULT_EXPLAIN_LEVEL = ExplainLevel.STRUCTURE;
        private static final int MAX_ALIAS_LENGTH = 36; // MAX UUID LENGTH

        private final TokenKind tokenKind;
        private final int[] tokenSizes;
        private final int[] minLengths;
        private final int[] maxLengths;

        public Token(final TokenKind tokenKind,
                     final int explainLevel,
                     final int minLength,
                     final int maxLength) {
            this(tokenKind, explainLevel, 1, minLength, maxLength);
        }

        public Token(final TokenKind tokenKind,
                     final int explainLevel,
                     final int tokenSize,
                     final int minLength,
                     final int maxLength) {
            //
            // Create a vector for this new token from the most elaborate level 0 to the indicated explainLevel.
            // If a token created at level n with length l, we mark it using l for all more elaborate levels < n as
            // well.
            //
            this(tokenKind, vectorForExplainLevel(explainLevel + 1, tokenSize),
                    vectorForExplainLevel(explainLevel + 1, minLength),
                    vectorForExplainLevel(explainLevel + 1, maxLength));
        }

        protected Token(final TokenKind tokenKind,
                        final int[] tokenSizes,
                        final int[] minLengths,
                        final int[] maxLengths) {
            this.tokenKind = tokenKind;
            this.tokenSizes = tokenSizes;
            this.minLengths = minLengths;
            this.maxLengths = maxLengths;
        }

        public TokenKind getTokenKind() {
            return tokenKind;
        }

        @SuppressWarnings("PMD.UnusedPrivateMethod") // PMD having hallucinations
        private int[] getTokenSizes() {
            return tokenSizes;
        }

        @SuppressWarnings("PMD.UnusedPrivateMethod") // PMD having hallucinations
        private int[] getMinLengths() {
            return minLengths;
        }

        @SuppressWarnings("PMD.UnusedPrivateMethod") // PMD having hallucinations
        private int[] getMaxLengths() {
            return maxLengths;
        }

        public int getExplainLevel() {
            Verify.verify(tokenSizes.length == minLengths.length);
            return tokenSizes.length - 1;
        }

        protected boolean isRenderingEnabled(final int renderingExplainLevel) {
            return getExplainLevel() >= renderingExplainLevel;
        }

        @SuppressWarnings("unchecked")
        protected <T extends Token> CharSequence renderIfEnabled(final int renderingExplainLevel,
                                                                 final String stringedToken,
                                                                 final BiFunction<T, CharSequence, CharSequence> renderFunction) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                return renderFunction.apply((T)this, stringedToken);
            }
            return "";
        }

        protected CharSequence cutOffIfNeeded(final int remainingCharacterBudget,
                                              final CharSequence stringedToken) {
            Verify.verify(remainingCharacterBudget >= 0);
            if (remainingCharacterBudget >= stringedToken.length()) {
                return stringedToken;
            }
            return new StringBuilder(stringedToken.subSequence(0, remainingCharacterBudget)).append("...");
        }


        public int getTokenSize(final int explainLevel) {
            return explainLevel < tokenSizes.length ? tokenSizes[explainLevel] : 0;
        }

        public int getMinLength(final int explainLevel) {
            return explainLevel < minLengths.length ? minLengths[explainLevel] : 0;
        }

        public int getMaxLength(final int explainLevel) {
            return explainLevel < maxLengths.length ? maxLengths[explainLevel] : 0;
        }

        public abstract CharSequence render(int renderingExplainLevel,
                                            ExplainFormatter explainFormatter,
                                            int remainingCharacterBudget);

        private static int[] vectorForExplainLevel(final int explainLevelExclusive, int value) {
            final int[] result = new int[explainLevelExclusive];
            Arrays.fill(result, value);
            return result;
        }
    }

    /**
     * A push token. This kind of token is used to interact with the symbol table of the formatter and does not render
     * into anything.
     */
    public static class PushToken extends Token {
        public PushToken(final int explainLevel) {
            super(TokenKind.PUSH, explainLevel, 0, 0);
        }

        @Override
        public String render(final int renderingExplainLevel, final ExplainFormatter explainFormatter, final int remainingCharacterBudget) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                explainFormatter.pushScope();
            }
            return "";
        }
    }

    /**
     * A pop token. This kind of token is used to interact with the symbol table of the formatter and does not render
     * into anything.
     */
    public static class PopToken extends Token {
        public PopToken(final int explainLevel) {
            super(TokenKind.POP, explainLevel, 0, 0);
        }

        @Override
        public String render(final int renderingExplainLevel,
                             final ExplainFormatter explainFormatter, final int remainingCharacterBudget) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                explainFormatter.popScope();
            }
            return "";
        }
    }

    /**
     * A nested token. A nested token can override the {@link ExplainLevel}s of their contained tokens. In order to
     * avoid recreating all contained tokens on a new explain level, we do some arithmetics here to compensate for
     * a different explain level.
     */
    public static class NestedToken extends Token {
        final int explainLevel;
        final int explainLevelAdjustment;
        private final ExplainTokens nestedExplainTokens;
        private final Token replacementToken;

        public NestedToken(final int explainLevel,
                           final ExplainTokens nestedExplainTokens,
                           final Token replacementToken) {
            this(explainLevel, DEFAULT_EXPLAIN_LEVEL - explainLevel, nestedExplainTokens,
                    replacementToken);
        }

        private NestedToken(final int explainLevel,
                           final int explainLevelAdjustment,
                           final ExplainTokens nestedExplainTokens,
                           final Token replacementToken) {
            super(TokenKind.NESTED, nestedExplainTokens.getTokenSizes(),
                    vectorCoalesce(shiftLengthsDownBy(nestedExplainTokens.getMinLengths(), explainLevelAdjustment),
                            zeroOutLengthsUpTo(replacementToken.getMinLengths(), explainLevel + 1)),
                    vectorCoalesce(shiftLengthsDownBy(nestedExplainTokens.getMaxLengths(), explainLevelAdjustment),
                            zeroOutLengthsUpTo(replacementToken.getMaxLengths(), explainLevel + 1)));
            this.explainLevel = explainLevel;
            this.explainLevelAdjustment = explainLevelAdjustment;
            if (explainLevelAdjustment != 0) {
                validateNestedTokens(nestedExplainTokens.getTokens());
            }
            this.nestedExplainTokens = nestedExplainTokens;
            this.replacementToken = replacementToken;
        }

        private int getExplainLevelAdjustment() {
            return explainLevelAdjustment;
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            final CharSequence renderedNested;
            if (explainLevel >= renderingExplainLevel) {
                renderedNested =
                        nestedExplainTokens.render(renderingExplainLevel + explainLevelAdjustment,
                                explainFormatter, remainingCharacterBudget);
            } else {
                renderedNested = replacementToken.render(Token.DEFAULT_EXPLAIN_LEVEL, explainFormatter, Integer.MAX_VALUE);
            }
            return cutOffIfNeeded(remainingCharacterBudget,
                    explainFormatter.visitNested(this, renderedNested));
        }

        private static int[] zeroOutLengthsUpTo(final int[] lengths, final int maxExplainLevelExclusive) {
            final var resultArray = Arrays.copyOf(lengths, lengths.length);
            Arrays.fill(resultArray, 0, maxExplainLevelExclusive, 0);
            return resultArray;
        }

        private static int[] shiftLengthsDownBy(final int[] lengths, final int explainLevelAdjustment) {
            Verify.verify(explainLevelAdjustment >= 0);
            if (explainLevelAdjustment == 0) {
                return lengths;
            }

            if (lengths.length - explainLevelAdjustment > 0) {
                final var resultArray = new int[lengths.length - explainLevelAdjustment];
                System.arraycopy(lengths, explainLevelAdjustment, resultArray, 0,
                        lengths.length - explainLevelAdjustment);
                return resultArray;
            }
            return new int[0];
        }

        private static void validateNestedTokens(final List<Token> nestedTokens) {
            for (final var token : nestedTokens) {
                if (token instanceof NestedToken) {
                    Verify.verify(((NestedToken)token).getExplainLevelAdjustment() == 0);
                    validateNestedTokens(((NestedToken)token).nestedExplainTokens.getTokens());
                } else {
                    Verify.verify(token.getExplainLevel() == DEFAULT_EXPLAIN_LEVEL);
                }
            }
        }
    }

    /**
     * A whitespace token.
     */
    public static class WhitespaceToken extends Token {
        public WhitespaceToken(final int explainLevel) {
            super(TokenKind.WHITESPACE, explainLevel, 1, 1);
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            return cutOffIfNeeded(remainingCharacterBudget,
                    renderIfEnabled(renderingExplainLevel, " ", explainFormatter::visitWhitespace));
        }
    }

    /**
     * An optional whitespace token.
     */
    public static class OptionalWhitespaceToken extends Token {
        public OptionalWhitespaceToken(final int explainLevel) {
            super(TokenKind.OPTIONAL_WHITESPACE, explainLevel, 0, 1);
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            return cutOffIfNeeded(remainingCharacterBudget,
                    renderIfEnabled(renderingExplainLevel, "", explainFormatter::visitOptionalWhitespace));
        }
    }

    /**
     * A line break or whitespace token.
     */
    public static class LineBreakOrSpaceToken extends Token {
        public LineBreakOrSpaceToken(final int explainLevel) {
            super(TokenKind.LINE_BREAK_OR_SPACE, explainLevel, 1, 1);
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            return cutOffIfNeeded(remainingCharacterBudget,
                    renderIfEnabled(renderingExplainLevel, "", explainFormatter::visitLineBreakOrSpace));
        }
    }

    /**
     * An identifier token.
     */
    public static class IdentifierToken extends Token {
        private final String identifier;

        public IdentifierToken(final int explainLevel, final String identifier) {
            super(TokenKind.IDENTIFIER, explainLevel, identifier.length(), identifier.length());
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return identifier;
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            return cutOffIfNeeded(remainingCharacterBudget,
                    renderIfEnabled(renderingExplainLevel, identifier, explainFormatter::visitIdentifier));
        }
    }

    /**
     * A keyword token.
     */
    public static class KeywordToken extends Token {
        private final String keyword;

        public KeywordToken(final int explainLevel, final String keyword) {
            super(TokenKind.KEYWORD, explainLevel, keyword.length(), keyword.length());
            this.keyword = keyword;
        }

        public String getKeyword() {
            return keyword;
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            return cutOffIfNeeded(remainingCharacterBudget,
                    renderIfEnabled(renderingExplainLevel, keyword, explainFormatter::visitKeyword));
        }
    }

    /**
     * A comma-like token. Can be {@code ,;:.} or similar.
     */
    public static class CommaLikeToken extends Token {
        private final String commaLike;

        public CommaLikeToken(final int explainLevel, final String commaLike) {
            super(TokenKind.COMMA_LIKE, explainLevel, 1, 1);
            this.commaLike = commaLike;
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            return cutOffIfNeeded(remainingCharacterBudget,
                    renderIfEnabled(renderingExplainLevel, commaLike, explainFormatter::visitCommaLike));
        }
    }

    /**
     * An alias definition token. Registers the alias with the symbol table of the formatter and renders the symbol.
     */
    public static class AliasDefinitionToken extends Token {
        private final CorrelationIdentifier alias;

        public AliasDefinitionToken(final int explainLevel, final CorrelationIdentifier alias) {
            super(TokenKind.ALIAS_DEFINITION, explainLevel, 1, Token.MAX_ALIAS_LENGTH);
            this.alias = alias;
        }

        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                explainFormatter.registerAlias(alias);

                return cutOffIfNeeded(remainingCharacterBudget,
                        explainFormatter.visitAliasDefinition(this,
                                explainFormatter.getSymbolForAliasMaybe(alias)
                                        .orElseThrow(() -> new RecordCoreException("must have resolved symbol"))));
            }
            return "";
        }
    }

    /**
     * Token that registers an alias explicitly to some symbol. This token is used to render the <em>current</em>
     * symbol in some special way (e.g. {@code _}). This token renders into the empty string.
     */
    public static class CurrentAliasDefinitionToken extends Token {
        private final CorrelationIdentifier alias;

        public CurrentAliasDefinitionToken(final int explainLevel, final CorrelationIdentifier alias) {
            super(TokenKind.ALIAS_DEFINITION, explainLevel, 0, 0);
            this.alias = alias;
        }

        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Override
        public String render(final int renderingExplainLevel,
                             final ExplainFormatter explainFormatter,
                             final int remainingCharacterBudget) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                explainFormatter.registerAliasExplicitly(alias, "_");
            }
            return "";
        }
    }

    /**
     * An alias reference token. Renders the symbol the alias is currently mapped to in the symbol table of the
     * formatter. Renders an error (e.g. the alias in an alerting color if using {@link PrettyExplainFormatter}) if the
     * alias has not been registered prior to this token.
     */
    public static class AliasReferenceToken extends Token {
        private final CorrelationIdentifier alias;

        public AliasReferenceToken(final int explainLevel, final CorrelationIdentifier alias) {
            super(TokenKind.ALIAS_REFERENCE, explainLevel, 1, Token.MAX_ALIAS_LENGTH + 2);
            this.alias = alias;
        }

        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            final CharSequence result;
            final var symbolForAliasOptional = explainFormatter.getSymbolForAliasMaybe(alias);
            if (symbolForAliasOptional.isPresent()) {
                result = renderIfEnabled(renderingExplainLevel, symbolForAliasOptional.get(),
                        explainFormatter::visitAliasReference);
            } else {
                result = renderIfEnabled(renderingExplainLevel, alias.getId(),
                        explainFormatter::visitError);
            }
            return cutOffIfNeeded(remainingCharacterBudget, result);
        }
    }

    /**
     * A brackets token. Can be {@code ()[]{}<>}.
     */
    public static class BracketLikeToken extends Token {
        private final boolean isOpen;
        private final String bracket;

        public BracketLikeToken(final int explainLevel,
                                final boolean isOpen,
                                final String bracket) {
            super(isOpen ? TokenKind.BRACKETS_OPEN : TokenKind.BRACKETS_CLOSE, explainLevel, 1, 1);
            this.isOpen = isOpen;
            this.bracket = bracket;
        }

        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter,
                                   final int remainingCharacterBudget) {
            return cutOffIfNeeded(remainingCharacterBudget,
                    renderIfEnabled(renderingExplainLevel, bracket, explainFormatter::visitBracketLike));
        }
    }

    /**
     * A ToString token. Can hold anything that can be stringified by calling {@link Object#toString()}.
     */
    public static class ToStringToken extends Token {
        @Nullable
        private final Object object;
        private final String objectAsString;

        public ToStringToken(final int explainLevel, @Nullable final Object object) {
            this(explainLevel, object, String.valueOf(object));
        }

        private ToStringToken(final int explainLevel,
                              @Nullable final Object object,
                              final String objectAsString) {
            super(TokenKind.TO_STRING, explainLevel, objectAsString.length(), objectAsString.length());
            this.object = object;
            this.objectAsString = objectAsString;
        }

        @Nullable
        public Object getObject() {
            return object;
        }

        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   final ExplainFormatter explainFormatter, final int remainingCharacterBudget) {
            return cutOffIfNeeded(remainingCharacterBudget,
                    renderIfEnabled(renderingExplainLevel, objectAsString, explainFormatter::visitToString));
        }
    }
}
