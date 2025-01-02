/*
 * ExplainTokensWithPrecedence.java
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

import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Holder class for the result of a call to {@link Value#explain()} and derivatives.
 */
public class ExplainTokens {
    @Nonnull
    private final List<Token> tokens;
    private int[] tokenSizes;
    private int[] minLengths;

    public ExplainTokens() {
        this.tokens = Lists.newArrayList();
        this.tokenSizes = new int[0];
        this.minLengths = new int[0];
    }

    public boolean isEmpty() {
        return tokens.isEmpty();
    }

    @Nonnull
    public List<Token> getTokens() {
        return tokens;
    }

    @Nonnull
    private int[] getTokenSizes() {
        return tokenSizes;
    }

    public int getTokenSize(final int explainLevel) {
        return explainLevel < tokenSizes.length ? tokenSizes[explainLevel] : 0;
    }

    @Nonnull
    private int[] getMinLengths() {
        return minLengths;
    }

    public int getMinLength(final int explainLevel) {
        return explainLevel < minLengths.length ? minLengths[explainLevel] : 0;
    }

    @Nonnull
    public CharSequence render(@Nonnull final ExplainFormatter formatter) {
        return render(ExplainLevel.ALL_DETAILS, formatter);
    }

    @Nonnull
    public CharSequence render(final int renderingExplainLevel, @Nonnull final ExplainFormatter formatter) {
        final StringBuilder stringBuilder = new StringBuilder();
        for (final var token : tokens) {
            stringBuilder.append(token.render(renderingExplainLevel, formatter));
        }
        return stringBuilder;
    }

    @Nonnull
    public ExplainTokens add(@Nonnull final Token token) {
        tokens.add(token);
        this.tokenSizes = vectorAdd(this.tokenSizes, token.getTokenSizes());
        this.minLengths = vectorAdd(this.minLengths, token.getMinLengths());
        return this;
    }

    @Nonnull
    public ExplainTokens addNested(@Nonnull final ExplainTokens additionalExplainTokens) {
        addNested(Token.DEFAULT_EXPLAIN_LEVEL, additionalExplainTokens);
        return this;
    }

    @Nonnull
    public ExplainTokens addNested(final int explainLevel,
                                   @Nonnull final ExplainTokens additionalExplainTokens) {
        add(new NestedToken(explainLevel, additionalExplainTokens, new OptionalWhitespaceToken(explainLevel)));
        return this;
    }

    @Nonnull
    public ExplainTokens addNested(final int explainLevel,
                                   @Nonnull final ExplainTokens additionalExplainTokens,
                                   @Nonnull final String replacementTokenString) {
        addNested(explainLevel, additionalExplainTokens,
                new ToStringToken(Token.DEFAULT_EXPLAIN_LEVEL, replacementTokenString));
        return this;
    }

    @Nonnull
    public ExplainTokens addNested(final int explainLevel,
                                   @Nonnull final ExplainTokens additionalExplainTokens,
                                   @Nonnull final Token replacementToken) {
        add(new NestedToken(explainLevel, additionalExplainTokens, replacementToken));
        return this;
    }

    @Nonnull
    public ExplainTokens addAll(@Nonnull final Collection<? extends Token> additionalTokens) {
        additionalTokens.forEach(this::add);
        return this;
    }

    @Nonnull
    public ExplainTokens addPush() {
        return addPush(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addPush(final int explainLevel) {
        add(new PushToken(explainLevel));
        return this;
    }

    @Nonnull
    public ExplainTokens addPop() {
        return addPop(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addPop(final int explainLevel) {
        add(new PopToken(explainLevel));
        return this;
    }

    @Nonnull
    public ExplainTokens addWhitespace() {
        return addWhitespace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addWhitespace(final int explainLevel) {
        add(new WhitespaceToken(explainLevel));
        return this;
    }

    @Nonnull
    public ExplainTokens addOptionalWhitespace() {
        return addOptionalWhitespace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addOptionalWhitespace(final int explainLevel) {
        add(new OptionalWhitespaceToken(explainLevel));
        return this;
    }

    @Nonnull
    public ExplainTokens addLinebreakOrWhitespace() {
        return addLinebreakOrWhitespace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addLinebreakOrWhitespace(final int explainLevel) {
        add(new LineBreakOrSpaceToken(explainLevel));
        return this;
    }

    @Nonnull
    public ExplainTokens addIdentifier(@Nonnull final String identifier) {
        return addIdentifier(Token.DEFAULT_EXPLAIN_LEVEL, identifier);
    }

    @Nonnull
    public ExplainTokens addIdentifier(final int explainLevel, @Nonnull final String identifier) {
        add(new IdentifierToken(explainLevel, identifier));
        return this;
    }

    @Nonnull
    public ExplainTokens addKeyword(@Nonnull final String keyword) {
        return addKeyword(Token.DEFAULT_EXPLAIN_LEVEL, keyword);
    }

    @Nonnull
    public ExplainTokens addKeyword(final int explainLevel, @Nonnull final String keyword) {
        add(new KeywordToken(explainLevel, keyword));
        return this;
    }

    @Nonnull
    public ExplainTokens addComma() {
        return addComma(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addComma(final int explainLevel) {
        add(new CommaLikeToken(explainLevel, ","));
        return this;
    }

    @Nonnull
    public ExplainTokens addCommaAndWhiteSpace() {
        return addCommaAndWhiteSpace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addCommaAndWhiteSpace(final int explainLevel) {
        addComma(explainLevel).addWhitespace(explainLevel);
        return this;
    }

    @Nonnull
    public ExplainTokens addAliasDefinition(@Nonnull final CorrelationIdentifier alias) {
        return addAliasDefinition(Token.DEFAULT_EXPLAIN_LEVEL, alias);
    }

    @Nonnull
    public ExplainTokens addAliasDefinition(final int explainLevel,
                                            @Nonnull final CorrelationIdentifier alias) {
        add(new AliasDefinitionToken(explainLevel, alias));
        return this;
    }

    @Nonnull
    public ExplainTokens addCurrentAliasDefinition(@Nonnull final CorrelationIdentifier alias) {
        return addCurrentAliasDefinition(Token.DEFAULT_EXPLAIN_LEVEL, alias);
    }

    @Nonnull
    public ExplainTokens addCurrentAliasDefinition(final int explainLevel,
                                                   @Nonnull final CorrelationIdentifier alias) {
        add(new CurrentAliasDefinitionToken(explainLevel, alias));
        return this;
    }

    @Nonnull
    public ExplainTokens addAliasReference(@Nonnull final CorrelationIdentifier alias) {
        return addAliasReference(Token.DEFAULT_EXPLAIN_LEVEL, alias);
    }

    @Nonnull
    public ExplainTokens addAliasReference(final int explainLevel,
                                           @Nonnull final CorrelationIdentifier alias) {
        add(new AliasReferenceToken(explainLevel, alias));
        return this;
    }

    @Nonnull
    public ExplainTokens addOpeningParen() {
        return addOpeningParen(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addOpeningParen(final int explainLevel) {
        add(new BracketsToken(explainLevel, true, "("));
        return this;
    }

    @Nonnull
    public ExplainTokens addClosingParen() {
        return addClosingParen(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addClosingParen(final int explainLevel) {
        add(new BracketsToken(explainLevel, false, ")"));
        return this;
    }

    @Nonnull
    public ExplainTokens addOpeningBracket() {
        return addOpeningBracket(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addOpeningBracket(final int explainLevel) {
        add(new BracketsToken(explainLevel, true, "["));
        return this;
    }

    @Nonnull
    public ExplainTokens addClosingBracket() {
        return addClosingBracket(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addClosingBracket(final int explainLevel) {
        add(new BracketsToken(explainLevel, false, "]"));
        return this;
    }

    @Nonnull
    public ExplainTokens addOpeningBrace() {
        return addOpeningBrace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addOpeningBrace(final int explainLevel) {
        add(new BracketsToken(explainLevel, true, "{"));
        return this;
    }

    @Nonnull
    public ExplainTokens addClosingBrace() {
        return addClosingBrace(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addClosingBrace(final int explainLevel) {
        add(new BracketsToken(explainLevel, false, "}"));
        return this;
    }

    @Nonnull
    public ExplainTokens addOpeningAngledBracket() {
        return addOpeningAngledBracket(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addOpeningAngledBracket(final int explainLevel) {
        add(new BracketsToken(explainLevel, true, "<"));
        return this;
    }

    @Nonnull
    public ExplainTokens addClosingAngledBracket() {
        return addClosingAngledBracket(Token.DEFAULT_EXPLAIN_LEVEL);
    }

    @Nonnull
    public ExplainTokens addClosingAngledBracket(final int explainLevel) {
        add(new BracketsToken(explainLevel, false, ">"));
        return this;
    }

    @Nonnull
    public ExplainTokens addToString(@Nullable final Object object) {
        return addToString(Token.DEFAULT_EXPLAIN_LEVEL, object);
    }

    @Nonnull
    public ExplainTokens addToString(final int explainLevel, @Nullable final Object object) {
        add(new ToStringToken(explainLevel, object));
        return this;
    }

    @Nonnull
    public ExplainTokens addToStrings(@Nonnull final Iterable<?> objects) {
        return addToStrings(Token.DEFAULT_EXPLAIN_LEVEL, objects);
    }

    @Nonnull
    public ExplainTokens addToStrings(final int explainLevel, @Nonnull final Iterable<?> objects) {
        final ExplainTokens resultTokens = new ExplainTokens();

        for (final var iterator = objects.iterator(); iterator.hasNext(); ) {
            final var object = iterator.next();
            resultTokens.addToString(explainLevel, object);
            if (iterator.hasNext()) {
                resultTokens.addCommaAndWhiteSpace(explainLevel);
            }
        }

        return resultTokens;
    }

    @Nonnull
    public ExplainTokens addFunctionCall(@Nonnull final String identifier) {
        return addFunctionCall(Token.DEFAULT_EXPLAIN_LEVEL, identifier);
    }

    @Nonnull
    public ExplainTokens addFunctionCall(final int explainLevel, @Nonnull final String identifier) {
        return addFunctionCall(explainLevel, identifier, null);
    }

    @Nonnull
    public ExplainTokens addFunctionCall(@Nonnull final String identifier,
                                         @Nullable final ExplainTokens childExplainTokens) {
        return addFunctionCall(Token.DEFAULT_EXPLAIN_LEVEL, identifier, childExplainTokens);
    }

    @Nonnull
    public ExplainTokens addFunctionCall(final int explainLevel,
                                         @Nonnull final String identifier,
                                         @Nullable final ExplainTokens childExplainTokens) {
        if (childExplainTokens == null || childExplainTokens.isEmpty()) {
            return addIdentifier(explainLevel, identifier).addOptionalWhitespace(explainLevel)
                    .addOpeningParen(explainLevel).addClosingParen(explainLevel);
        }
        return addIdentifier(explainLevel, identifier).addOptionalWhitespace(explainLevel).addOpeningParen(explainLevel)
                .addOptionalWhitespace(explainLevel).addNested(childExplainTokens).addOptionalWhitespace(explainLevel)
                .addClosingParen(explainLevel);
    }

    @Nonnull
    public ExplainTokens addSequence(@Nonnull final Supplier<ExplainTokens> delimiterSupplier,
                                     @Nonnull final ExplainTokens... childrenExplainTokens) {
        return addSequence(delimiterSupplier, Arrays.asList(childrenExplainTokens));
    }

    @Nonnull
    public ExplainTokens addSequence(@Nonnull final Supplier<ExplainTokens> delimiterSupplier,
                                     @Nonnull final Iterable<ExplainTokens> childrenExplainTokens) {
        for (final var iterator = childrenExplainTokens.iterator(); iterator.hasNext(); ) {
            final var childExplainTokens = iterator.next();
            addNested(childExplainTokens);
            if (iterator.hasNext()) {
                addAll(delimiterSupplier.get().getTokens());
            }
        }
        return this;
    }

    @Nonnull
    private static int[] vectorAdd(@Nonnull final int[] left, @Nonnull final int[] right) {
        final var maxSize = Math.max(left.length, right.length);
        final var result = new int[maxSize];

        for (int i = 0; i < maxSize; i ++) {
            result[i] = (i < left.length ? left[i] : 0) + (i < right.length ? right[i] : 0);
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
        protected static final int DEFAULT_EXPLAIN_LEVEL = ExplainLevel.STRUCTURE;
        @Nonnull
        private final TokenKind tokenKind;
        @Nonnull
        private final int[] tokenSizes;
        @Nonnull
        private final int[] minLengths;

        public Token(@Nonnull final TokenKind tokenKind, final int explainLevel, final int minLength) {
            this(tokenKind, explainLevel, 1, minLength);
        }

        public Token(@Nonnull final TokenKind tokenKind, final int explainLevel, final int tokenSize, final int minLength) {
            this(tokenKind, vectorFromExplainLevel(explainLevel, tokenSize), vectorFromExplainLevel(explainLevel, minLength));
        }

        protected Token(@Nonnull final TokenKind tokenKind, @Nonnull final int[] tokenSizes,
                        @Nonnull final int[] minLengths) {
            this.tokenKind = tokenKind;
            this.tokenSizes = tokenSizes;
            this.minLengths = minLengths;
        }

        @Nonnull
        public TokenKind getTokenKind() {
            return tokenKind;
        }

        @Nonnull
        private int[] getTokenSizes() {
            return tokenSizes;
        }

        @Nonnull
        private int[] getMinLengths() {
            return minLengths;
        }

        @Nonnull
        public int getExplainLevel() {
            Verify.verify(tokenSizes.length == minLengths.length);
            return tokenSizes.length - 1;
        }

        protected boolean isRenderingEnabled(final int renderingExplainLevel) {
            return getExplainLevel() >= renderingExplainLevel;
        }

        @SuppressWarnings("unchecked")
        protected <T extends Token> CharSequence renderIfEnabled(final int renderingExplainLevel,
                                                                 @Nonnull final String stringedToken,
                                                                 @Nonnull final BiFunction<T, CharSequence, CharSequence> renderFunction) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                return renderFunction.apply((T)this, stringedToken);
            }
            return "";
        }

        public int getTokenSize(final int explainLevel) {
            return explainLevel < tokenSizes.length ? tokenSizes[explainLevel] : 0;
        }

        public int getMinLength(final int explainLevel) {
            return explainLevel < minLengths.length ? minLengths[explainLevel] : 0;
        }

        @Nonnull
        public abstract CharSequence render(int renderingExplainLevel,
                                            @Nonnull ExplainFormatter explainFormatter);

        @Nonnull
        private static int[] vectorFromExplainLevel(final int explainLevel, int value) {
            final int[] result = new int[explainLevel + 1];
            Arrays.fill(result, value);
            return result;
        }
    }

    /**
     * A push token.
     */
    public static class PushToken extends Token {
        public PushToken(final int explainLevel) {
            super(TokenKind.PUSH, explainLevel, 0);
        }

        @Nonnull
        @Override
        public String render(final int renderingExplainLevel, @Nonnull final ExplainFormatter explainFormatter) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                explainFormatter.pushScope();
            }
            return "";
        }
    }

    /**
     * A push token.
     */
    public static class PopToken extends Token {
        public PopToken(final int explainLevel) {
            super(TokenKind.POP, explainLevel, 0, 0);
        }

        @Nonnull
        @Override
        public String render(final int renderingExplainLevel,
                             @Nonnull final ExplainFormatter explainFormatter) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                explainFormatter.popScope();
            }
            return "";
        }
    }

    /**
     * A nested token.
     */
    public static class NestedToken extends Token {
        final int explainLevel;
        final int explainLevelAdjustment;
        @Nonnull
        private final ExplainTokens nestedExplainTokens;
        @Nonnull
        private final Token replacementToken;

        public NestedToken(final int explainLevel,
                           @Nonnull final ExplainTokens nestedExplainTokens,
                           @Nonnull final Token replacementToken) {
            super(TokenKind.NESTED, nestedExplainTokens.getTokenSizes(), nestedExplainTokens.getMinLengths());
            this.explainLevel = explainLevel;
            this.explainLevelAdjustment = DEFAULT_EXPLAIN_LEVEL - explainLevel;
            if (explainLevelAdjustment != 0) {
                validateNestedTokens(nestedExplainTokens.getTokens());
            }
            this.nestedExplainTokens = nestedExplainTokens;
            this.replacementToken = replacementToken;
        }

        private int getExplainLevelAdjustment() {
            return explainLevelAdjustment;
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   @Nonnull final ExplainFormatter explainFormatter) {
            final CharSequence renderedNested;
            if (explainLevel >= renderingExplainLevel) {
                renderedNested =
                        nestedExplainTokens.render(renderingExplainLevel + explainLevelAdjustment,
                                explainFormatter);
            } else {
                renderedNested = replacementToken.render(Token.DEFAULT_EXPLAIN_LEVEL, explainFormatter);
            }
            return explainFormatter.visitNested(this, renderedNested);
        }

        private static void validateNestedTokens(@Nonnull final List<Token> nestedTokens) {
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
            super(TokenKind.WHITESPACE, explainLevel, 1);
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                             @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, " ", explainFormatter::visitWhitespace);
        }
    }

    /**
     * An optional whitespace token.
     */
    public static class OptionalWhitespaceToken extends Token {
        public OptionalWhitespaceToken(final int explainLevel) {
            super(TokenKind.OPTIONAL_WHITESPACE, explainLevel, 0);
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                             @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, "", explainFormatter::visitOptionalWhitespace);
        }
    }

    /**
     * An optional whitespace token.
     */
    public static class LineBreakOrSpaceToken extends Token {
        public LineBreakOrSpaceToken(final int explainLevel) {
            super(TokenKind.LINE_BREAK_OR_SPACE, explainLevel, 0);
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                             @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, "", explainFormatter::visitLineBreakOrSpace);
        }
    }

    /**
     * An identifier token.
     */
    public static class IdentifierToken extends Token {
        @Nonnull
        private final String identifier;

        public IdentifierToken(final int explainLevel, @Nonnull final String identifier) {
            super(TokenKind.IDENTIFIER, explainLevel, identifier.length());
            this.identifier = identifier;
        }

        @Nonnull
        public String getIdentifier() {
            return identifier;
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, identifier, explainFormatter::visitIdentifier);
        }
    }

    /**
     * An identifier token.
     */
    public static class KeywordToken extends Token {
        @Nonnull
        private final String keyword;

        public KeywordToken(final int explainLevel, @Nonnull final String keyword) {
            super(TokenKind.KEYWORD, explainLevel, keyword.length());
            this.keyword = keyword;
        }

        @Nonnull
        public String getKeyword() {
            return keyword;
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, keyword, explainFormatter::visitKeyword);
        }
    }

    /**
     * An identifier token.
     */
    public static class CommaLikeToken extends Token {
        @Nonnull
        private final String commaLike;

        public CommaLikeToken(final int explainLevel, @Nonnull final String commaLike) {
            super(TokenKind.COMMA_LIKE, explainLevel, 1);
            this.commaLike = commaLike;
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, commaLike, explainFormatter::visitCommaLike);
        }
    }

    /**
     * An alias definition token.
     */
    public static class AliasDefinitionToken extends Token {
        @Nonnull
        private final CorrelationIdentifier alias;

        public AliasDefinitionToken(final int explainLevel, @Nonnull final CorrelationIdentifier alias) {
            super(TokenKind.ALIAS_DEFINITION, explainLevel, 1);
            this.alias = alias;
        }

        @Nonnull
        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   @Nonnull final ExplainFormatter explainFormatter) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                explainFormatter.registerAlias(alias);

                return explainFormatter.visitAliasDefinition(this,
                        explainFormatter.getSymbolForAlias(alias));
            }
            return "";
        }
    }

    /**
     * An alias definition token.
     */
    public static class CurrentAliasDefinitionToken extends Token {
        @Nonnull
        private final CorrelationIdentifier alias;

        public CurrentAliasDefinitionToken(final int explainLevel, @Nonnull final CorrelationIdentifier alias) {
            super(TokenKind.ALIAS_DEFINITION, explainLevel, 1);
            this.alias = alias;
        }

        @Nonnull
        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Nonnull
        @Override
        public String render(final int renderingExplainLevel,
                             @Nonnull final ExplainFormatter explainFormatter) {
            if (isRenderingEnabled(renderingExplainLevel)) {
                explainFormatter.registerAliasExplicitly(alias, "_");
            }
            return "";
        }
    }

    /**
     * An alias reference token.
     */
    public static class AliasReferenceToken extends Token {
        @Nonnull
        private final CorrelationIdentifier alias;

        public AliasReferenceToken(final int explainLevel, @Nonnull final CorrelationIdentifier alias) {
            super(TokenKind.ALIAS_REFERENCE, explainLevel, 1);
            this.alias = alias;
        }

        @Nonnull
        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, explainFormatter.getSymbolForAlias(alias),
                    explainFormatter::visitAliasReference);
        }
    }

    /**
     * An identifier token.
     */
    public static class BracketsToken extends Token {
        private final boolean isOpen;
        @Nonnull
        private final String bracket;

        public BracketsToken(final int explainLevel,
                             final boolean isOpen,
                             @Nonnull final String bracket) {
            super(isOpen ? TokenKind.BRACKETS_OPEN : TokenKind.BRACKETS_CLOSE, explainLevel, 1);
            this.isOpen = isOpen;
            this.bracket = bracket;
        }

        public boolean isOpen() {
            return isOpen;
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, bracket, explainFormatter::visitBrackets);
        }
    }

    /**
     * An identifier token.
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
                              @Nonnull final String objectAsString) {
            super(TokenKind.TO_STRING, explainLevel, objectAsString.length());
            this.object = object;
            this.objectAsString = objectAsString;
        }

        @Nullable
        public Object getObject() {
            return object;
        }

        @Nonnull
        @Override
        public CharSequence render(final int renderingExplainLevel,
                                   @Nonnull final ExplainFormatter explainFormatter) {
            return renderIfEnabled(renderingExplainLevel, objectAsString, explainFormatter::visitToString);
        }
    }
}
