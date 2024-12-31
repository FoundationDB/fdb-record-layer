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
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Holder class for the result of a call to {@link Value#explain()} and derivatives.
 */
public class ExplainTokens {
    @Nonnull
    private final List<Token> tokens;
    private int tokensSize;
    int minLength;

    public ExplainTokens() {
        this.tokens = Lists.newArrayList();
        this.tokensSize = 0;
        this.minLength = 0;
    }

    @Nonnull
    public List<Token> getTokens() {
        return tokens;
    }

    public int size() {
        return tokensSize;
    }

    public boolean isEmpty() {
        return tokensSize == 0;
    }

    public int getMinLength() {
        return minLength;
    }

    @Nonnull
    public String render(@Nonnull final ExplainFormatter formatter) {
        final StringBuilder stringBuilder = new StringBuilder();
        for (final var token : tokens) {
            stringBuilder.append(token.render(formatter));
        }
        return stringBuilder.toString();
    }

    @Nonnull
    public ExplainTokens add(@Nonnull final Token token) {
        tokens.add(token);
        tokensSize += token.getTokensSize();
        minLength += token.getMinLength();
        return this;
    }

    @Nonnull
    public ExplainTokens addNested(@Nonnull final ExplainTokens additionalExplainTokens) {
        add(new NestedToken(additionalExplainTokens));
        return this;
    }

    @Nonnull
    public ExplainTokens addAll(@Nonnull final Collection<? extends Token> additionalTokens) {
        additionalTokens.forEach(this::add);
        return this;
    }

    @Nonnull
    public ExplainTokens addWhitespace() {
        add(new WhiteSpaceToken());
        return this;
    }

    @Nonnull
    public ExplainTokens addOptionalWhitespace() {
        add(new OptionalWhiteSpaceToken());
        return this;
    }

    @Nonnull
    public ExplainTokens addIdentifier(@Nonnull final String identifier) {
        add(new IdentifierToken(identifier));
        return this;
    }

    @Nonnull
    public ExplainTokens addComma() {
        add(new CommaLikeToken(","));
        return this;
    }

    @Nonnull
    public ExplainTokens addCommaAndWhiteSpace() {
        addComma().addWhitespace();
        return this;
    }

    @Nonnull
    public ExplainTokens addAliasDefinition(@Nonnull final CorrelationIdentifier alias) {
        add(new AliasDefinitionToken(alias));
        return this;
    }

    @Nonnull
    public ExplainTokens addAliasReference(@Nonnull final CorrelationIdentifier alias) {
        add(new AliasReferenceToken(alias));
        return this;
    }

    @Nonnull
    public ExplainTokens addOpeningParen() {
        add(new BracketsToken(true, "("));
        return this;
    }

    @Nonnull
    public ExplainTokens addClosingParen() {
        add(new BracketsToken(false, ")"));
        return this;
    }

    @Nonnull
    public ExplainTokens addOpeningBracket() {
        add(new BracketsToken(true, "["));
        return this;
    }

    @Nonnull
    public ExplainTokens addClosingBracket() {
        add(new BracketsToken(false, "]"));
        return this;
    }

    @Nonnull
    public ExplainTokens addOpeningBrace() {
        add(new BracketsToken(true, "{"));
        return this;
    }

    @Nonnull
    public ExplainTokens addClosingBrace() {
        add(new BracketsToken(false, "}"));
        return this;
    }

    @Nonnull
    public ExplainTokens addOpeningAngledBracket() {
        add(new BracketsToken(true, "<"));
        return this;
    }

    @Nonnull
    public ExplainTokens addClosingAngledBracket() {
        add(new BracketsToken(false, ">"));
        return this;
    }

    @Nonnull
    public ExplainTokens addToString(@Nullable final Object object) {
        add(new ToStringToken(object));
        return this;
    }

    @Nonnull
    public ExplainTokens addToStrings(@Nonnull final Iterable<? extends Object> objects) {
        final ExplainTokens resultTokens = new ExplainTokens();

        for (final var iterator = objects.iterator(); iterator.hasNext(); ) {
            final var object = iterator.next();
            resultTokens.addToString(object);
            if (iterator.hasNext()) {
                resultTokens.addCommaAndWhiteSpace();
            }
        }

        return resultTokens;
    }

    @Nonnull
    public ExplainTokens addFunctionCall(@Nonnull final String identifier) {
        return addFunctionCall(identifier, null);
    }

    @Nonnull
    public ExplainTokens addFunctionCall(@Nonnull final String identifier, @Nullable final ExplainTokens childExplainTokens) {
        if (childExplainTokens == null || childExplainTokens.isEmpty()) {
            return addIdentifier(identifier).addOptionalWhitespace().addOpeningParen().addClosingParen();
        }
        return addIdentifier(identifier).addOptionalWhitespace().addOpeningParen().addOptionalWhitespace()
                .addNested(childExplainTokens).addOptionalWhitespace().addClosingParen();
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

    /**
     * A token enumeration that is used intermittently when something is explained.
     */
    public enum TokenKind {
        NESTED,
        WHITESPACE,
        OPTIONAL_WHITESPACE,
        IDENTIFIER,
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
        @Nonnull
        private final TokenKind tokenKind;
        private final int tokensSize;
        private final int minLength;

        public Token(@Nonnull final TokenKind tokenKind, final int minLength) {
            this(tokenKind, 1, minLength);
        }

        public Token(@Nonnull final TokenKind tokenKind, final int tokensSize, final int minLength) {
            this.tokenKind = tokenKind;
            this.tokensSize = tokensSize;
            this.minLength = minLength;
        }

        public int getTokensSize() {
            return tokensSize;
        }

        @Nonnull
        public TokenKind getTokenKind() {
            return tokenKind;
        }

        public int getMinLength() {
            return minLength;
        }

        @Nonnull
        public abstract String render(@Nonnull ExplainFormatter explainFormatter);
    }

    /**
     * A nested token.
     */
    public static class NestedToken extends Token {
        @Nonnull
        private final ExplainTokens nestedExplainTokens;

        public NestedToken(@Nonnull final ExplainTokens nestedExplainTokens) {
            super(TokenKind.NESTED, nestedExplainTokens.size(), nestedExplainTokens.getMinLength());
            this.nestedExplainTokens = nestedExplainTokens;
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return nestedExplainTokens.render(explainFormatter);
        }
    }

    /**
     * A whitespace token.
     */
    public static class WhiteSpaceToken extends Token {
        public WhiteSpaceToken() {
            super(TokenKind.WHITESPACE, 1);
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return " ";
        }
    }

    /**
     * An optional whitespace token.
     */
    public static class OptionalWhiteSpaceToken extends Token {
        public OptionalWhiteSpaceToken() {
            super(TokenKind.OPTIONAL_WHITESPACE, 0);
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return "";
        }
    }

    /**
     * An identifier token.
     */
    public static class IdentifierToken extends Token {
        @Nonnull
        private final String identifier;

        public IdentifierToken(@Nonnull final String identifier) {
            super(TokenKind.IDENTIFIER, identifier.length());
            this.identifier = identifier;
        }

        @Nonnull
        public String getIdentifier() {
            return identifier;
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return identifier;
        }
    }

    /**
     * An identifier token.
     */
    public static class CommaLikeToken extends Token {
        @Nonnull
        private final String commaLike;

        public CommaLikeToken(@Nonnull final String commaLike) {
            super(TokenKind.COMMA_LIKE, 1);
            this.commaLike = commaLike;
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return commaLike;
        }
    }

    /**
     * An alias definition token.
     */
    public static class AliasDefinitionToken extends Token {
        @Nonnull
        private final CorrelationIdentifier alias;

        public AliasDefinitionToken(@Nonnull final CorrelationIdentifier alias) {
            super(TokenKind.ALIAS_DEFINITION, alias.getId().length());
            this.alias = alias;
        }

        @Nonnull
        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return explainFormatter.getSymbolForAlias(alias);
        }
    }

    /**
     * An alias reference token.
     */
    public static class AliasReferenceToken extends Token {
        @Nonnull
        private final CorrelationIdentifier alias;

        public AliasReferenceToken(@Nonnull final CorrelationIdentifier alias) {
            super(TokenKind.ALIAS_REFERENCE, alias.getId().length());
            this.alias = alias;
        }

        @Nonnull
        public CorrelationIdentifier getAlias() {
            return alias;
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return explainFormatter.getSymbolForAlias(alias);
        }
    }

    /**
     * An identifier token.
     */
    public static class BracketsToken extends Token {
        private final boolean isOpen;
        @Nonnull
        private final String bracket;

        public BracketsToken(final boolean isOpen, @Nonnull final String bracket) {
            super(isOpen ? TokenKind.BRACKETS_OPEN : TokenKind.BRACKETS_CLOSE, 1);
            this.isOpen = isOpen;
            this.bracket = bracket;
        }

        public boolean isOpen() {
            return isOpen;
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return bracket;
        }
    }

    /**
     * An identifier token.
     */
    public static class ToStringToken extends Token {
        @Nullable
        private final Object object;
        private final String objectAsString;

        public ToStringToken(@Nullable final Object object) {
            this(object, String.valueOf(object));
        }

        private ToStringToken(@Nullable final Object object, @Nonnull final String objectAsString) {
            super(TokenKind.TO_STRING, objectAsString.length());
            this.object = object;
            this.objectAsString = objectAsString;
        }

        @Nullable
        public Object getObject() {
            return object;
        }

        @Nonnull
        @Override
        public String render(@Nonnull final ExplainFormatter explainFormatter) {
            return objectAsString;
        }
    }
}
