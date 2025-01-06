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

import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.AliasDefinitionToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.AliasReferenceToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.BracketsToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.CommaLikeToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.IdentifierToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.KeywordToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.LineBreakOrSpaceToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.NestedToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.OptionalWhitespaceToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.ToStringToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.Token;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.WhitespaceToken;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * A formatter for tokens.
 */
public interface ExplainFormatter {

    void registerAlias(@Nonnull CorrelationIdentifier alias);

    void registerAliasExplicitly(@Nonnull CorrelationIdentifier alias, @Nonnull String symbol);

    @Nonnull
    Optional<String> getSymbolForAliasMaybe(@Nonnull CorrelationIdentifier alias);

    void pushScope();

    void popScope();

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitNested(@Nonnull final NestedToken nestedToken,
                                     @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitWhitespace(@Nonnull final WhitespaceToken whiteSpaceToken,
                                         @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitOptionalWhitespace(@Nonnull final OptionalWhitespaceToken optionalWhiteSpaceToken,
                                                 @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitLineBreakOrSpace(@Nonnull final LineBreakOrSpaceToken lineBreakOrSpaceToken,
                                               @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitIdentifier(@Nonnull final IdentifierToken identifierToken,
                                         @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitKeyword(@Nonnull final KeywordToken keywordToken,
                                      @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitCommaLike(@Nonnull final CommaLikeToken commaLikeToken,
                                        @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitAliasDefinition(@Nonnull final AliasDefinitionToken aliasDefinitionToken,
                                              @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitAliasReference(@Nonnull final AliasReferenceToken aliasReferenceToken,
                                             @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitBrackets(@Nonnull final BracketsToken bracketsToken,
                                       @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitToString(@Nonnull final ToStringToken toStringToken,
                                       @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }

    @Nonnull
    @SuppressWarnings("unused")
    default CharSequence visitError(@Nonnull final Token token,
                                    @Nonnull final CharSequence stringedToken) {
        return stringedToken;
    }
}
