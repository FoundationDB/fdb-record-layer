/*
 * ExplainFormatter.java
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
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.AliasDefinitionToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.AliasReferenceToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.BracketLikeToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.CommaLikeToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.IdentifierToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.KeywordToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.LineBreakOrSpaceToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.NestedToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.OptionalWhitespaceToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.ToStringToken;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.Token;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens.WhitespaceToken;

import java.util.Optional;

/**
 * A formatter for tokens.
 */
public interface ExplainFormatter {

    void registerAlias(CorrelationIdentifier alias);

    void registerAliasExplicitly(CorrelationIdentifier alias, String symbol);

    Optional<String> getSymbolForAliasMaybe(CorrelationIdentifier alias);

    void pushScope();

    void popScope();

    @SuppressWarnings("unused")
    default CharSequence visitNested(final NestedToken nestedToken,
                                     final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitWhitespace(final WhitespaceToken whiteSpaceToken,
                                         final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitOptionalWhitespace(final OptionalWhitespaceToken optionalWhiteSpaceToken,
                                                 final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitLineBreakOrSpace(final LineBreakOrSpaceToken lineBreakOrSpaceToken,
                                               final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitIdentifier(final IdentifierToken identifierToken,
                                         final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitKeyword(final KeywordToken keywordToken,
                                      final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitCommaLike(final CommaLikeToken commaLikeToken,
                                        final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitAliasDefinition(final AliasDefinitionToken aliasDefinitionToken,
                                              final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitAliasReference(final AliasReferenceToken aliasReferenceToken,
                                             final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitBracketLike(final BracketLikeToken bracketLikeToken,
                                          final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitToString(final ToStringToken toStringToken,
                                       final CharSequence stringedToken) {
        return stringedToken;
    }

    @SuppressWarnings("unused")
    default CharSequence visitError(final Token token,
                                    final CharSequence stringedToken) {
        return stringedToken;
    }
}
