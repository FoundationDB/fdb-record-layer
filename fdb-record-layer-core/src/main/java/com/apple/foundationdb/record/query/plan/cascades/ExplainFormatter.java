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
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.NestedToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.OptionalWhiteSpaceToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.ToStringToken;
import com.apple.foundationdb.record.query.plan.cascades.ExplainTokens.WhiteSpaceToken;

import javax.annotation.Nonnull;

/**
 * A formatter for tokens.
 */
public interface ExplainFormatter {

    void registerAlias(@Nonnull CorrelationIdentifier alias);

    void registerAliasExplicitly(@Nonnull CorrelationIdentifier alias, @Nonnull String symbol);

    @Nonnull
    String getSymbolForAlias(@Nonnull CorrelationIdentifier alias);

    void pushScope();

    void popScope();

    @Nonnull
    @SuppressWarnings("unused")
    default String enterNested(@Nonnull final NestedToken nestedToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String leaveNested(@Nonnull final NestedToken nestedToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String enterWhitespace(@Nonnull final WhiteSpaceToken whiteSpaceToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String leaveWhiteSpace(@Nonnull final WhiteSpaceToken whiteSpaceToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String enterOptionalWhitespace(@Nonnull final OptionalWhiteSpaceToken optionalWhiteSpaceToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String leaveOptionalWhitespace(@Nonnull final OptionalWhiteSpaceToken optionalWhiteSpaceToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String enterIdentifier(@Nonnull final IdentifierToken identifierToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String leaveIdentifier(@Nonnull final IdentifierToken identifierToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String enterCommaLike(@Nonnull final CommaLikeToken commaLikeToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String leaveCommaLike(@Nonnull final CommaLikeToken commaLikeToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String enterAliasDefinition(@Nonnull final AliasDefinitionToken aliasDefinitionToken) {
        return "";
    }

    @Nonnull
    @SuppressWarnings("unused")
    default String leaveAliasDefinition(@Nonnull final AliasDefinitionToken aliasDefinitionToken) {
        return "";
    }

    @Nonnull
    default String enterAliasReference(@Nonnull final AliasReferenceToken aliasReferenceToken) {
        return "";
    }

    @Nonnull
    default String leaveAliasReference(@Nonnull final AliasReferenceToken aliasReferenceToken) {
        return "";
    }

    @Nonnull
    default String enterBrackets(@Nonnull final BracketsToken bracketsToken) {
        return "";
    }

    @Nonnull
    default String leaveBrackets(@Nonnull final BracketsToken bracketsToken) {
        return "";
    }

    @Nonnull
    default String enterToString(@Nonnull final ToStringToken toStringToken) {
        return "";
    }

    @Nonnull
    default String leaveToString(@Nonnull final ToStringToken toStringToken) {
        return "";
    }
}
