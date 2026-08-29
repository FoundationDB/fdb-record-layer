/*
 * ExplainTokensWithPrecedence.java
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

import com.apple.foundationdb.record.query.plan.cascades.values.Value;


/**
 * Holder class for the result of a call to {@link Value#explain()} and derivatives.
 */
public class ExplainTokensWithPrecedence {
    /**
     * Precedence which informs the explain logic whether to put parentheses around explain terms or not.
     */
    public enum Precedence {
        NEVER_PARENS(-2),
        ALWAYS_PARENS(-1),
        DOT(0),
        UNARY_MINUS_BITWISE_NOT(1),
        BITWISE_XOR(2),
        MULTIPLICATIVE(3),
        ADDITIVE(4),
        SHIFT(5),
        BITWISE_AND(6),
        BITWISE_OR(7),
        COMPARISONS(8),
        BETWEEN(9),
        NOT(10),
        AND(11),
        XOR(12),
        OR(13),
        ASSIGNMENT(13);

        private final int precedenceOrdinal;

        Precedence(final int precedenceOrdinal) {
            this.precedenceOrdinal = precedenceOrdinal;
        }

        public int getPrecedenceOrdinal() {
            return precedenceOrdinal;
        }

        public ExplainTokens parenthesizeChild(final ExplainTokensWithPrecedence childExplainInfo) {
            return parenthesizeChild(childExplainInfo, false);
        }

        public ExplainTokens parenthesizeChild(final ExplainTokensWithPrecedence childExplainInfo, final boolean isStrict) {
            return parenthesizeChild(childExplainInfo.getPrecedence(), childExplainInfo.getExplainTokens(), isStrict);
        }

        public ExplainTokens parenthesizeChild(final Precedence childPrecedence,
                                               final ExplainTokens childExplainTokens, final boolean isStrict) {
            if (childPrecedence == ALWAYS_PARENS ||
                    (isStrict && childPrecedence.getPrecedenceOrdinal() == getPrecedenceOrdinal()) ||
                    childPrecedence.getPrecedenceOrdinal() > getPrecedenceOrdinal()) {
                return new ExplainTokens().addOpeningParen().addOptionalWhitespace().addNested(childExplainTokens)
                        .addOptionalWhitespace().addClosingParen();
            }
            return childExplainTokens;
        }
    }

    private final Precedence precedence;

    private final ExplainTokens explainTokens;

    private ExplainTokensWithPrecedence(final Precedence precedence, final ExplainTokens explainTokens) {
        this.precedence = precedence;
        this.explainTokens = explainTokens;
    }

    public Precedence getPrecedence() {
        return precedence;
    }

    public ExplainTokens getExplainTokens() {
        return explainTokens;
    }

    public static ExplainTokensWithPrecedence of(final ExplainTokens explainTokens) {
        return of(Precedence.NEVER_PARENS, explainTokens);
    }

    public static ExplainTokensWithPrecedence of(final Precedence precedence, final ExplainTokens explainTokens) {
        return new ExplainTokensWithPrecedence(precedence, explainTokens);
    }
}
