/*
 * OrderQueryKeyExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.OrderFunctionKeyExpression;
import com.apple.foundationdb.record.util.pair.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Handle {@link OrderFunctionKeyExpression} in a query.
 */
@API(API.Status.EXPERIMENTAL)
public class OrderQueryKeyExpression extends QueryKeyExpression {

    public OrderQueryKeyExpression(@Nonnull OrderFunctionKeyExpression keyExpression) {
        super(keyExpression);
    }

    /**
     * Turn a comparison on the inner key (usually a field) into the corresponding comparison on this
     * ordering key. In addition to encoding the comparand, the direction of the comparison may need to
     * be reversed if the ordering is (@code DESC).
     * @param comparison the comparison on the inner key expression
     * @return a comparison on this order key expression, plus a possible second comparison to exclude nulls,
     * or {@code null} if not supported
     */
    @Nullable
    public Pair<Comparisons.Comparison, Comparisons.Comparison> adjustComparison(@Nonnull Comparisons.Comparison comparison) {
        final boolean inverted = ((OrderFunctionKeyExpression)keyExpression).getDirection().isInverted();
        Comparisons.Type type = comparison.getType();
        switch (type) {
            case EQUALS:
            case NOT_EQUALS:
                break;
            case LESS_THAN:
                if (inverted) {
                    type = Comparisons.Type.GREATER_THAN;
                }
                break;
            case LESS_THAN_OR_EQUALS:
                if (inverted) {
                    type = Comparisons.Type.GREATER_THAN_OR_EQUALS;
                }
                break;
            case GREATER_THAN:
                if (inverted) {
                    type = Comparisons.Type.LESS_THAN;
                }
                break;
            case GREATER_THAN_OR_EQUALS:
                if (inverted) {
                    type = Comparisons.Type.LESS_THAN_OR_EQUALS;
                }
                break;
            case IS_NULL:
                return Pair.of(adjustedNullComparison(Comparisons.Type.EQUALS), null);
            case NOT_NULL:
                return Pair.of(adjustedNullComparison(Comparisons.Type.NOT_EQUALS), null);
            default:
                return null;
        }
        final QueryKeyExpressionWithComparison adjustedComponent;
        if (comparison instanceof Comparisons.ComparisonWithParameter) {
            adjustedComponent = parameterComparison(type, ((Comparisons.ComparisonWithParameter)comparison).getParameter());
        } else {
            adjustedComponent = simpleComparison(type, comparison.getComparand());
        }
        final Comparisons.Comparison adjustedComparison = adjustedComponent.getComparison();
        Comparisons.Comparison nullComparison = null;
        switch (type) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUALS:
                if (((OrderFunctionKeyExpression)keyExpression).getDirection().isNullsFirst()) {
                    // nulls on less end
                    nullComparison = adjustedNullComparison(Comparisons.Type.GREATER_THAN);
                }
                break;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUALS:
                if (((OrderFunctionKeyExpression)keyExpression).getDirection().isNullsLast()) {
                    // nulls on greater end
                    nullComparison = adjustedNullComparison(Comparisons.Type.LESS_THAN);
                }
                break;
            default:
                break;
        }
        return Pair.of(adjustedComparison, nullComparison);
    }

    @Nonnull
    private Comparisons.SimpleComparison adjustedNullComparison(@Nonnull Comparisons.Type type) {
        // super.nullComparison doesn't deal with getComparandConversionFunction.
        return new Comparisons.SimpleComparison(type, keyExpression.getComparandConversionFunction().apply(null));
    }
}
