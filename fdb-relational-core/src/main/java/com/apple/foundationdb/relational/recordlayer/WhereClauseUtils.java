/*
 * WhereClauseUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.relational.api.WhereClause;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.AndClause;
import com.apple.foundationdb.relational.recordlayer.query.OrClause;
import com.apple.foundationdb.relational.recordlayer.query.ValueComparisonClause;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

/**
 * Package-private class to support conversions between Relational API Where clause expressions and RecordLayer
 * QueryComponent expressions.
 */
final class WhereClauseUtils {

    static QueryComponent convertClause(WhereClause clause) {
        if (clause instanceof ValueComparisonClause) {
            return convertValueComparison((ValueComparisonClause) clause);
        } else if (clause instanceof AndClause) {
            List<WhereClause> childClauses = ((AndClause) clause).getChildClauses();
            List<QueryComponent> childComponents = new ArrayList<>();
            for (WhereClause childClause : childClauses) {
                childComponents.add(convertClause(childClause));
            }

            return new AndComponent(childComponents);
        } else if (clause instanceof OrClause) {
            List<WhereClause> childClauses = ((OrClause) clause).getChildClauses();
            List<QueryComponent> childComponents = new ArrayList<>();
            for (WhereClause childClause : childClauses) {
                childComponents.add(convertClause(childClause));
            }

            return new OrComponent(childComponents);
        } else {
            throw new RelationalException("Cannot understand WhereClause of type " + clause.getClass(), RelationalException.ErrorCode.UNKNOWN);
        }
    }

    private static QueryComponent convertValueComparison(ValueComparisonClause vcc) {
        String field = vcc.getField();
        ValueComparisonClause.ComparisonType cType = vcc.getType();

        Comparisons.Type compType = convertComparisonType(cType);
        Object value = vcc.getComparisonValue();
        return new FieldWithComparison(field, new Comparisons.SimpleComparison(compType, value));
    }

    private static Comparisons.Type convertComparisonType(@Nonnull ValueComparisonClause.ComparisonType cType) {
        Comparisons.Type compType;
        switch (cType) {
            case EQUALS:
                compType = Comparisons.Type.EQUALS;
                break;
            case NOT_EQUALS:
                compType = Comparisons.Type.NOT_EQUALS;
                break;
            case GT:
                compType = Comparisons.Type.GREATER_THAN;
                break;
            case GREATER_OR_EQUALS:
                compType = Comparisons.Type.GREATER_THAN_OR_EQUALS;
                break;
            case LT:
                compType = Comparisons.Type.LESS_THAN;
                break;
            case LESS_OR_EQUALS:
                compType = Comparisons.Type.LESS_THAN_OR_EQUALS;
                break;
            case LIKE:
                compType = Comparisons.Type.TEXT_CONTAINS_PHRASE;
                break;
            case IS:
                compType = Comparisons.Type.IS_NULL;
                break;
            case IS_NOT:
                compType = Comparisons.Type.NOT_NULL;
                break;
            default:
                throw new IllegalArgumentException("Unexpected comparison type <" + cType + ">");
        }
        return compType;
    }

    private WhereClauseUtils() {
    }
}
