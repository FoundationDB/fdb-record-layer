/*
 * QueryKeyExpressionWithComparison.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.util.HashUtils;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A {@link QueryComponent} that implements a {@link Comparisons.Comparison} against a {@link QueryableKeyExpression}.
 */
@API(API.Status.EXPERIMENTAL)
public class QueryKeyExpressionWithComparison implements ComponentWithComparison {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Query-Key-Expression-With-Comparison");

    @Nonnull
    private final QueryableKeyExpression keyExpression;
    @Nonnull
    private final Comparisons.Comparison comparison;

    public QueryKeyExpressionWithComparison(@Nonnull QueryableKeyExpression keyExpression, @Nonnull Comparisons.Comparison comparison) {
        this.keyExpression = keyExpression;
        this.comparison = keyExpression.evalForQueryAsTuple() ? new Comparisons.MultiColumnComparison(comparison) : comparison;
    }

    @Nonnull
    public QueryableKeyExpression getKeyExpression() {
        return keyExpression;
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable FDBRecord<M> rec, @Nullable Message message) {
        return getComparison().eval(store, context, keyExpression.evalForQuery(store, context, rec, message));
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        keyExpression.validate(descriptor);
    }

    @Override
    @Nonnull
    public Comparisons.Comparison getComparison() {
        return this.comparison;
    }

    @Nonnull
    @Override
    public GraphExpansion expand(@Nonnull final Quantifier.ForEach baseQuantifier,
                                 @Nonnull final Supplier<Quantifier.ForEach> outerQuantifierSupplier,
                                 @Nonnull final List<String> fieldNamePrefix) {
        return GraphExpansion.ofPredicate(keyExpression.toValue(baseQuantifier, fieldNamePrefix).withComparison(comparison));
    }

    @Override
    public String toString() {
        return keyExpression + " " + getComparison();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryKeyExpressionWithComparison that = (QueryKeyExpressionWithComparison) o;
        return Objects.equals(keyExpression, that.keyExpression) &&
               Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyExpression, getComparison());
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return keyExpression.planHash(mode) + getComparison().planHash(mode);
            case FOR_CONTINUATION:
                return PlanHashable.planHash(mode, BASE_HASH, keyExpression, getComparison());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public int queryHash(@Nonnull final QueryHashKind hashKind) {
        return HashUtils.queryHash(hashKind, BASE_HASH, keyExpression, getComparison());
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        return new QueryKeyExpressionWithComparison(keyExpression, comparison);
    }

    @Override
    public String getName() {
        return keyExpression.getName();
    }
}
