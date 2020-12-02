/*
 * OneOfThemWithComparison.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpandedPredicates;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.predicates.ObjectValue;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * A {@link QueryComponent} that evaluates a {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison} against each of the values of a repeated field and is satisfied if any of those are.
 */
@API(API.Status.MAINTAINED)
public class OneOfThemWithComparison extends BaseRepeatedField implements ComponentWithComparison {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("One-Of-Them-With-Comparison");

    @Nonnull
    private final Comparisons.Comparison comparison;

    public OneOfThemWithComparison(@Nonnull String fieldName, @Nonnull Comparisons.Comparison comparison) {
        this(fieldName, Field.OneOfThemEmptyMode.EMPTY_UNKNOWN, comparison);
    }

    public OneOfThemWithComparison(@Nonnull String fieldName, Field.OneOfThemEmptyMode emptyMode, @Nonnull Comparisons.Comparison comparison) {
        super(fieldName, emptyMode);
        this.comparison = comparison;
    }

    @Override
    @Nullable
    public <M extends Message> Boolean evalMessage(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                   @Nullable FDBRecord<M> record, @Nullable Message message) {
        if (message == null ) {
            return getComparison().eval(store, context, null);
        }
        List<Object> values = getValues(message);
        if (values == null) {
            return null;
        } else {
            for (Object value : values) {
                final Boolean val = getComparison().eval(store, context, value);
                if (val != null && val) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void validate(@Nonnull Descriptors.Descriptor descriptor) {
        final Descriptors.FieldDescriptor field = validateRepeatedField(descriptor);
        requirePrimitiveField(field);
        getComparison().validate(field, true);
    }

    @Override
    @Nonnull
    public Comparisons.Comparison getComparison() {
        return comparison;
    }

    @Override
    public QueryComponent withOtherComparison(Comparisons.Comparison comparison) {
        return new OneOfThemWithComparison(getFieldName(), comparison);
    }

    @Override
    public ExpandedPredicates normalizeForPlanner(@Nonnull final CorrelationIdentifier baseAlias, @Nonnull final List<String> fieldNamePrefix) {
        List<String> fieldNames = ImmutableList.<String>builder()
                .addAll(fieldNamePrefix)
                .add(getFieldName())
                .build();
        final Quantifier childBase = Quantifier.forEach(GroupExpressionRef.of(new ExplodeExpression(baseAlias, fieldNames)));
        final SelectExpression selectExpression = ExpandedPredicates.ofPredicate(new ValuePredicate(
                new ObjectValue(childBase.getAlias()), comparison)).buildSelectWithBase(childBase);
        final Quantifier.Existential childQuantifier = Quantifier.existential(GroupExpressionRef.of(selectExpression));

        QueryComponent withPrefix = this;
        for (int i = fieldNamePrefix.size() - 1; i >= 0;  i--) {
            final String fieldName = fieldNames.get(i);
            withPrefix = Query.field(fieldName).matches(withPrefix);
        }

        return ExpandedPredicates.ofPredicateAndQuantifier(new ExistsPredicate(childQuantifier.getAlias(), withPrefix), childQuantifier);
    }

    @Override
    public String toString() {
        return "one of " + getFieldName() + " " + getComparison();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OneOfThemWithComparison that = (OneOfThemWithComparison) o;
        return Objects.equals(getComparison(), that.getComparison());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getComparison());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getComparison().planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return super.basePlanHash(hashKind, BASE_HASH, comparison);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }
}
