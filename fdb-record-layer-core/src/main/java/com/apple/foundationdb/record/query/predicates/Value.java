package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.temp.Correlated;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;

import javax.annotation.Nonnull;

/**
 * A scalar value type.
 */
@API(API.Status.EXPERIMENTAL)
public interface Value extends Correlated<Value>, PlanHashable {
    @Nonnull
    default ValuePredicate withComparison(@Nonnull Comparisons.Comparison comparison) {
        return new ValuePredicate(this, comparison);
    }

    @Nonnull
    default ValueComparisonRangePredicate withParameterAlias(@Nonnull final CorrelationIdentifier parameterAlias) {
        return ValueComparisonRangePredicate.placeholder(this, parameterAlias);
    }
}
