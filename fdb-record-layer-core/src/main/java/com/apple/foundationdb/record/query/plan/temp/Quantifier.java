/*
 * Quantifier.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A quantifier describes the data flow between the output of one {@link RelationalExpression} {@code R} and the
 * consumption of that data by another {@code RelationalExpression} {@code S}. {@code S} is said to own the quantifier, while the
 * quantifier is said to range over {@code R}. Quantifiers come in very few but very distinct flavors. All flavors are
 * implemented by static inner final classes in order to emulate a sealed trait.
 *
 * Quantifiers separate what it means to be producing versus consuming records. The expression a quantifier ranges over
 * produces records, the quantifier flows information (in a manner determined by the flavor) that is consumed by the expression
 * containing or owning the quantifier. That expression can consume the data in a way independent of how the data was
 * produced in the first place.
 *
 * A quantifier works closely with the expression that owns it. Depending on the semantics of the owning expression
 * it becomes possible to model correlations. For example, in a logical join expression the quantifier can provide a binding
 * of the record being currently consumed by the join's outer to other (inner) parts of the data flow that are also rooted
 * at the owning (join) expression.
 */
public abstract class Quantifier implements Bindable {
    /**
     * A quantifier that conceptually flows one record at a time from the expression it ranges over to
     * the owning expression.
     */
    public static final class ForEach extends Quantifier {
        private final ExpressionRef<? extends RelationalExpression> rangesOver;

        private ForEach(ExpressionRef<? extends RelationalExpression> rangesOver) {
            this.rangesOver = rangesOver;
        }

        @Override
        @Nonnull
        public ExpressionRef<? extends RelationalExpression> getRangesOver() {
            return rangesOver;
        }

        @Override
        @Nonnull
        public String getShorthand() {
            return "ƒ";
        }
    }

    /**
     * Factory method to create a for-each quantifier over a given expression reference containing relational
     * expressions.
     * @param rangesOver expression reference to {@link RelationalExpression}s
     * @return a for-each quantifier ranging over the given expression reference
     */
    @Nonnull
    public static ForEach forEach(final ExpressionRef<? extends RelationalExpression> rangesOver) {
        return new ForEach(rangesOver);
    }

    /**
     * A quantifier that conceptually flows exactly one record containing a boolean to the owning
     * expression indicating whether the sub-graph that the quantifier ranges over produced a non-empty or an empty
     * result. When the semantics of this quantifiers are realized in an execution strategy that strategy should
     * facilitate a boolean "short-circuit" mechanism as the result will be {@code record(true)} as soon as the sub-graph produces
     * the first record.
     */
    @SuppressWarnings("squid:S2160") // sonarqube thinks .equals() and hashCode() should be overwritten which is not necessary
    public static final class Existential extends Quantifier {
        private final ExpressionRef<? extends RelationalExpression> rangesOver;

        private Existential(final ExpressionRef<? extends RelationalExpression> rangesOver) {
            this.rangesOver = rangesOver;
        }

        @Override
        @Nonnull
        public ExpressionRef<? extends RelationalExpression> getRangesOver() {
            return rangesOver;
        }

        @Override
        @Nonnull
        public String getShorthand() {
            return "∃";
        }
    }

    /**
     * Factory method to create an existential quantifier over a given expression reference containing relational
     * expressions.
     * @param rangesOver expression reference to {@link RelationalExpression}s
     * @return a for-each quantifier ranging over the given expression reference
     */
    @Nonnull
    public static Existential existential(@Nonnull final ExpressionRef<? extends RelationalExpression> rangesOver) {
        return new Existential(rangesOver);
    }

    /**
     * Physical quantifier. This kind of quantifier is the conduit between two {@link RecordQueryPlan}s. It does
     * not have any associated semantics; all semantics and execution details must be subsumed
     * by the query plans themselves.
     */
    @SuppressWarnings("squid:S2160") // sonarqube thinks .equals() and hashCode() should be overwritten which is not necessary
    public static final class Physical extends Quantifier {
        private final ExpressionRef<? extends RecordQueryPlan> rangesOver;

        private Physical(@Nonnull final ExpressionRef<? extends RecordQueryPlan> rangesOver) {
            this.rangesOver = rangesOver;
        }

        @Override
        @Nonnull
        public ExpressionRef<? extends RecordQueryPlan> getRangesOver() {
            return rangesOver;
        }

        @Override
        @Nonnull
        public String getShorthand() {
            return "𝓅";
        }
    }

    /**
     * Factory method to create a physical quantifier over a given expression reference containing query plans.
     * @param rangesOver expression reference to {@link RecordQueryPlan}s
     * @return a physical quantifier ranging over the given reference
     */
    @Nonnull
    public static Physical physical(@Nonnull final ExpressionRef<? extends RecordQueryPlan> rangesOver) {
        return new Physical(rangesOver);
    }

    /**
     * Factory method to create a physical quantifier over newly created expression reference containing the given
     * record plan.
     * @param rangesOverPlan {@link RecordQueryPlan} the new quantifier should range over.
     * @return a physical quantifier ranging over a new expression reference containing the given expression reference
     */
    @Nonnull
    public static Physical physical(@Nonnull final RecordQueryPlan rangesOverPlan) {
        return new Physical(GroupExpressionRef.of(rangesOverPlan));
    }

    /**
     * Return the reference that the quantifier ranges over.
     * @return {@link ExpressionRef} this quantifier ranges over
     */
    @Nonnull
    public abstract ExpressionRef<? extends RelationalExpression> getRangesOver();

    /**
     * Return a short hand string for the quantifier. As a quantifier's semantics is usually quite subtle and should
     * not distract from expressions. For example, when a data flow is visualized the returned string should be <em>short</em>.
``
     * @return a short string representing the quantifier.
     */
    @Nonnull
    public abstract String getShorthand();

    @Nonnull
    @Override
    public Stream<PlannerBindings> bindTo(@Nonnull final ExpressionMatcher<? extends Bindable> matcher) {
        Stream<PlannerBindings> bindings = matcher.matchWith(this);
        return bindings.flatMap(outerBindings -> matcher.getChildrenMatcher().matches(ImmutableList.of(getRangesOver()))
                .map(outerBindings::mergedWith));
    }

    /**
     * Allow the computation of {@link PlannerProperty}s across the quantifiers in the data flow graph.
     * @param visitor the planner property that is being computed
     * @param <U> the type of the property being computed
     * @return the property
     */
    @Nullable
    public <U> U acceptPropertyVisitor(@Nonnull PlannerProperty<U> visitor) {
        if (visitor.shouldVisit(this)) {
            return visitor.evaluateAtQuantifier(this, getRangesOver().acceptPropertyVisitor(visitor));
        }
        return null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ForEach)) {
            return false;
        }
        final ForEach forEach = (ForEach)o;
        return Objects.equals(getRangesOver(), forEach.getRangesOver());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getRangesOver());
    }
}
