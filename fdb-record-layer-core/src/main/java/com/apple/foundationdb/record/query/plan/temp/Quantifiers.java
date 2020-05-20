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
import com.apple.foundationdb.record.query.plan.temp.Quantifier.Existential;
import com.apple.foundationdb.record.query.plan.temp.Quantifier.ForEach;
import com.apple.foundationdb.record.query.plan.temp.Quantifier.Physical;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Auxiliary class containing factory methods and helpers fro {@link Quantifier}.
 */
public class Quantifiers {
    /**
     * Factory method to create a list of for-each quantifiers given a list of expression references these quantifiers should range over.
     * @param rangesOverExpressions iterable {@link ExpressionRef}s of {@link RelationalExpression}s
     * @param <E> type parameter to constrain expressions to {@link RelationalExpression}
     * @return a list of for-each quantifiers where each quantifier ranges over an expression reference contained in the list of
     *         references of relational expressions passed in in order of the references in that iterable.
     */
    @Nonnull
    public static <E extends RelationalExpression> List<ForEach> forEachQuantifiers(final Iterable<ExpressionRef<E>> rangesOverExpressions) {
        return fromExpressions(rangesOverExpressions, Quantifier::forEach);
    }

    /**
     * Factory method to create a list of existential quantifiers given a list of expression references these quantifiers should range over.
     * @param rangesOverPlans iterable {@link ExpressionRef}s of of {@link RelationalExpression}s.
     * @param <E> type parameter to constrain expressions to {@link RelationalExpression}
     * @return a list of physical quantifiers where each quantifier ranges over an expression reference contained in the list of
     *         references of relational expressions passed in in order of the references in that iterable.
     */
    @Nonnull
    public static <E extends RelationalExpression> List<Existential> existentialQuantifiers(final Iterable<? extends ExpressionRef<E>> rangesOverPlans) {
        return fromExpressions(rangesOverPlans, Quantifier::existential);
    }

    /**
     * Factory method to create a list of physical quantifiers given a list of expression references these quantifiers should range over.
     * @param rangesOverPlans iterable {@link ExpressionRef}s of {@link RecordQueryPlan}
     * @param <E> type parameter to constrain expressions to {@link RecordQueryPlan}
     * @return a list of physical quantifiers where each quantifier ranges over an expression reference contained in the list of
     *         references of record query plans passed in in order of the references in that iterable.
     */
    @Nonnull
    public static <E extends RecordQueryPlan> List<Physical> fromPlans(final Iterable<? extends ExpressionRef<E>> rangesOverPlans) {
        return fromExpressions(rangesOverPlans, Quantifier::physical);
    }

    /**
     * Factory method to create a list of quantifiers given a list of expression references these quantifiers should range over.
     * @param rangesOverExpressions iterable of {@link ExpressionRef}s the quantifiers will be created to range over
     * @param creator lambda to to be called for each expression reference contained in {@code rangesOverExpression}s
     *        to create the actual quantifier. This allows for callers to create different kinds of quantifier based
     *        on needs.
     * @param <E> type parameter to constrain expressions to {@link RelationalExpression}
     * @param <Q> the type of the quantifier to be created
     * @return a list of quantifiers where each quantifier ranges over an expression reference contained in the iterable of
     *         references passed in in order of iteration.
     */
    @Nonnull
    public static <E extends RelationalExpression, Q extends Quantifier> List<Q> fromExpressions(final Iterable<? extends ExpressionRef<E>> rangesOverExpressions,
                                                                                                 final Function<ExpressionRef<E>, Q> creator) {
        return StreamSupport
                .stream(rangesOverExpressions.spliterator(), false)
                .map(creator)
                .collect(Collectors.toList());
    }

    private Quantifiers() {
        // prevent instantiation
    }
}
