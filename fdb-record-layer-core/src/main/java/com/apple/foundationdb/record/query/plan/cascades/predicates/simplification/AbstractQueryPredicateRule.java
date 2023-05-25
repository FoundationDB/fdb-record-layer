/*
 * AbstractQueryPredicateRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.predicates.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.AbstractRule;

import javax.annotation.Nonnull;

/**
 * Tag class to bind the base {@code BASE} to {@link QueryPredicate}.
 * @param <RESULT> the type of the result being yielded by rule implementations
 * @param <CALL> the type of rule call that is used in calls to {@link #onMatch(PlannerRuleCall)} )}
 * @param <TYPE> a value type of all possible subclasses of {@link QueryPredicate} that this rule could match
 * @see com.apple.foundationdb.record.query.plan.cascades
 * @see PlannerRuleCall
 */
@API(API.Status.EXPERIMENTAL)
public abstract class AbstractQueryPredicateRule<RESULT, CALL extends AbstractQueryPredicateRuleCall<RESULT, CALL>, TYPE extends QueryPredicate> extends AbstractRule<RESULT, CALL, QueryPredicate, TYPE> {
    public AbstractQueryPredicateRule(@Nonnull BindingMatcher<TYPE> matcher) {
        super(matcher);
    }
}
