/*
 * QueryPredicateSimplificationRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.AbstractRuleCall;

import javax.annotation.Nonnull;

/**
 * Intermediate class that fixes the base type of the {@link AbstractRuleCall} to be {@link QueryPredicate}.
 * @param <TYPE> the type of the object that this rule helps simplify
 */
@API(API.Status.EXPERIMENTAL)
public abstract class QueryPredicateSimplificationRule<TYPE extends QueryPredicate> extends AbstractQueryPredicateRule<QueryPredicate, QueryPredicateSimplificationRuleCall, TYPE> {
    public QueryPredicateSimplificationRule(@Nonnull final BindingMatcher<TYPE> matcher) {
        super(matcher);
    }
}
