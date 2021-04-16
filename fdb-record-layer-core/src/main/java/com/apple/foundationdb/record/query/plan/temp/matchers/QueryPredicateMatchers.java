/*
 * QueryPredicateMatchers.java
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

package com.apple.foundationdb.record.query.plan.temp.matchers;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * A <code>BindingMatcher</code> is an expression that can be matched against a
 * {@link RelationalExpression} tree, while binding certain expressions/references in the tree to expression matcher objects.
 * The bindings can be retrieved from the rule call once the binding is matched.
 *
 * <p>
 * Extreme care should be taken when implementing <code>ExpressionMatcher</code>, since it can be very delicate.
 * In particular, expression matchers may (or may not) be reused between successive rule calls and should be stateless.
 * Additionally, implementors of <code>ExpressionMatcher</code> must use the (default) reference equals.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class QueryPredicateMatchers {
    private QueryPredicateMatchers() {
        // do not instantiate
    }

    public static TypedMatcher<QueryPredicate> anyPredicate() {
        return ofType(QueryPredicate.class);
    }

    public static <P extends QueryPredicate> TypedMatcher<P> ofType(@Nonnull final Class<P> bindableClass) {
        return new TypedMatcher<>(bindableClass);
    }

    public static <P extends QueryPredicate, C extends Collection<? extends QueryPredicate>> BindingMatcher<P> ofTypeWithChildren(@Nonnull final Class<P> bindableClass,
                                                                                                                                  @Nonnull final BindingMatcher<C> downstream) {
        return TypedMatcherWithExtractAndDownstream.of(bindableClass,
                QueryPredicate::getChildren,
                downstream);
    }
}
