/*
 * ExpressionChildrenMatcher.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * An {@code ExpressionChildrenMatcher} describes how to match the children of a {@link RelationalExpression} (i.e., the
 * references returned by the {@link RelationalExpression#getQuantifiers()} method). Bindings can be
 * retrieved from the rule call using the {@code ExpressionChildMatcher} that produced them.
 *
 * <p>
 * In most cases, the most natural way to bind to the children of a planner expression is by defining a matcher for each
 * child. This behavior is implemented in the {@link ListChildrenMatcher} and exposed by the
 * {@link TypeMatcher#of(Class, ExpressionMatcher[])} helper method. However, this does not work when there is no
 * <i>a priori</i> bound on the number of children returned by {@link RelationalExpression#getQuantifiers()}
 * For example, an {@link com.apple.foundationdb.record.query.expressions.AndComponent} can have an arbitrary number of
 * other {@code QueryComponent}s as children.
 * </p>
 * <p>
 * Note that an {@code ExpressionChildrenMatcher} should generally only define how to distribute children to one or more
 * child matchers, and that these child matchers should generally match to the children themselves. For example, see
 * the implementation of {@link ListChildrenMatcher}. Extreme care should be taken when implementing an
 * {@code ExpressionChildrenMatcher}. In particular, expression matchers may (or may not) be reused between successive
 * rule calls and should be stateless. Additionally, implementors of <code>ExpressionMatcher</code> must use the
 * (default) reference {@code equals()} method.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public interface ExpressionChildrenMatcher {
    /**
     * Apply this matcher to the children provided by the given iterator and produce a stream of possible bindings.
     * If the match is not successful, produce an empty stream. Note that this method should not generally match to the
     * children themselves; instead, it should delegate that work to one or more inner {@link ExpressionMatcher}s.
     *
     * @param outerBindings preexisting bindings supplied by the caller
     * @param children a list of references to the children of a planner expression
     * @return a stream of the possible bindings from applying this match to the children in the given iterator
     */
    @Nonnull
    Stream<PlannerBindings> matches(@Nonnull PlannerBindings outerBindings, @Nonnull List<? extends Bindable> children);
}
