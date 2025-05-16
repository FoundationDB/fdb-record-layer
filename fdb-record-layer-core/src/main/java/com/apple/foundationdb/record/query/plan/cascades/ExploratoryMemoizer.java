/*
 * ExploratoryMemoizer.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.Memoizer.ReferenceBuilder;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.Collection;

/**
 * Interface to capture the parts of the memoizer API that only exploration rules are allowed to call.
 */
public interface ExploratoryMemoizer {
    /**
     * Memoize the given exploratory {@link RelationalExpression}. If a previously memoized expression is found that is
     * semantically equivalent to the expression that is passed in, the reference containing the previously memoized
     * expression is returned allowing the planner to reuse that reference. If no such previously memoized expression is
     * found, a new reference is created and returned to the caller.
     *
     * <p>
     * The expression that is passed in must be an exploratory expression of the current planner phase. If this method
     * is used in an incompatible context and a reused reference is returned, the effects of mutating such a reference
     * through other planner logic are undefined and should be avoided.
     * </p>
     *
     * @param expression exploratory expression to memoize
     *
     * @return a new or a reused reference
     */
    @Nonnull
    Reference memoizeExploratoryExpression(@Nonnull RelationalExpression expression);

    /**
     * Memoize the given collection of {@link RelationalExpression}s. If a reference of previously memoized expressions
     * is found that contains all of the given expressions, then this will return the existing reference. Otherwise,
     * this will add all of the expressions to the memoization structure and return a new reference.
     *
     * <p>
     * Like with {@link #memoizeExploratoryExpression(RelationalExpression)}, all of the passed in expressions must
     * be exploratory expressions of the current planner phase. See that method for details.
     * </p>
     *
     * @param expressions the collection of exploratory expressions to memoize
     * @return a new or reused reference
     * @see #memoizeExploratoryExpression(RelationalExpression)
     * */
    @Nonnull
    Reference memoizeExploratoryExpressions(@Nonnull Collection<? extends RelationalExpression> expressions);

    /**
     * Return a new {@link ReferenceBuilder} for exploratory expressions. The expression passed in is memoized when
     * the builder's {@link ReferenceBuilder#reference()} is called. The expression is treated as an exploratory
     * expression meaning that the reference that is eventually obtained may be a new {@link Reference} of a reused
     * {@link Reference}.
     * @param expression the expression to potentially memoize
     * @return a new {@link ReferenceBuilder}
     */
    @Nonnull
    ReferenceBuilder memoizeExploratoryExpressionBuilder(@Nonnull RelationalExpression expression);
}
