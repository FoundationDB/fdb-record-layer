/*
 * WithPlanGenerationSideEffects.java
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.relational.recordlayer.query.Literals;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Trait used to capture side effects resulting from the integration of a
 * {@link com.apple.foundationdb.record.query.plan.cascades.UserDefinedFunction} into a query plan.
 * <br>
 * These side effects necessitate processing to ensure proper query plan construction and optimization.
 * Currently, the only side effect to consider is the set of {@link com.apple.foundationdb.relational.recordlayer.query.OrderedLiteral}
 * instances.  These literals represent either extracted constant values or prepared parameters encountered during
 * the function's processing.
 */
public interface WithPlanGenerationSideEffects {

    /**
     * Retrieve any extra literals that might have been either extracted away, or provided as a prepared parameter.
     * @return any extra function literals.
     */
    @Nonnull
    Literals getAuxiliaryLiterals();

    /**
     * Retrieve any value-free (unbound) {@link ConstantObjectValue}s produced inside the function body — e.g. a typed
     * signature parameter warmed with no value. Unlike ordinary literals these carry no {@link
     * com.apple.foundationdb.relational.recordlayer.query.OrderedLiteral}, so they are invisible to {@link
     * #getAuxiliaryLiterals}; they must be propagated separately so the enclosing query's plan constraint carries their
     * {@code OfType}/nullness. Defaults to empty for functions that produce none.
     * @return any value-free constant object values produced by the function body.
     */
    @Nonnull
    default List<ConstantObjectValue> getAuxiliaryConstantObjectValues() {
        return List.of();
    }

}
