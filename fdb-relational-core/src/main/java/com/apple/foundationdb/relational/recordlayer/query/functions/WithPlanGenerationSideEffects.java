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

import com.apple.foundationdb.relational.recordlayer.query.Literals;

import javax.annotation.Nonnull;

/**
 Trait used by functions with plan generation side effects. Currently, functions can only have side effects of providing
 extra {@link com.apple.foundationdb.relational.recordlayer.query.OrderedLiteral}s that fix the state of the generated
 plan, saving it for subsequent expansion of the function. It is important check if a function has literals during plan
 generation. If so, combine them with the query’s literals.
 */
public interface WithPlanGenerationSideEffects {

    /**
     * Retrieve any extra literals that might have been either extracted away, or provided as a prepared parameter.
     * @return any extra function literals.
     */
    @Nonnull
    Literals getAuxiliaryLiterals();
}
