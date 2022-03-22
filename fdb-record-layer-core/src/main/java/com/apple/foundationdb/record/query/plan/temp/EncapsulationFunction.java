/*
 * EncapsulationFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A functional interface that provides an encapsulation of a runtime computation against a set of arguments.
 * @param <T> The resulting type which carries the operation at runtime.
 */
public interface EncapsulationFunction<T extends Typed> {

    /**
     * Produces a {@link Typed} object that is able to carry out a computation against a list of arguments.
     *
     * @param parserContext The parsing context.
     * @param builtInFunction The function that refers to the computation.
     * @param arguments The arguments needed by the computation.
     * @return A {@link Typed} object capable of doing a runtime computation against a list of arguments.
     */
    T encapsulate(@Nonnull ParserContext parserContext, @Nonnull BuiltInFunction<T> builtInFunction, List<Typed> arguments);
}
