/*
 * BuiltInWindowFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowedValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;


/**
 * A second-order built-in function that models aggregate and window functions with optional windowing semantics
 * (frame specification and sort order).
 *
 * <p>This class extends {@link CatalogedFunction}{@code <Object>} rather than {@link BuiltInFunction} because it
 * acts as a <em>second-order</em> function: its {@link #encapsulate(List)} method does not directly produce a value
 * but instead returns a first-order {@link BuiltInFunction} that, when subsequently called with the actual column
 * arguments, produces the result value. This two-phase invocation mirrors SQL's syntax where windowing clauses
 * (frame, order) are separate from the aggregate's column arguments.</p>
 *
 * <p><b>Invocation protocol:</b></p>
 * <ol>
 *   <li><b>Second-order call</b> &mdash; {@code encapsulate(windowArgs)} where {@code windowArgs} is a
 *       {@code List<Object>} containing zero or more of the following, in order:
 *       <ul>
 *         <li>An optional {@link WindowedValue.FrameSpecification} (if present, must be first)</li>
 *         <li>An optional {@code List<OrderingPart.RequestedOrderingPart>} representing the requested window
 *             sort order</li>
 *       </ul>
 *       Since Java lists do not permit {@code null} elements, the absence of a frame specification is conveyed
 *       by omitting it from the list (not by inserting {@code null}). This means a list containing only a sort
 *       order is valid.</li>
 *   <li><b>First-order call</b> &mdash; the returned {@link BuiltInFunction}{@code <T>} is called with the
 *       actual column arguments to produce the result {@code T}.</li>
 * </ol>
 *
 * <p>For contexts where no windowing semantics are needed (e.g. aggregate index expansion),
 * {@link #encapsulatePureAggregate()} provides a shortcut that skips the second-order step and returns a
 * first-order function with both frame specification and sort order set to {@code null}.</p>
 *
 * @param <T> the result type of the encapsulated function (e.g. {@code AggregateValue})
 */
public abstract class BuiltInWindowFunction<T extends Typed> extends CatalogedFunction<Object> {

    @Nonnull
    private final EncapsulationWindowFunction<T> encapsulationFunction;

    protected BuiltInWindowFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes,
                                    @Nonnull final EncapsulationWindowFunction<T> encapsulationFunction) {
        super(functionName, parameterTypes, null);
        this.encapsulationFunction = encapsulationFunction;
    }

    protected BuiltInWindowFunction(@Nonnull final String functionName, @Nonnull final List<Type> parameterTypes,
                                    @Nullable final Type variadicSuffixType, @Nonnull final EncapsulationWindowFunction<T> encapsulationFunction) {
        super(functionName, parameterTypes, variadicSuffixType);
        this.encapsulationFunction = encapsulationFunction;
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public BuiltInFunction<T> encapsulate(@Nonnull final List<Object> secondOrderArguments) {
        WindowedValue.FrameSpecification frameSpecification = null;
        List<OrderingPart.RequestedOrderingPart> sortOrder = null;

        int index = 0;
        if (index < secondOrderArguments.size() && secondOrderArguments.get(index) instanceof WindowedValue.FrameSpecification) {
            frameSpecification = (WindowedValue.FrameSpecification) secondOrderArguments.get(index);
            index++;
        }
        if (index < secondOrderArguments.size()) {
            SemanticException.check(secondOrderArguments.get(index) instanceof List<?>, SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);
            sortOrder = (List<OrderingPart.RequestedOrderingPart>) secondOrderArguments.get(index);
            index++;
        }
        SemanticException.check(index == secondOrderArguments.size(), SemanticException.ErrorCode.FUNCTION_UNDEFINED_FOR_GIVEN_ARGUMENT_TYPES);

        final WindowedValue.FrameSpecification finalFrameSpecification = frameSpecification;
        final List<OrderingPart.RequestedOrderingPart> finalSortOrder = sortOrder;

        return new BuiltInFunction<>(getFunctionName(), getParameterTypes(), getVariadicSuffixType(),
                (builtInFunction, firstOrderArguments) -> encapsulationFunction.encapsulate(this, finalFrameSpecification, finalSortOrder, firstOrderArguments));
    }

    /**
     * Returns a first-order {@link BuiltInFunction} with no windowing semantics (both frame specification and
     * sort order are {@code null}). This is used in contexts such as aggregate index expansion where the function
     * operates as a plain aggregate without any window clause.
     *
     * @return a first-order {@link BuiltInFunction} ready to be called with column arguments
     */
    @Nonnull
    public BuiltInFunction<T> encapsulatePureAggregate() {
        return new BuiltInFunction<>(getFunctionName(), getParameterTypes(), getVariadicSuffixType(),
                (builtInFunction, firstOrderArguments) -> encapsulationFunction.encapsulate(this, null, null, firstOrderArguments));
    }

    @Nonnull
    @Override
    public Typed encapsulate(@Nonnull final Map<String, Object> namedArguments) {
        throw new RecordCoreException("built-in window functions do not support named argument calling conventions");
    }
}
