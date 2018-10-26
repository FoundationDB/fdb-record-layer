/*
 * EvaluationContext.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * A context for query evaluation.
 * <p>
 * The primary state of an evaluation context is a set of parameter {@link Bindings}.
 * </p>
 * @see com.apple.foundationdb.record.query.expressions.QueryComponent#eval
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan#execute
 */
@API(API.Status.MAINTAINED)
public abstract class EvaluationContext {
    @Nonnull
    private final Bindings bindings;

    /**
     * Construct a new {@link EvaluationContext} around a given set of {@link Bindings}
     * from parameter names to values. This constructor exists for implementations
     * of this abstract class to pass state about their parameter bindings to the
     * super-class interface. Users of this class should use a builder of one of
     * this class's children.
     * @param bindings a mapping from parameter name to values
     */
    protected EvaluationContext(@Nonnull Bindings bindings) {
        this.bindings = bindings;
    }

    /**
     * Retrieve the mapping from parameter names to values associated with
     * this context.
     *
     * @return a mapping from parameter names to to values
     */
    @Nonnull
    public Bindings getBindings() {
        return bindings;
    }

    /**
     * Get the value bound to a single parameter.
     *
     * @param name the name of the parameter to retrieve the binding of
     * @return the value bound to the given parameter
     * @see Bindings#get(String)
     */
    @Nullable
    public Object getBinding(@Nonnull String name) {
        return bindings.get(name);
    }

    /**
     * Get the {@link Executor} to use when executing asynchronous calls.
     *
     * @return the {@link Executor} associated with this context
     */
    @Nonnull
    public Executor getExecutor() {
        return ForkJoinPool.commonPool();
    }

    /**
     * Create a new <code>EvaluationContext</code> with a new set of parameter bindings.
     * Implementations should provide an implementation of this method
     * that ensures that all data contained within the context other than the
     * bindings is transferred over to the new context.
     *
     * @param newBindings the complete mapping of new {@link Bindings} to associate with the new context
     * @return a new context with the given bindings
     */
    @Nonnull
    protected abstract EvaluationContext withBindings(@Nonnull Bindings newBindings);

    /**
     * Construct a builder from this context. This allows the user to create
     * a new <code>EvaluationContext</code> that has all of the same data
     * as the current context except for a few modifications expressed as
     * mutations made to the builder.
     *
     * @return a builder for this class based on this instance
     */
    @Nonnull
    public EvaluationContextBuilder childBuilder() {
        return new EvaluationContextBuilder(this);
    }

    /**
     * Create a new <code>EvaluationContext</code> with an additional binding.
     * The returned context will have all of the same state as the current
     * context included all bindings except that it will bind an additional
     * parameter to an additional value.
     *
     * @param bindingName the binding name to add
     * @param value the value to bind the name to
     * @return a new <code>EvaluationContext</code> with the new binding
     */
    public EvaluationContext withBinding(@Nonnull String bindingName, @Nullable Object value) {
        return childBuilder().setBinding(bindingName, value).build();
    }

}
