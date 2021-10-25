/*
 * EvaluationContextBuilder.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.norse.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A builder for {@link EvaluationContext}.
 * <pre><code>
 * context.childBuilder().setBinding("x", x).build()
 * </code></pre>
 */
@API(API.Status.MAINTAINED)
public class EvaluationContextBuilder {
    @Nonnull
    protected final Bindings.Builder bindings;
    @Nonnull
    protected final DynamicSchema dynamicSchema;

    /**
     * Create an empty builder.
     */
    protected EvaluationContextBuilder() {
        this.bindings = Bindings.newBuilder();
        this.dynamicSchema = DynamicSchema.empty();
    }

    /**
     * Create a builder based on an existing {@link EvaluationContext}.
     * This ensures that the resulting <code>EvaluationContext</code>
     * has all of the bindings contained in the original context (except
     * for those which have had their value over-ridden).
     * @param original the original {@link EvaluationContext} to build a new one around
     */
    protected EvaluationContextBuilder(@Nonnull EvaluationContext original) {
        this.bindings = original.getBindings().childBuilder();
        this.dynamicSchema = original.getDynamicSchema();
    }

    /**
     * Get the current binding for some parameter in the current {@link Bindings}.
     * This will reflect any mutations that have been made to the state
     * through calls to {@link #setBinding(String, Object)} since the
     * builder was created.
     *
     * @param name the name of the parameter to retrieve the binding of
     * @return the current value bound to the given parameter
     * @see EvaluationContext#getBinding(String)
     */
    @Nullable
    public Object getBinding(@Nonnull String name) {
        return bindings.get(name);
    }

    /**
     * Bind one name to a value. This mutation will be
     * reflected in the {@link EvaluationContext} returned by
     * calling {@link #build()}.
     *
     * @param name the name of the binding
     * @param value the value to associate with the name
     * @return this <code>EvaluationContextBuilder</code>
     */
    @Nonnull
    public EvaluationContextBuilder setBinding(@Nonnull String name, @Nullable Object value) {
        bindings.set(name, value);
        return this;
    }

    public EvaluationContextBuilder setBinding(@Nonnull CorrelationIdentifier alias, @Nullable Object value) {
        return setBinding(Bindings.Internal.CORRELATION.bindingName(alias.getId()), value);
    }

    /**
     * Construct an {@link EvaluationContext} with updated bindings.
     * This should include all bindings specified though the original
     * {@link EvaluationContext} included in this object's constructor
     * as well as any bindings that have been added through calls to
     * {@link #setBinding(String, Object)}. All other state included
     * in the context should remain the same.
     *
     * @return an {@link EvaluationContext} with updated bindings
     */
    @Nonnull
    public EvaluationContext build() {
        return EvaluationContext.forBindingsAndDynamicSchema(bindings.build(), dynamicSchema);
    }
}
