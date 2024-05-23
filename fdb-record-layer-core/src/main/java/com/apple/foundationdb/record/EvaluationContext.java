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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * A context for query evaluation.
 * <p>
 * The primary state of an evaluation context is a set of parameter {@link Bindings}.
 * </p>
 * @see com.apple.foundationdb.record.query.expressions.QueryComponent#eval
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan#execute
 */
@API(API.Status.MAINTAINED)
public class EvaluationContext {
    @Nonnull
    private final Bindings bindings;

    @Nonnull
    private final TypeRepository typeRepository;

    public static final EvaluationContext EMPTY = new EvaluationContext(Bindings.EMPTY_BINDINGS, TypeRepository.EMPTY_SCHEMA);

    /**
     * Get an empty evaluation context.
     * @return an evaluation context with no bindings
     */
    @SuppressWarnings("squid:S1845") // The field and the function mean the same thing.
    public static EvaluationContext empty() {
        return EMPTY;
    }

    private EvaluationContext(@Nonnull Bindings bindings, @Nonnull TypeRepository typeRepository) {
        this.bindings = bindings;
        this.typeRepository = typeRepository;
    }

    /**
     * Create a new {@link EvaluationContext} around a given set of {@link Bindings}
     * from parameter names to values.
     * @param bindings a mapping from parameter name to values
     * @return a new evaluation context with the bindings
     */
    @Nonnull
    public static EvaluationContext forBindings(@Nonnull Bindings bindings) {
        return new EvaluationContext(bindings, TypeRepository.EMPTY_SCHEMA);
    }

    /**
     * Create a new {@link EvaluationContext} around a given set of {@link Bindings} and a {@link TypeRepository}.
     * from parameter names to values.
     * @param bindings a mapping from parameter name to values
     * @param typeRepository a type repository
     * @return a new evaluation context with the bindings and the schema.
     */
    @Nonnull
    public static EvaluationContext forBindingsAndTypeRepository(@Nonnull Bindings bindings, @Nonnull TypeRepository typeRepository) {
        return new EvaluationContext(bindings, typeRepository);
    }

    @Nonnull
    public static EvaluationContext forTypeRepository(@Nonnull TypeRepository typeRepository) {
        return new EvaluationContext(Bindings.EMPTY_BINDINGS, typeRepository);
    }

    /**
     * Create a new <code>EvaluationContext</code> with a single binding.
     *
     * @param bindingName the binding name to add
     * @param value the value to bind the name to
     * @return a new <code>EvaluationContext</code> with the new binding
     */
    @Nonnull
    public static EvaluationContext forBinding(@Nonnull String bindingName, @Nullable Object value) {
        return new EvaluationContext(Bindings.newBuilder().set(bindingName, value).build(), TypeRepository.EMPTY_SCHEMA);
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
     * Get the value bound to a single parameter.
     *
     * @param alias the correlation identifier
     * @return the value bound to the given parameter
     */
    public Object getBinding(@Nonnull CorrelationIdentifier alias) {
        return bindings.get(Bindings.Internal.CORRELATION.bindingName(alias.getId()));
    }

    /**
     * Dereferences the constant.
     *
     * @param alias the correlation identifier
     * @param constantId the constant id
     * @return de-referenced constant
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public Object dereferenceConstant(@Nonnull final CorrelationIdentifier alias, @Nonnull final String constantId) {
        final var constantsMap = (Map<String, ?>)bindings.get(Bindings.Internal.CONSTANT.bindingName(alias.getId()));
        if (constantsMap == null) {
            throw new RecordCoreException("could not find constant in the evaluation context")
                    .addLogInfo(LogMessageKeys.KEY, "'" + alias.getId() + "' - '" + constantId + "'");
        }
        return constantsMap.get(constantId);
    }

    @Nonnull
    public TypeRepository getTypeRepository() {
        return typeRepository;
    }

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
     * Construct a new builder from this context.
     *
     * @return a builder for this class based on this instance
     */
    @Nonnull
    public static EvaluationContextBuilder newBuilder() {
        return new EvaluationContextBuilder();
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
    @Nonnull
    public EvaluationContext withBinding(@Nonnull String bindingName, @Nullable Object value) {
        return childBuilder().setBinding(bindingName, value).build(typeRepository);
    }

    /**
     * Create a new <code>EvaluationContext</code> with an additional binding.
     * The returned context will have all of the same state as the current
     * context included all bindings except that it will bind an additional
     * parameter to an additional value.
     *
     * @param alias the alias determining the binding name to add
     * @param value the value to bind the name to
     * @return a new <code>EvaluationContext</code> with the new binding
     */
    public EvaluationContext withBinding(@Nonnull CorrelationIdentifier alias, @Nullable Object value) {
        return childBuilder().setBinding(Bindings.Internal.CORRELATION.bindingName(alias.getId()), value).build(typeRepository);
    }
}
