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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.google.common.collect.ImmutableMap;

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
@API(API.Status.UNSTABLE)
public class EvaluationContext {
    @Nonnull
    private final Bindings bindings;

    @Nonnull
    private final TypeRepository typeRepository;

    @Nonnull
    private final ImmutableMap<String, FDBRecordStoreBase<?>> auxiliaryStores;

    public static final EvaluationContext EMPTY = new EvaluationContext(Bindings.EMPTY_BINDINGS, TypeRepository.EMPTY_SCHEMA, ImmutableMap.of());

    /**
     * Get an empty evaluation context.
     * @return an evaluation context with no bindings
     */
    @SuppressWarnings("squid:S1845") // The field and the function mean the same thing.
    public static EvaluationContext empty() {
        return EMPTY;
    }

    private EvaluationContext(@Nonnull Bindings bindings, @Nonnull TypeRepository typeRepository,
                              @Nonnull ImmutableMap<String, FDBRecordStoreBase<?>> auxiliaryStores) {
        this.bindings = bindings;
        this.typeRepository = typeRepository;
        this.auxiliaryStores = auxiliaryStores;
    }

    /**
     * Create a new {@link EvaluationContext} around a given set of {@link Bindings}
     * from parameter names to values.
     * @param bindings a mapping from parameter name to values
     * @return a new evaluation context with the bindings
     */
    @Nonnull
    public static EvaluationContext forBindings(@Nonnull Bindings bindings) {
        return new EvaluationContext(bindings, TypeRepository.EMPTY_SCHEMA, ImmutableMap.of());
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
        return new EvaluationContext(bindings, typeRepository, ImmutableMap.of());
    }

    @Nonnull
    public static EvaluationContext forBindingsAndTypeRepository(@Nonnull Bindings bindings, @Nonnull TypeRepository typeRepository,
                                                                 @Nonnull ImmutableMap<String, FDBRecordStoreBase<?>> auxiliaryStores) {
        return new EvaluationContext(bindings, typeRepository, auxiliaryStores);
    }

    @Nonnull
    public static EvaluationContext forTypeRepository(@Nonnull TypeRepository typeRepository) {
        return new EvaluationContext(Bindings.EMPTY_BINDINGS, typeRepository, ImmutableMap.of());
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
        return new EvaluationContext(Bindings.newBuilder().set(bindingName, value).build(), TypeRepository.EMPTY_SCHEMA, ImmutableMap.of());
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
     * @param type the type of the parameter
     * @param alias the correlation identifier
     *
     * @return the value bound to the given parameter
     */
    public Object getBinding(@Nonnull final Bindings.Internal type, @Nonnull final CorrelationIdentifier alias) {
        return bindings.get(type.bindingName(alias.getId()));
    }

    /**
     * Whether a value is bound to the provided parameter.
     *
     * @param name the name of the parameter to check
     * @return whether a value is bound to the given parameter
     * @see Bindings#containsBinding(String) 
     */
    public boolean containsBinding(@Nonnull final String name) {
        return bindings.containsBinding(name);
    }

    /**
     * Whether the bindings contain a given special internal correlation.
     *
     * @param type the type of the parameter
     * @param alias the parameter's alias
     * @return whether the bindings contain that special correlation value
     */
    public boolean containsBinding(@Nonnull final Bindings.Internal type, @Nonnull final CorrelationIdentifier alias) {
        return containsBinding(type.bindingName(alias.getId()));
    }

    /**
     * Whether a value is bound to the given constant.
     *
     * @param alias the alias of the constant map
     * @param constantId the identity of the constant within the map
     * @return whether a value is bound to the given constant
     */
    public boolean containsConstantBinding(@Nonnull final CorrelationIdentifier alias, @Nonnull final String constantId) {
        if (!containsBinding(Bindings.Internal.CONSTANT, alias)) {
            return false;
        }
        final var constantsMap = getConstantsMap(alias);
        return constantsMap.containsKey(constantId);
    }


    /**
     * Dereferences the constant.
     *
     * @param alias the correlation identifier
     * @param constantId the constant id
     * @return de-referenced constant
     */
    @Nullable
    public Object dereferenceConstant(@Nonnull final CorrelationIdentifier alias, @Nonnull final String constantId) {
        final var constantsMap = getConstantsMap(alias);
        return constantsMap.get(constantId);
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private Map<String, ?> getConstantsMap(@Nonnull final CorrelationIdentifier alias) {
        return (Map<String, ?>) getBinding(Bindings.Internal.CONSTANT, alias);
    }

    @Nonnull
    public TypeRepository getTypeRepository() {
        return typeRepository;
    }

    /**
     * Returns the auxiliary store bound to the given schema name, or {@code null} if no such
     * store has been injected. Used by {@code RecordQueryStoreBindingPlan} to redirect execution
     * to a secondary schema's record store.
     *
     * @param schemaName the name of the secondary schema
     * @return the bound store, or {@code null}
     */
    @Nullable
    public FDBRecordStoreBase<?> getAuxiliaryStore(@Nonnull final String schemaName) {
        return auxiliaryStores.get(schemaName);
    }

    /**
     * Returns a new {@link EvaluationContext} identical to this one except that the given
     * auxiliary stores are injected. Existing bindings and type repository are preserved.
     *
     * @param stores map from schema name to pre-opened record store
     * @return new context with auxiliary stores
     */
    @Nonnull
    public EvaluationContext withAuxiliaryStores(@Nonnull final ImmutableMap<String, FDBRecordStoreBase<?>> stores) {
        return new EvaluationContext(bindings, typeRepository, stores);
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
        return new EvaluationContext(
                bindings.childBuilder().set(bindingName, value).build(),
                typeRepository,
                auxiliaryStores);
    }

    /**
     * Create a new <code>EvaluationContext</code> with an additional binding.
     * The returned context will have all of the same state as the current
     * context included all bindings except that it will bind an additional
     * parameter to an additional value.
     *
     * @param type the type of the binding.
     * @param alias the alias determining the binding name to add
     * @param value the value to bind the name to
     *
     * @return a new <code>EvaluationContext</code> with the new binding
     */
    public EvaluationContext withBinding(final Bindings.Internal type, @Nonnull CorrelationIdentifier alias, @Nullable Object value) {
        return withBinding(type.bindingName(alias.getId()), value);
    }
}
