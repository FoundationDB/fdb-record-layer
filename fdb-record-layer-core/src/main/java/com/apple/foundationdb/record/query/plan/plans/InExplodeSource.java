/*
 * InExplodeSource.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PInExplodeSource;
import com.apple.foundationdb.record.planprotos.PInSource;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArraySortValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ExplodeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ParameterObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.explain.ExplainLevel;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Source of values for an "IN" query that is planned from a quantifier ranging over an {@link ExplodeValue}. Like
 * the explode itself, it binds the struct of each element of the collection, so the correlated inner plan can read
 * the element and, if any, the ordinal the same way it would read them from the explode.
 * <p>
 * The source is sorted if the collection of the explode is an {@link ArraySortValue}. Sorting the collection would
 * renumber the ordinals, which is why only the collections of explodes that are not
 * {@linkplain ExplodeValue#hasOrdinality() with ordinality} may be sorted.
 * </p>
 */
@API(API.Status.INTERNAL)
public class InExplodeSource extends InComparandSource {
    @Nonnull
    private static final ObjectPlanHash OBJECT_PLAN_HASH_IN_EXPLODE_SOURCE = new ObjectPlanHash("In-Explode");

    @Nonnull
    private final ExplodeValue explodeValue;

    protected InExplodeSource(@Nonnull final PlanSerializationContext serializationContext,
                              @Nonnull final PInExplodeSource inExplodeSourceProto) {
        super(serializationContext, Objects.requireNonNull(inExplodeSourceProto.getSuper()));
        this.explodeValue = (ExplodeValue)Value.fromValueProto(serializationContext,
                Objects.requireNonNull(inExplodeSourceProto.getExplodeValue()));
    }

    public InExplodeSource(@Nonnull final String bindingName, @Nonnull final ExplodeValue explodeValue) {
        super(bindingName, new Comparisons.ValueComparison(Comparisons.Type.IN, explodeValue.getCollectionValue()));
        Verify.verify(!explodeValue.hasOrdinality() || !(explodeValue.getCollectionValue() instanceof ArraySortValue));
        this.explodeValue = explodeValue;
    }

    /**
     * Returns the collection the explode of this source ranges over before it is sorted.
     * @return the unsorted collection
     */
    @Nonnull
    private Value getUnsortedCollectionValue() {
        final Value collectionValue = explodeValue.getCollectionValue();
        return collectionValue instanceof ArraySortValue ? ((ArraySortValue)collectionValue).getChild() : collectionValue;
    }

    /**
     * Returns the name of the parameter this source explodes, if it explodes a parameter.
     * @return the parameter name or {@code null}
     */
    @Nullable
    public String getParameterName() {
        final Value collectionValue = getUnsortedCollectionValue();
        return collectionValue instanceof ParameterObjectValue
                ? ((ParameterObjectValue)collectionValue).getParameterName()
                : null;
    }

    /**
     * Returns the elements of the literal list this source explodes, in the order the source produces them, if it
     * explodes a literal list.
     * @return the elements or {@code null}
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public List<Object> getLiteralValues() {
        final Value collectionValue = getUnsortedCollectionValue();
        if (collectionValue instanceof LiteralValue<?> &&
                ((LiteralValue<?>)collectionValue).getLiteralValue() instanceof List<?>) {
            final List<Object> elements = (List<Object>)((LiteralValue<?>)collectionValue).getLiteralValue();
            return isSorted() ? ArraySortValue.sort(elements, isReverse()) : elements;
        }
        return null;
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        if (mode.getKind() == PlanHashKind.LEGACY) {
            // not every collection value supports the legacy mode
            return PlanHashable.objectsPlanHash(mode, baseHash(mode, OBJECT_PLAN_HASH_IN_EXPLODE_SOURCE), getComparison(),
                    isSorted(), isReverse());
        }
        // the comparison does not hash its comparand, so hash the explode, which hashes the collection
        return PlanHashable.objectsPlanHash(mode, baseHash(mode, OBJECT_PLAN_HASH_IN_EXPLODE_SOURCE), explodeValue);
    }

    @Override
    public boolean isSorted() {
        return explodeValue.getCollectionValue() instanceof ArraySortValue;
    }

    @Override
    public boolean isReverse() {
        final Value collectionValue = explodeValue.getCollectionValue();
        return collectionValue instanceof ArraySortValue && ((ArraySortValue)collectionValue).isReverse();
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain() {
        // explain the unsorted collection, the suffix tells whether it is sorted
        return ExplainTokensWithPrecedence.of(
                new ExplainTokens().addNested(ExplainLevel.ALL_DETAILS,
                        new Comparisons.ValueComparison(Comparisons.Type.IN, getUnsortedCollectionValue())
                                .explain().getExplainTokens(), "...").addNested(explainSuffix()));
    }

    @Nonnull
    @Override
    protected List<Object> getValues(@Nullable final EvaluationContext context) {
        return explodeValue.evalAsList(null, context == null ? EvaluationContext.empty() : context);
    }

    @Nonnull
    @Override
    public String toString() {
        return getBindingName() + " IN " + explodeValue;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final InExplodeSource inExplodeSource = (InExplodeSource)o;
        return super.equals(inExplodeSource) && explodeValue.equals(inExplodeSource.explodeValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getComparison(), explodeValue);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return explodeValue.getResultType();
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        return explodeValue.getDynamicTypes();
    }

    @Nonnull
    @Override
    public PInExplodeSource toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PInExplodeSource.newBuilder()
                .setSuper(toInComparandSourceProto(serializationContext))
                .setExplodeValue(explodeValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    protected PInSource toInSourceProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PInSource.newBuilder().setInExplodeSource(toProto(serializationContext)).build();
    }

    @Nonnull
    public static InExplodeSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                            @Nonnull final PInExplodeSource inExplodeSourceProto) {
        return new InExplodeSource(serializationContext, inExplodeSourceProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PInExplodeSource, InExplodeSource> {
        @Nonnull
        @Override
        public Class<PInExplodeSource> getProtoMessageClass() {
            return PInExplodeSource.class;
        }

        @Nonnull
        @Override
        public InExplodeSource fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                         @Nonnull final PInExplodeSource inExplodeSourceProto) {
            return InExplodeSource.fromProto(serializationContext, inExplodeSourceProto);
        }
    }
}
