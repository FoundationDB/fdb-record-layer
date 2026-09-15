/*
 * QuantifierMatchers.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;

import javax.annotation.Nonnull;
import java.util.Collection;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithExtractAndDownstream.typedWithDownstream;

/**
 * Matchers for {@link Quantifier}s.
 */
@API(API.Status.EXPERIMENTAL)
public final class QuantifierMatchers {
    private QuantifierMatchers() {
        // do not instantiate
    }

    @Nonnull
    public static <Q extends Quantifier> TypedMatcher<Q> ofType(@Nonnull final Class<Q> bindableClass) {
        return typed(bindableClass);
    }

    @Nonnull
    public static <Q extends Quantifier> BindingMatcher<Q> ofTypeRangingOver(
            @Nonnull final Class<Q> bindableClass,
            @Nonnull final BindingMatcher<? extends Collection<? extends RelationalExpression>> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(
                        q -> q.getRangesOver().getAllMemberExpressions(),
                        name -> "rangesOver(getAllMemberExpressions(" + name + "))"),
                downstream);
    }

    @Nonnull
    public static <Q extends Quantifier> BindingMatcher<Q> ofTypeRangingOverRef(
            @Nonnull final Class<Q> bindableClass,
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(Quantifier::getRangesOver, name -> "rangesOver(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier() {
        return ofTypeRangingOverRef(Quantifier.class, ReferenceMatchers.anyRef());
    }

    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier(
            @Nonnull final CollectionMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> scalarQuantifier(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.Scalar.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> scalarQuantifier(
            @Nonnull final CollectionMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.Scalar.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> scalarQuantifier() {
        return ofTypeRangingOverRef(Quantifier.Scalar.class, ReferenceMatchers.anyRef());
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> scalarQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.Scalar.class, downstream);
    }

    /**
     * Matches a scalar quantifier of kind {@link Quantifier.Scalar.Kind#EXISTENTIAL}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> existentialQuantifier(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return withKind(Quantifier.Scalar.Kind.EXISTENTIAL, scalarQuantifier(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> existentialQuantifier(
            @Nonnull final CollectionMatcher<? extends RelationalExpression> downstream) {
        return withKind(Quantifier.Scalar.Kind.EXISTENTIAL, scalarQuantifier(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> existentialQuantifier() {
        return withKind(Quantifier.Scalar.Kind.EXISTENTIAL, scalarQuantifier());
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> existentialQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return withKind(Quantifier.Scalar.Kind.EXISTENTIAL, scalarQuantifierOverRef(downstream));
    }

    /**
     * Constrains the given scalar quantifier matcher to quantifiers whose {@link Quantifier.Scalar#getKind()} equals
     * {@code kind}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Scalar> withKind(
            @Nonnull final Quantifier.Scalar.Kind kind,
            @Nonnull final BindingMatcher<Quantifier.Scalar> downstream) {
        return typedWithDownstream(Quantifier.Scalar.class,
                Extractor.of(Quantifier.Scalar::getKind, name -> "withKind(" + name + ")"),
                PrimitiveMatchers.equalsObject(kind)).where(downstream);
    }

    /**
     * Matches a for-each quantifier (with or without null-on-empty semantics).
     */
    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifier() {
        return ofTypeRangingOverRef(Quantifier.ForEach.class, ReferenceMatchers.anyRef());
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifier(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.ForEach.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.ForEach.class, downstream);
    }

    /**
     * Matches a for-each quantifier without null-on-empty semantics.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierWithoutNullOnEmpty(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return withNullOnEmpty(false, forEachQuantifier(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierWithoutNullOnEmptyOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return withNullOnEmpty(false, forEachQuantifierOverRef(downstream));
    }

    /**
     * Constrains the given for-each quantifier matcher to quantifiers whose {@link Quantifier.ForEach#isNullOnEmpty()}
     * flag equals {@code nullOnEmpty}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> withNullOnEmpty(
            final boolean nullOnEmpty,
            @Nonnull final BindingMatcher<Quantifier.ForEach> downstream) {
        return typedWithDownstream(Quantifier.ForEach.class,
                Extractor.of(Quantifier.ForEach::isNullOnEmpty, name -> "withNullOnEmpty(" + name + ")"),
                PrimitiveMatchers.equalsObject(nullOnEmpty)).where(downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier() {
        return ofTypeRangingOverRef(Quantifier.Physical.class, ReferenceMatchers.anyRef());
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier(
            @Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return ofTypeRangingOver(Quantifier.Physical.class, AnyMatcher.any(downstream));
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier(
            @Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return ofTypeRangingOver(Quantifier.Physical.class, downstream);
    }

    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.Physical.class, downstream);
    }
}
