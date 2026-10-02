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
 * Matchers for {@link Quantifier} objects. Most of these match a quantifier of a particular class and hand either the
 * {@link Reference} the quantifier ranges over, or the member expressions of that reference, to a downstream matcher.
 * Methods whose names end in {@code OverRef} match on the reference; the others match on its member expressions.
 *
 * <p><b>For-each quantifiers.</b> A {@link Quantifier.ForEach} may carry semantics beyond flowing each item of the
 * expression it ranges over, such as null-on-empty. A quantifier without any such semantics is called <em>plain</em>
 * (see {@link Quantifier.ForEach#isPlain()}). A rule that matches a non-plain quantifier must honor its semantics,
 * so there are two families of for-each matchers:
 * <ul>
 * <li>{@link #plainForEachQuantifier} and {@link #plainForEachQuantifierOverRef} match plain for-each quantifiers only.
 * They are the default choice for rules that assume the quantifier simply flows the items of its inner, which is the
 * case for most rules.
 * <li>{@link #forEachQuantifier} and {@link #forEachQuantifierOverRef} match any for-each quantifier. Only rules that
 * honor the extra semantics should use them, for example by handing the quantifier on unchanged, or by implementing its
 * semantics in the plans they yield.
 * </ul>
 * Rules that use the plain matchers are expected never to encounter a non-plain quantifier. If that assumption turns
 * out to be wrong, the rule does not fire, rather than silently dropping the semantics of the quantifier and producing
 * wrong results. If no other rule can implement the expression either, planning fails, which surfaces the gap.
 */
@API(API.Status.EXPERIMENTAL)
public final class QuantifierMatchers {
    private QuantifierMatchers() {
        // do not instantiate
    }

    /**
     * Matches any quantifier of the given class, regardless of what it ranges over.
     */
    @Nonnull
    public static <Q extends Quantifier> TypedMatcher<Q> ofType(@Nonnull final Class<Q> bindableClass) {
        return typed(bindableClass);
    }

    /**
     * Matches a quantifier of the given class whose member expressions, as a collection, match {@code downstream}.
     */
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

    /**
     * Matches a quantifier of the given class whose reference matches {@code downstream}.
     */
    @Nonnull
    public static <Q extends Quantifier> BindingMatcher<Q> ofTypeRangingOverRef(
            @Nonnull final Class<Q> bindableClass,
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return typedWithDownstream(bindableClass,
                Extractor.of(Quantifier::getRangesOver, name -> "rangesOver(" + name + ")"),
                downstream);
    }

    /**
     * Matches any quantifier.
     */
    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier() {
        return ofTypeRangingOverRef(Quantifier.class, ReferenceMatchers.anyRef());
    }

    /**
     * Matches any quantifier that has at least one member expression matching {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.class, AnyMatcher.any(downstream));
    }

    /**
     * Matches any quantifier whose member expressions match {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifier(
            @Nonnull final CollectionMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.class, downstream);
    }

    /**
     * Matches any quantifier whose reference matches {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier> anyQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.class, downstream);
    }

    /**
     * Matches an existential quantifier that has at least one member expression matching {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Existential> existentialQuantifier(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.Existential.class, AnyMatcher.any(downstream));
    }

    /**
     * Matches an existential quantifier whose member expressions match {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Existential> existentialQuantifier(
            @Nonnull final CollectionMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.Existential.class, downstream);
    }

    /**
     * Matches any existential quantifier.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Existential> existentialQuantifier() {
        return ofTypeRangingOverRef(Quantifier.Existential.class, ReferenceMatchers.anyRef());
    }

    /**
     * Matches an existential quantifier whose reference matches {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Existential> existentialQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.Existential.class, downstream);
    }

    /**
     * Matches any for-each quantifier, plain or not. Only use this in rules that honor the semantics of non-plain
     * quantifiers; see the class documentation.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifier() {
        return ofTypeRangingOverRef(Quantifier.ForEach.class, ReferenceMatchers.anyRef());
    }

    /**
     * Matches a for-each quantifier, plain or not, that has at least one member expression matching
     * {@code downstream}. Only use this in rules that honor the semantics of non-plain quantifiers; see the class
     * documentation.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifier(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return ofTypeRangingOver(Quantifier.ForEach.class, AnyMatcher.any(downstream));
    }

    /**
     * Matches a for-each quantifier, plain or not, whose reference matches {@code downstream}. Only use this in rules
     * that honor the semantics of non-plain quantifiers; see the class documentation.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> forEachQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.ForEach.class, downstream);
    }

    /**
     * Matches a plain for-each quantifier that has at least one member expression matching {@code downstream}. This is
     * the default choice for rules that match for-each quantifiers; see the class documentation.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> plainForEachQuantifier(
            @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return plain(forEachQuantifier(downstream));
    }

    /**
     * Matches a plain for-each quantifier whose reference matches {@code downstream}. This is the default choice for
     * rules that match for-each quantifiers; see the class documentation.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.ForEach> plainForEachQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return plain(forEachQuantifierOverRef(downstream));
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

    /**
     * Constrains the given for-each quantifier matcher to plain quantifiers (see
     * {@link Quantifier.ForEach#isPlain()}).
     */
    @Nonnull
    private static BindingMatcher<Quantifier.ForEach> plain(
            @Nonnull final BindingMatcher<Quantifier.ForEach> downstream) {
        return typedWithDownstream(Quantifier.ForEach.class,
                Extractor.of(Quantifier.ForEach::isPlain, name -> "plain(" + name + ")"),
                PrimitiveMatchers.equalsObject(true)).where(downstream);
    }

    /**
     * Matches any physical quantifier.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier() {
        return ofTypeRangingOverRef(Quantifier.Physical.class, ReferenceMatchers.anyRef());
    }

    /**
     * Matches a physical quantifier that has at least one member plan matching {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier(
            @Nonnull final BindingMatcher<? extends RecordQueryPlan> downstream) {
        return ofTypeRangingOver(Quantifier.Physical.class, AnyMatcher.any(downstream));
    }

    /**
     * Matches a physical quantifier whose member plans match {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifier(
            @Nonnull final CollectionMatcher<? extends RecordQueryPlan> downstream) {
        return ofTypeRangingOver(Quantifier.Physical.class, downstream);
    }

    /**
     * Matches a physical quantifier whose reference matches {@code downstream}.
     */
    @Nonnull
    public static BindingMatcher<Quantifier.Physical> physicalQuantifierOverRef(
            @Nonnull final BindingMatcher<? extends Reference> downstream) {
        return ofTypeRangingOverRef(Quantifier.Physical.class, downstream);
    }
}
