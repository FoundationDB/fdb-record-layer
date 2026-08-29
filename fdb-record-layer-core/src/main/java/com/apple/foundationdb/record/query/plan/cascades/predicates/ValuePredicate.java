/*
 * ValuePredicate.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PQueryPredicate;
import com.apple.foundationdb.record.planprotos.PValuePredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence.Precedence;
import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A predicate consisting of a {@link Value} and a {@link Comparison}.
 */
@API(API.Status.EXPERIMENTAL)
public class ValuePredicate extends AbstractQueryPredicate implements PredicateWithValue, PredicateWithComparisons {
    private final Value value;
    private final Comparison comparison;
    @SuppressWarnings("this-escape")
    private final Supplier<Boolean> isIndexOnlySupplier = Suppliers.memoize(() -> getValue().isIndexOnly());

    private ValuePredicate(final PlanSerializationContext serializationContext,
                           final PValuePredicate valuePredicate) {
        super(serializationContext, Objects.requireNonNull(valuePredicate.getSuper()));
        this.value = Value.fromValueProto(serializationContext, Objects.requireNonNull(valuePredicate.getValue()));
        this.comparison = Comparison.fromComparisonProto(serializationContext, Objects.requireNonNull(valuePredicate.getComparison()));
    }

    public ValuePredicate(final Value value, final Comparison comparison) {
        super(false);
        this.value = value;
        this.comparison = comparison;
    }

    public Comparison getComparison() {
        return comparison;
    }

    @Override
    public List<Comparison> getComparisons() {
        return ImmutableList.of(getComparison());
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Optional<PredicateWithValue> translateValueAndComparisonsMaybe(final Function<Value, Optional<Value>> valueTranslator,
                                                                          final Function<Comparison, Optional<Comparison>> comparisonTranslator) {
        final var newValueOptional = Verify.verifyNotNull(valueTranslator.apply(this.getValue()));
        if (newValueOptional.isEmpty()) {
            return Optional.empty();
        }
        final var newValue = newValueOptional.get();
        return comparisonTranslator.apply(comparison)
                .flatMap(newComparison -> {
                    if (newValue == value && newComparison == comparison) {
                        return Optional.of(this);
                    }
                    return Optional.of(new ValuePredicate(newValue, newComparison));
                });
    }

    @Override
    public Value getValue() {
        return value;
    }

    @Override
    public ValuePredicate withValue(final Value value) {
        return new ValuePredicate(value, comparison);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nullable final FDBRecordStoreBase<M> store, final EvaluationContext context) {
        return comparison.eval(store, context, value.eval(store, context));
    }

    @Override
    public boolean isIndexOnly() {
        return isIndexOnlySupplier.get();
    }

    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        final var builder = ImmutableSet.<CorrelationIdentifier>builder();
        builder.addAll(value.getCorrelatedTo());
        builder.addAll(comparison.getCorrelatedTo());
        return builder.build();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public QueryPredicate translateLeafPredicate(final TranslationMap translationMap, final boolean shouldSimplifyValues) {
        final var translatedValue = value.translateCorrelations(translationMap, shouldSimplifyValues);
        final Comparison newComparison;
        if (comparison.getCorrelatedTo().stream().anyMatch(translationMap::containsSourceAlias)) {
            newComparison = comparison.translateCorrelations(translationMap, shouldSimplifyValues);
        } else {
            newComparison = comparison;
        }
        if (value != translatedValue || newComparison != comparison) { // reference comparison intended
            return new ValuePredicate(translatedValue, newComparison);
        }
        return this;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeSemanticHashCode() {
        return PredicateWithValue.super.computeSemanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(value.semanticHashCode(), comparison.semanticHashCode());
    }

    @Override
    public ConstrainedBoolean equalsWithoutChildren(final QueryPredicate other, final ValueEquivalence valueEquivalence) {
        return PredicateWithValue.super.equalsWithoutChildren(other, valueEquivalence)
                .compose(ignored -> {
                    final ValuePredicate that = (ValuePredicate)other;
                    return value.semanticEquals(that.value, valueEquivalence);
                })
                .compose(ignored -> {
                    final ValuePredicate that = (ValuePredicate)other;
                    return comparison.semanticEquals(that.comparison, valueEquivalence);
                });
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, value, comparison);
    }


    @Override
    public ExplainTokensWithPrecedence explain(final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        Verify.verify(Iterables.isEmpty(explainSuppliers));
        return ExplainTokensWithPrecedence.of(Precedence.COMPARISONS,
                Precedence.COMPARISONS.parenthesizeChild(value.explain()).addWhitespace()
                        .addNested(Precedence.COMPARISONS.parenthesizeChild(comparison.explain())));
    }

    @Override
    public Message toProto(final PlanSerializationContext serializationContext) {
        return toValuePredicateProto(serializationContext);
    }

    @Override
    public PQueryPredicate toQueryPredicateProto(final PlanSerializationContext serializationContext) {
        return PQueryPredicate.newBuilder().setValuePredicate(toValuePredicateProto(serializationContext)).build();
    }

    public PValuePredicate toValuePredicateProto(final PlanSerializationContext serializationContext) {
        return PValuePredicate.newBuilder()
                .setSuper(toAbstractQueryPredicateProto(serializationContext))
                .setValue(value.toValueProto(serializationContext))
                .setComparison(comparison.toComparisonProto(serializationContext))
                .build();
    }

    public static ValuePredicate fromProto(final PlanSerializationContext serializationContext, final PValuePredicate valuePredicateProto) {
        return new ValuePredicate(serializationContext, valuePredicateProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PValuePredicate, ValuePredicate> {
        @Override
        public Class<PValuePredicate> getProtoMessageClass() {
            return PValuePredicate.class;
        }

        @Override
        public ValuePredicate fromProto(final PlanSerializationContext serializationContext,
                                        final PValuePredicate valuePredicateProto) {
            return ValuePredicate.fromProto(serializationContext, valuePredicateProto);
        }
    }
}
