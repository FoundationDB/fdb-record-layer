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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ScalarTranslationVisitor;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.KeyExpressionUtils;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * A predicate consisting of a {@link Value} and a {@link Comparison}.
 */
@API(API.Status.EXPERIMENTAL)
public class ValuePredicate implements PredicateWithValue, QueryPredicate.Serializable {
    @Nonnull
    private final Value value;
    @Nonnull
    private final Comparison comparison;

    public ValuePredicate(@Nonnull Value value, @Nonnull Comparison comparison) {
        this.value = value;
        this.comparison = comparison;
    }

    @Nonnull
    public Comparison getComparison() {
        return comparison;
    }

    @Nonnull
    @Override
    public Value getValue() {
        return value;
    }

    @Nonnull
    @Override
    public ValuePredicate withValue(@Nonnull final Value value) {
        return new ValuePredicate(value, comparison);
    }

    @Nullable
    @Override
    public <M extends Message> Boolean eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        return comparison.eval(store, context, value.eval(store, context));
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        final var builder = ImmutableSet.<CorrelationIdentifier>builder();
        builder.addAll(value.getCorrelatedTo());
        builder.addAll(comparison.getCorrelatedTo());
        return builder.build();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public QueryPredicate translateLeafPredicate(@Nonnull final TranslationMap translationMap) {
        final var translatedValue = value.translateCorrelations(translationMap);
        final Comparison newComparison;
        if (comparison.getCorrelatedTo().stream().anyMatch(translationMap::containsSourceAlias)) {
            newComparison = comparison.translateCorrelations(translationMap);
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
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final QueryPredicate other, @Nonnull final AliasMap equivalenceMap) {
        if (!PredicateWithValue.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }
        final ValuePredicate that = (ValuePredicate)other;
        return value.semanticEquals(that.value, equivalenceMap) &&
               comparison.semanticEquals(that.comparison, equivalenceMap);
    }
    
    @Override
    public int semanticHashCode() {
        return Objects.hash(value.semanticHashCode(), comparison.semanticHashCode());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, value, comparison);
    }

    @Override
    public String toString() {
        return value + " " + comparison;
    }

    @Nonnull
    @Override
    public RecordMetaDataProto.Predicate toProto() {
        return RecordMetaDataProto.Predicate.newBuilder()
                .setValuePredicate(RecordMetaDataProto.ValuePredicate.newBuilder()
                        .setValue(KeyExpressionUtils.toKeyExpression(value).toKeyExpression())
                        .setComparison(((Comparison.Serializable)comparison).toProto())
                        .build())
                .build();
    }

    @Override
    public boolean isSerializable() {
        return comparison.isSerializable() && KeyExpressionUtils.convertibleToKeyExpression(value);
    }

    @Nonnull
    public static ValuePredicate deserialize(@Nonnull final RecordMetaDataProto.ValuePredicate proto,
                                             @Nonnull final CorrelationIdentifier alias,
                                             @Nonnull final Type inputType) {
        Verify.verify(proto.hasValue(), String.format("attempt to deserialize %s without value", ValuePredicate.class));
        Verify.verify(proto.hasComparison(), String.format("attempt to deserialize %s without comparison", ValuePredicate.class));
        final var value = new ScalarTranslationVisitor(KeyExpression.fromProto(proto.getValue())).toResultValue(alias, inputType);
        final var comparison = Comparison.deserialize(proto.getComparison());
        return new ValuePredicate(value, comparison);
    }
}
