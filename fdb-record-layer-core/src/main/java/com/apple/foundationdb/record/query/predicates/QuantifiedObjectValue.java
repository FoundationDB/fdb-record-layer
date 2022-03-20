/*
 * QuantifiedObjectValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Formatter;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A value representing the quantifier as an object.
 *
 * For example, this is used to represent non-nested repeated fields.
 */
@API(API.Status.EXPERIMENTAL)
public class QuantifiedObjectValue implements QuantifiedValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Quantified-Object-Value");

    @Nonnull
    private final CorrelationIdentifier alias;
    @Nonnull
    private final Type resultType;

    private QuantifiedObjectValue(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type resultType) {
        this.alias = alias;
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public QuantifiedObjectValue rebaseLeaf(@Nonnull final AliasMap translationMap) {
        if (translationMap.containsSource(alias)) {
            return QuantifiedObjectValue.of(translationMap.getTargetOrThrow(alias), resultType);
        }
        return this;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        return context.getBinding(alias);
    }

    @Nonnull
    @Override
    public CorrelationIdentifier getAlias() {
        return alias;
    }

    @Nonnull
    @Override
    public String describe(@Nonnull final Formatter formatter) {
        return formatter.getQuantifierName(alias);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH);
    }

    @Override
    public String toString() {
        return "$" + alias;
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(ImmutableSet.of(alias)));
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        if (otherValue instanceof QuantifiedObjectValue) {
            return getAlias().equals(((QuantifiedObjectValue)otherValue).getAlias());
        }
        return false;
    }

    @Nonnull
    public static QuantifiedObjectValue of(@Nonnull final CorrelationIdentifier alias) {
        return new QuantifiedObjectValue(alias, Type.primitiveType(Type.TypeCode.UNKNOWN));
    }

    @Nonnull
    public static QuantifiedObjectValue of(@Nonnull final CorrelationIdentifier alias, @Nonnull final Type resultType) {
        return new QuantifiedObjectValue(alias, resultType);
    }
}
