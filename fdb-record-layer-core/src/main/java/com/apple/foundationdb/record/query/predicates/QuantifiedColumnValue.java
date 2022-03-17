/*
 * QuantifiedColumnValue.java
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
import java.util.Objects;

/**
 * A value representing the quantifier as an object.
 *
 * For example, this is used to represent non-nested repeated fields.
 */
@API(API.Status.EXPERIMENTAL)
public class QuantifiedColumnValue implements QuantifiedValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Quantifier-Column-Value");

    @Nonnull
    private final CorrelationIdentifier alias;
    private final int ordinalPosition;
    @Nonnull
    private final Type.Record recordType;

    private QuantifiedColumnValue(@Nonnull final CorrelationIdentifier alias,
                                 final int ordinalPosition,
                                 final Type.Record recordType) {
        this.alias = alias;
        this.ordinalPosition = ordinalPosition;
        this.recordType = recordType;
    }

    public int getOrdinalPosition() {
        return ordinalPosition;
    }

    private Type.Record.Field getFieldForOrdinal() {
        return Objects.requireNonNull(recordType.getFields()).get(ordinalPosition);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return getFieldForOrdinal().getFieldType();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return formatter.getQuantifierName(alias) + "#" + ordinalPosition;
    }

    @Nonnull
    @Override
    public QuantifiedColumnValue rebaseLeaf(@Nonnull final AliasMap translationMap) {
        if (translationMap.containsSource(alias)) {
            return new QuantifiedColumnValue(translationMap.getTargetOrThrow(alias), ordinalPosition, recordType);
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

    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (!QuantifiedValue.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }
        final QuantifiedColumnValue that = (QuantifiedColumnValue)other;
        return ordinalPosition == that.getOrdinalPosition();
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, ordinalPosition);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, ordinalPosition);
    }

    @Override
    public String toString() {
        return "$" + alias + "[" + ordinalPosition + "]";
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
        if (otherValue instanceof QuantifiedColumnValue) {
            final QuantifiedColumnValue otherQuantifierColumnValue = (QuantifiedColumnValue)otherValue;
            return getAlias().equals(otherQuantifierColumnValue.getAlias()) &&
                   getOrdinalPosition() == (otherQuantifierColumnValue.getOrdinalPosition());
        }
        return false;
    }

    /**
     * Creates a new instance of {@link CorrelationIdentifier}.
     *
     * @param alias The alias of the correlation containing the column.
     * @param ordinal The ordinal position of the column.
     * @return a new instance of {@link CorrelationIdentifier}.
     * note: this method will be replaced by {@link #of(CorrelationIdentifier, int, Type.Record)}.
     */
    @Nonnull
    public static QuantifiedColumnValue of(@Nonnull CorrelationIdentifier alias, int ordinal) {
        // TODO get record type information.
        return new QuantifiedColumnValue(alias, ordinal, Type.Record.erased());
    }

    /**
     * Creates a new instance of {@link CorrelationIdentifier}.
     *
     * @param alias The alias of the correlation containing the column.
     * @param ordinal The ordinal position of the column.
     * @param recordType The {@link Type} of the record.
     * @return a new instance of {@link CorrelationIdentifier}.
     */
    @Nonnull
    public static QuantifiedColumnValue of(@Nonnull CorrelationIdentifier alias, int ordinal, @Nonnull final Type.Record recordType) {
        return new QuantifiedColumnValue(alias, ordinal, recordType);
    }
}
