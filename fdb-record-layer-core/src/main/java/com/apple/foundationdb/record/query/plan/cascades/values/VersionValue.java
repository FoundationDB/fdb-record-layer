/*
 * VersionValue.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * A value representing a version stamp derived from a quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class VersionValue extends AbstractValue implements QuantifiedValue {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Version-Value");

    @Nonnull
    private final CorrelationIdentifier baseAlias;

    public VersionValue(@Nonnull CorrelationIdentifier baseAlias) {
        this.baseAlias = baseAlias;
    }

    @Override
    public CorrelationIdentifier getAlias() {
        return baseAlias;
    }

    @Nullable
    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        QueryResult binding = (QueryResult) context.getBinding(baseAlias);
        return binding.getQueriedRecordMaybe()
                .map(FDBRecord::getVersion)
                .orElse(null);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return Type.primitiveType(Type.TypeCode.VERSION);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return QuantifiedValue.super.getCorrelatedToWithoutChildren();
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return toString();
    }

    @Nonnull
    @Override
    public String toString() {
        return "version(" + baseAlias + ")";
    }

    @Nonnull
    @Override
    public VersionValue rebaseLeaf(@Nonnull final CorrelationIdentifier targetAlias) {
        return new VersionValue(targetAlias);
    }

    @Nonnull
    @Override
    public VersionValue replaceReferenceWithField(@Nonnull final FieldValue fieldValue) {
        return this;
    }

    @Override
    public boolean isFunctionallyDependentOn(@Nonnull final Value otherValue) {
        if (otherValue instanceof QuantifiedValue) {
            return getAlias().equals(((QuantifiedValue)otherValue).getAlias());
        }
        return false;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(getCorrelatedTo()));
    }
}
