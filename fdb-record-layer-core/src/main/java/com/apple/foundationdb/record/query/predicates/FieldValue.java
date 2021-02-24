/*
 * FieldValue.java
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
import com.apple.foundationdb.record.query.plan.temp.MessageValue;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A value representing the contents of a (non-repeated, arbitrarily-nested) field of a quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class FieldValue implements Value {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Value");

    @Nonnull
    private final CorrelationIdentifier identifier;
    @Nonnull
    private final List<String> fieldNames;

    public FieldValue(@Nonnull CorrelationIdentifier identifier, @Nonnull List<String> fieldNames) {
        this.identifier = identifier;
        this.fieldNames = fieldNames;
    }

    @Nonnull
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return Collections.singleton(identifier);
    }

    @Nonnull
    @Override
    public FieldValue rebase(@Nonnull final AliasMap translationMap) {
        if (translationMap.containsSource(identifier)) {
            return new FieldValue(translationMap.getTargetOrThrow(identifier), fieldNames);
        }
        return this;
    }

    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        if (message == null) {
            return null;
        }
        return MessageValue.getFieldValue(message, fieldNames);
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap equivalenceMap) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final FieldValue that = (FieldValue)other;
        return equivalenceMap.containsMapping(identifier, that.identifier) &&
               fieldNames.equals(that.fieldNames);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, fieldNames);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, fieldNames);
    }

    @Override
    public String toString() {
        return "$" + identifier + "/" + String.join(".", fieldNames);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(ImmutableSet.of(identifier)));
    }
}
