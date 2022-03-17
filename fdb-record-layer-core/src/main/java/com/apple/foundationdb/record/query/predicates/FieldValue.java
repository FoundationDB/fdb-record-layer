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
import com.apple.foundationdb.record.query.plan.temp.Formatter;
import com.apple.foundationdb.record.query.plan.temp.MessageValue;
import com.apple.foundationdb.record.query.plan.temp.SemanticException;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A value representing the contents of a (non-repeated, arbitrarily-nested) field of a quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class FieldValue implements ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Value");

    @Nonnull
    private final QuantifiedValue columnValue;
    @Nonnull
    private final List<String> fieldPath;
    @Nonnull
    private final Type resultType;

    public FieldValue(@Nonnull QuantifiedValue columnValue, @Nonnull List<String> fieldPath) {
        this(columnValue, fieldPath, resolveTypeForPath(columnValue.getResultType(), fieldPath));
    }

    @VisibleForTesting
    public FieldValue(@Nonnull QuantifiedValue columnValue, @Nonnull List<String> fieldPath, @Nonnull Type resultType) {
        Preconditions.checkArgument(!fieldPath.isEmpty());
        this.columnValue = columnValue;
        this.fieldPath = ImmutableList.copyOf(fieldPath);
        this.resultType = resultType;
    }

    @Nonnull
    public List<String> getFieldPath() {
        return fieldPath;
    }

    @Nonnull
    public List<String> getFieldPrefix() {
        return fieldPath.subList(0, fieldPath.size() - 1);
    }

    @Nonnull
    public String getFieldName() {
        return fieldPath.get(fieldPath.size() - 1);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return resultType;
    }

    @Nonnull
    @Override
    public QuantifiedValue getChild() {
        return columnValue;
    }

    @Nonnull
    @Override
    public FieldValue withNewChild(@Nonnull final Value child) {
        return new FieldValue((QuantifiedValue)child, fieldPath);
    }

    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final FDBRecord<M> record, @Nullable final M message) {
        if (message == null) {
            return null;
        }
        final var childResult = columnValue.eval(store, context, record, message);
        if (!(childResult instanceof Message)) {
            return null;
        }
        return MessageValue.getFieldValue((Message)childResult, fieldPath);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (!ValueWithChild.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }

        final var that = (FieldValue)other;
        return columnValue.semanticEquals(that.columnValue, equivalenceMap) &&
               fieldPath.equals(that.fieldPath);
    }

    @Override
    public int semanticHashCode() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, fieldPath);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, fieldPath);
    }

    @Override
    public String toString() {
        return columnValue.toString() + "/" + String.join(".", fieldPath);
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return columnValue.explain(formatter) + "." + String.join(".", fieldPath);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(columnValue.getCorrelatedTo()));
    }

    private static Type resolveTypeForPath(@Nonnull final Type inputType, @Nonnull final List<String> fieldPath) {
        var currentType = inputType;
        for (final var fieldName : fieldPath) {
            if (currentType.getTypeCode() == Type.TypeCode.ANY) {
                return new Type.Any();
            }
            SemanticException.check(inputType.getTypeCode() == Type.TypeCode.RECORD, "field type can only be resolved on records");
            final var recordType = (Type.Record)currentType;
            final var fieldTypeMap = recordType.getFieldTypeMap();
            SemanticException.check(fieldTypeMap.containsKey(fieldName), "record does not contain specified field");
            currentType = fieldTypeMap.get(fieldName);
        }
        return currentType;
    }
}
