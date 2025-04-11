/*
 * FDBSimpleQueryGraphTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Helpers for easily creating expressions, quantifiers, etc.
 */
@Tag(Tags.RequiresFDB)
public class FDBQueryGraphTestHelpers extends FDBRecordStoreQueryTestBase {
    @Nonnull
    public static Quantifier fullScan(@Nonnull RecordMetaData metaData, AccessHints hints) {
        Set<String> allRecordTypes = ImmutableSet.copyOf(metaData.getRecordTypes().keySet());
        return Quantifier.forEach(Reference.of(
                new FullUnorderedScanExpression(allRecordTypes,
                        new Type.AnyRecord(false),
                        hints)));
    }

    @Nonnull
    public static Quantifier fullScan(@Nonnull RecordMetaData metaData) {
        return fullScan(metaData, new AccessHints());
    }

    @Nonnull
    public static Quantifier fullTypeScan(@Nonnull RecordMetaData metaData, @Nonnull String typeName, @Nonnull Quantifier fullScanQun) {
        return Quantifier.forEach(Reference.of(
                new LogicalTypeFilterExpression(ImmutableSet.of(typeName),
                        fullScanQun,
                        Record.fromDescriptor(metaData.getRecordType(typeName).getDescriptor()))));
    }

    @Nonnull
    public static Quantifier fullTypeScan(@Nonnull RecordMetaData metaData, @Nonnull String typeName) {
        return fullTypeScan(metaData, typeName, fullScan(metaData));
    }

    @Nonnull
    public static Column<FieldValue> projectColumn(@Nonnull Quantifier qun, @Nonnull String columnName) {
        return projectColumn(qun.getFlowedObjectValue(), columnName);
    }

    @Nonnull
    public static Column<FieldValue> projectColumn(@Nonnull Value value, @Nonnull String columnName) {
        return Column.of(Optional.of(columnName), FieldValue.ofFieldNameAndFuseIfPossible(value, columnName));
    }

    @Nonnull
    public static <V extends Value> Column<V> resultColumn(@Nonnull V value, @Nullable String name) {
        return Column.of(Optional.ofNullable(name), value);
    }

    public static RecordCursor<QueryResult> executeCascades(FDBRecordStore store, RecordQueryPlan plan) {
        return executeCascades(store, plan, Bindings.EMPTY_BINDINGS);
    }

    public static RecordCursor<QueryResult> executeCascades(FDBRecordStore store, RecordQueryPlan plan, Bindings bindings) {
        Set<Type> usedTypes = UsedTypesProperty.evaluate(plan);
        TypeRepository typeRepository = TypeRepository.newBuilder()
                .addAllTypes(usedTypes)
                .build();
        EvaluationContext evaluationContext = EvaluationContext.forBindingsAndTypeRepository(bindings, typeRepository);
        return plan.executePlan(store, evaluationContext, null, ExecuteProperties.SERIAL_EXECUTE);
    }

    public static <T> T getField(QueryResult result, Class<T> type, String... path) {
        Message message = result.getMessage();
        for (int i = 0; i < path.length; i++) {
            String fieldName = path[i];
            Descriptors.Descriptor messageDescriptor = message.getDescriptorForType();
            Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType().findFieldByName(fieldName);
            assertNotNull(fieldDescriptor, () -> "expected to find field " + fieldName + " in descriptor: " + messageDescriptor);
            Object field = message.getField(fieldDescriptor);
            if (i < path.length - 1) {
                assertThat(field, Matchers.instanceOf(Message.class));
                message = (Message) field;
            } else {
                if (field == null) {
                    return null;
                }
                assertThat(field, Matchers.instanceOf(type));
                return type.cast(field);
            }
        }
        return null;
    }

    @Nonnull
    public static LogicalSortExpression sortExpression(@Nonnull List<Value> sortValues,
                                                final boolean reverse,
                                                @Nonnull final Quantifier inner) {
        return new LogicalSortExpression(LogicalSortExpression.buildRequestedOrdering(sortValues, reverse, inner), inner);
    }
}
