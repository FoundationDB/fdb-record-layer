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
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty.usedTypes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Helpers for easily creating expressions, quantifiers, etc.
 */
@Tag(Tags.RequiresFDB)
public class FDBQueryGraphTestHelpers extends FDBRecordStoreQueryTestBase {
    @Nonnull
    public static Quantifier forEach(RelationalExpression relationalExpression) {
        return Quantifier.forEach(Reference.initialOf(relationalExpression));
    }

    @Nonnull
    public static Quantifier forEach(Reference reference) {
        return Quantifier.forEach(reference);
    }

    @Nonnull
    public static Quantifier forEachWithNullOnEmpty(RelationalExpression relationalExpression) {
        return Quantifier.forEachWithNullOnEmpty(Reference.initialOf(relationalExpression));
    }

    @Nonnull
    public static Quantifier exists(RelationalExpression relationalExpression) {
        return Quantifier.existential(Reference.initialOf(relationalExpression));
    }

    @Nonnull
    public static Quantifier fullScan(@Nonnull RecordMetaData metaData, AccessHints hints) {
        return forEach(fullScanExpression(metaData, hints));
    }

    @Nonnull
    public static Quantifier fullScan(@Nonnull RecordMetaData metaData) {
        return fullScan(metaData, new AccessHints());
    }

    @Nonnull
    public static RelationalExpression fullScanExpression(@Nonnull RecordMetaData metaData, AccessHints hints) {
        Set<String> allRecordTypes = ImmutableSet.copyOf(metaData.getRecordTypes().keySet());
        return new FullUnorderedScanExpression(allRecordTypes, new Type.AnyRecord(false), hints);
    }

    @Nonnull
    public static RelationalExpression fullScanExpression(@Nonnull RecordMetaData metaData) {
        return fullScanExpression(metaData, new AccessHints());
    }

    @Nonnull
    public static Reference reference(@Nonnull final RelationalExpression expression) {
        return Reference.initialOf(expression);
    }

    @Nonnull
    public static Quantifier fullTypeScan(@Nonnull RecordMetaData metaData, @Nonnull String typeName, @Nonnull Quantifier fullScanQun) {
        return forEach(
                new LogicalTypeFilterExpression(ImmutableSet.of(typeName),
                        fullScanQun,
                        metaData.getPlannerType(typeName)));
    }

    @Nonnull
    public static Quantifier fullTypeScan(@Nonnull RecordMetaData metaData, @Nonnull String typeName) {
        return fullTypeScan(metaData, typeName, fullScan(metaData));
    }

    @Nonnull
    public static FieldValue fieldValue(Value value, String fieldName) {
        int dotPos = fieldName.indexOf('.');
        if (dotPos >= 0) {
            String parentFieldName = fieldName.substring(0, dotPos);
            FieldValue parentField = FieldValue.ofFieldNameAndFuseIfPossible(value, parentFieldName);
            String childFieldName = fieldName.substring(dotPos + 1);
            return fieldValue(parentField, childFieldName);
        } else {
            return FieldValue.ofFieldNameAndFuseIfPossible(value, fieldName);
        }
    }

    @Nonnull
    public static FieldValue fieldValue(Quantifier qun, String fieldName) {
        return fieldValue(qun.getFlowedObjectValue(), fieldName);
    }

    @Nonnull
    public static Column<FieldValue> projectColumn(@Nonnull Quantifier qun, @Nonnull String columnName) {
        return projectColumn(qun.getFlowedObjectValue(), columnName);
    }

    @Nonnull
    public static Column<FieldValue> projectColumn(@Nonnull Value value, @Nonnull String columnName) {
        return Column.of(Optional.of(columnName), fieldValue(value, columnName));
    }

    @Nonnull
    public static Column<FieldValue> column(@Nonnull Quantifier qun, @Nonnull String fieldName, @Nonnull String resultName) {
        return resultColumn(fieldValue(qun, fieldName), resultName);
    }

    @Nonnull
    public static <V extends Value> Column<V> resultColumn(@Nonnull V value, @Nullable String name) {
        return Column.of(Optional.ofNullable(name), value);
    }

    public static RecordCursor<QueryResult> executeCascades(FDBRecordStore store, RecordQueryPlan plan) {
        return executeCascades(store, plan, Bindings.EMPTY_BINDINGS);
    }

    public static RecordCursor<QueryResult> executeCascades(FDBRecordStore store, RecordQueryPlan plan, Bindings bindings) {
        Set<Type> usedTypes = usedTypes().evaluate(plan);
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
    public static QueryPredicate fieldPredicate(Quantifier qun, String fieldName, Comparisons.Comparison comparison) {
        return fieldValue(qun, fieldName).withComparison(comparison);
    }

    @Nonnull
    public static SelectExpression selectWithPredicates(Quantifier qun, Map<String, String> projection, QueryPredicate... predicates) {
        GraphExpansion.Builder builder = GraphExpansion.builder().addQuantifier(qun);
        for (Map.Entry<String, String> p : projection.entrySet()) {
            builder.addResultColumn(column(qun, p.getKey(), p.getValue()));
        }
        builder.addAllPredicates(List.of(predicates));
        return builder.build().buildSelect();
    }

    @Nonnull
    public static SelectExpression selectWithPredicates(Quantifier qun, List<String> projection, QueryPredicate... predicates) {
        Map<String, String> identityProjectionMap = projection.stream().collect(ImmutableMap.toImmutableMap(Function.identity(), Function.identity()));
        return selectWithPredicates(qun, identityProjectionMap, predicates);
    }

    @Nonnull
    public static SelectExpression selectWithPredicates(Quantifier qun, QueryPredicate... predicates) {
        return new SelectExpression(qun.getFlowedObjectValue(), List.of(qun), List.of(predicates));
    }

    @Nonnull
    public static LogicalFilterExpression logicalFilterExpressionWithPredicates(Quantifier qun, QueryPredicate... predicates) {
        return new LogicalFilterExpression(List.of(predicates), qun);
    }

    @Nonnull
    public static LogicalSortExpression sortExpression(@Nonnull List<Value> sortValues,
                                                       final boolean reverse,
                                                       @Nonnull final Quantifier inner) {
        return new LogicalSortExpression(LogicalSortExpression.buildRequestedOrdering(sortValues, reverse, inner), inner);
    }
}
