/*
 * ValueLineageVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

public final class ValueLineageVisitor implements RelationalExpressionVisitorWithDefaults<Value> {

    @Nonnull
    @Override
    public Value visitDefault(@Nonnull final RelationalExpression element) {
        var translationMapBuilder = TranslationMap.regularBuilder();
        final var quantifiers = element.getQuantifiers();
        for (final var quantifier : quantifiers) {
            translationMapBuilder
                    .when(quantifier.getAlias())
                    .then(((sourceAlias, leafValue) -> visit(quantifier.getRangesOver().get())));
        }
        return element.getResultValue().translateCorrelations(translationMapBuilder.build());
    }


    @Nonnull
    @Override
    public Value visitLogicalTypeFilterExpression(@Nonnull final LogicalTypeFilterExpression element) {
        return projectFields(Assert.castUnchecked(element.getResultValue(), QuantifiedValue.class));
    }

    @Nonnull
    @Override
    public Value visitExplodeExpression(@Nonnull final ExplodeExpression element) {
        return element.getCollectionValue();
    }

    @Nonnull
    private static Value projectFields(@Nonnull final QuantifiedValue value) {
        final var type = value.getResultType();
        if (!type.isRecord()) {
            return value;
        }
        final var recordType = Assert.castUnchecked(type, Type.Record.class);
        final var columns = ImmutableList.<Column<? extends Value>>builder();
        for (final var field : recordType.getFields()) {
            final var fieldName = field.getFieldName();
            columns.add(Column.of(field, FieldValue.ofFieldName(value, fieldName)));
        }
        return RecordConstructorValue.ofColumns(columns.build());
    }
}
