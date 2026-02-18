/*
 * PlanPropertiesMap.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An {@link ExpressionPropertiesMap} for properties that only apply to {@link RecordQueryPlan}s.
 * @see ExpressionPropertiesMap
 */
public class PlanPropertiesMap extends ExpressionPropertiesMap<RecordQueryPlan> {
    /**
     * This set works a bit like an enumeration; it defines the domain of {@link ExpressionProperty}s that are being maintained
     * by the properties map.
     */
    private static final Set<ExpressionProperty<?>> expressionProperties =
            ImmutableSet.<ExpressionProperty<?>>builder()
                    .add(OrderingProperty.ordering())
                    .add(DistinctRecordsProperty.distinctRecords())
                    .add(StoredRecordProperty.storedRecord())
                    .add(PrimaryKeyProperty.primaryKey())
                    .add(CardinalitiesProperty.cardinalities())
                    .build();

    public PlanPropertiesMap() {
        this(ImmutableSet.of());
    }

    public PlanPropertiesMap(@Nonnull Collection<? extends RelationalExpression> plans) {
        super(RecordQueryPlan.class, expressionProperties, ImmutableSet.of(), plans);
    }

    @Nonnull
    @Override
    public <P> Map<RecordQueryPlan, P> propertyValueForPlans(@Nonnull final ExpressionProperty<P> expressionProperty) {
        return propertyValueForExpressions(expressionProperty);
    }

    @Nonnull
    @Override
    public Map<Map<ExpressionProperty<?>, ?>, Set<RecordQueryPlan>> getGroupingPropertiesPlansMap() {
        return getPartitioningPropertiesExpressionsMap();
    }

    @Nonnull
    public static Set<ExpressionProperty<?>> allAttributesExcept(@Nonnull final ExpressionProperty<?>... exceptAttributes) {
        final var exceptAttributesSet = ImmutableSet.copyOf(Arrays.asList(exceptAttributes));
        return ImmutableSet.copyOf(Sets.difference(expressionProperties, exceptAttributesSet));
    }
}
