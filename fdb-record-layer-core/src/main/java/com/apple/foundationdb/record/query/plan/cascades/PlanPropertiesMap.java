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

import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to manage properties for plans. A properties map is part of an expression reference ({@link Reference}).
 */
public class PlanPropertiesMap extends ExpressionPropertiesMap<RecordQueryPlan> {
    /**
     * This set works a bit like an enumeration; it defines the domain of {@link ExpressionProperty}s that are being
     * maintained by the properties map.
     */
    private static final Set<ExpressionProperty<?>> expressionProperties =
            ImmutableSet.<ExpressionProperty<?>>builder()
                    .add(OrderingProperty.ORDERING)
                    .add(DistinctRecordsProperty.DISTINCT_RECORDS)
                    .add(StoredRecordProperty.STORED_RECORD)
                    .add(PrimaryKeyProperty.PRIMARY_KEY)
                    .build();

    public PlanPropertiesMap(@Nonnull Collection<RecordQueryPlan> plans) {
        super(expressionProperties, plans);
    }

    @Nonnull
    @Override
    public <P> Map<RecordQueryPlan, P> propertyValueForPlans(@Nonnull final ExpressionProperty<P> expressionProperty) {
        return propertyValueForExpressions(expressionProperty);
    }

    @Nonnull
    @Override
    public List<PlanPartition> toPlanPartitions() {
        update();
        return PlanPartitions.toPartitions(Multimaps.asMap(getPropertyGroupedExpressionsMap()));
    }

    // TODO needs to be contextualized wrt to the planning phase we are in
    @Nonnull
    public static Set<ExpressionProperty<?>> allAttributesExcept(final ExpressionProperty<?>... exceptAttributes) {
        final var exceptAttributesSet = ImmutableSet.copyOf(Arrays.asList(exceptAttributes));
        return ImmutableSet.copyOf(Sets.difference(expressionProperties, exceptAttributesSet));
    }
}
