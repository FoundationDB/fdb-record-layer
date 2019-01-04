/*
 * RecordTypesProperty.java
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

package com.apple.foundationdb.record.query.plan.temp.properties;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.TypeFilterExpression;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A property visitor that determines the set of record type names (as Strings) that a {@link RelationalPlannerExpression}
 * could produce. This property is used in determining whether type filters are necessary, among other things.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordTypesProperty implements PlannerProperty {
    @Nonnull
    private final PlanContext context;
    @Nonnull
    private Deque<Set<String>> possibleTypeSets = new ArrayDeque<>();

    private RecordTypesProperty(@Nonnull PlanContext context) {
        this.context = context;
    }

    @Override
    public boolean visitEnter(@Nonnull PlannerExpression expression) {
        return expression instanceof RelationalPlannerExpression;
    }

    @Override
    public boolean visitEnter(@Nonnull ExpressionRef<? extends PlannerExpression> ref) {
        return true;
    }


    @Override
    public boolean visitLeave(@Nonnull PlannerExpression expression) {
        if (expression instanceof RecordQueryScanPlan) {
            possibleTypeSets.push(context.getMetaData().getRecordTypes().keySet());
        } else if (expression instanceof RecordQueryIndexPlan) {
            Index index = context.getIndexByName(((RecordQueryIndexPlan)expression).getIndexName());
            possibleTypeSets.push(context.getMetaData().recordTypesForIndex(index).stream()
                    .map(RecordType::getName).collect(Collectors.toSet()));
        } else if (expression instanceof TypeFilterExpression) {
            Set<String> childTypeSet = possibleTypeSets.pop();
            possibleTypeSets.push(Sets.filter(childTypeSet,
                    ((TypeFilterExpression)expression).getRecordTypes()::contains));
        } else if (expression instanceof RelationalExpressionWithChildren &&
                   ((RelationalExpressionWithChildren)expression).getRelationalChildCount() > 1) {
            // If we have a single child, then there is a reasonable default for how most relational expressions will
            // change the set of record types (i.e., they won't change them at all). However, if you have several relational
            // children (like a union or intersection expression) then we must specify some way to combine them.
            int relationalChildCount = ((RelationalExpressionWithChildren)expression).getRelationalChildCount();
            if (expression instanceof RecordQueryUnionPlan || expression instanceof RecordQueryIntersectionPlan) {
                Set<String> union = new HashSet<>();
                for (int i = 0; i < relationalChildCount; i++) {
                    union.addAll(possibleTypeSets.pop());
                }
                possibleTypeSets.push(union);
            } else {
                throw new RecordCoreException("tried to find record types for a relational expression with multiple " +
                                              "relational children, but no combiner was specified");
            }
        }

        return true; // always proceed to inspect siblings
    }

    @Override
    public boolean visitLeave(@Nonnull ExpressionRef<? extends PlannerExpression> ref) {
        return true;
    }

    @Nonnull
    public static Set<String> evaluate(@Nonnull PlanContext context, ExpressionRef<? extends PlannerExpression> ref) {
        RecordTypesProperty visitor = new RecordTypesProperty(context);
        ref.acceptPropertyVisitor(visitor);
        if (visitor.possibleTypeSets.size() != 1) {
            throw new RecordCoreException("error while finding record types: several possible type sets left after completing traversal");
        }
        return visitor.possibleTypeSets.getFirst();
    }

}
