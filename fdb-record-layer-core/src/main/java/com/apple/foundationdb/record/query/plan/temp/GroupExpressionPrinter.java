/*
 * GroupExpressionPrinter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordCoreException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A utility class for printing out a {@link GroupExpressionRef} in a readable form.
 *
 * It is generally a bit tricky to look at memoized {@link PlannerExpression}s inside a complex {@link GroupExpressionRef}
 * structure because the structure is a directed acyclic graph rather than a tree. The {@code GroupExpressionPrinter}
 * walks the reference structure and produces a compact representation of the DAG by referring to groups by
 * pre-determined identifiers.
 */
public class GroupExpressionPrinter {
    @Nonnull
    private final GroupExpressionRef<? extends PlannerExpression> rootGroup;
    @Nonnull
    private final Map<ExpressionRef<? extends PlannerExpression>, Integer> seenGroups;
    @Nonnull
    private final List<ExpressionRef<? extends PlannerExpression>> groups;
    private int nextId;

    public GroupExpressionPrinter(@Nonnull GroupExpressionRef<? extends PlannerExpression> rootGroup) {
        this.rootGroup = rootGroup;
        this.seenGroups = new HashMap<>();
        this.groups = new ArrayList<>();
        this.nextId = 0;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        exploreGroup(rootGroup);
        for (int i = 0; i < groups.size(); i++) {
            builder.append("Group ")
                    .append(i)
                    .append(": [");
            for (PlannerExpression member : groups.get(i).getMembers()) {
                Iterator<? extends ExpressionRef<? extends PlannerExpression>> children = member.getPlannerExpressionChildren();
                if (!children.hasNext()) {
                    builder.append(member);
                } else {
                    builder.append(member.getClass().getSimpleName())
                            .append("{ ");
                    while (children.hasNext()) {
                        builder.append(seenGroups.get(children.next()))
                                .append(", ");
                    }
                    builder.append("}");
                }
                builder.append(", ");
            }
            builder.append("]\n");
        }
        return builder.toString();
    }

    private void exploreGroup(@Nonnull ExpressionRef<? extends PlannerExpression> ref) {
        if (!(ref instanceof GroupExpressionRef)) {
            throw new RecordCoreException("tried to print a non-group reference with the GroupExpressionPrinter");
        }
        GroupExpressionRef<? extends PlannerExpression> groupRef = (GroupExpressionRef<? extends PlannerExpression>) ref;
        seenGroups.put(groupRef, nextId);
        groups.add(groupRef);
        nextId++;
        for (PlannerExpression member : groupRef.getMembers()) {
            exploreExpression(member);
        }
    }

    private void exploreExpression(@Nonnull PlannerExpression expression) {
        final Iterator<? extends ExpressionRef<? extends PlannerExpression>> childIterator = expression.getPlannerExpressionChildren();
        while (childIterator.hasNext()) {
            ExpressionRef<? extends PlannerExpression> childRef = childIterator.next();
            if (!seenGroups.containsKey(childRef)) {
                exploreGroup(childRef);
            }
        }
    }

}
