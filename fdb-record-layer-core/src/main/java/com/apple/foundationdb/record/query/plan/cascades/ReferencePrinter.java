/*
 * ReferencePrinter.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class for printing out a {@link Reference} in a readable form.
 * <br>
 * It is generally a bit tricky to look at memoized {@link RelationalExpression}s inside a complex {@link Reference}
 * structure because the structure is a directed acyclic graph rather than a tree. The {@code ReferencePrinter}
 * walks the reference structure and produces a compact representation of the DAG by referring to groups by
 * pre-determined identifiers.
 */
public class ReferencePrinter {
    @Nonnull
    private final Reference rootGroup;
    @Nonnull
    private final Map<Reference, Integer> seenGroups;
    @Nonnull
    private final List<Reference> groups;
    private int nextId;

    public ReferencePrinter(@Nonnull Reference rootGroup) {
        this.rootGroup = rootGroup;
        this.seenGroups = new HashMap<>();
        this.groups = new ArrayList<>();
        this.nextId = 0;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        exploreGroup(rootGroup);
        for (Reference group : groups) {
            builder.append("Group ")
                    .append(group.hashCode())
                    .append(": [");
            for (RelationalExpression member : group.getMembers()) {
                final List<? extends Quantifier> quantifiers = member.getQuantifiers();
                if (quantifiers.isEmpty()) {
                    builder.append(member);
                } else {
                    builder.append(member.getClass().getSimpleName())
                            .append("{ ");
                    for (final Quantifier quantifier : quantifiers) {
                        builder.append(quantifier.getShorthand()).append(" ");
                        final Reference rangesOver = quantifier.getRangesOver();
                        builder.append(rangesOver.hashCode())
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

    private void exploreGroup(@Nonnull Reference ref) {
        seenGroups.put(ref, nextId);
        groups.add(ref);
        nextId++;
        for (RelationalExpression member : ref.getMembers()) {
            exploreExpression(member);
        }
    }

    private void exploreExpression(@Nonnull RelationalExpression expression) {
        final List<? extends Quantifier> quantifiers = expression.getQuantifiers();
        for (final Quantifier quantifier : quantifiers) {
            final Reference rangesOver = quantifier.getRangesOver();
            if (!seenGroups.containsKey(rangesOver)) {
                exploreGroup(rangesOver);
            }
        }
    }
}
