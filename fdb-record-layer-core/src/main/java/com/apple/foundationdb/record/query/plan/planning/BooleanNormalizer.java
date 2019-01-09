/*
 * BooleanNormalizer.java
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.ComponentWithChildren;
import com.apple.foundationdb.record.query.expressions.ComponentWithSingleChild;
import com.apple.foundationdb.record.query.expressions.NotComponent;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A normalizer of a tree of {@link QueryComponent} predicates into disjunctive normal form.
 * <p>
 * The full normal form has a single {@code Or} at the root, all of whose children at {@code And}, none of whose children have other Boolean operators.
 * This is abbreviated to exclude parent Boolean nodes with only one child.
 * The intermediate form for the normalizer is a list of lists.
 * </p>
 */
@API(API.Status.INTERNAL)
public class BooleanNormalizer {
    private BooleanNormalizer() {
    }

    /**
     * Convert given predicate to disjunctive normal form, if necessary.
     * @param predicate the query predicate to be normalized
     * @return the predicate in disjunctive normal form
     */
    @Nullable
    public static QueryComponent normalize(@Nullable final QueryComponent predicate) {
        if (!needsNormalize(predicate)) {
            return predicate;
        } else {
            final List<List<QueryComponent>> orOfAnd = toDNF(predicate, false);
            return normalOr(orOfAnd.stream().map(BooleanNormalizer::normalAnd).collect(Collectors.toList()));
        }
    }

    protected static boolean needsNormalize(@Nullable final QueryComponent predicate) {
        return isBooleanPredicate(predicate) &&
            (predicate instanceof ComponentWithChildren ?
             ((ComponentWithChildren)predicate).getChildren().stream().anyMatch(BooleanNormalizer::isBooleanPredicate) :
             isBooleanPredicate(((ComponentWithSingleChild)predicate).getChild()));
    }

    protected static boolean isBooleanPredicate(@Nullable final QueryComponent predicate) {
        return predicate instanceof AndComponent ||
               predicate instanceof OrComponent ||
               predicate instanceof NotComponent;
    }

    @Nonnull
    protected static QueryComponent normalOr(@Nonnull final List<QueryComponent> children) {
        if (children.size() == 1) {
            return children.get(0);
        } else {
            return OrComponent.from(children);
        }
    }

    @Nonnull
    protected static QueryComponent normalAnd(@Nonnull final List<QueryComponent> children) {
        if (children.size() == 1) {
            return children.get(0);
        } else {
            return AndComponent.from(children);
        }
    }

    /**
     * Convert given predicate to list (to be Or'ed) of lists (to be And'ed).
     * @param predicate a predicate subtree
     * @param negate whether this subtree is negated
     * @return a list (to be Or'ed) of lists (to be And'ed)
     */
    @Nonnull
    protected static List<List<QueryComponent>> toDNF(@Nonnull final QueryComponent predicate, final boolean negate) {
        if (predicate instanceof AndComponent) {
            final List<QueryComponent> children = ((AndComponent)predicate).getChildren();
            return negate ? orToDNF(children, true) : andToDNF(children, false);
        } else if (predicate instanceof OrComponent) {
            final List<QueryComponent> children = ((OrComponent)predicate).getChildren();
            return negate ? andToDNF(children, true) : orToDNF(children, false);
        } else if (predicate instanceof NotComponent) {
            return toDNF(((NotComponent)predicate).getChild(), !negate);
        } else {
            return Collections.singletonList(Collections.singletonList(negate ? Query.not(predicate) : predicate));
        }
    }

    /**
     * <code>Or</code> flattens all normalized Or's of its children.
     * @param children arguments to Or
     * @param negate whether the Or is negated
     * @return a list (to be Or'ed) of lists (to be And'ed)
     */
    @Nonnull
    protected static List<List<QueryComponent>> orToDNF(@Nonnull final List<QueryComponent> children, final boolean negate) {
        final List<List<QueryComponent>> result = new ArrayList<>();
        children.stream().map(p -> toDNF(p, negate)).forEach(result::addAll);
        return result;
    }

    /**
     * <code>And</code> combines all the normalized Or's into a cross-product, And'ing one choice from each.
     * @param children arguments to And
     * @param negate whether the And is negated
     * @return a list (to be Or'ed) of lists (to be And'ed)
     */
    @Nonnull
    protected static List<List<QueryComponent>> andToDNF(@Nonnull final List<QueryComponent> children, final boolean negate) {
        return andToDNF(children, 0, negate, Collections.singletonList(Collections.emptyList()));
    }

    @Nonnull
    protected static List<List<QueryComponent>> andToDNF(@Nonnull final List<QueryComponent> children, int index,
                                                         final boolean negate,
                                                         @Nonnull final List<List<QueryComponent>> crossProductSoFar) {
        if (index >= children.size()) {
            return crossProductSoFar;
        }
        return andToDNF(children, index + 1, negate,
                // Add each of the next child's alternatives to the each of the elements of the cross product so far.
                toDNF(children.get(index), negate).stream().flatMap(right -> crossProductSoFar.stream().map(left -> {
                    final List<QueryComponent> combined = new ArrayList<>(left);
                    combined.addAll(right);
                    return combined;
                })).collect(Collectors.toList()));
    }

}
