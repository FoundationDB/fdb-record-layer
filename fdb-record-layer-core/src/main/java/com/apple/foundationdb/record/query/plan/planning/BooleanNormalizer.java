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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
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
 * The full normal form has a single {@code Or} at the root, all of whose children are {@code And}, none of whose
 * children have other Boolean operators. This is abbreviated to exclude parent Boolean nodes with only one child.
 * The intermediate form for the normalizer is a list of lists.
 * </p>
 *
 * <p>
 * The <em>size</em> of a Boolean expression in disjunctive normal form (DNF) is the number of terms in the outermost
 * {@code Or} <a href="http://www.contrib.andrew.cmu.edu/~ryanod/?p=646">[O'Donnell 2014]</a>.
 * The {@code BooleanNormalizer} will not normalize a {@link QueryComponent} if the normalized form would have a size
 * that exceeds the size limit. This limit is useful to avoid wastefully normalizing expressions with a very large DNF.
 * In some cases, such as a large expression in conjunctive normal form (CNF), attempting to normalize such an expression
 * will cause out-of-memory errors.
 * </p>
 */
@API(API.Status.INTERNAL)
public class BooleanNormalizer {
    /**
     * The default limit on the size of the DNF that will be produced by the normalizer.
     */
    public static final int DEFAULT_SIZE_LIMIT = 1_000_000;
    private static final BooleanNormalizer DEFAULT = new BooleanNormalizer(DEFAULT_SIZE_LIMIT);

    private final int sizeLimit;

    private BooleanNormalizer(int sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    /**
     * Obtain a normalizer with the default size limit {@link BooleanNormalizer#DEFAULT_SIZE_LIMIT}.
     * @return a normalizer with the default size limit
     */
    @Nonnull
    public static BooleanNormalizer getDefaultInstance() {
        return DEFAULT;
    }

    /**
     * Obtain a normalizer with the given size limit.
     * @param sizeLimit a limit on the size of DNF that this normalizer will produce
     * @return a normalizer with the given size limit
     */
    @Nonnull
    public static BooleanNormalizer withLimit(int sizeLimit) {
        if (sizeLimit == DEFAULT_SIZE_LIMIT) {
            return DEFAULT;
        }
        return new BooleanNormalizer(sizeLimit);
    }

    /**
     * Convert the given predicate to disjunctive normal form, if necessary. If the size of the DNF would exceed the
     * size limit, return the un-normalized predicate.
     * @param predicate the query predicate to be normalized
     * @return the predicate in disjunctive normal form if it does not exceed the size limit or the predicate otherwise
     */
    @Nullable
    public QueryComponent normalizeIfPossible(@Nullable final QueryComponent predicate) {
        return normalize(predicate, false);
    }

    /**
     * Convert the given predicate to disjunctive normal form, if necessary. If the size of the DNF would exceed the
     * size limit, throw a {@link DNFTooLargeException}.
     * @param predicate the query predicate to be normalized
     * @return the predicate in disjunctive normal form
     * @throws DNFTooLargeException if the DNF would exceed the size limit
     */
    @Nullable
    public QueryComponent normalize(@Nullable final QueryComponent predicate) {
        return normalize(predicate, true);
    }

    @Nullable
    private QueryComponent normalize(@Nullable final QueryComponent predicate, boolean failIfTooLarge) {
        if (!needsNormalize(predicate)) {
            return predicate;
        } else if (!shouldNormalize(predicate)) {
            if (failIfTooLarge) {
                throw new DNFTooLargeException(predicate);
            } else {
                return predicate;
            }
        } else {
            final List<List<QueryComponent>> orOfAnd = toDNF(predicate, false);
            return normalOr(orOfAnd.stream().map(this::normalAnd).collect(Collectors.toList()));
        }
    }

    private boolean needsNormalize(@Nullable final QueryComponent predicate) {
        return isBooleanPredicate(predicate) &&
               (predicate instanceof ComponentWithChildren ?
                ((ComponentWithChildren)predicate).getChildren().stream().anyMatch(this::isBooleanPredicate) :
                isBooleanPredicate(((ComponentWithSingleChild)predicate).getChild()));
    }

    private boolean shouldNormalize(@Nullable final QueryComponent predicate) {
        try {
            return getNormalizedSize(predicate) <= sizeLimit;
        } catch (ArithmeticException e) {
            // Our computation caused an integer overflow so the DNF is _definitely_ too big.
            return false;
        }
    }

    private boolean isBooleanPredicate(@Nullable final QueryComponent predicate) {
        return predicate instanceof AndComponent ||
               predicate instanceof OrComponent ||
               predicate instanceof NotComponent;
    }

    int getNormalizedSize(@Nullable final QueryComponent predicate) {
        if (predicate == null) {
            return 0;
        }
        return toDNFSize(predicate, false);
    }

    private int toDNFSize(@Nonnull final QueryComponent predicate, final boolean negate) {
        if (predicate instanceof AndComponent) {
            final List<QueryComponent> children = ((AndComponent)predicate).getChildren();
            return negate ? orToDNFSize(children, true) : andToDNFSize(children, false);
        } else if (predicate instanceof OrComponent) {
            final List<QueryComponent> children = ((OrComponent)predicate).getChildren();
            return negate ? andToDNFSize(children, true) : orToDNFSize(children, false);
        } else if (predicate instanceof NotComponent) {
            return toDNFSize(((NotComponent)predicate).getChild(), !negate);
        } else {
            return 1;
        }
    }

    private int orToDNFSize(@Nonnull final List<QueryComponent> children, final boolean negate) {
        return children.stream().mapToInt(p -> toDNFSize(p, negate)).reduce(0, Math::addExact);
    }

    private int andToDNFSize(@Nonnull final List<QueryComponent> children, final boolean negate) {
        return children.stream().mapToInt(child -> toDNFSize(child, negate)).reduce(1, Math::multiplyExact);
    }

    @Nonnull
    private QueryComponent normalOr(@Nonnull final List<QueryComponent> children) {
        if (children.size() == 1) {
            return children.get(0);
        } else {
            return OrComponent.from(children);
        }
    }

    @Nonnull
    private QueryComponent normalAnd(@Nonnull final List<QueryComponent> children) {
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
    private List<List<QueryComponent>> toDNF(@Nonnull final QueryComponent predicate, final boolean negate) {
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
    private List<List<QueryComponent>> orToDNF(@Nonnull final List<QueryComponent> children, final boolean negate) {
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
    private List<List<QueryComponent>> andToDNF(@Nonnull final List<QueryComponent> children, final boolean negate) {
        return andToDNF(children, 0, negate, Collections.singletonList(Collections.emptyList()));
    }

    @Nonnull
    private List<List<QueryComponent>> andToDNF(@Nonnull final List<QueryComponent> children, int index,
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

    class DNFTooLargeException extends RecordCoreException {
        private static final long serialVersionUID = 1L;

        public DNFTooLargeException(@Nonnull final QueryComponent predicate) {
            super("tried to normalize to a DNF but the size would have been too big");
            addLogInfo(LogMessageKeys.FILTER, predicate);
            addLogInfo(LogMessageKeys.DNF_SIZE_LIMIT, sizeLimit);
        }
    }
}
