/*
 * BooleanPredicateNormalizer.java
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
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.NotPredicate;
import com.apple.foundationdb.record.query.predicates.OrPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A normalizer of a tree of {@link QueryPredicate}s into disjunctive normal form.
 * <p>
 * The full normal form has a single {@code Or} at the root, all of whose children are {@code And}, none of whose
 * children have other Boolean operators. This is abbreviated to exclude parent Boolean nodes with only one child.
 * The intermediate form for the normalizer is a list of lists.
 * </p>
 *
 * <p>
 * The <em>size</em> of a Boolean expression in disjunctive normal form (DNF) is the number of terms in the outermost
 * {@code Or} <a href="http://www.contrib.andrew.cmu.edu/~ryanod/?p=646">[O'Donnell 2014]</a>.
 * The {@code BooleanPredicateNormalizer} will not normalize a {@link QueryPredicate} if the normalized form would have a size
 * that exceeds the size limit. This limit is useful to avoid wastefully normalizing expressions with a very large DNF.
 * In some cases, such as a large expression in conjunctive normal form (CNF), attempting to normalize such an expression
 * will cause out-of-memory errors.
 * </p>
 */
@API(API.Status.INTERNAL)
public class BooleanPredicateNormalizer {
    /**
     * The default limit on the size of the DNF that will be produced by the normalizer.
     */
    public static final int DEFAULT_SIZE_LIMIT = 1_000_000;
    private static final BooleanPredicateNormalizer DEFAULT = new BooleanPredicateNormalizer(DEFAULT_SIZE_LIMIT, false);

    private final int sizeLimit;
    private final boolean checkForDuplicateConditions;

    private BooleanPredicateNormalizer(int sizeLimit, boolean checkForDuplicateConditions) {
        this.sizeLimit = sizeLimit;
        this.checkForDuplicateConditions = checkForDuplicateConditions;
    }

    /**
     * Obtain a normalizer with the default size limit {@link BooleanPredicateNormalizer#DEFAULT_SIZE_LIMIT}.
     * @return a normalizer with the default size limit
     */
    @Nonnull
    public static BooleanPredicateNormalizer getDefaultInstance() {
        return DEFAULT;
    }

    /**
     * Obtain a normalizer with the given size limit.
     * @param sizeLimit a limit on the size of DNF that this normalizer will produce
     * @return a normalizer with the given size limit
     */
    @Nonnull
    public static BooleanPredicateNormalizer withLimit(int sizeLimit) {
        if (sizeLimit == DEFAULT_SIZE_LIMIT) {
            return DEFAULT;
        }
        return new BooleanPredicateNormalizer(sizeLimit, false);
    }

    /**
     * Obtain a normalizer for the given planner configuration.
     * @param configuration a a planner configuration specifying the DNF limit that this normalizer will produce
     * @return a normalizer for the given planner configuration
     */
    public static BooleanPredicateNormalizer forConfiguration(RecordQueryPlannerConfiguration configuration) {
        if (configuration.getComplexityThreshold() == DEFAULT_SIZE_LIMIT && !configuration.shouldCheckForDuplicateConditions()) {
            return DEFAULT;
        }
        return new BooleanPredicateNormalizer(configuration.getComplexityThreshold(), configuration.shouldCheckForDuplicateConditions());
    }

    /**
     * Get the limit on the size of the DNF that this normalizer will produce.
     * @return size limit
     */
    public int getSizeLimit() {
        return sizeLimit;
    }

    /**
     * Get whether this normalizer attempts to remove redundant conditions.
     * @return {@code true} if some redundant conditions
     */
    public boolean isCheckForDuplicateConditions() {
        return checkForDuplicateConditions;
    }

    /**
     * Convert the given predicate to disjunctive normal form, if necessary. If the size of the DNF would exceed the
     * size limit, return the un-normalized predicate.
     * @param predicate the query predicate to be normalized
     * @return the predicate in disjunctive normal form if it does not exceed the size limit or the predicate otherwise
     */
    @Nullable
    public QueryPredicate normalizeIfPossible(@Nullable final QueryPredicate predicate) {
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
    public QueryPredicate normalize(@Nullable final QueryPredicate predicate) {
        return normalize(predicate, true);
    }

    @Nullable
    private QueryPredicate normalize(@Nullable final QueryPredicate predicate, boolean failIfTooLarge) {
        if (!needsNormalize(predicate)) {
            return predicate;
        } else if (!shouldNormalize(predicate)) {
            if (failIfTooLarge) {
                throw new DNFTooLargeException(Objects.requireNonNull(predicate));
            } else {
                return predicate;
            }
        } else {
            final List<List<? extends QueryPredicate>> orOfAnd = toDNF(Objects.requireNonNull(predicate), false);
            if (checkForDuplicateConditions) {
                removeDuplicateConditions(orOfAnd);
            }
            return normalOr(orOfAnd.stream().map(this::normalAnd).collect(Collectors.toList()));
        }
    }

    private boolean needsNormalize(@Nullable final QueryPredicate predicate) {
        if (predicate == null) {
            return false;
        }

        return isBooleanPredicate(predicate) &&
               StreamSupport.stream(predicate.getChildren().spliterator(), false).anyMatch(this::isBooleanPredicate);
    }

    private boolean shouldNormalize(@Nullable final QueryPredicate predicate) {
        try {
            return getNormalizedSize(predicate) <= sizeLimit;
        } catch (ArithmeticException e) {
            // Our computation caused an integer overflow so the DNF is _definitely_ too big.
            return false;
        }
    }

    private boolean isBooleanPredicate(@Nullable final QueryPredicate predicate) {
        return predicate instanceof AndOrPredicate ||
               predicate instanceof NotPredicate;
    }

    int getNormalizedSize(@Nullable final QueryPredicate predicate) {
        if (predicate == null) {
            return 0;
        }
        return toDNFSize(predicate, false);
    }

    private int toDNFSize(@Nonnull final QueryPredicate predicate, final boolean negate) {
        if (predicate instanceof AndPredicate) {
            final List<? extends QueryPredicate> children = ((AndPredicate)predicate).getChildren();
            return negate ? orToDNFSize(children, true) : andToDNFSize(children, false);
        } else if (predicate instanceof OrPredicate) {
            final List<? extends QueryPredicate> children = ((OrPredicate)predicate).getChildren();
            return negate ? andToDNFSize(children, true) : orToDNFSize(children, false);
        } else if (predicate instanceof NotPredicate) {
            return toDNFSize(((NotPredicate)predicate).getChild(), !negate);
        } else {
            return 1;
        }
    }

    private int orToDNFSize(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        return children.stream().mapToInt(p -> toDNFSize(p, negate)).reduce(0, Math::addExact);
    }

    private int andToDNFSize(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        return children.stream().mapToInt(child -> toDNFSize(child, negate)).reduce(1, Math::multiplyExact);
    }

    @Nonnull
    private QueryPredicate normalOr(@Nonnull final List<? extends QueryPredicate> children) {
        if (children.size() == 1) {
            return children.get(0);
        } else {
            return OrPredicate.or(children);
        }
    }

    @Nonnull
    private QueryPredicate normalAnd(@Nonnull final List<? extends QueryPredicate> children) {
        if (children.size() == 1) {
            return children.get(0);
        } else {
            return AndPredicate.and(children);
        }
    }

    /**
     * Convert given predicate to list (to be Or'ed) of lists (to be And'ed).
     * @param predicate a predicate subtree
     * @param negate whether this subtree is negated
     * @return a list (to be Or'ed) of lists (to be And'ed)
     */
    @Nonnull
    private List<List<? extends QueryPredicate>> toDNF(@Nonnull final QueryPredicate predicate, final boolean negate) {
        if (predicate instanceof AndPredicate) {
            final List<? extends QueryPredicate> children = ((AndPredicate)predicate).getChildren();
            return negate ? orToDNF(children, true) : andToDNF(children, false);
        } else if (predicate instanceof OrPredicate) {
            final List<? extends QueryPredicate> children = ((OrPredicate)predicate).getChildren();
            return negate ? andToDNF(children, true) : orToDNF(children, false);
        } else if (predicate instanceof NotPredicate) {
            return toDNF(((NotPredicate)predicate).getChild(), !negate);
        } else {
            return Collections.singletonList(Collections.singletonList(negate ? new NotPredicate(predicate) : predicate));
        }
    }

    /**
     * <code>Or</code> flattens all normalized Or's of its children.
     * @param children arguments to Or
     * @param negate whether the Or is negated
     * @return a list (to be Or'ed) of lists (to be And'ed)
     */
    @Nonnull
    private List<List<? extends QueryPredicate>> orToDNF(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        final List<List<? extends QueryPredicate>> result = new ArrayList<>();
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
    private List<List<? extends QueryPredicate>> andToDNF(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        return andToDNF(children, 0, negate, Collections.singletonList(Collections.emptyList()));
    }

    @Nonnull
    private List<List<? extends QueryPredicate>> andToDNF(@Nonnull final List<? extends QueryPredicate> children, int index,
                                                          final boolean negate,
                                                          @Nonnull final List<List<? extends QueryPredicate>> crossProductSoFar) {
        if (index >= children.size()) {
            return crossProductSoFar;
        }
        return andToDNF(children, index + 1, negate,
                // Add each of the next child's alternatives to the each of the elements of the cross product so far.
                toDNF(children.get(index), negate).stream().flatMap(right -> crossProductSoFar.stream().map(left -> {
                    final List<QueryPredicate> combined = new ArrayList<>(left);
                    combined.addAll(right);
                    return combined;
                })).collect(Collectors.toList()));
    }

    @SuppressWarnings({"java:S1119", "java:S135", "java:S1066", "SuspiciousMethodCalls"})
    private void removeDuplicateConditions(final List<List<? extends QueryPredicate>> orOfAnd) {
        int size = orOfAnd.size();
        if (size < 2) {
            return;
        }
        int i = 0;
        nexti:
        while (i < size) {
            Collection<? extends QueryPredicate> ci = orOfAnd.get(i);
            if (ci.size() > 1) {
                // Should be faster for contains.
                // If QueryComponent's cached their hashCode, even single element might be faster as Set.
                ci = new HashSet<>(ci);
            }
            // If any other disjunct has conjuncts that are a (not necessarily strict) subset of this one's, then
            // whenever this one is satisfied, that one will be as well.
            // There is therefore no need to keep having this one.
            for (int j = 0; j < size; j++) {
                if (i == j) {
                    continue;
                }
                List<? extends QueryPredicate> cj = orOfAnd.get(j);
                if (ci.size() > cj.size() || (ci.size() == cj.size() && i < j)) {
                    if (ci.containsAll(cj)) {
                        orOfAnd.remove(i);
                        size--;
                        continue nexti;
                    }
                }
            }
            i++;
        }
    }

    @SuppressWarnings("java:S110")
    class DNFTooLargeException extends RecordCoreException {
        private static final long serialVersionUID = 1L;

        public DNFTooLargeException(@Nonnull final QueryPredicate predicate) {
            super("tried to normalize to a DNF but the size would have been too big");
            addLogInfo(LogMessageKeys.FILTER, predicate);
            addLogInfo(LogMessageKeys.DNF_SIZE_LIMIT, sizeLimit);
        }
    }
}
