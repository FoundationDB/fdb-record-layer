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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A normalizer of a tree of {@link QueryPredicate}s into a normal form which may be its conjunctive or disjunctive
 * normal form.
 * <p>
 * The full normal form has a single {@code Or} or {@code And} at the root, all of whose children are {@code And} resp. {@code Or},
 * none of whose children have other Boolean operators. This is abbreviated to exclude parent Boolean nodes with only one child.
 * The intermediate form for the normalizer is a list of lists.
 * </p>
 *
 * <p>
 * The <em>size</em> of a Boolean expression in normal form is the number of terms in the outermost
 * {@code Or} <a href="http://www.contrib.andrew.cmu.edu/~ryanod/?p=646">[O'Donnell 2014]</a>.
 * The {@code BooleanPredicateNormalizer} will not normalize a {@link QueryPredicate} if the normalized form would have a size
 * that exceeds the size limit. This limit is useful to avoid wastefully normalizing expressions with a very large normal form.
 *
 * We distinguish CNFs (conjunctive normal forms) and DNF (disjunctive normal forms). The logic to obtain these normal
 * forms is identical except that the roles or {@code Or}s and {@code And}s are reversed. In order to name identifiers
 * in the code in a meaningful way, we talk about major and minor where CNF uses a major of {@code And} and a minor
 * of {@code Or} and a DNF uses a major of {@code Or} and a minor of {@code And}.
 * </p>
 */
@API(API.Status.INTERNAL)
public class BooleanPredicateNormalizer {
    @SpotBugsSuppressWarnings(justification = "https://github.com/spotbugs/spotbugs/issues/740", value = "SE_BAD_FIELD")
    public enum Mode {
        DNF(OrPredicate.class, OrPredicate::or, AndPredicate.class, AndPredicate::and),
        CNF(AndPredicate.class, AndPredicate::and, OrPredicate.class, OrPredicate::or);

        @Nonnull
        private final Class<? extends AndOrPredicate> majorClass;
        @Nonnull
        private final Function<Collection<? extends QueryPredicate>, QueryPredicate> majorGenerator;
        @Nonnull
        private final Class<? extends AndOrPredicate> minorClass;
        @Nonnull
        private final Function<Collection<? extends QueryPredicate>, QueryPredicate> minorGenerator;

        Mode(@Nonnull final Class<? extends AndOrPredicate> majorClass,
                 @Nonnull final Function<Collection<? extends QueryPredicate>, QueryPredicate> majorGenerator,
                 @Nonnull final Class<? extends AndOrPredicate> minorClass,
                 @Nonnull final Function<Collection<? extends QueryPredicate>, QueryPredicate> minorGenerator) {
            this.majorClass = majorClass;
            this.majorGenerator = majorGenerator;
            this.minorClass = minorClass;
            this.minorGenerator = minorGenerator;
        }

        public boolean instanceOfMajorClass(@Nullable final QueryPredicate andOrPredicate) {
            return majorClass.isInstance(andOrPredicate);
        }

        public boolean instanceOfMinorClass(@Nullable final QueryPredicate andOrPredicate) {
            return minorClass.isInstance(andOrPredicate);
        }

        @Nonnull
        public QueryPredicate majorWithChildren(@Nonnull Collection<? extends QueryPredicate> children) {
            return majorGenerator.apply(children);
        }

        @Nonnull
        public QueryPredicate minorWithChildren(@Nonnull Collection<? extends QueryPredicate> children) {
            return minorGenerator.apply(children);
        }
    }

    /**
     * The default limit on the size of the normal form that will be produced by the normalizer.
     */
    public static final int DEFAULT_SIZE_LIMIT = 1_000_000;
    private static final BooleanPredicateNormalizer DEFAULT_DNF = new BooleanPredicateNormalizer(Mode.DNF, DEFAULT_SIZE_LIMIT);
    private static final BooleanPredicateNormalizer DEFAULT_CNF = new BooleanPredicateNormalizer(Mode.CNF, DEFAULT_SIZE_LIMIT);

    @Nonnull
    private final Mode mode;
    private final int sizeLimit;

    private BooleanPredicateNormalizer(@Nonnull final Mode mode, int sizeLimit) {
        this.mode = mode;
        this.sizeLimit = sizeLimit;
    }

    /**
     * Obtain a (dnf-) normalizer with the default size limit {@link BooleanPredicateNormalizer#DEFAULT_SIZE_LIMIT}.
     * @return a normalizer with the default size limit
     */
    @Nonnull
    public static BooleanPredicateNormalizer getDefaultInstanceForDnf() {
        return DEFAULT_DNF;
    }

    /**
     * Obtain a (cnf-) normalizer with the default size limit {@link BooleanPredicateNormalizer#DEFAULT_SIZE_LIMIT}.
     * @return a normalizer with the default size limit
     */
    @Nonnull
    public static BooleanPredicateNormalizer getDefaultInstanceForCnf() {
        return DEFAULT_CNF;
    }

    /**
     * Obtain a normalizer with the given size limit.
     * @param mode the mode of the new normalizer
     * @param sizeLimit a limit on the size of normalized form that this normalizer will produce
     * @return a normalizer with the given size limit
     */
    @Nonnull
    public static BooleanPredicateNormalizer withLimit(@Nonnull Mode mode, int sizeLimit) {
        if (sizeLimit == DEFAULT_SIZE_LIMIT) {
            return getDefaultInstance(mode);
        }
        return new BooleanPredicateNormalizer(mode, sizeLimit);
    }

    @Nonnull
    private static BooleanPredicateNormalizer getDefaultInstance(@Nonnull final Mode mode) {
        return mode == Mode.CNF
               ? DEFAULT_CNF
               : DEFAULT_DNF;
    }

    /**
     * Obtain a normalizer for the given planner configuration.
     * @param mode the mode of the new normalizer
     * @param configuration a a planner configuration specifying the normalization limit that this normalizer will produce
     * @return a normalizer for the given planner configuration
     */
    public static BooleanPredicateNormalizer forConfiguration(@Nonnull Mode mode, @Nonnull final RecordQueryPlannerConfiguration configuration) {
        if (configuration.getComplexityThreshold() == DEFAULT_SIZE_LIMIT) {
            return getDefaultInstance(mode);
        }
        return new BooleanPredicateNormalizer(mode, configuration.getComplexityThreshold());
    }

    /**
     * Get the mode of this normalizer.
     * @return the mode used by this normalizer
     */
    @Nonnull
    public Mode getMode() {
        return mode;
    }

    /**
     * Get the limit on the size of the normalized form that this normalizer will produce.
     * @return size limit
     */
    public int getSizeLimit() {
        return sizeLimit;
    }

    /**
     * Convert the given predicate to a normal form, if necessary. If the size of the normal form would exceed the
     * size limit, return {@code Optional.empty()}.
     * @param predicate the query predicate to be normalized
     * @return the predicate in normal form if it does not exceed the size limit or {@code Optional.empty()} otherwise
     */
    @Nonnull
    public Optional<QueryPredicate> normalizeIfPossible(@Nullable final QueryPredicate predicate) {
        return normalize(predicate, false);
    }

    /**
     * Convert the given predicate to a normal form, if necessary. If the size of the normal form would exceed the
     * size limit, throw a {@link NormalFormTooLargeException}.
     * @param predicate the query predicate to be normalized
     * @return the predicate in normal form
     * @throws NormalFormTooLargeException if the normal form would exceed the size limit
     */
    @Nonnull
    public Optional<QueryPredicate> normalize(@Nullable final QueryPredicate predicate) {
        return normalize(predicate, true);
    }

    /**
     * Convert the given predicate to its respective normal form, if necessary. If the size of the normal form would
     * exceed the size limit, throw a {@link NormalFormTooLargeException}.
     * @param predicate the query predicate to be normalized
     * @param failIfTooLarge indicates whether we should throw an exception if the normalization is too big
     *        (beyond {@link #sizeLimit}).
     * @return the predicate in normal form
     * @throws NormalFormTooLargeException if the normal form would exceed the size limit
     */
    @Nonnull
    public Optional<QueryPredicate> normalize(@Nullable final QueryPredicate predicate, boolean failIfTooLarge) {
        if (!needsNormalize(predicate)) {
            return Optional.empty();
        } else if (!shouldNormalize(predicate)) {
            if (failIfTooLarge) {
                throw new NormalFormTooLargeException(Objects.requireNonNull(predicate));
            } else {
                return Optional.empty();
            }
        } else {
            final List<Collection<? extends QueryPredicate>> majorOfMinor = toNormalized(Objects.requireNonNull(predicate), false);
            trimTerms(majorOfMinor);
            return Optional.of(mode.majorWithChildren(majorOfMinor.stream().map(mode::minorWithChildren).collect(Collectors.toList())));
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
            // Our computation caused an integer overflow so the normal form would  _definitely_ be too big.
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
        return toNormalizedSize(predicate, false);
    }

    private int toNormalizedSize(@Nonnull final QueryPredicate predicate, final boolean negate) {
        if (mode.instanceOfMinorClass(predicate)) {
            final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
            return negate ? majorToNormalizedSize(children, true) : minorToNormalizedSize(children, false);
        } else if (mode.instanceOfMajorClass(predicate)) {
            final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
            return negate ? minorToNormalizedSize(children, true) : majorToNormalizedSize(children, false);
        } else if (predicate instanceof NotPredicate) {
            return toNormalizedSize(((NotPredicate)predicate).getChild(), !negate);
        } else {
            return 1;
        }
    }

    private int majorToNormalizedSize(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        return children.stream().mapToInt(p -> toNormalizedSize(p, negate)).reduce(0, Math::addExact);
    }

    private int minorToNormalizedSize(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        return children.stream().mapToInt(child -> toNormalizedSize(child, negate)).reduce(1, Math::multiplyExact);
    }

    /**
     * Convert given predicate to list (to be combined using the major) of lists (to be combined using the minor).
     * @param predicate a predicate subtree
     * @param negate whether this subtree is negated
     * @return a list (to be major'ed) of lists (to be minor'ed)
     */
    @Nonnull
    private List<Collection<? extends QueryPredicate>> toNormalized(@Nonnull final QueryPredicate predicate, final boolean negate) {
        if (mode.instanceOfMinorClass(predicate)) {
            final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
            return negate ? majorToNormalized(children, true) : minorToNormalized(children, false);
        } else if (mode.instanceOfMajorClass(predicate)) {
            final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
            return negate ? minorToNormalized(children, true) : majorToNormalized(children, false);
        } else if (predicate instanceof NotPredicate) {
            return toNormalized(((NotPredicate)predicate).getChild(), !negate);
        } else {
            return Collections.singletonList(Collections.singletonList(negate ? NotPredicate.not(predicate) : predicate));
        }
    }

    /**
     * <code>Major</code> flattens all normalized majors of its children.
     * @param children arguments to the major
     * @param negate whether the major is negated
     * @return a list (to be major'ed) of lists (to be minor'ed)
     */
    @Nonnull
    private List<Collection<? extends QueryPredicate>> majorToNormalized(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        final List<Collection<? extends QueryPredicate>> result = new ArrayList<>();
        children.stream().map(p -> toNormalized(p, negate)).forEach(result::addAll);
        return result;
    }

    /**
     * <code>Minor</code> combines all the normalized majors into a cross-product, combining (via minor) one choice from each.
     * @param children arguments to the minor
     * @param negate whether the major is negated
     * @return a list (to be major'ed) of lists (to be minor'ed)
     */
    @Nonnull
    private List<Collection<? extends QueryPredicate>> minorToNormalized(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        return minorToNormalized(children, 0, negate, Collections.singletonList(Collections.emptyList()));
    }

    @Nonnull
    private List<Collection<? extends QueryPredicate>> minorToNormalized(@Nonnull final List<? extends QueryPredicate> children, int index,
                                                                         final boolean negate,
                                                                         @Nonnull final List<Collection<? extends QueryPredicate>> crossProductSoFar) {
        if (index >= children.size()) {
            return crossProductSoFar;
        }
        return minorToNormalized(children, index + 1, negate,
                // Add each of the next child's alternatives to the each of the elements of the cross product so far.
                toNormalized(children.get(index), negate).stream().flatMap(right -> crossProductSoFar.stream().map(left -> {
                    final List<QueryPredicate> combined = new ArrayList<>(left);
                    combined.addAll(right);
                    return combined;
                })).collect(Collectors.toList()));
    }

    @SuppressWarnings({"java:S1119", "java:S135", "java:S1066", "SuspiciousMethodCalls"})
    private void trimTerms(final List<Collection<? extends QueryPredicate>> majorOfMinor) {
        int size = majorOfMinor.size();
        if (size < 2) {
            return;
        }

        //
        // The following loop eliminates repetitions of an atom within a list of atoms to be minored, since
        // a ^ a == a and a v a == a
        //
        for (int i = 0; i < majorOfMinor.size(); i++) {
            final Collection<? extends QueryPredicate> minors = majorOfMinor.get(i);
            // de-dup and put the terms back into the list of lists
            majorOfMinor.set(i, new LinkedHashSet<>(minors));
        }
        
        //
        // The following loop attempts to find a list of terms (to be minored) within another list of terms (to be minored)
        // which would eventually be combined together using the major. This works both ways as both
        //
        // Major: Or; Minor: And
        // a v (a ^ x) = a
        //
        // as well as
        // Major: And; Minor: Or
        // a ^ (a v x) = a
        //
        int i = 0;
        nexti:
        while (i < size) {
            // this is a linked hash set -- see above
            Collection<? extends QueryPredicate> ci = majorOfMinor.get(i);

            // There is therefore no need to keep having this one.
            for (int j = 0; j < size; j++) {
                if (i == j) {
                    continue;
                }
                Collection<? extends QueryPredicate> cj = majorOfMinor.get(j);
                if (ci.size() > cj.size() || (ci.size() == cj.size() && i < j)) {
                    if (ci.containsAll(cj)) {
                        majorOfMinor.remove(i);
                        size--;
                        continue nexti;
                    }
                }
            }
            i++;
        }
    }

    @SuppressWarnings("java:S110")
    class NormalFormTooLargeException extends RecordCoreException {
        private static final long serialVersionUID = 1L;

        public NormalFormTooLargeException(@Nonnull final QueryPredicate predicate) {
            super("tried to normalize to a normal form but the size would have been too big");
            addLogInfo(LogMessageKeys.FILTER, predicate);
            addLogInfo(LogMessageKeys.DNF_SIZE_LIMIT, sizeLimit);
        }
    }
}
