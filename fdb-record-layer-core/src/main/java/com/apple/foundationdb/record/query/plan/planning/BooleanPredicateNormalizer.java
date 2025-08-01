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
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.LeafQueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A normalizer of a tree of {@link QueryPredicate}s into a normal form which may be its conjunctive or disjunctive
 * normal form.
 * <br>
 * The full normal form has a single {@code Or} or {@code And} at the root, all of whose children are {@code And} resp. {@code Or},
 * none of whose children have other Boolean operators. This is abbreviated to exclude parent Boolean nodes with only one child.
 * The intermediate form for the normalizer is a list of lists.
 * <br>
 * The <em>size</em> of a Boolean expression in normal form is the number of terms in the outermost
 * {@code Or} <a href="http://www.contrib.andrew.cmu.edu/~ryanod/?p=646">[O'Donnell 2014]</a>.
 * The {@code BooleanPredicateNormalizer} will not normalize a {@link QueryPredicate} if the normalized form would have a size
 * that exceeds the size limit. This limit is useful to avoid wastefully normalizing expressions with a very large normal form.
 * <br>
 * We distinguish CNFs (conjunctive normal forms) and DNF (disjunctive normal forms). The logic to obtain these normal
 * forms is identical except that the roles or {@code Or}s and {@code And}s are reversed. In order to name identifiers
 * in the code in a meaningful way, we talk about major and minor where CNF uses a major of {@code And} and a minor
 * of {@code Or} and a DNF uses a major of {@code Or} and a minor of {@code And}.
 */
@API(API.Status.INTERNAL)
public class BooleanPredicateNormalizer {
    /**
     * The target normal form.
     */
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
     * Convert the given predicate to its respective normal form, if necessary. If the size of the normal form would
     * exceed the size limit, throw a {@link NormalFormTooLargeException}.
     * @param predicate the query predicate to be normalized
     * @param failIfTooLarge indicates whether we should throw an exception if the normalization is too big
     *        (beyond {@link #sizeLimit}).
     * @return the predicate in normal form
     * @throws NormalFormTooLargeException if the normal form would exceed the size limit
     */
    @Nonnull
    public Optional<QueryPredicate> normalizeAndSimplify(@Nullable final QueryPredicate predicate, boolean failIfTooLarge) {
        return normalizeInternal(predicate, failIfTooLarge)
                .map(majorOfMinor -> {
                    final var absorbed = applyAbsorptionLaw(majorOfMinor);
                    return mode.majorWithChildren(absorbed.stream().map(mode::minorWithChildren).collect(Collectors.toList()));
                });
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
        return normalizeInternal(predicate, failIfTooLarge)
                .map(majorOfMinor -> mode.majorWithChildren(majorOfMinor.stream().map(mode::minorWithChildren).collect(Collectors.toList())));
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
    private Optional<List<Collection<? extends QueryPredicate>>> normalizeInternal(@Nullable final QueryPredicate predicate, boolean failIfTooLarge) {
        if (isInNormalForm(predicate)) {
            return Optional.empty();
        } else if (!shouldNormalize(predicate)) {
            if (failIfTooLarge) {
                throw new NormalFormTooLargeException(Objects.requireNonNull(predicate));
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.of(toNormalized(Objects.requireNonNull(predicate), false));
        }
    }

    private boolean isInNormalForm(@Nullable final QueryPredicate predicate) {
        if (predicate == null) {
            return true;
        }

        if (isNormalFormVariableOrNotPredicate(predicate)) {
            return true;
        }

        if (predicate instanceof AndOrPredicate) {
            final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
            if (mode.instanceOfMajorClass(predicate)) {
                return children.stream()
                        .allMatch(child -> {
                            if (isNormalFormVariableOrNotPredicate(child)) {
                                return true;
                            }
                            if (mode.instanceOfMinorClass(child)) {
                                return ((AndOrPredicate)child).getChildren()
                                        .stream()
                                        .allMatch(BooleanPredicateNormalizer::isNormalFormVariableOrNotPredicate);
                            }
                            return false;
                        });
            } else if (mode.instanceOfMinorClass(predicate)) {
                return children
                        .stream()
                        .allMatch(BooleanPredicateNormalizer::isNormalFormVariableOrNotPredicate);
            }
        }

        if (predicate instanceof NotPredicate) {
            // this is a not(...) over something other than a variable
            return false;
        }

        throw new RecordCoreException("unknown boolean expression");
    }

    private boolean shouldNormalize(@Nullable final QueryPredicate predicate) {
        try {
            return getNormalizedSize(predicate) <= sizeLimit;
        } catch (ArithmeticException e) {
            // Our computation caused an integer overflow so the normal form would  _definitely_ be too big.
            return false;
        }
    }

    long getNormalizedSize(@Nullable final QueryPredicate predicate) {
        if (predicate == null) {
            return 0L;
        }
        return getMetrics(predicate).getNormalFormSize();
    }

    @Nonnull
    public PredicateMetrics getMetrics(@Nullable QueryPredicate predicate) {
        if (predicate == null) {
            return new PredicateMetrics(0, 0, 0);
        }
        return getMetrics(predicate, false);
    }

    private PredicateMetrics getMetrics(@Nonnull final QueryPredicate predicate, final boolean negate) {
        if (mode.instanceOfMinorClass(predicate)) {
            final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
            final PredicateMetrics metricsFromChildren = negate ? getMetricsForMajor(children, true) : getMetricsForMinor(children, false);
            return predicate.isAtomic() ? new PredicateMetrics(1, metricsFromChildren.getNormalFormFullSize(), 1) : metricsFromChildren;
        } else if (mode.instanceOfMajorClass(predicate)) {
            final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
            final PredicateMetrics metricsFromChildren = negate ? getMetricsForMinor(children, true) : getMetricsForMajor(children, false);
            return predicate.isAtomic() ? new PredicateMetrics(1, metricsFromChildren.getNormalFormFullSize(), 1) : metricsFromChildren;
        } else if (predicate instanceof NotPredicate) {
            final PredicateMetrics metricsFromChildren = getMetrics(((NotPredicate)predicate).getChild(), !negate);
            return predicate.isAtomic() ? new PredicateMetrics(1, metricsFromChildren.getNormalFormFullSize(), 1) : metricsFromChildren;
        } else {
            return new PredicateMetrics(1, 1, 1);
        }
    }

    private PredicateMetrics getMetricsForMajor(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        long normalFormSize = 0L;
        long normalFormFullSize = 0L;
        int normalFormMaximumNumMinors = 0;
        for (final QueryPredicate child : children) {
            final PredicateMetrics childMetrics = getMetrics(child, negate);
            normalFormSize = Math.addExact(normalFormSize, childMetrics.getNormalFormSize());
            normalFormFullSize = Math.addExact(normalFormFullSize, childMetrics.getNormalFormFullSize());
            normalFormMaximumNumMinors = Math.max(normalFormMaximumNumMinors, childMetrics.getNormalFormMaximumNumMinors());
        }
        return new PredicateMetrics(normalFormSize, normalFormFullSize, normalFormMaximumNumMinors);
    }

    private PredicateMetrics getMetricsForMinor(@Nonnull final List<? extends QueryPredicate> children, final boolean negate) {
        long normalFormSize = 1L;
        long normalFormFullSize = 1L;
        int normalFormMaximumNumMinors = 0;

        for (final QueryPredicate child : children) {
            final PredicateMetrics childMetrics = getMetrics(child, negate);
            normalFormSize = Math.multiplyExact(normalFormSize, childMetrics.getNormalFormSize());
            normalFormFullSize = Math.multiplyExact(normalFormFullSize, childMetrics.getNormalFormFullSize());
            normalFormMaximumNumMinors = Math.addExact(normalFormMaximumNumMinors, childMetrics.getNormalFormMaximumNumMinors());
        }
        return new PredicateMetrics(normalFormSize, normalFormFullSize, normalFormMaximumNumMinors);
    }

    /**
     * Convert given predicate to list (to be combined using the major) of lists (to be combined using the minor).
     * @param predicate a predicate subtree
     * @param negate whether this subtree is negated
     * @return a list (to be major'ed) of lists (to be minor'ed)
     */
    @Nonnull
    private List<Collection<? extends QueryPredicate>> toNormalized(@Nonnull final QueryPredicate predicate, final boolean negate) {
        if (!predicate.isAtomic()) {
            if (mode.instanceOfMinorClass(predicate)) {
                final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
                return negate ? majorToNormalized(children, true) : minorToNormalized(children, false);
            } else if (mode.instanceOfMajorClass(predicate)) {
                final List<? extends QueryPredicate> children = ((AndOrPredicate)predicate).getChildren();
                return negate ? minorToNormalized(children, true) : majorToNormalized(children, false);
            } else if (predicate instanceof NotPredicate) {
                return toNormalized(((NotPredicate)predicate).getChild(), !negate);
            }
        }
        return Collections.singletonList(Collections.singletonList(negate ? NotPredicate.not(predicate) : predicate));
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
    public static List<Collection<? extends QueryPredicate>> applyAbsorptionLaw(final List<Collection<? extends QueryPredicate>> majorOfMinor) {
        int size = majorOfMinor.size();
        if (size < 2) {
            return majorOfMinor;
        }

        final List<Collection<? extends QueryPredicate>> absorbed = Lists.newArrayList();

        //
        // The following loop eliminates repetitions of an atom within a list of atoms to be minored, since
        // a ^ a == a and a v a == a
        //
        // de-dup and put the terms back into the list of lists
        for (Collection<? extends QueryPredicate> minors : majorOfMinor) {
            absorbed.add(Sets.newLinkedHashSet(minors));
        }

        //
        // The following loop attempts to find a list of terms (to be minored) within another list of terms (to be minored)
        // which would eventually be combined using the major. This works both ways as both
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
            Collection<? extends QueryPredicate> ci = absorbed.get(i);

            // There is therefore no need to keep having this one.
            for (int j = 0; j < size; j++) {
                if (i == j) {
                    continue;
                }
                Collection<? extends QueryPredicate> cj = absorbed.get(j);
                if (ci.size() > cj.size() || (ci.size() == cj.size() && i < j)) {
                    if (ci.containsAll(cj)) {
                        absorbed.remove(i);
                        size--;
                        continue nexti;
                    }
                }
            }
            i++;
        }
        return absorbed;
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

    private static boolean isNormalFormVariable(@Nonnull final QueryPredicate queryPredicate) {
        return queryPredicate.isAtomic() || queryPredicate instanceof LeafQueryPredicate;
    }

    private static boolean isNormalFormVariableOrNotPredicate(@Nonnull final QueryPredicate queryPredicate) {
        if (isNormalFormVariable(queryPredicate)) {
            return true;
        }

        if (queryPredicate instanceof NotPredicate) {
            return isNormalFormVariable(((NotPredicate)queryPredicate).getChild());
        }

        return false;
    }

    /**
     * Case class for metrics we need to keep track of for various purposes.
     */
    public static class PredicateMetrics {
        /**
         * Number of majors. This metric treats atomic {@link QueryPredicate}s as such and will hold the size of the
         * normal form that does not multiply out such predicates.
         */
        private final long normalFormSize;

        /**
         * Full number of majors. This metric disregards {@link QueryPredicate#isAtomic()}.
         */
        private final long normalFormFullSize;

        /**
         * Maximum number of minors in the normal form.
         */
        private final int normalFormMaximumNumMinors;

        public PredicateMetrics(final long normalFormSize, final long normalFormFullSize, final int normalFormMaximumNumMinors) {
            this.normalFormSize = normalFormSize;
            this.normalFormFullSize = normalFormFullSize;
            this.normalFormMaximumNumMinors = normalFormMaximumNumMinors;
        }

        public long getNormalFormSize() {
            return normalFormSize;
        }

        public long getNormalFormFullSize() {
            return normalFormFullSize;
        }

        public int getNormalFormMaximumNumMinors() {
            return normalFormMaximumNumMinors;
        }
    }
}
