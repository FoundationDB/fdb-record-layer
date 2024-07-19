/*
 * Reference.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * <p>
 * The <em>memo</em> data structure can compactly represent a large set of similar {@link RelationalExpression}s through
 * careful memoization. The Cascades "group expression", represented by the {@code Reference}, is the key to
 * that memoization by sharing optimization work on a sub-expression with other parts of the expression that reference
 * the same sub-expression.
 * </p>
 *
 * <p>
 * The reference abstraction is designed to make it difficult for authors of rules to mutate group expressions directly,
 * which is undefined behavior. Note that a {@link Reference} cannot be "de-referenced" using the {@link #get()}
 * method if it contains more than one member. Expressions with more than one member should not be used outside of the
 * query planner, and {@link #get()} should not be used inside the query planner.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class Reference implements Correlated<Reference>, Typed {
    @Nonnull
    private final LinkedIdentitySet<RelationalExpression> members;

    @Nonnull
    private final SetMultimap<MatchCandidate, PartialMatch> partialMatchMap;
    @Nonnull
    private final ConstraintsMap constraintsMap;

    @Nonnull
    private final PropertiesMap propertiesMap;

    private Reference() {
        this(new LinkedIdentitySet<>());
    }

    private Reference(@Nonnull LinkedIdentitySet<RelationalExpression> members) {
        this.members = members;
        this.partialMatchMap = LinkedHashMultimap.create();
        this.constraintsMap = new ConstraintsMap();
        this.propertiesMap = new PropertiesMap(members);
        // Call debugger hook for this new reference.
        Debugger.registerReference(this);
    }

    /**
     * Return the {@link RecordQueryPlan} contained in this reference.
     * @return the {@link RecordQueryPlan} contained in this reference
     * @throws UngettableReferenceException if the reference does not support retrieving its expression
     * @throws ClassCastException if the only member of this reference is not a {@link RecordQueryPlan}
     */
    @Nonnull
    public RecordQueryPlan getAsPlan() {
        return (RecordQueryPlan)get();
    }

    /**
     * Return the expression contained in this reference. If the reference does not support getting its expresssion
     * (for example, because it holds more than one expression, or none at all), this should throw an exception.
     * @return the expression contained in this reference
     * @throws UngettableReferenceException if the reference does not support retrieving its expression
     */
    @Nonnull
    public RelationalExpression get() {
        if (members.size() == 1) {
            return members.iterator().next();
        }
        throw new UngettableReferenceException("tried to dereference reference with " + members.size() + " members");
    }

    @Nonnull
    public ConstraintsMap getConstraintsMap() {
        return constraintsMap;
    }

    /**
     * Legacy method that calls {@link #pruneWith(RelationalExpression)} while being synchronized on {@code this}.
     * @param newValue new expression to replace members of this reference.
     */
    public synchronized void replace(@Nonnull RelationalExpression newValue) {
        pruneWith(newValue);
    }

    /**
     * Method that replaces the current members of this reference with a new value. This is called by the planner
     * to prune the variations of a reference down to exactly one new member.
     * @param newValue new value to replace existing members
     */
    public void pruneWith(@Nonnull RelationalExpression newValue) {
        final Map<PlanProperty<?>, ?> propertiesForPlan;
        if (newValue instanceof RecordQueryPlan) {
            propertiesForPlan = propertiesMap.getCurrentPropertiesForPlan((RecordQueryPlan)newValue);
        } else {
            propertiesForPlan = null;
        }
        clear();
        insertUnchecked(newValue, propertiesForPlan);
    }

    /**
     * Inserts a new expression into this reference. This particular overload utilized the precomputed propertied of
     * a {@link RecordQueryPlan} that already is thought to reside in another {@link Reference}.
     * @param newValue new expression to be inserted
     * @param otherRef a reference that already contains {@code newValue}
     * @return {@code true} if and only if the new expression was successfully inserted into this reference, {@code false}
     *         otherwise.
     */
    public boolean insertFrom(@Nonnull final RelationalExpression newValue, @Nonnull final Reference otherRef) {
        if (newValue instanceof RecordQueryPlan) {
            final var propertiesForPlan =
                    Objects.requireNonNull(otherRef.propertiesMap.getPropertiesForPlan((RecordQueryPlan)newValue));

            return insert(newValue, propertiesForPlan);
        }
        return insert(newValue, null);
    }

    /**
     * Inserts a new expression into this reference. This method checks for prior memoization of the expression passed
     * in within the reference. If the expression is already contained in this reference, the reference is not modified.
     * @param newValue new expression to be inserted
     * @return {@code true} if and only if the new expression was successfully inserted into this reference, {@code false}
     *         otherwise.
     */
    public boolean insert(@Nonnull final RelationalExpression newValue) {
        return insert(newValue, null);
    }

    /**
     * Inserts a new expression into this reference. This method checks for prior memoization of the expression passed
     * in within the reference. If the expression is already contained in this reference, the reference is not modified.
     * @param newValue new expression to be inserted
     * @param precomputedPropertiesMap if not {@code null}, a map of precomputed properties for a {@link RecordQueryPlan}
     *        that will be inserted into this reference verbatim, otherwise it will be computed
     * @return {@code true} if and only if the new expression was successfully inserted into this reference, {@code false}
     *         otherwise.
     */
    private boolean insert(@Nonnull final RelationalExpression newValue, @Nullable final Map<PlanProperty<?>, ?> precomputedPropertiesMap) {
        Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.InsertIntoMemoEvent(Debugger.Location.BEGIN)));
        try {
            final boolean containsInMemo = containsInMemo(newValue);
            Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.InsertIntoMemoEvent(containsInMemo ? Debugger.Location.REUSED : Debugger.Location.NEW)));

            if (!containsInMemo) {
                insertUnchecked(newValue, precomputedPropertiesMap);
                return true;
            }
            return false;
        } finally {
            Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.InsertIntoMemoEvent(Debugger.Location.END)));
        }
    }

    /**
     * Inserts a new expression into this reference. Unlike {{@link #insert(RelationalExpression, Map)}}, this method does
     * not check for prior memoization of the expression passed in within the reference. The caller needs to exercise
     * caution to only call this method on a reference if it is known that the reference cannot possibly already have
     * the expression memoized.
     * @param newValue new expression to be inserted (without check)
     */
    public void insertUnchecked(@Nonnull final RelationalExpression newValue) {
        insertUnchecked(newValue, null);
    }

    /**
     * Inserts a new expression into this reference. Unlike {{@link #insert(RelationalExpression, Map)}}, this method does
     * not check for prior memoization of the expression passed in within the reference. The caller needs to exercise
     * caution to only call this method on a reference if it is known that the reference cannot possibly already have
     * the expression memoized.
     * @param newValue new expression to be inserted (without check)
     * @param precomputedPropertiesMap if not {@code null}, a map of precomputed properties for a {@link RecordQueryPlan}
     *        that will be inserted into this reference verbatim, otherwise it will be computed
     */
    public void insertUnchecked(@Nonnull final RelationalExpression newValue, @Nullable final Map<PlanProperty<?>, ?> precomputedPropertiesMap) {
        // Call debugger hook to potentially register this new expression.
        Debugger.registerExpression(newValue);
        members.add(newValue);
        if (newValue instanceof RecordQueryPlan) {
            final var newRecordQueryPlan = (RecordQueryPlan)newValue;
            if (precomputedPropertiesMap != null) {
                propertiesMap.add(newRecordQueryPlan, precomputedPropertiesMap);
            } else {
                propertiesMap.add(newRecordQueryPlan);
            }
        }
    }

    public boolean containsExactly(@Nonnull RelationalExpression expression) {
        return members.contains(expression);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean containsAllInMemo(@Nonnull final Reference otherRef,
                                     @Nonnull final AliasMap equivalenceMap) {
        if (this == otherRef) {
            return true;
        }

        for (final RelationalExpression otherMember : otherRef.getMembers()) {
            if (!containsInMemo(otherMember, equivalenceMap, members)) {
                return false;
            }
        }
        return true;
    }

    public boolean containsInMemo(@Nonnull final RelationalExpression expression) {
        if (!getCorrelatedTo().equals(expression.getCorrelatedTo())) {
            return false;
        }
        return containsInMemo(expression, AliasMap.emptyMap());
    }

    private boolean containsInMemo(@Nonnull final RelationalExpression expression,
                                   @Nonnull final AliasMap equivalenceMap) {
        return containsInMemo(expression, equivalenceMap, members);
    }

    private boolean containsInMemo(@Nonnull final RelationalExpression expression,
                                   @Nonnull final AliasMap equivalenceMap,
                                   @Nonnull final Iterable<? extends RelationalExpression> membersToSearch) {
        for (final RelationalExpression member : membersToSearch) {
            if (isMemoizedExpression(member, expression, equivalenceMap)) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        for (final RelationalExpression member : getMembers()) {
            builder.addAll(member.getCorrelatedTo());
        }
        return builder.build();
    }

    @SuppressWarnings("java:S1905")
    @Nonnull
    @Override
    public Reference rebase(@Nonnull final AliasMap translationMap) {
        return translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap));
    }

    @Nonnull
    public Reference translateCorrelations(@Nonnull final TranslationMap translationMap) {
        final var translatedRefs = References.translateCorrelations(ImmutableList.of(this), translationMap);
        return Iterables.getOnlyElement(translatedRefs);
    }

    /**
     * Method that resolves the result type by looking and unifying the result types from all the members.
     * @return {@link Type} representing result type
     */
    @Nonnull
    @Override
    public Type getResultType() {
        return getMembers()
                .stream()
                .map(RelationalExpression::getResultType)
                .reduce((left, right) -> {
                    Verify.verify(left.equals(right));
                    return left;
                })
                .orElseThrow(() -> new RecordCoreException("unable to resolve result values"));
    }

    public void clear() {
        propertiesMap.clear();
        members.clear();
    }

    public void startExploration() {
        constraintsMap.startExploration();
    }

    public void commitExploration() {
        constraintsMap.commitExploration();
    }

    public boolean needsExploration() {
        return !constraintsMap.isExploring() && !constraintsMap.isExplored();
    }

    public boolean isExploring() {
        return constraintsMap.isExploring();
    }

    public boolean hasNeverBeenExplored() {
        return constraintsMap.hasNeverBeenExplored();
    }

    public boolean isFullyExploring() {
        return constraintsMap.isFullyExploring();
    }

    public boolean isExploredForAttributes(@Nonnull final Set<PlannerConstraint<?>> dependencies) {
        return constraintsMap.isExploredForAttributes(dependencies);
    }

    @Nonnull
    public LinkedIdentitySet<RelationalExpression> getMembers() {
        return members;
    }

    /**
     * Re-reference members of this group, i.e., use a subset of members to from a new {@link Reference}.
     * Note that {@code this} group must not need exploration.
     *
     * @param expressions a collection of expressions that all have to be members of this group
     * @return a new explored {@link Reference}
     */
    @Nonnull
    public Reference referenceFromMembers(@Nonnull Collection<? extends RelationalExpression> expressions) {
        Verify.verify(!needsExploration());
        Verify.verify(getMembers().containsAll(expressions));

        final var members = new LinkedIdentitySet<RelationalExpression>();
        members.addAll(expressions);
        final var newRef = new Reference(members);
        newRef.getConstraintsMap().setExplored();
        return newRef;
    }

    @Nonnull
    public <A> Map<RecordQueryPlan, A> getPlannerAttributeForMembers(@Nonnull final PlanProperty<A> planProperty) {
        return propertiesMap.getPlannerAttributeForAllPlans(planProperty);
    }

    @Nonnull
    public List<PlanPartition> getPlanPartitions() {
        return propertiesMap.getPlanPartitions();
    }

    @Nullable
    public <U> U acceptPropertyVisitor(@Nonnull ExpressionProperty<U> property) {
        if (property.shouldVisit(this)) {
            final List<U> memberResults = new ArrayList<>(members.size());
            for (RelationalExpression member : members) {
                final U result = property.shouldVisit(member) ? property.visit(member) : null;
                if (result == null) {
                    return null;
                }
                memberResults.add(result);
            }
            return property.evaluateAtRef(this, memberResults);
        }
        return null;
    }

    @SuppressWarnings("checkstyle:Indentation")
    @Override
    public String toString() {
        return Debugger.mapDebugger(debugger -> debugger.nameForObject(this) + "[" +
                        getMembers().stream()
                                .map(debugger::nameForObject)
                                .collect(Collectors.joining(",")) + "]")
                .orElse("Reference@" + hashCode() + "(" + "isExplored=" + constraintsMap.isExplored() + ")");
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final Reference otherRef = (Reference)other;

        final Iterator<RelationalExpression> iterator = members.iterator();
        final ImmutableMultimap.Builder<Integer, RelationalExpression> expressionsMapBuilder = ImmutableMultimap.builder();

        while (iterator.hasNext()) {
            final RelationalExpression next = iterator.next();
            expressionsMapBuilder.put(next.semanticHashCode(), next);
        }
        final ImmutableMultimap<Integer, RelationalExpression> expressionsMap = expressionsMapBuilder.build();

        for (final RelationalExpression otherMember : otherRef.getMembers()) {
            if (expressionsMap.get(otherMember.semanticHashCode()).stream().anyMatch(member -> member.semanticEquals(otherMember, aliasMap))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int semanticHashCode() {
        final Iterator<RelationalExpression> iterator = members.iterator();
        final ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();

        while (iterator.hasNext()) {
            final RelationalExpression next = iterator.next();
            builder.add(next.semanticHashCode());
        }

        return Objects.hash(builder.build());
    }

    /**
     * Return all match candidates that partially match this reference. This set must be a subset of all {@link MatchCandidate}s
     * in the {@link PlanContext} during planning. Note that it is possible that a particular match candidate matches this
     * reference more than once.
     * @return a set of match candidates that partially match this reference.
     */
    @Nonnull
    public Set<MatchCandidate> getMatchCandidates() {
        return partialMatchMap.keySet();
    }

    /**
     * Return all partial matches that match a given expression. This method is agnostic of the {@link MatchCandidate}
     * that created the partial match.
     * @param expression expression to return partial matches for. This expression has to be a member of this reference
     * @return a collection of partial matches that matches the give expression to some candidate
     */
    @Nonnull
    public Collection<PartialMatch> getPartialMatchesForExpression(@Nonnull final RelationalExpression expression) {
        return partialMatchMap.values()
                .stream()
                .filter(partialMatch ->
                        // partial matches need to be compared by identity
                        partialMatch.getQueryExpression() == expression)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Return all partial matches for the {@link MatchCandidate} passed in.
     * @param candidate match candidate
     * @return a set of partial matches for {@code candidate}
     */
    @Nonnull
    public Set<PartialMatch> getPartialMatchesForCandidate(final MatchCandidate candidate) {
        return partialMatchMap.get(candidate);
    }

    /**
     * Add the {@link PartialMatch} passed in to this reference for the {@link MatchCandidate} passed in.
     * @param candidate match candidate this partial match relates to
     * @param partialMatch a new partial match.
     * @return {@code true} if this call added a new partial, {@code false} if the partial match passed in was already
     *         contained in this reference
     */
    @SuppressWarnings("UnusedReturnValue")
    public boolean addPartialMatchForCandidate(final MatchCandidate candidate, final PartialMatch partialMatch) {
        return partialMatchMap.put(candidate, partialMatch);
    }

    /**
     * Method to render the graph rooted at this reference. This is needed for graph integration into IntelliJ as
     * IntelliJ only ever evaluates selfish methods. Add this method as a custom renderer for the type
     * {@link Reference}. During debugging you can then click show() on an instance and enjoy the query graph
     * it represents rendered in your standard browser.
     * @param renderSingleGroups whether to render group references with just one member
     * @return the String "done"
     */
    @Nonnull
    public String show(final boolean renderSingleGroups) {
        return PlannerGraphProperty.show(renderSingleGroups, this);
    }

    public static boolean isMemoizedExpression(@Nonnull final RelationalExpression expression,
                                               @Nonnull final RelationalExpression otherExpression) {
        if (!expression.getCorrelatedTo().equals(otherExpression.getCorrelatedTo())) {
            return false;
        }

        return isMemoizedExpression(expression, otherExpression, AliasMap.emptyMap());
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static boolean isMemoizedExpression(@Nonnull final RelationalExpression member,
                                                @Nonnull final RelationalExpression otherExpression,
                                                @Nonnull final AliasMap equivalenceMap) {
        if (member == otherExpression) {
            return true;
        }
        if (member.getClass() != otherExpression.getClass()) {
            return false;
        }

        final List<? extends Quantifier> quantifiers = member.getQuantifiers();
        final List<? extends Quantifier> otherQuantifiers = otherExpression.getQuantifiers();
        if (member.getQuantifiers().size() != otherQuantifiers.size()) {
            return false;
        }

        if (member.hashCodeWithoutChildren() != otherExpression.hashCodeWithoutChildren()) {
            return false;
        }

        // We know member and otherMember are of the same class. canCorrelate() needs to match as well.
        Verify.verify(member.canCorrelate() == otherExpression.canCorrelate());

        // Bind all unbound correlated aliases in this member and otherMember that refer to the same
        // quantifier by alias.
        final AliasMap identitiesMap = member.bindIdentities(otherExpression, equivalenceMap);
        final AliasMap combinedEquivalenceMap = equivalenceMap.combine(identitiesMap);

        final Iterable<AliasMap> aliasMapIterable;
        if (member instanceof RelationalExpressionWithChildren.ChildrenAsSet) {
            // Use match the contained quantifier list against the quantifier list of other in order to find
            // a correspondence between quantifiers and otherQuantifiers. While we match we recursively call
            // containsAllInMemo() and early out on that map if such a correspondence cannot be established
            // on the given pair of quantifiers. The result of this method is an iterable of matches. While it's
            // possible that there is more than one match, this iterable should mostly contain at most one
            // match.
            aliasMapIterable =
                    Quantifiers.findMatches(
                            combinedEquivalenceMap,
                            member.getQuantifiers(),
                            otherExpression.getQuantifiers(),
                            ((quantifier, otherQuantifier, nestedEquivalencesMap) -> {
                                final Reference rangesOver = quantifier.getRangesOver();
                                final Reference otherRangesOver = otherQuantifier.getRangesOver();
                                return rangesOver.containsAllInMemo(otherRangesOver, nestedEquivalencesMap);
                            }));
        } else {
            final AliasMap.Builder aliasMapBuilder = combinedEquivalenceMap.toBuilder(quantifiers.size());
            for (int i = 0; i < quantifiers.size(); i++) {
                final Quantifier quantifier = Objects.requireNonNull(quantifiers.get(i));
                final Quantifier otherQuantifier = Objects.requireNonNull(otherQuantifiers.get(i));
                if (!quantifier.getRangesOver()
                        .containsAllInMemo(otherQuantifier.getRangesOver(), aliasMapBuilder.build())) {
                    return false;
                }
                aliasMapBuilder.put(quantifier.getAlias(), otherQuantifier.getAlias());
            }

            aliasMapIterable = ImmutableList.of(aliasMapBuilder.build());
        }

        // if there is more than one match we only need one such match that also satisfies the equality condition between
        // member and otherMember (no children considered).
        return StreamSupport.stream(aliasMapIterable.spliterator(), false)
                .anyMatch(aliasMap -> member.equalsWithoutChildren(otherExpression, aliasMap));
    }

    public static Reference empty() {
        return new Reference();
    }

    public static Reference of(@Nonnull RelationalExpression expression) {
        LinkedIdentitySet<RelationalExpression> members = new LinkedIdentitySet<>();
        // Call debugger hook to potentially register this new expression.
        Debugger.registerExpression(expression);
        members.add(expression);
        return new Reference(members);
    }

    public static Reference from(@Nonnull RelationalExpression... expressions) {
        return from(Arrays.asList(expressions));
    }

    public static Reference from(@Nonnull Collection<? extends RelationalExpression> expressions) {
        LinkedIdentitySet<RelationalExpression> members = new LinkedIdentitySet<>();
        expressions.forEach(Debugger::registerExpression);
        members.addAll(expressions);
        return new Reference(members);
    }

    /**
     * An exception thrown when {@link #get()} is called on a reference that does not support it,
     * such as a reference containing more than one expression.
     * A client that encounters this exception is buggy and should not try to recover any information from the reference
     * that threw it.
     */
    public static class UngettableReferenceException extends RecordCoreException {
        private static final long serialVersionUID = 1;

        public UngettableReferenceException(String message) {
            super(message);
        }
    }
}
