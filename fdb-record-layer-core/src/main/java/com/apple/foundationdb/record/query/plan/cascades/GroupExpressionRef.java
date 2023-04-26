/*
 * GroupExpressionRef.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

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
 * A Cascades-style group expression, representing the members of set of {@link RelationalExpression}s that belong to
 * the same equivalence class.
 *
 * <p>
 * The <em>memo</em> data structure can compactly represent a large set of similar {@link RelationalExpression}s through
 * careful memoization. The Cascades "group expression", represented by the {@code GroupExpressionRef}, is the key to
 * that memoization by sharing optimization work on a sub-expression with other parts of the expression that reference
 * the same sub-expression.
 * </p>
 *
 * <p>
 * The reference abstraction is designed to make it difficult for authors of rules to mutate group expressions directly,
 * which is undefined behavior. Note that a {@link GroupExpressionRef} cannot be "dereferenced" using the {@link #get()}
 * method if it contains more than one member. Expressions with more than one member should not be used outside of the
 * query planner, and {@link #get()} should not be used inside the query planner.
 * </p>
 * @param <T> the type of planner expression that is contained in this reference
 */
@API(API.Status.EXPERIMENTAL)
public class GroupExpressionRef<T extends RelationalExpression> implements ExpressionRef<T> {
    @Nonnull
    private final LinkedIdentitySet<T> members;

    @Nonnull
    private final SetMultimap<MatchCandidate, PartialMatch> partialMatchMap;
    @Nonnull
    private final ConstraintsMap constraintsMap;

    @Nonnull
    private final PropertiesMap propertiesMap;

    private GroupExpressionRef() {
        this(new LinkedIdentitySet<>());
    }

    private GroupExpressionRef(@Nonnull LinkedIdentitySet<T> members) {
        this.members = members;
        this.partialMatchMap = LinkedHashMultimap.create();
        this.constraintsMap = new ConstraintsMap();
        this.propertiesMap = new PropertiesMap(members);
        // Call debugger hook for this new reference.
        Debugger.registerReference(this);
    }

    @Nonnull
    @Override
    public T get() {
        if (members.size() == 1) {
            return members.iterator().next();
        }
        throw new UngettableReferenceException("tried to dereference GroupExpressionRef with " + members.size() + " members");
    }

    @Nonnull
    @Override
    public ConstraintsMap getConstraintsMap() {
        return constraintsMap;
    }

    /**
     * Legacy method that calls {@link #pruneWith(RelationalExpression)} while being synchronized on {@code this}.
     * @param newValue new expression to replace members of this reference.
     */
    public synchronized void replace(@Nonnull T newValue) {
        pruneWith(newValue);
    }

    /**
     * Method that replaces the current members of this reference with a new value. This is called by the planner
     * to prune the variations of a reference down to exactly one new member.
     * @param newValue new value to replace existing members
     */
    public void pruneWith(@Nonnull T newValue) {
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
     * a {@link RecordQueryPlan} that already is thought to reside in another {@link ExpressionRef}.
     * @param newValue new expression to be inserted
     * @param otherRef a reference that already contains {@code newValue}
     * @return {@code true} if and only if the new expression was successfully inserted into this reference, {@code false}
     *         otherwise.
     */
    @Override
    public boolean insertFrom(@Nonnull final T newValue, @Nonnull final ExpressionRef<T> otherRef) {
        if (newValue instanceof RecordQueryPlan && otherRef instanceof GroupExpressionRef) {
            final var propertiesForPlan =
                    Objects.requireNonNull(((GroupExpressionRef<T>)otherRef).propertiesMap.getPropertiesForPlan((RecordQueryPlan)newValue));

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
    @Override
    public boolean insert(@Nonnull final T newValue) {
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
    private boolean insert(@Nonnull final T newValue, @Nullable final Map<PlanProperty<?>, ?> precomputedPropertiesMap) {
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
    public void insertUnchecked(@Nonnull final T newValue) {
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
    public void insertUnchecked(@Nonnull final T newValue, @Nullable final Map<PlanProperty<?>, ?> precomputedPropertiesMap) {
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

    public boolean containsExactly(@Nonnull T expression) {
        return members.contains(expression);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean containsAllInMemo(@Nonnull final ExpressionRef<? extends RelationalExpression> otherRef,
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
        final Set<CorrelationIdentifier> correlatedTo = getCorrelatedTo();
        final Set<CorrelationIdentifier> otherCorrelatedTo = expression.getCorrelatedTo();

        final Sets.SetView<CorrelationIdentifier> commonUnbound = Sets.intersection(correlatedTo, otherCorrelatedTo);
        final AliasMap identityMap = AliasMap.identitiesFor(commonUnbound);

        return containsInMemo(expression, identityMap);
    }

    private boolean containsInMemo(@Nonnull final RelationalExpression expression,
                                   @Nonnull final AliasMap equivalenceMap) {
        return containsInMemo(expression, equivalenceMap, members);
    }

    private boolean containsInMemo(@Nonnull final RelationalExpression expression,
                                   @Nonnull final AliasMap equivalenceMap,
                                   @Nonnull final Iterable<? extends RelationalExpression> membersToSearch) {
        for (final RelationalExpression member : membersToSearch) {
            if (containsInMember(member, expression, equivalenceMap)) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        for (final T member : getMembers()) {
            builder.addAll(member.getCorrelatedTo());
        }
        return builder.build();
    }

    @SuppressWarnings("java:S1905")
    @Nonnull
    @Override
    public GroupExpressionRef<T> rebase(@Nonnull final AliasMap translationMap) {
        final var expressionRef = translateCorrelations(TranslationMap.rebaseWithAliasMap(translationMap));
        if (expressionRef instanceof GroupExpressionRef<?>) {
            return (GroupExpressionRef<T>)expressionRef;
        } else {
            return GroupExpressionRef.from(expressionRef.getMembers());
        }
    }

    @Nonnull
    @Override
    @SuppressWarnings("unchecked")
    public ExpressionRef<T> translateCorrelations(@Nonnull final TranslationMap translationMap) {
        final var translatedRefs = ExpressionRefs.translateCorrelations(ImmutableList.of(this), translationMap);
        return (ExpressionRef<T>)Iterables.getOnlyElement(translatedRefs);
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

    public boolean isFullyExploring() {
        return constraintsMap.isFullyExploring();
    }

    public boolean isExploredForAttributes(@Nonnull final Set<PlannerConstraint<?>> dependencies) {
        return constraintsMap.isExploredForAttributes(dependencies);
    }

    @Nonnull
    @Override
    public LinkedIdentitySet<T> getMembers() {
        return members;
    }

    /**
     * Re-reference members of this group, i.e., use a subset of members to from a new {@link GroupExpressionRef}.
     * Note that {@code this} group must not need exploration.
     *
     * @param expressions a collection of expressions that all have to be members of this group
     * @return a new explored {@link GroupExpressionRef}
     */
    @SuppressWarnings({"SuspiciousMethodCalls", "unchecked"})
    @Nonnull
    @Override
    public ExpressionRef<T> referenceFromMembers(@Nonnull Collection<? extends RelationalExpression> expressions) {
        Verify.verify(!needsExploration());
        Verify.verify(getMembers().containsAll(expressions));

        final var members = new LinkedIdentitySet<T>();
        expressions.forEach(expression -> members.add((T)expression));
        final var newRef = new GroupExpressionRef<>(members);
        newRef.getConstraintsMap().setExplored();
        return newRef;
    }

    @Nonnull
    @Override
    public <A> Map<RecordQueryPlan, A> getPlannerAttributeForMembers(@Nonnull final PlanProperty<A> planProperty) {
        return propertiesMap.getPlannerAttributeForAllPlans(planProperty);
    }

    @Nonnull
    @Override
    public List<PlanPartition> getPlanPartitions() {
        return propertiesMap.getPlanPartitions();
    }

    @Nullable
    @Override
    public <U> U acceptPropertyVisitor(@Nonnull ExpressionProperty<U> property) {
        if (property.shouldVisit(this)) {
            final List<U> memberResults = new ArrayList<>(members.size());
            for (T member : members) {
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
                .orElse("ExpressionRef@" + hashCode() + "(" + "isExplored=" + constraintsMap.isExplored() + ")");
    }

    @Override
    @SuppressWarnings({"unchecked", "PMD.CompareObjectsWithEquals"})
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final GroupExpressionRef<T> otherRef = (GroupExpressionRef<T>)other;

        final Iterator<T> iterator = members.iterator();
        final ImmutableMultimap.Builder<Integer, T> expressionsMapBuilder = ImmutableMultimap.builder();

        while (iterator.hasNext()) {
            final T next = iterator.next();
            expressionsMapBuilder.put(next.semanticHashCode(), next);
        }
        final ImmutableMultimap<Integer, T> expressionsMap = expressionsMapBuilder.build();

        for (final T otherMember : otherRef.getMembers()) {
            if (expressionsMap.get(otherMember.semanticHashCode()).stream().anyMatch(member -> member.semanticEquals(otherMember, aliasMap))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int semanticHashCode() {
        final Iterator<T> iterator = members.iterator();
        final ImmutableSet.Builder<Integer> builder = ImmutableSet.builder();

        while (iterator.hasNext()) {
            final T next = iterator.next();
            builder.add(next.semanticHashCode());
        }

        return Objects.hash(builder.build());
    }

    @Nonnull
    @Override
    public Set<MatchCandidate> getMatchCandidates() {
        return partialMatchMap.keySet();
    }

    @Nonnull
    @Override
    public Collection<PartialMatch> getPartialMatchesForExpression(@Nonnull final RelationalExpression expression) {
        return partialMatchMap.values()
                .stream()
                .filter(partialMatch ->
                        // partial matches need to be compared by identity
                        partialMatch.getQueryExpression() == expression)
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public Set<PartialMatch> getPartialMatchesForCandidate(final MatchCandidate candidate) {
        return partialMatchMap.get(candidate);
    }

    @Override
    public boolean addPartialMatchForCandidate(final MatchCandidate candidate, final PartialMatch partialMatch) {
        return partialMatchMap.put(candidate, partialMatch);
    }

    /**
     * Method to render the graph rooted at this reference. This is needed for graph integration into IntelliJ as
     * IntelliJ only ever evaluates selfish methods. Add this method as a custom renderer for the type
     * {@link GroupExpressionRef}. During debugging you can then click show() on an instance and enjoy the query graph
     * it represents rendered in your standard browser.
     * @param renderSingleGroups whether to render group references with just one member
     * @return the String "done"
     */
    @Nonnull
    public String show(final boolean renderSingleGroups) {
        return PlannerGraphProperty.show(renderSingleGroups, this);
    }

    public static boolean containsInMember(@Nonnull final RelationalExpression expression,
                                           @Nonnull final RelationalExpression otherExpression) {
        final Set<CorrelationIdentifier> correlatedTo = expression.getCorrelatedTo();
        final Set<CorrelationIdentifier> otherCorrelatedTo = otherExpression.getCorrelatedTo();

        final Sets.SetView<CorrelationIdentifier> commonUnbound = Sets.intersection(correlatedTo, otherCorrelatedTo);
        final AliasMap identityMap = AliasMap.identitiesFor(commonUnbound);

        return containsInMember(expression, otherExpression, identityMap);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private static boolean containsInMember(@Nonnull final RelationalExpression member,
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
                                final ExpressionRef<? extends RelationalExpression> rangesOver = quantifier.getRangesOver();
                                final ExpressionRef<? extends RelationalExpression> otherRangesOver = otherQuantifier.getRangesOver();
                                return rangesOver.containsAllInMemo(otherRangesOver, nestedEquivalencesMap);
                            }));
        } else {
            final AliasMap.Builder aliasMapBuilder = combinedEquivalenceMap.derived(quantifiers.size());
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

    public static <T extends RelationalExpression> GroupExpressionRef<T> empty() {
        return new GroupExpressionRef<>();
    }

    public static <T extends RelationalExpression> GroupExpressionRef<T> of(@Nonnull T expression) {
        LinkedIdentitySet<T> members = new LinkedIdentitySet<>();
        // Call debugger hook to potentially register this new expression.
        Debugger.registerExpression(expression);
        members.add(expression);
        return new GroupExpressionRef<>(members);
    }

    @SuppressWarnings("unchecked")
    public static <T extends RelationalExpression> GroupExpressionRef<T> from(@Nonnull T... expressions) {
        return from(Arrays.asList(expressions));
    }

    public static <T extends RelationalExpression> GroupExpressionRef<T> from(@Nonnull Collection<T> expressions) {
        LinkedIdentitySet<T> members = new LinkedIdentitySet<>();
        expressions.forEach(Debugger::registerExpression);
        members.addAll(expressions);
        return new GroupExpressionRef<>(members);
    }
}
