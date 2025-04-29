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
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * The <em>memo</em> data structure can compactly represent a large set of similar {@link RelationalExpression}s through
 * careful memoization. The Cascades "group expression", represented by the {@code Reference}, is the key to
 * that memoization by sharing optimization work on a sub-expression with other parts of the expression that reference
 * the same sub-expression.
 * <br>
 * The reference abstraction is designed to make it difficult for authors of rules to mutate group expressions directly,
 * which is undefined behavior. Note that a {@link Reference} cannot be "de-referenced" using the {@link #get()}
 * method if it contains more than one member. Expressions with more than one member should not be used outside of the
 * query planner, and {@link #get()} should not be used inside the query planner.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class Reference implements Correlated<Reference>, Typed {
    @Nonnull
    private PlannerStage plannerStage;
    @Nonnull
    private final Members exploratoryMembers;
    @Nonnull
    private final Members finalMembers;

    @Nonnull
    private final SetMultimap<MatchCandidate, PartialMatch> partialMatchMap;
    @Nonnull
    private final ConstraintsMap constraintsMap;

    @Nonnull
    private ExpressionPropertiesMap<? extends RelationalExpression> propertiesMap;

    @Nonnull
    private final Supplier<Collection<RelationalExpression>> allMembersSupplier;

    private Reference() {
        this(PlannerStage.INITIAL, new LinkedIdentitySet<>(), new LinkedIdentitySet<>());
    }

    private Reference(@Nonnull final PlannerStage plannerStage,
                      @Nonnull final LinkedIdentitySet<RelationalExpression> exploratoryExpressions,
                      @Nonnull final LinkedIdentitySet<RelationalExpression> finalExpressions) {
        Debugger.sanityCheck(() -> {
            Verify.verify(plannerStage == PlannerStage.PLANNED ||
                    (exploratoryExpressions.stream()
                             .noneMatch(member -> member instanceof RecordQueryPlan) &&
                             finalExpressions.stream()
                                     .noneMatch(member -> member instanceof RecordQueryPlan)));
        });
        this.plannerStage = plannerStage;
        this.exploratoryMembers = new Members(exploratoryExpressions);
        this.finalMembers = new Members(finalExpressions);
        this.partialMatchMap = LinkedHashMultimap.create();
        this.constraintsMap = new ConstraintsMap();
        // TODO create based on the (yet) non-existing planner stage
        this.propertiesMap = new PlanPropertiesMap(finalExpressions.stream()
                .filter(m -> m instanceof RecordQueryPlan)
                .collect(Collectors.toList()));
        allMembersSupplier = Suppliers.memoize(() -> exploratoryMembers.concatExpressions(finalMembers));
        // Call debugger hook for this new reference.
        Debugger.registerReference(this);
    }

    @Nonnull
    public PlannerStage getPlannerStage() {
        return plannerStage;
    }

    @Nonnull
    public ConstraintsMap getConstraintsMap() {
        return constraintsMap;
    }

    @Nonnull
    public ExpressionPropertiesMap<? extends RelationalExpression> getPropertiesMap() {
        return propertiesMap;
    }

    public void advancePlannerStage(@Nonnull final PlannerStage newStage) {
        this.plannerStage = newStage;
        this.propertiesMap = plannerStage.createPropertiesMap();
        exploratoryMembers.clear();
        exploratoryMembers.add(finalMembers.getOnlyElement());
        finalMembers.clear();
    }

    /**
     * Return the {@link RecordQueryPlan} contained in this reference.
     * @return the {@link RecordQueryPlan} contained in this reference
     * @throws UngettableReferenceException if the reference does not support retrieving its expression
     * @throws ClassCastException if the only member of this reference is not a {@link RecordQueryPlan}
     */
    @Nonnull
    public RecordQueryPlan getOnlyElementAsPlan() {
        Verify.verify(exploratoryMembers.isEmpty(), "exploratory members must be empty");
        return finalMembers.getOnlyElementAsPlan();
    }

    public int getTotalMembersSize() {
        return exploratoryMembers.size() + finalMembers.size();
    }

    /**
     * Return the expression contained in this reference. If the reference does not support getting its expresssion
     * (for example, because it holds more than one expression, or none at all), this should throw an exception.
     * @return the expression contained in this reference
     * @throws UngettableReferenceException if the reference does not support retrieving its expression
     */
    @Nonnull
    public RelationalExpression get() {
        final int totalMembersSize = getTotalMembersSize();
        if (totalMembersSize != 1) {
            throw new UngettableReferenceException("tried to dereference reference with " + totalMembersSize +
                    " members");
        }
        return exploratoryMembers.isEmpty() ? finalMembers.getOnlyElement() : exploratoryMembers.getOnlyElement();
    }

    /**
     * Legacy method that calls {@link #pruneWith(RelationalExpression)} while being synchronized on {@code this}.
     * @param plan new expression to replace members of this reference.
     */
    public synchronized void replace(@Nonnull final RecordQueryPlan plan) {
        pruneWith(plan);
    }

    /**
     * Method that replaces the current members of this reference with a new value. This is called by the planner
     * to prune the variations of a reference down to exactly one new member.
     * @param expression expression to replace existing members with
     */
    public void pruneWith(@Nonnull final RelationalExpression expression) {
        Verify.verify(isFinal(expression));
        final var properties = propertiesMap.getCurrentProperties(expression);
        clear();
        insertUnchecked(expression, true, properties);
    }

    /**
     * Inserts a new expression into this reference. This particular overload utilized the precomputed properties of
     * a final {@link RelationalExpression} that already is thought to reside in another {@link Reference}.
     * @param expression new expression to be inserted
     * @param otherRef a reference that already contains {@code expression}
     * @return {@code true} if and only if the new expression was successfully inserted into this reference, {@code false}
     *         otherwise.
     */
    public boolean insertFrom(@Nonnull final RelationalExpression expression, @Nonnull final Reference otherRef) {
        if (otherRef.isFinal(expression)) {
            final var propertiesForPlan =
                    Objects.requireNonNull(otherRef.getPropertiesMap().getProperties(expression));
            return insert(expression, true, propertiesForPlan);
        }

        return insert(expression, false, null);
    }

    /**
     * Inserts a new expression into this reference. This method checks for prior memoization of the expression passed
     * in within the reference. If the expression is already contained in this reference, the reference is not modified.
     * @param newValue new expression to be inserted
     * @param isFinal indicator whether the new expression is final or not
     * @return {@code true} if and only if the new expression was successfully inserted into this reference, {@code false}
     *         otherwise.
     */
    public boolean insert(@Nonnull final RelationalExpression newValue, final boolean isFinal) {
        return insert(newValue, isFinal, null);
    }

    /**
     * Inserts a new expression into this reference. This method checks for prior memoization of the expression passed
     * in within the reference. If the expression is already contained in this reference, the reference is not modified.
     * @param newExpression new expression to be inserted
     * @param isFinal indicator whether the new expression is final or not
     * @param precomputedPropertiesMap if not {@code null}, a map of precomputed properties for a final expression
     *        that will be inserted into this reference verbatim, otherwise it will be computed
     * @return {@code true} if and only if the new expression was successfully inserted into this reference, {@code false}
     *         otherwise.
     */
    private boolean insert(@Nonnull final RelationalExpression newExpression,
                           final boolean isFinal,
                           @Nullable final Map<ExpressionProperty<?>, ?> precomputedPropertiesMap) {
        Debugger.withDebugger(debugger -> debugger.onEvent(Debugger.InsertIntoMemoEvent.begin()));
        try {
            final boolean containsInMemo = containsInMemo(newExpression, isFinal);
            Debugger.withDebugger(debugger -> {
                if (containsInMemo) {
                    debugger.onEvent(Debugger.InsertIntoMemoEvent.reusedExpWithReferences(newExpression, ImmutableList.of(this)));
                } else {
                    debugger.onEvent(Debugger.InsertIntoMemoEvent.newExp(newExpression));
                }
            });
            if (!containsInMemo) {
                insertUnchecked(newExpression, isFinal, precomputedPropertiesMap);
                return true;
            }
            return false;
        } finally {
            Debugger.withDebugger(debugger -> debugger.onEvent(Debugger.InsertIntoMemoEvent.end()));
        }
    }

    /**
     * Inserts a new expression into this reference. Unlike {{@link #insert(RelationalExpression, boolean, Map)}}, this
     * method does not check for prior memoization of the expression passed in within the reference. The caller needs to
     * exercise caution to only call this method on a reference if it is known that the reference cannot possibly
     * already have the expression memoized.
     * @param newExpression new expression to be inserted (without check)
     * @param isFinal indicator whether the new expression is final or not
     */
    public void insertUnchecked(@Nonnull final RelationalExpression newExpression, final boolean isFinal) {
        insertUnchecked(newExpression, isFinal, null);
    }

    /**
     * Inserts a new expression into this reference. Unlike {{@link #insert(RelationalExpression, boolean, Map)}}, this
     * method does not check for prior memoization of the expression passed in within the reference. The caller needs to
     * exercise caution to only call this method on a reference if it is known that the reference cannot possibly
     * already have the expression memoized.
     * @param newExpression new expression to be inserted (without check)
     * @param isFinal indicator whether the new expression is final or not
     * @param precomputedPropertiesMap if not {@code null}, a map of precomputed properties for a final expression
     *        that will be inserted into this reference verbatim, otherwise it will be computed
     */
    public void insertUnchecked(@Nonnull final RelationalExpression newExpression,
                                final boolean isFinal,
                                @Nullable final Map<ExpressionProperty<?>, ?> precomputedPropertiesMap) {
        // Call debugger hook to potentially register this new expression.
        Debugger.registerExpression(newExpression);

        Debugger.sanityCheck(() -> Verify.verify(getTotalMembersSize() == 0 ||
                getResultType().equals(newExpression.getResultType())));

        if (isFinal) {
            finalMembers.add(newExpression);
            if (precomputedPropertiesMap != null) {
                propertiesMap.add(newExpression, precomputedPropertiesMap);
            } else {
                propertiesMap.add(newExpression);
            }
        } else {
            exploratoryMembers.add(newExpression);
        }
    }

    public boolean containsExactly(@Nonnull final RelationalExpression expression) {
        return exploratoryMembers.containsExactly(expression) || finalMembers.containsExactly(expression);
    }

    public boolean isFinal(@Nonnull final RelationalExpression expression) {
        if (finalMembers.containsExactly(expression)) {
            return true;
        }
        if (exploratoryMembers.containsExactly(expression)) {
            return false;
        }
        throw new RecordCoreException("expression has to be a member of this reference");
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean containsAllInMemo(@Nonnull final Reference otherRef,
                                     @Nonnull final AliasMap equivalenceMap) {
        if (this == otherRef) {
            return true;
        }

        for (final RelationalExpression otherExpression : otherRef.getExploratoryExpressions()) {
            if (!exploratoryMembers.containsInMemo(otherExpression, equivalenceMap)) {
                return false;
            }
        }

        for (final RelationalExpression otherExpression : otherRef.getFinalExpressions()) {
            if (!finalMembers.containsInMemo(otherExpression, equivalenceMap)) {
                return false;
            }
        }
        return true;
    }

    public boolean containsInMemo(@Nonnull final RelationalExpression expression,
                                  final boolean isFinal) {
        if (!getCorrelatedTo().equals(expression.getCorrelatedTo())) {
            return false;
        }
        return isFinal ? finalMembers.containsInMemo(expression, AliasMap.emptyMap()) :
               exploratoryMembers.containsInMemo(expression, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        final ImmutableSet.Builder<CorrelationIdentifier> builder = ImmutableSet.builder();
        for (final RelationalExpression member : getAllMemberExpressions()) {
            builder.addAll(member.getCorrelatedTo());
        }
        return builder.build();
    }

    @SuppressWarnings("java:S1905")
    @Nonnull
    @Override
    public Reference rebase(@Nonnull final AliasMap translationMap) {
        throw new UnsupportedOperationException("rebase unsupported for reference");
    }

    @Nonnull
    public Reference translateCorrelations(@Nonnull final TranslationMap translationMap,
                                           final boolean shouldSimplifyValues) {
        final var translatedRefs =
                References.translateCorrelations(ImmutableList.of(this), translationMap, shouldSimplifyValues);
        return Iterables.getOnlyElement(translatedRefs);
    }

    /**
     * Method that resolves the result type by looking and unifying the result types from all the members.
     * @return {@link Type} representing result type
     */
    @Nonnull
    @Override
    public Type getResultType() {
        return getAllMemberExpressions()
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
        exploratoryMembers.clear();
        finalMembers.clear();
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
    public Set<RelationalExpression> getExploratoryExpressions() {
        return exploratoryMembers.getExpressions();
    }

    @Nonnull
    public Set<RelationalExpression> getFinalExpressions() {
        return finalMembers.getExpressions();
    }

    @Nonnull
    public Collection<RelationalExpression> getAllMemberExpressions() {
        return allMembersSupplier.get();
    }

    /**
     * Re-reference final members of this group, i.e., use a subset of members to from a new {@link Reference}.
     * Note that {@code this} group must not need exploration.
     *
     * @param expressions a collection of expressions that all have to be members of this group
     * @return a new explored {@link Reference}
     */
    @Nonnull
    public Reference newReferenceFromFinalMembers(@Nonnull final Collection<? extends RelationalExpression> expressions) {
        Verify.verify(!needsExploration());
        Verify.verify(getFinalExpressions().containsAll(expressions));

        final var newRef = Reference.of(getPlannerStage(), ImmutableList.of(), expressions);
        newRef.getConstraintsMap().setExplored();
        return newRef;
    }

    @Nonnull
    public <A> Map<RecordQueryPlan, A> getProperty(@Nonnull final ExpressionProperty<A> expressionProperty) {
        return propertiesMap.propertyValueForPlans(expressionProperty);
    }

    @Nonnull
    public List<PlanPartition> toPlanPartitions() {
        return propertiesMap.toPlanPartitions();
    }

    @Nullable
    public <U> U acceptVisitor(@Nonnull SimpleExpressionVisitor<U> simpleExpressionVisitor) {
        if (simpleExpressionVisitor.shouldVisit(this)) {
            final List<U> memberResults = new ArrayList<>(getAllMemberExpressions().size());
            for (RelationalExpression member : getAllMemberExpressions()) {
                final U result = simpleExpressionVisitor.shouldVisit(member)
                                 ? simpleExpressionVisitor.visit(member) : null;
                if (result == null) {
                    return null;
                }
                memberResults.add(result);
            }
            return simpleExpressionVisitor.evaluateAtRef(this, memberResults);
        }
        return null;
    }

    @Override
    public String toString() {
        return Debugger.mapDebugger(debugger -> debugger.nameForObject(this) + "[" +
                        getAllMemberExpressions().stream()
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

        final var otherReference = (Reference)other;
        return (exploratoryMembers.semanticEquals(otherReference.getExploratoryExpressions(), aliasMap) &&
                        finalMembers.semanticEquals(otherReference.getExploratoryExpressions(), aliasMap));
    }

    @Override
    public int semanticHashCode() {
        return Objects.hash(exploratoryMembers.semanticHashCode(), finalMembers.semanticHashCode());
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
        return PlannerGraphVisitor.show(renderSingleGroups, this);
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

    @Nonnull
    public static Reference empty() {
        return new Reference();
    }

    @Nonnull
    public static Reference initial(@Nonnull RelationalExpression expression) {
        return ofFinalExpression(PlannerStage.INITIAL, expression);
    }

    @Nonnull
    public static Reference initials(@Nonnull RelationalExpression... expressions) {
        return initials(Arrays.asList(expressions));
    }

    @Nonnull
    public static Reference initials(@Nonnull Collection<? extends RelationalExpression> expressions) {
        return ofFinalExpressions(PlannerStage.INITIAL, expressions);
    }

    @Nonnull
    public static Reference ofExploratoryExpression(@Nonnull final PlannerStage plannerStage,
                                                    @Nonnull final RelationalExpression expression) {
        LinkedIdentitySet<RelationalExpression> members = new LinkedIdentitySet<>();
        // Call debugger hook to potentially register this new expression.
        Debugger.registerExpression(expression);
        members.add(expression);
        return new Reference(plannerStage, members, new LinkedIdentitySet<>());
    }

    @Nonnull
    public static Reference ofFinalExpression(@Nonnull final PlannerStage plannerStage,
                                              @Nonnull final RelationalExpression expression) {
        LinkedIdentitySet<RelationalExpression> members = new LinkedIdentitySet<>();
        // Call debugger hook to potentially register this new expression.
        Debugger.registerExpression(expression);
        members.add(expression);
        return new Reference(plannerStage, new LinkedIdentitySet<>(), members);
    }

    @Nonnull
    public static Reference of(@Nonnull final PlannerStage plannerStage,
                               @Nonnull final Collection<? extends RelationalExpression> exploratoryExpressions,
                               @Nonnull final Collection<? extends RelationalExpression> finalExpressions) {
        // Call debugger hook to potentially register this new expression.
        exploratoryExpressions.forEach(Debugger::registerExpression);
        finalExpressions.forEach(Debugger::registerExpression);

        return new Reference(plannerStage, new LinkedIdentitySet<>(exploratoryExpressions),
                new LinkedIdentitySet<>(finalExpressions));
    }

    @Nonnull
    public static Reference ofExploratoryExpressions(@Nonnull final PlannerStage plannerStage,
                                                     @Nonnull final Collection<? extends RelationalExpression> expressions) {
        return of(plannerStage, expressions, new LinkedIdentitySet<>());
    }

    @Nonnull
    public static Reference ofFinalExpressions(@Nonnull final PlannerStage plannerStage,
                                               @Nonnull final Collection<? extends RelationalExpression> expressions) {
        return of(plannerStage, new LinkedIdentitySet<>(), expressions);
    }

    private static class Members {
        @Nonnull
        private final Set<RelationalExpression> expressions;

        private Members(@Nonnull final LinkedIdentitySet<RelationalExpression> expressions) {
            this.expressions = expressions;
        }

        @Nonnull
        private Set<RelationalExpression> getExpressions() {
            return expressions;
        }

        private boolean isEmpty() {
            return expressions.isEmpty();
        }

        private int size() {
            return expressions.size();
        }

        private boolean containsExactly(@Nonnull RelationalExpression expression) {
            return expressions.contains(expression);
        }

        @Nonnull
        private RelationalExpression getOnlyElement() {
            return Iterables.getOnlyElement(expressions);
        }

        @Nonnull
        private RecordQueryPlan getOnlyElementAsPlan() {
            return (RecordQueryPlan)Iterables.getOnlyElement(expressions);
        }

        @Nonnull
        private Collection<RelationalExpression> concatExpressions(@Nonnull final Members other) {
            return concatSetsView(expressions, other.getExpressions());
        }

        private void clear() {
            expressions.clear();
        }

        public int semanticHashCode() {
            final var iterator = expressions.iterator();
            final var builder = ImmutableSet.builder();

            while (iterator.hasNext()) {
                final RelationalExpression next = iterator.next();
                builder.add(next.semanticHashCode());
            }

            return Objects.hash(builder.build());
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
            if (this == other) {
                return true;
            }

            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            final var otherMembers = (Members)other;

            final var iterator = expressions.iterator();
            final var expressionsMapBuilder =
                    ImmutableMultimap.<Integer, RelationalExpression>builder();

            while (iterator.hasNext()) {
                final RelationalExpression next = iterator.next();
                expressionsMapBuilder.put(next.semanticHashCode(), next);
            }
            final var expressionsMap = expressionsMapBuilder.build();

            for (final RelationalExpression otherExpression : otherMembers.getExpressions()) {
                if (expressionsMap.get(otherExpression.semanticHashCode())
                        .stream()
                        .anyMatch(member -> member.semanticEquals(otherExpression, aliasMap))) {
                    return true;
                }
            }
            return false;
        }

        @CanIgnoreReturnValue
        private boolean add(@Nonnull final RelationalExpression expression) {
            return expressions.add(expression);
        }

        private boolean containsInMemo(@Nonnull final RelationalExpression expression,
                                       @Nonnull final AliasMap equivalenceMap) {
            for (final RelationalExpression member : expressions) {
                if (isMemoizedExpression(member, expression, equivalenceMap)) {
                    return true;
                }
            }
            return false;
        }

        @Nonnull
        private static Members copyOf(@Nonnull final Members members) {
            return copyOf(members.getExpressions());
        }

        @Nonnull
        private static Members copyOf(@Nonnull final Set<RelationalExpression> expressions) {
            return new Members(new LinkedIdentitySet<>(expressions));
        }
    }

    private static <T> Collection<T> concatSetsView(@Nonnull final Set<T> first,
                                                    @Nonnull final Set<T> second) {
        return new AbstractCollection<>() {
            @Override
            public Iterator<T> iterator() {
                return new Iterator<>() {
                    private final Iterator<T> it1 = first.iterator();
                    private final Iterator<T> it2 = second.iterator();

                    @Override
                    public boolean hasNext() {
                        return it1.hasNext() || it2.hasNext();
                    }

                    @Override
                    public T next() {
                        if (it1.hasNext()) {
                            return it1.next();
                        }
                        return it2.next();
                    }
                };
            }

            @Override
            public int size() {
                return first.size() + second.size();
            }
        };
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
