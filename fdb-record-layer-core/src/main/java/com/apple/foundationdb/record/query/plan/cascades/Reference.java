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
import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventListeners;
import com.apple.foundationdb.record.query.plan.cascades.events.InsertIntoMemoPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithChildren;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 * method if it contains more than one member. Expressions with more than one member should not be used outside the
 * query planner, and {@link #get()} should not be used inside the query planner.
 * <br>
 * A reference contains expressions. It further differentiates between contained exploratory expressions and final
 * expressions. Utilized during planning, exploratory expressions are expressions that designated exploration rules
 * explore and yield whereas final expressions are those expressions that are yielded by implementation rules and which
 * take part in a best-plan selection process called pruning that will eventually distill one remaining final expression
 * in the reference that is then considered the <em>best</em> variant among the members of this reference. For the
 * planning phase {@link PlannerPhase#PLANNING}, all exploratory expressions are expressions that are not
 * {@link RecordQueryPlan}s while all final expressions are {@link RecordQueryPlan}s. Note that other planner phases do
 * not make such a type-based distinction between exploratory and final expressions. For instance
 * {@link PlannerPhase#REWRITING} uses {@link RelationalExpression} for both exploratory and final expressions.
 * <br>
 * All exploratory expressions form a set and all final expressions form a set as well. It is, however, possible, that
 * an expression is part of both sets.
 * <br>
 * Both sets of exploratory and final expressions are mutated during in the following contexts during the planning
 * process:
 * <ul>
 *     <li>
 *         rules yielding new expressions through {@link CascadesRuleCall},
 *     </li>
 *     <li>
 *         rules memoizing expressions through the {@link Memoizer} interface directly, or
 *     </li>
 *     <li>
 *         by the planner itself when the reference is pruned.
 *     </li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class Reference implements Correlated<Reference>, Typed {
    /**
     * The current planner stage. This is initially set through the constructor but is mutable through
     * {@link #advancePlannerStage(PlannerStage)}.
     */
    @Nonnull
    private PlannerStage plannerStage;

    /**
     * The exploratory members of this reference. Exploratory members are members that are actively being explored
     * and are not considered for costing and pruning. That distinction allows for more flexible exploration rules
     * and more flexible reference sharing in the memoization structures.
     */
    @Nonnull
    private final Members exploratoryMembers;

    /**
     * The final members of this reference. Final members are members that are the result of implementation rules.
     * Final members can only have descendant references that on consist oof final members themselves. During the
     * {@link PlannerPhase#PLANNING}, the invariant {@code member instanceof RecordQueryPlanner <=> member is final}
     * holds.
     */
    @Nonnull
    private final Members finalMembers;

    /**
     * A map linking this reference to {@link MatchCandidate}s via corresponding {@link PartialMatch}es. We use this
     * map during matching.
     */
    @Nonnull
    private final SetMultimap<MatchCandidate, PartialMatch> partialMatchMap;

    /**
     * A map that captures constraints to the reference that are imposed to it by quantifiers (and therefore consumers).
     * A constraint can be e.g. an ordering constraint. The ordering constraint's value is the actual ordering we would
     * like the planner to find plans for. Constraints can be amended and combined during planning. A constraints map
     * combines the different constraints that have been pushed to this reference by treating them all orthogonally,
     * i.e. the plans we eventually would like to create should satisfy the cross products of all constraint values.
     * If we look for orderings of {@code _.a.b} and {@code _.c}, as well as for distinct and non-distinct plans, we
     * actually look for plans satisfying: {@code _.a.b} and distinct, {@code _.a.b} and non-distinct, {@code _.c}
     * and distinct, {@code _.c} and non-distinct.
     */
    @Nonnull
    private final ConstraintsMap constraintsMap;

    /**
     * Properties map capturing properties for all final members of this reference. {@link ExpressionPropertiesMap},
     * together with the more specialized {@link PlanPropertiesMap}, are the basis for creating partitions of
     * expressions that satisfy certain properties and powerful matchers for implementation rules.
     * @see ExpressionPartition
     * @see PlanPartition
     */
    @Nonnull
    private ExpressionPropertiesMap<? extends RelationalExpression> propertiesMap;

    /**
     * View of both exploratory and final members. Note that the collection is a multiset as an expression can be part
     * of both exploratory and final expressions.
     */
    @Nonnull
    private final Collection<RelationalExpression> allMembersView;

    private Reference() {
        this(PlannerStage.INITIAL, new LinkedIdentitySet<>(), new LinkedIdentitySet<>());
    }

    private Reference(@Nonnull final PlannerStage plannerStage,
                      @Nonnull final LinkedIdentitySet<RelationalExpression> exploratoryExpressions,
                      @Nonnull final LinkedIdentitySet<RelationalExpression> finalExpressions) {
        Debugger.sanityCheck(() ->
                Verify.verify(plannerStage == PlannerStage.PLANNED ||
                        (exploratoryExpressions.stream()
                                 .noneMatch(member -> member instanceof RecordQueryPlan) &&
                                 finalExpressions.stream()
                                         .noneMatch(member -> member instanceof RecordQueryPlan))));
        this.plannerStage = plannerStage;
        this.exploratoryMembers = new Members(exploratoryExpressions);
        this.finalMembers = new Members(finalExpressions);
        this.partialMatchMap = LinkedHashMultimap.create();
        this.constraintsMap = new ConstraintsMap();
        this.propertiesMap = plannerStage.createPropertiesMap();
        finalExpressions.forEach(finalExpression -> propertiesMap.add(finalExpression));
        this.allMembersView = exploratoryMembers.concatExpressions(finalMembers);
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

    /**
     * Advance the planner stage of this reference to the given one. This method cleans up internal state of this
     * reference such that exploration in a new phase can start again.
     * @param newStage the new stage we should advance to
     */
    public void advancePlannerStage(@Nonnull final PlannerStage newStage) {
        Verify.verify(plannerStage.directlyPrecedes(newStage));
        Verify.verify(finalMembers.size() == 1);
        advancePlannerStageUnchecked(newStage);
    }

    /**
     * Advance the planner stage of this reference to the given one. This method cleans up internal state of this
     * reference such that exploration in a new phase can start again.
     * @param newStage the new stage we should advance to
     */
    @VisibleForTesting
    void advancePlannerStageUnchecked(@Nonnull final PlannerStage newStage) {
        this.plannerStage = newStage;
        constraintsMap.advancePlannerStage();
        this.propertiesMap = newStage.createPropertiesMap();
        exploratoryMembers.clear();
        exploratoryMembers.addAll(finalMembers);
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
     * Note that unlike for {@link #pruneWith(RelationalExpression)}, the given expression does not have to be part of
     * this reference already. This method is only used by the heuristic planner.
     * @param plan new plan to replace members of this reference.
     */
    @HeuristicPlanner
    public synchronized void replace(@Nonnull final RecordQueryPlan plan) {
        Debugger.verifyHeuristicPlanner();
        pruneWithUnchecked(plan);
    }

    /**
     * Method that replaces the current final members of this reference with a new value. This is called by the planner
     * to prune the variations of a reference down to exactly one new member.
     * @param expression expression to replace existing members with
     */
    public void pruneWith(@Nonnull final RelationalExpression expression) {
        Verify.verify(isFinal(expression));
        pruneWithUnchecked(expression);
    }

    /**
     * Method to prune the final expressions in this reference. The given expression must be one of the final
     * expressions. This operation does not touch the exploratory expressions.
     * @param expression an expression that is assumed to be among the final expressions
     */
    private void pruneWithUnchecked(@Nonnull final RelationalExpression expression) {
        final var properties = propertiesMap.getCurrentProperties(expression);
        clearFinalExpressions();
        insertUnchecked(expression, true, properties);
    }

    /**
     * Inserts a new exploratory expression into this reference. This method checks for prior memoization of the
     * expression passed in within the reference. If the expression is already contained in this reference, the
     * reference is not modified.
     * @param newValue new expression to be inserted
     * @return {@code true} if and only if the new expression was successfully inserted into this reference,
     *         {@code false} otherwise.
     */
    public boolean insertExploratoryExpression(@Nonnull final RelationalExpression newValue) {
        return insert(newValue, false, null);
    }

    /**
     * Inserts a new final expression into this reference. This method checks for prior memoization of the expression
     * passed in within the reference. If the expression is already contained in this reference, the reference is not
     * modified.
     * @param newValue new expression to be inserted
     * @return {@code true} if and only if the new expression was successfully inserted into this reference,
     *         {@code false} otherwise.
     */
    public boolean insertFinalExpression(@Nonnull final RelationalExpression newValue) {
        return insert(newValue, true, null);
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
        PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent.begin());
        try {
            final boolean containsInMemo = containsInMemo(newExpression, isFinal);

            if (containsInMemo) {
                PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent.reusedExpWithReferences(newExpression, ImmutableList.of(this)));
            } else {
                PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent.newExp(newExpression));
            }

            if (!containsInMemo) {
                insertUnchecked(newExpression, isFinal, precomputedPropertiesMap);
                return true;
            }
            return false;
        } finally {
            PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent.end());
        }
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
    private void insertUnchecked(@Nonnull final RelationalExpression newExpression,
                                 final boolean isFinal,
                                 @Nullable final Map<ExpressionProperty<?>, ?> precomputedPropertiesMap) {
        // Call debugger hook to potentially register this new expression.
        Debugger.registerExpression(newExpression);

        Debugger.sanityCheck(() -> Verify.verify(getTotalMembersSize() == 0 ||
                getResultType().equals(newExpression.getResultType())));
        Debugger.sanityCheck(() -> Verify.verify(getTotalMembersSize() == 0 ||
                getCorrelatedTo().containsAll(newExpression.getCorrelatedTo())));

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

    /**
     * Method to return if the given expression is contained in this reference (by Java object identity, that is not by
     * equality, using {@link #containsInMemo(RelationalExpression, boolean)} or other methods). This method
     * does not distinguish between exploratory and final expressions.
     * @param expression expression to check
     * @return {@code true} iff this reference contains the expression passed in.
     */
    public boolean containsExactly(@Nonnull final RelationalExpression expression) {
        return exploratoryMembers.containsExactly(expression) || finalMembers.containsExactly(expression);
    }

    /**
     * Method to indicate if the given expression is an exploratory expression (by Java object identity) of this
     * reference. This method is not sensitive to whether the expression is also a final expression of this reference.
     * @param expression expression to check
     * @return {@code true} if the expression passed in is an exploratory expression, {@code false} if the expression
     *         passed in is not an exploratory expression.
     * @throws RecordCoreException if the expression is not contained in the reference at all.
     */
    public boolean isExploratory(@Nonnull final RelationalExpression expression) {
        if (exploratoryMembers.containsExactly(expression)) {
            return true;
        }
        if (finalMembers.containsExactly(expression)) {
            return false;
        }
        throw new RecordCoreException("expression has to be a member of this reference");
    }

    /**
     * Method to indicate if the given expression is a final expression (by Java object identity) of this reference.
     * This method is not sensitive to whether the expression is also an exploratory expression of this reference.
     * @param expression expression to check
     * @return {@code true} if the expression passed in is a final expression, {@code false} if the expression passed in
     *         is not a final expression.
     * @throws RecordCoreException if the expression is not contained in the reference at all.
     */
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
    @VisibleForTesting
    boolean containsAllInMemo(@Nonnull final Reference otherRef,
                              @Nonnull final AliasMap equivalenceMap) {
        if (this == otherRef) {
            return true;
        }

        return containsAllInMemo(otherRef.getExploratoryExpressions(), equivalenceMap, false)
                && containsAllInMemo(otherRef.getFinalExpressions(), equivalenceMap, true);
    }

    @API(API.Status.INTERNAL)
    boolean containsAllInMemo(@Nonnull final Collection<? extends RelationalExpression> expressions,
                              @Nonnull final AliasMap equivalenceMap,
                              boolean isFinal) {
        Members members = isFinal ? finalMembers : exploratoryMembers;
        for (final RelationalExpression otherExpression : expressions) {
            if (!members.containsInMemo(otherExpression, equivalenceMap)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Method to determine is this reference already memoized the expression that is passed in. An expression is
     * memoized if a semantically equal expression (that includes equality by java object id) is contained in this
     * reference and if the expression's children expressions are all memoized compatibly as well.
     * @param expression expression to check
     * @param isFinal indicator iff the expression passed in should be considered a final expression (or an exploratory
     *        expression
     * @return {@code true} iff the expression is already memoized in this reference
     */
    @VisibleForTesting
    boolean containsInMemo(@Nonnull final RelationalExpression expression,
                           final boolean isFinal) {
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
    public Reference translateGraph(@Nonnull final Memoizer memoizer,
                                    @Nonnull final TranslationMap translationMap,
                                    final boolean shouldSimplifyValues) {
        final var translatedRefs =
                References.translateCorrelationsInGraphs(ImmutableList.of(this), memoizer, translationMap, shouldSimplifyValues);
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

    public void clearExploratoryExpressions() {
        exploratoryMembers.clear();
    }

    public void clearFinalExpressions() {
        propertiesMap.clear();
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

    public boolean isExplored() {
        return constraintsMap.isExplored();
    }

    public void setExplored() {
        constraintsMap.setExplored();
    }

    public void inheritConstraintsFromOther(@Nonnull final Reference otherReference) {
        constraintsMap.inheritFromOther(otherReference.getConstraintsMap());
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
        return allMembersView;
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
        newRef.setExplored();
        return newRef;
    }

    @Nonnull
    public <A> Map<? extends RelationalExpression, A> getPropertyForExpressions(@Nonnull final ExpressionProperty<A> expressionProperty) {
        return propertiesMap.propertyValueForExpressions(expressionProperty);
    }

    @Nonnull
    public <A> Map<RecordQueryPlan, A> getPropertyForPlans(@Nonnull final ExpressionProperty<A> expressionProperty) {
        return propertiesMap.propertyValueForPlans(expressionProperty);
    }

    @Nonnull
    public List<? extends ExpressionPartition<? extends RelationalExpression>> toExpressionPartitions() {
        return ExpressionPartitions.toPartitions(propertiesMap);
    }

    @Nonnull
    public List<PlanPartition> toPlanPartitions() {
        return PlanPartitions.toPartitions((PlanPropertiesMap)propertiesMap);
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
        return (exploratoryMembers.semanticEquals(otherReference.exploratoryMembers, aliasMap) &&
                        finalMembers.semanticEquals(otherReference.finalMembers, aliasMap));
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

    @Nonnull
    public String showExploratory() {
        return PlannerGraphVisitor.show(PlannerGraphVisitor.REMOVE_FINAL_EXPRESSIONS | PlannerGraphVisitor.RENDER_SINGLE_GROUPS, this);
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

        //
        // We need to check if the expressions' correlatedTo sets are identical under the consideration of the given
        // map of alias equivalences. If they are not, the member is not memoizing the otherExpression.
        //
        // Specific example:
        // SELECT *
        // FROM A a, SELECT * FROM B WHERE a.x = b.y
        //
        // Let's assume the inner 'SELECT * FROM B WHERE a.x = b.y' already is member (in the memoization structure).
        // This member is correlated to 'a'.
        // However, if we didn't do the check for correlation sets here, we would satisfy the following otherExpression:
        // SELECT * FROM A WHERE a.x = b.y is correlated to b
        //
        final Set<CorrelationIdentifier> originalCorrelatedTo = member.getCorrelatedTo();
        final Set<CorrelationIdentifier> translatedCorrelatedTo =
                equivalenceMap.definesOnlyIdentities()
                ? originalCorrelatedTo
                : originalCorrelatedTo.stream()
                        .map(alias -> equivalenceMap.getTargetOrDefault(alias, alias))
                        .collect(ImmutableSet.toImmutableSet());
        if (!translatedCorrelatedTo.equals(otherExpression.getCorrelatedTo())) {
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
    public static Reference initialOf(@Nonnull final RelationalExpression expression) {
        return ofFinalExpression(PlannerStage.INITIAL, expression);
    }

    @Nonnull
    public static Reference initialOf(@Nonnull final RelationalExpression... expressions) {
        return initialOf(Arrays.asList(expressions));
    }

    @Nonnull
    public static Reference initialOf(@Nonnull final Collection<? extends RelationalExpression> expressions) {
        return ofFinalExpressions(PlannerStage.INITIAL, expressions);
    }

    @Nonnull
    public static Reference plannedOf(@Nonnull final RecordQueryPlan plan) {
        return ofFinalExpression(PlannerStage.PLANNED, plan);
    }

    @Nonnull
    public static Reference ofExploratoryExpression(@Nonnull final PlannerStage plannerStage,
                                                    @Nonnull final RelationalExpression expression) {
        return of(plannerStage, ImmutableList.of(expression), ImmutableList.of());
    }

    @Nonnull
    public static Reference ofFinalExpression(@Nonnull final PlannerStage plannerStage,
                                              @Nonnull final RelationalExpression expression) {
        return of(plannerStage, ImmutableList.of(), ImmutableList.of(expression));
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

        public Members(@Nonnull final LinkedIdentitySet<RelationalExpression> expressions) {
            this.expressions = expressions;
        }

        @Nonnull
        public Set<RelationalExpression> getExpressions() {
            return expressions;
        }

        public boolean isEmpty() {
            return expressions.isEmpty();
        }

        public int size() {
            return expressions.size();
        }

        public boolean containsExactly(@Nonnull RelationalExpression expression) {
            return expressions.contains(expression);
        }

        @Nonnull
        public RelationalExpression getOnlyElement() {
            return Iterables.getOnlyElement(expressions);
        }

        @Nonnull
        public RecordQueryPlan getOnlyElementAsPlan() {
            return (RecordQueryPlan)Iterables.getOnlyElement(expressions);
        }

        @Nonnull
        public Collection<RelationalExpression> concatExpressions(@Nonnull final Members other) {
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
                        .noneMatch(member -> member.semanticEquals(otherExpression, aliasMap))) {
                    return false;
                }
            }
            return true;
        }

        @CanIgnoreReturnValue
        public boolean add(@Nonnull final RelationalExpression expression) {
            return expressions.add(expression);
        }

        @CanIgnoreReturnValue
        public boolean addAll(@Nonnull final Members members) {
            return expressions.addAll(members.getExpressions());
        }

        @CanIgnoreReturnValue
        public boolean addAll(@Nonnull final Collection<? extends RelationalExpression> newExpressions) {
            return expressions.addAll(newExpressions);
        }

        public boolean containsInMemo(@Nonnull final RelationalExpression otherExpression,
                                      @Nonnull final AliasMap equivalenceMap) {
            // short circuit if possible
            if (expressions.contains(otherExpression)) {
                //
                // Make sure that the equivalence map either only defines identities, i.e. aliases are only mapped to
                // themselves, or, if not, that neither sources nor targets have anything to do with the expression's
                // correlation set.
                //
                final var otherCorrelatedTo = otherExpression.getCorrelatedTo();
                if (equivalenceMap.definesOnlyIdentities() ||
                        (Collections.disjoint(equivalenceMap.targets(), otherCorrelatedTo) &&
                                 Collections.disjoint(equivalenceMap.sources(), otherCorrelatedTo))) {
                    return true;
                }
            }

            for (final RelationalExpression member : expressions) {
                if (isMemoizedExpression(member, otherExpression, equivalenceMap)) {
                    return true;
                }
            }
            return false;
        }
    }

    @Nonnull
    private static <T> Collection<T> concatSetsView(@Nonnull final Set<T> first,
                                                    @Nonnull final Set<T> second) {
        return new AbstractCollection<>() {
            @Nonnull
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
