/*
 * CascadesRuleCall.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers.AliasResolver;
import com.apple.foundationdb.record.query.plan.cascades.events.InsertIntoMemoPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Location;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventListeners;
import com.apple.foundationdb.record.query.plan.cascades.events.TransformRuleCallPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A rule call implementation for the {@link CascadesPlanner}. The life cycle of a rule call object starts with the
 * planner's decision to call a rule and ends after yielded objects have been processed. The planner instantiates a new
 * instance of this class and passes it to {@link AbstractCascadesRule#onMatch(PlannerRuleCall)} when the rule is executed.
 * During the execution of a rule, the rule implementation can invoke various yielding methods (most prominently
 * {@link #yieldExploratoryExpression(RelationalExpression)}, {@link #yieldFinalExpression(RelationalExpression)},
 * and {@link #yieldPartialMatch}) which modify the query graph if needed, and (if necessary) queue up those newly
 * yielded objects for further exploration by the planner.
 * <br>
 * Specifically, for {@link RelationalExpression}s and {@link Reference}s, the rule call implementation also implements
 * {@link Memoizer} which interfaces directly with {@link CascadesPlanner}s memoization structures. This allows rules
 * to also directly memoize (or reuse memoized) references directly that are not yielded themselves.
 * <br>
 * Among other things, each object of this class maintains a reference to a {@link PlannerPhase} and a reference to the
 * rule we are executing. Note that the planner phase that the rule is implemented for is the same as this planner
 * stage.
 * <br>
 * Note that this class implements a number of interfaces that were carefully constructed to partition the set of
 * methods by whether this rule call is targeted for an exploration rule or an implementation rule. Please take a look
 * at {@link ExplorationCascadesRule} and {@link ImplementationCascadesRule} for the definition of these rules. There
 * are two corresponding rule calls (i.e. {@link ExplorationCascadesRuleCall} and
 * {@link ImplementationCascadesRuleCall}) that the aforementioned rules use for their respective {@code onMatch(...)}
 * methods which hide away parts of the rule call API that should not ever be called by those rules. For instance,
 * implementation rules should never memoize or yield exploratory expressions. In the past, <em>illegal</em> memoization
 * and/or yielding methods were called from within cascades rules that then caused problems later that are incredibly
 * hard to debug.
 */
@API(API.Status.EXPERIMENTAL)
public class CascadesRuleCall implements ExplorationCascadesRuleCall, ImplementationCascadesRuleCall, Memoizer, Yields {
    private final PlannerPhase plannerPhase;
    private final CascadesRule<?> rule;
    private final Reference root;
    private final Traversal traversal;
    private final Deque<CascadesPlanner.Task> taskStack;
    private final PlannerBindings bindings;
    private final PlanContext context;
    private final LinkedIdentitySet<RelationalExpression> newExploratoryExpressions;
    private final LinkedIdentitySet<RelationalExpression> newFinalExpressions;
    private final LinkedIdentitySet<PartialMatch> newPartialMatches;
    private final LinkedIdentitySet<Reference> newReferences;
    private final Set<Reference> referencesWithPushedConstraints;
    private final EvaluationContext evaluationContext;

    public CascadesRuleCall(final PlannerPhase plannerPhase,
                            final PlanContext context,
                            final CascadesRule<?> rule,
                            final Reference root,
                            final Traversal traversal,
                            final Deque<CascadesPlanner.Task> taskStack,
                            final PlannerBindings bindings,
                            final EvaluationContext evaluationContext) {
        this.plannerPhase = plannerPhase;
        this.context = context;
        this.rule = rule;
        this.root = root;
        this.traversal = traversal;
        this.taskStack = taskStack;
        this.bindings = bindings;
        this.newExploratoryExpressions = new LinkedIdentitySet<>();
        this.newFinalExpressions = new LinkedIdentitySet<>();
        this.newPartialMatches = new LinkedIdentitySet<>();
        this.newReferences = new LinkedIdentitySet<>();
        this.referencesWithPushedConstraints = Sets.newLinkedHashSet();
        this.evaluationContext = evaluationContext;
    }

    public void run() {
        rule.onMatch(this);
    }

    @Override
    public PlannerPhase getPlannerPhase() {
        return plannerPhase;
    }

    @Override
    public Reference getRoot() {
        return root;
    }

    @Override
    public AliasResolver newAliasResolver() {
        return new AliasResolver(traversal);
    }

    @Override
    public PlanContext getContext() {
        return context;
    }

    @Override
    public PlannerBindings getBindings() {
        return bindings;
    }

    @Override
    public <T> Optional<T> getPlannerConstraintMaybe(final PlannerConstraint<T> plannerConstraint) {
        if (rule.getConstraintDependencies().contains(plannerConstraint)) {
            return root.getConstraintsMap().getConstraintOptional(plannerConstraint);
        }

        throw new RecordCoreArgumentException("rule is not dependent on requested planner requirement");
    }

    @Override
    @SuppressWarnings({"PMD.CompareObjectsWithEquals"}) // deliberate use of id equality check for short-circuit condition
    public <T> void pushConstraint(final Reference reference,
                                   final PlannerConstraint<T> plannerConstraint,
                                   final T constraintValue) {
        Verify.verify(root != reference);
        final ConstraintsMap requirementsMap = reference.getConstraintsMap();
        if (requirementsMap.pushProperty(plannerConstraint, constraintValue).isPresent()) {
            referencesWithPushedConstraints.add(reference);
        }
    }

    @Override
    public void emitEvent(final Location location) {
        Verify.verify(location != Location.BEGIN && location != Location.END);
        PlannerEventListeners.dispatchEvent(() ->
                new TransformRuleCallPlannerEvent(plannerPhase, root, taskStack, location, root,
                        bindings.get(rule.getMatcher()), rule, this));
    }

    @Override
    public void yieldExploratoryExpression(final RelationalExpression expression) {
        yieldExpression(expression, false);
    }

    @Override
    public void yieldPlan(final RecordQueryPlan plan) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        yieldFinalExpression(plan);
    }

    @Override
    public void yieldFinalExpression(final RelationalExpression expression) {
        yieldExpression(expression, true);
    }

    @Override
    public void yieldUnknownExpression(final RelationalExpression expression) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        if (expression instanceof RecordQueryPlan) {
            yieldPlan((RecordQueryPlan)expression);
        } else {
            yieldExploratoryExpression(expression);
        }
    }

    private void yieldExpression(final RelationalExpression expression, final boolean isFinal) {
        verifyChildrenMemoized(expression);
        if (isFinal) {
            if (root.insertFinalExpression(expression)) {
                newFinalExpressions.add(expression);
                traversal.addExpression(root, expression);
            }
        } else {
            if (root.insertExploratoryExpression(expression)) {
                newExploratoryExpressions.add(expression);
                traversal.addExpression(root, expression);
            }
        }
    }

    private void verifyChildrenMemoized(RelationalExpression expression) {
        for (final var quantifier : expression.getQuantifiers()) {
            final var rangesOver = quantifier.getRangesOver();
            Verify.verify(traversal.getRefs().contains(rangesOver));
        }
    }

    /**
     * Notify the planner's data structures that a new partial match has been produced by the rule. This method may be
     * called zero or more times by the rule's <code>onMatch()</code> method.
     *
     * @param boundAliasMap the alias map of bound correlated identifiers between query and candidate
     * @param matchCandidate the match candidate
     * @param queryExpression the query expression
     * @param candidateRef the matching reference on the candidate side
     * @param matchInfo an auxiliary structure to keep additional information about the match
     */
    @Override
    public void yieldPartialMatch(final AliasMap boundAliasMap,
                                  final MatchCandidate matchCandidate,
                                  final RelationalExpression queryExpression,
                                  final Reference candidateRef,
                                  final MatchInfo matchInfo) {
        final PartialMatch newPartialMatch =
                new PartialMatch(boundAliasMap,
                        matchCandidate,
                        root,
                        queryExpression,
                        candidateRef,
                        matchInfo);
        root.addPartialMatchForCandidate(matchCandidate, newPartialMatch);
        newPartialMatches.add(newPartialMatch);
    }

    public Collection<RelationalExpression> getNewExploratoryExpressions() {
        return Collections.unmodifiableCollection(newExploratoryExpressions);
    }

    public Collection<RelationalExpression> getNewFinalExpressions() {
        return Collections.unmodifiableCollection(newFinalExpressions);
    }

    public Set<PartialMatch> getNewPartialMatches() {
        return newPartialMatches;
    }

    public Set<Reference> getReferencesWithPushedConstraints() {
        return referencesWithPushedConstraints;
    }

    @Override
    public EvaluationContext getEvaluationContext() {
        return evaluationContext;
    }

    private Reference addNewReference(final Reference newRef) {
        for (RelationalExpression expression : newRef.getAllMemberExpressions()) {
            PlannerEventListeners.dispatchEvent(() -> InsertIntoMemoPlannerEvent.newExp(expression));
            traversal.addExpression(newRef, expression);
        }
        newReferences.add(newRef);
        return newRef;
    }

    public void pruneUnusedNewReferences() {
        // Not all of the references created during the course of a rule call end up in a yielded expression.
        // At the end of rule call execution, remove any of the newly created references that aren't referenced
        // by another reference in the graph
        traversal.pruneUnreferencedRefs(newReferences);
        newReferences.clear();
    }

    @Override
    public Reference memoizeExpressions(final Collection<? extends RelationalExpression> exploratoryExpressions,
                                        final Collection<? extends RelationalExpression> finalExpressions) {
        return memoizeExpressionsExactly(exploratoryExpressions, finalExpressions,
                (exploratoryExpressionsSet, finalExpressionsSet) ->
                        Reference.of(getPlannerPhase().getTargetPlannerStage(), exploratoryExpressionsSet, finalExpressionsSet));
    }

    @Override
    public Reference memoizeExploratoryExpression(final RelationalExpression expression) {
        return memoizeExploratoryExpressions(ImmutableSet.of(expression));
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Reference memoizeExploratoryExpressions(final Collection<? extends RelationalExpression> expressions) {
        Preconditions.checkArgument(!expressions.isEmpty(), "Cannot create reference over empty expression collection");
        if (expressions.stream().allMatch(expression -> expression.getQuantifiers().isEmpty())) {
            return memoizeLeafExpressions(expressions);
        }
        //
        // This requires that for each expression in the expressions collection, all of its children have already been
        // placed in the memo structure. We then look to find a
        //
        //   1. Pick one variation.
        //   2. For that variation, find the set of references that point to each of its children. These form a set
        //      of sets of candidate parents
        //   3. Take the set intersection of the candidates to get a new candidate set that is made up of all
        //      expressions that point to _all_ children
        //   4. For each reference including an expression in the parent candidate set, look to see if that
        //      candidate already contains all variations in its memo
        //
        // In other words, this uses the topology of the graph to find a candidate set of potential references.
        // Once that has been done, it checks if any of those already contain all variations. Note that we only
        // need to use one variation for that initial topology check. If we repeated steps 2-4 with the other
        // variations, then we will either re-tread existing candidate references (which must be missing at
        // least one variation) or it will be a new reference, but that reference must be missing at least
        // one child from the first variation and therefore cannot be reused
        //
        PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::begin);
        try {
            Preconditions.checkArgument(expressions.stream().noneMatch(expression -> expression instanceof RecordQueryPlan));

            final Set<CorrelationIdentifier> requiredCorrelations = correlatedTo(expressions);

            // Pick a candidate expression from the expressions collection. This will be used to do the topological check
            final RelationalExpression expression = expressions.iterator().next();

            // For each child quantifier of this expression, look up the set of parent nodes in the
            // memo structure that point to the (already memoized) child
            final var referencePathsList = expression.getQuantifiers().stream()
                            .map(Quantifier::getRangesOver)
                            .map(traversal::getParentRefPaths)
                            .collect(ImmutableList.toImmutableList());

            // Gather the references to the expressions that include each child
            final var expressionToReferenceMap = new LinkedIdentityMap<RelationalExpression, Reference>();
            referencePathsList.stream()
                    .flatMap(Collection::stream)
                    .filter(referencePath -> isEligibleForReuse(requiredCorrelations, referencePath))
                    .forEach(referencePath -> {
                        final var referencingExpression = referencePath.getExpression();
                        if (expressionToReferenceMap.containsKey(referencingExpression)) {
                            if (expressionToReferenceMap.get(referencingExpression) != referencePath.getReference()) {
                                throw new RecordCoreException("expression used in multiple references");
                            }
                        } else {
                            expressionToReferenceMap.put(referencePath.getExpression(), referencePath.getReference());
                        }
                    });

            // From the traversal, get the sets of expressions that point to each child
            final List<Set<RelationalExpression>> referencingExpressions = referencePathsList.stream()
                    .map(referencePaths ->
                            referencePaths.stream()
                                    .filter(referencePath -> isEligibleForReuse(requiredCorrelations, referencePath))
                                    .map(Traversal.ReferencePath::getExpression)
                                    .collect(LinkedIdentitySet.toLinkedIdentitySet()))
                    .collect(ImmutableList.toImmutableList());

            // Compute the set of expressions that point to _all_ of the needed children
            final var referencingExpressionsIterator = referencingExpressions.iterator();
            final var commonReferencingExpressions = new LinkedIdentitySet<>(referencingExpressionsIterator.next());
            while (referencingExpressionsIterator.hasNext()) {
                commonReferencingExpressions.retainAll(referencingExpressionsIterator.next());
            }

            // For each reference that has the right topology (that is, it points to all the children),
            // limit the candidates to those that actually contain all the variations in its memo
            final List<Reference> existingRefs = commonReferencingExpressions.stream()
                    .map(expressionToReferenceMap::get)
                    .filter(ref -> ref.containsAllInMemo(expressions, AliasMap.emptyMap(), false))
                    .collect(ImmutableList.toImmutableList());

            // If we found such a reference, re-use it
            if (!existingRefs.isEmpty()) {
                Reference existingReference = existingRefs.get(0);
                PlannerEventListeners.withListeners(() ->
                        expressions.forEach(expr ->
                            PlannerEventListeners.dispatchEvent(() -> InsertIntoMemoPlannerEvent.reusedExpWithReferences(expr, existingRefs))));
                Verify.verify(existingReference != this.root);
                return existingReference;
            }

            // If we didn't find one, create a new reference and add it to the memo
            return addNewReference(Reference.ofExploratoryExpressions(plannerPhase.getTargetPlannerStage(), expressions));
        } finally {
            PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::end);
        }
    }

    // Only look for references that are: (1) in the right planner stage and (2) have appropriate correlations.
    // It's important that there aren't any extra correlations as if we choose to re-use a reference that
    // contains a correlation that the original set of expressions don't have, then we might try to use it
    // in a place that we are not allowed to
    private boolean isEligibleForReuse(Set<CorrelationIdentifier> requiredCorrelations,
                                       Traversal.ReferencePath path) {
        return path.getReference().getPlannerStage() == plannerPhase.getTargetPlannerStage() && path.getReference().getCorrelatedTo().equals(requiredCorrelations);
    }

    private Reference memoizeLeafExpressions(final Collection<? extends RelationalExpression> expressions) {
        PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::begin);
        try {
            Preconditions.checkArgument(expressions.stream()
                    .allMatch(expression -> !(expression instanceof RecordQueryPlan) && expression.getQuantifiers().isEmpty()));

            final Set<CorrelationIdentifier> requiredCorrelations = correlatedTo(expressions);
            final var leafRefs = traversal.getLeafReferences();

            for (final var leafRef : leafRefs) {
                if (leafRef.getPlannerStage() != plannerPhase.getTargetPlannerStage() || !requiredCorrelations.equals(leafRef.getCorrelatedTo())) {
                    continue;
                }
                if (leafRef.containsAllInMemo(expressions, AliasMap.emptyMap(), false)) {
                    PlannerEventListeners.withListeners(() ->
                            expressions.forEach(expression ->
                                PlannerEventListeners.dispatchEvent(() -> InsertIntoMemoPlannerEvent.reusedExp(expression))));
                    return leafRef;
                }
            }

            return addNewReference(Reference.ofExploratoryExpressions(plannerPhase.getTargetPlannerStage(), expressions));
        } finally {
            PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::end);
        }
    }

    private Set<CorrelationIdentifier> correlatedTo(Collection<? extends RelationalExpression> expressions) {
        if (expressions.isEmpty()) {
            return ImmutableSet.of();
        } else if (expressions.size() == 1) {
            return Iterables.getOnlyElement(expressions).getCorrelatedTo();
        } else {
            // Note: this makes a copy, whereas an approach based on using Sets.union wouldn't need to.
            // However, we expect there to be a lot of duplicate values amongst the different expressions
            // here, and we don't want to have to re-do the de-duplication checks like we would with
            // an approach that uses Sets.union. So we just make a copy here
            return expressions.stream()
                    .map(Correlated::getCorrelatedTo)
                    .flatMap(Collection::stream)
                    .collect(ImmutableSet.toImmutableSet());
        }
    }

    @Override
    public Reference memoizeFinalExpressionsFromOther(final Reference reference,
                                                      final Collection<? extends RelationalExpression> expressions) {
        return memoizeExpressionsExactly(ImmutableList.of(), expressions,
                (ignored, finalExpressions) -> reference.newReferenceFromFinalMembers(finalExpressions));
    }

    @Override
    public Reference memoizeFinalExpression(final RelationalExpression expression) {
        return memoizeFinalExpressions(ImmutableList.of(expression));
    }

    @Override
    public Reference memoizeFinalExpressions(final Collection<RelationalExpression> expressions) {
        return memoizeExpressionsExactly(ImmutableList.of(), expressions,
                (ignored, finalExpressions) ->
                        Reference.ofFinalExpressions(getPlannerPhase().getTargetPlannerStage(), finalExpressions));
    }

    @Override
    public Reference memoizeUnknownExpression(final RelationalExpression expression) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        if (expression instanceof RecordQueryPlan) {
            return memoizePlan((RecordQueryPlan)expression);
        }
        return memoizeExploratoryExpression(expression);
    }

    @Override
    public Reference memoizeMemberPlansFromOther(final Reference reference,
                                                 final Collection<? extends RecordQueryPlan> plans) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        return memoizeExpressionsExactly(ImmutableList.of(), plans,
                (ignored, finalExpressions) -> reference.newReferenceFromFinalMembers(finalExpressions));
    }

    @Override
    public Reference memoizePlan(final RecordQueryPlan plan) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        return memoizeFinalExpression(plan);
    }

    private Reference memoizeExpressionsExactly(final Collection<? extends RelationalExpression> exploratoryExpressions,
                                                final Collection<? extends RelationalExpression> finalExpressions,
                                                BiFunction<Set<? extends RelationalExpression>, Set<? extends RelationalExpression>, Reference> referenceCreator) {
        final var allExpressions =
                Iterables.concat(exploratoryExpressions, finalExpressions);
        PlannerEventListeners.withListeners(() ->
                allExpressions.forEach(expression -> PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::begin)));
        try {
            final var exploratoryExpressionSet = new LinkedIdentitySet<>(exploratoryExpressions);
            final var finalExpressionSet = new LinkedIdentitySet<>(finalExpressions);
            return addNewReference(referenceCreator.apply(exploratoryExpressionSet, finalExpressionSet));
        } finally {
            PlannerEventListeners.withListeners(() ->
                    allExpressions.forEach(expression -> PlannerEventListeners.dispatchEvent(InsertIntoMemoPlannerEvent::end)));
        }
    }

    @Override
    public ReferenceBuilder memoizeExploratoryExpressionBuilder(final RelationalExpression expression) {
        return new ReferenceBuilder() {
            @Override
            public Reference reference() {
                return memoizeExploratoryExpression(expression);
            }

            @Override
            public Set<? extends RelationalExpression> members() {
                return LinkedIdentitySet.of(expression);
            }
        };
    }

    @Override
    public ReferenceBuilder memoizeFinalExpressionsBuilder(final Collection<? extends RelationalExpression> expressions) {
        return memoizeFinalExpressionsBuilder(expressions,
                e -> Reference.ofFinalExpressions(getPlannerPhase().getTargetPlannerStage(), e));
    }

    private ReferenceBuilder memoizeFinalExpressionsBuilder(final Collection<? extends RelationalExpression> expressions,
                                                            final Function<Set<? extends RelationalExpression>, Reference> refAction) {
        final var finalExpressionsSet = new LinkedIdentitySet<>(expressions);
        return new ReferenceBuilder() {
            @Override
            public Reference reference() {
                return memoizeExpressionsExactly(ImmutableList.of(),
                        expressions,
                        (ignored, finalExpressions) -> refAction.apply(finalExpressions));
            }

            @Override
            public Set<? extends RelationalExpression> members() {
                return finalExpressionsSet;
            }
        };
    }

    @Override
    public ReferenceOfPlansBuilder memoizeMemberPlansBuilder(final Reference reference,
                                                             final Collection<? extends RecordQueryPlan> plans) {
        return memoizePlansBuilder(plans, reference::newReferenceFromFinalMembers);
    }

    @Override
    public ReferenceOfPlansBuilder memoizePlansBuilder(final Collection<? extends RecordQueryPlan> plans) {
        return memoizePlansBuilder(plans,
                expressions ->
                        Reference.ofFinalExpressions(getPlannerPhase().getTargetPlannerStage(), expressions));
    }

    private ReferenceOfPlansBuilder memoizePlansBuilder(final Collection<? extends RecordQueryPlan> plans,
                                                        final Function<Set<? extends RelationalExpression>, Reference> refAction) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        final var plansSet = new LinkedIdentitySet<>(plans);
        return new ReferenceOfPlansBuilder() {
            @Override
            public Reference reference() {
                return memoizeExpressionsExactly(ImmutableList.of(),
                        plans,
                        (ignored, finalExpressions) -> refAction.apply(finalExpressions));
            }

            @Override
            public Set<? extends RecordQueryPlan> members() {
                return plansSet;
            }
        };
    }
}
