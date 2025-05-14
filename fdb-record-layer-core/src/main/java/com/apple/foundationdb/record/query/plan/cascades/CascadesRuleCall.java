/*
 * CascadesRuleCall.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.InsertIntoMemoEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * A rule call implementation for the {@link CascadesPlanner}. The life cycle of a rule call object starts with the
 * planner's decision to call a rule and ends after yielded objects have been processed. The planner instantiates a new
 * instance of this class and passes it to {@link CascadesRule#onMatch(CascadesRuleCall)} when the rule is executed.
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
    @Nonnull
    private final PlannerPhase plannerPhase;
    @Nonnull
    private final CascadesRule<?> rule;
    @Nonnull
    private final Reference root;
    @Nonnull
    private final Traversal traversal;
    @Nonnull
    private final Deque<CascadesPlanner.Task> taskStack;
    @Nonnull
    private final PlannerBindings bindings;
    @Nonnull
    private final PlanContext context;
    @Nonnull
    private final LinkedIdentitySet<RelationalExpression> newExploratoryExpressions;
    @Nonnull
    private final LinkedIdentitySet<RelationalExpression> newFinalExpressions;
    @Nonnull
    private final LinkedIdentitySet<PartialMatch> newPartialMatches;
    @Nonnull
    private final Set<Reference> referencesWithPushedConstraints;
    @Nonnull
    private final EvaluationContext evaluationContext;

    public CascadesRuleCall(@Nonnull final PlannerPhase plannerPhase,
                            @Nonnull final PlanContext context,
                            @Nonnull final CascadesRule<?> rule,
                            @Nonnull final Reference root,
                            @Nonnull final Traversal traversal,
                            @Nonnull final Deque<CascadesPlanner.Task> taskStack,
                            @Nonnull final PlannerBindings bindings,
                            @Nonnull final EvaluationContext evaluationContext) {
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
        this.referencesWithPushedConstraints = Sets.newLinkedHashSet();
        this.evaluationContext = evaluationContext;
    }

    public void run() {
        rule.onMatch(this);
    }

    @Nonnull
    @Override
    public PlannerPhase getPlannerPhase() {
        return plannerPhase;
    }

    @Nonnull
    @Override
    public Reference getRoot() {
        return root;
    }

    @Nonnull
    @Override
    public AliasResolver newAliasResolver() {
        return new AliasResolver(traversal);
    }

    @Nonnull
    @Override
    public PlanContext getContext() {
        return context;
    }

    @Override
    @Nonnull
    public PlannerBindings getBindings() {
        return bindings;
    }

    @Nonnull
    @Override
    public <T> Optional<T> getPlannerConstraintMaybe(@Nonnull final PlannerConstraint<T> plannerConstraint) {
        if (rule.getConstraintDependencies().contains(plannerConstraint)) {
            return root.getConstraintsMap().getConstraintOptional(plannerConstraint);
        }

        throw new RecordCoreArgumentException("rule is not dependent on requested planner requirement");
    }

    @Override
    @SuppressWarnings({"PMD.CompareObjectsWithEquals"}) // deliberate use of id equality check for short-circuit condition
    public <T> void pushConstraint(@Nonnull final Reference reference,
                                   @Nonnull final PlannerConstraint<T> plannerConstraint,
                                   @Nonnull final T constraintValue) {
        Verify.verify(root != reference);
        final ConstraintsMap requirementsMap = reference.getConstraintsMap();
        if (requirementsMap.pushProperty(plannerConstraint, constraintValue).isPresent()) {
            referencesWithPushedConstraints.add(reference);
        }
    }

    @Override
    public void emitEvent(@Nonnull final Debugger.Location location) {
        Verify.verify(location != Debugger.Location.BEGIN && location != Debugger.Location.END);
        Debugger.withDebugger(debugger ->
                debugger.onEvent(
                        new Debugger.TransformRuleCallEvent(plannerPhase, root, taskStack, location, root,
                                bindings.get(rule.getMatcher()), rule, this)));
    }

    @Override
    public void yieldExploratoryExpression(@Nonnull final RelationalExpression expression) {
        yieldExpression(expression, false);
    }

    @Override
    public void yieldPlan(@Nonnull final RecordQueryPlan plan) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        yieldFinalExpression(plan);
    }

    @Override
    public void yieldFinalExpression(@Nonnull final RelationalExpression expression) {
        yieldExpression(expression, true);
    }

    @Override
    public void yieldUnknownExpression(@Nonnull final RelationalExpression expression) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        if (expression instanceof RecordQueryPlan) {
            yieldPlan((RecordQueryPlan)expression);
        } else {
            yieldExploratoryExpression(expression);
        }
    }

    private void yieldExpression(@Nonnull final RelationalExpression expression, final boolean isFinal) {
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

    private void verifyChildrenMemoized(@Nonnull RelationalExpression expression) {
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
    public void yieldPartialMatch(@Nonnull final AliasMap boundAliasMap,
                                  @Nonnull final MatchCandidate matchCandidate,
                                  @Nonnull final RelationalExpression queryExpression,
                                  @Nonnull final Reference candidateRef,
                                  @Nonnull final MatchInfo matchInfo) {
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

    @Nonnull
    public Collection<RelationalExpression> getNewExploratoryExpressions() {
        return Collections.unmodifiableCollection(newExploratoryExpressions);
    }

    @Nonnull
    public Collection<RelationalExpression> getNewFinalExpressions() {
        return Collections.unmodifiableCollection(newFinalExpressions);
    }

    @Nonnull
    public Set<PartialMatch> getNewPartialMatches() {
        return newPartialMatches;
    }

    @Nonnull
    public Set<Reference> getReferencesWithPushedConstraints() {
        return referencesWithPushedConstraints;
    }

    @Nonnull
    public EvaluationContext getEvaluationContext() {
        return evaluationContext;
    }

    @Nonnull
    @Override
    public Reference memoizeExploratoryExpression(@Nonnull final RelationalExpression expression) {
        return memoizeExploratoryExpressions(ImmutableSet.of(expression));
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Reference memoizeExploratoryExpressions(@Nonnull final Collection<? extends RelationalExpression> expressions) {
        Preconditions.checkArgument(!expressions.isEmpty(), "Cannot create reference over empty expression collection");
        if (expressions.stream().allMatch(expression -> expression.getQuantifiers().isEmpty())) {
            return memoizeLeafExpressions(expressions);
        }
        Debugger.withDebugger(debugger -> debugger.onEvent(InsertIntoMemoEvent.begin()));
        try {
            Preconditions.checkArgument(expressions.stream().noneMatch(expression -> expression instanceof RecordQueryPlan));

            final var referencePathsList =
                    expressions.stream()
                            .flatMap(expression -> expression.getQuantifiers().stream())
                            .map(Quantifier::getRangesOver)
                            .map(traversal::getParentRefPaths)
                            .collect(ImmutableList.toImmutableList());

            final var expressionToReferenceMap = new LinkedIdentityMap<RelationalExpression, Reference>();
            referencePathsList.stream()
                    .flatMap(Collection::stream)
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

            final var referencingExpressions =
                    referencePathsList.stream()
                            .map(referencePaths ->
                                    referencePaths.stream()
                                            .map(Traversal.ReferencePath::getExpression)
                                            .collect(LinkedIdentitySet.toLinkedIdentitySet()))
                            .collect(ImmutableList.toImmutableList());

            final var referencingExpressionsIterator = referencingExpressions.iterator();
            final var commonReferencingExpressions = new LinkedIdentitySet<>(referencingExpressionsIterator.next());
            while (referencingExpressionsIterator.hasNext()) {
                commonReferencingExpressions.retainAll(referencingExpressionsIterator.next());
            }

            final RelationalExpression baseExpression = Iterables.getFirst(expressions, null);
            Verify.verify(baseExpression != null);

            commonReferencingExpressions.removeIf(commonReferencingExpression ->
                    !Reference.isMemoizedExpression(baseExpression, commonReferencingExpression));

            final List<Reference> commonReferences = commonReferencingExpressions.stream()
                    .map(expressionToReferenceMap::get)
                    .filter(ref -> ref.containsAllInMemo(expressions, AliasMap.emptyMap(), false))
                    .collect(ImmutableList.toImmutableList());
            final var existingReference = Iterables.getFirst(commonReferences, null);
            if (existingReference != null) {
                for (RelationalExpression expression : expressions) {
                    Debugger.withDebugger(debugger ->
                            debugger.onEvent(InsertIntoMemoEvent.reusedExpWithReferences(expression,
                                    commonReferences)));
                }
                Verify.verify(existingReference != this.root);
                return existingReference;
            }

            final var newRef = Reference.ofExploratoryExpressions(plannerPhase.getTargetPlannerStage(), expressions);
            for (RelationalExpression expression : expressions) {
                Debugger.withDebugger(debugger -> debugger.onEvent(InsertIntoMemoEvent.newExp(expression)));
                traversal.addExpression(newRef, expression);
            }
            return newRef;
        } finally {
            Debugger.withDebugger(debugger -> debugger.onEvent(InsertIntoMemoEvent.end()));
        }
    }

    @Nonnull
    private Reference memoizeLeafExpressions(@Nonnull final Collection<? extends RelationalExpression> expressions) {
        Debugger.withDebugger(debugger -> debugger.onEvent(InsertIntoMemoEvent.begin()));
        try {
            Preconditions.checkArgument(expressions.stream()
                    .allMatch(expression -> !(expression instanceof RecordQueryPlan) && expression.getQuantifiers().isEmpty()));

            final var leafRefs = traversal.getLeafReferences();

            for (final var leafRef : leafRefs) {
                if (leafRef.containsAllInMemo(expressions, AliasMap.emptyMap(), false)) {
                    for (RelationalExpression expression : expressions) {
                        Debugger.withDebugger(debugger -> debugger.onEvent(InsertIntoMemoEvent.reusedExp(expression)));
                    }
                    return leafRef;
                }
            }

            final Reference newRef = Reference.ofExploratoryExpressions(plannerPhase.getTargetPlannerStage(), expressions);
            for (RelationalExpression expression : expressions) {
                Debugger.withDebugger(debugger -> debugger.onEvent(InsertIntoMemoEvent.newExp(expression)));
                traversal.addExpression(newRef, expression);
            }
            return newRef;
        } finally {
            Debugger.withDebugger(debugger -> debugger.onEvent(InsertIntoMemoEvent.end()));
        }
    }

    @Nonnull
    @Override
    public Reference memoizeFinalExpressionsFromOther(@Nonnull final Reference reference,
                                                      @Nonnull final Collection<? extends RelationalExpression> expressions) {
        return memoizeFinalExpressionsExactly(expressions, reference::newReferenceFromFinalMembers);
    }

    @Nonnull
    @Override
    public Reference memoizeFinalExpression(@Nonnull final RelationalExpression expression) {
        return memoizeFinalExpressionsExactly(ImmutableList.of(expression),
                expressions ->
                        Reference.ofFinalExpressions(getPlannerPhase().getTargetPlannerStage(), expressions));
    }

    @Nonnull
    @Override
    public Reference memoizeUnknownExpression(@Nonnull final RelationalExpression expression) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        if (expression instanceof RecordQueryPlan) {
            return memoizePlan((RecordQueryPlan)expression);
        }
        return memoizeExploratoryExpression(expression);
    }

    @Nonnull
    @Override
    public Reference memoizeMemberPlansFromOther(@Nonnull final Reference reference,
                                                 @Nonnull final Collection<? extends RecordQueryPlan> plans) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        return memoizeFinalExpressionsExactly(plans, reference::newReferenceFromFinalMembers);
    }

    @Nonnull
    @Override
    public Reference memoizePlan(@Nonnull final RecordQueryPlan plan) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        return memoizeFinalExpression(plan);
    }

    @Nonnull
    private Reference memoizeFinalExpressionsExactly(@Nonnull final Collection<? extends RelationalExpression> expressions,
                                                     @Nonnull Function<Set<? extends RelationalExpression>, Reference> referenceCreator) {
        Debugger.withDebugger(debugger -> expressions.forEach(
                expression -> debugger.onEvent(InsertIntoMemoEvent.begin())));
        try {
            final var expressionSet = new LinkedIdentitySet<>(expressions);
            final var newRef = referenceCreator.apply(expressionSet);
            for (final var plan : expressionSet) {
                Debugger.withDebugger(debugger -> expressions.forEach(
                        expression -> debugger.onEvent(InsertIntoMemoEvent.newExp(expression))));
                traversal.addExpression(newRef, plan);
            }
            return newRef;
        } finally {
            Debugger.withDebugger(debugger -> expressions.forEach(
                    expression -> debugger.onEvent(InsertIntoMemoEvent.end())));
        }
    }

    @Nonnull
    @Override
    public ReferenceBuilder memoizeExploratoryExpressionBuilder(@Nonnull final RelationalExpression expression) {
        return new ReferenceBuilder() {
            @Nonnull
            @Override
            public Reference reference() {
                return memoizeExploratoryExpression(expression);
            }

            @Nonnull
            @Override
            public Set<? extends RelationalExpression> members() {
                return LinkedIdentitySet.of(expression);
            }
        };
    }

    @Nonnull
    @Override
    public ReferenceBuilder memoizeFinalExpressionsBuilder(@Nonnull final Collection<? extends RelationalExpression> expressions) {
        return memoizeFinalExpressionsBuilder(expressions,
                e -> Reference.ofFinalExpressions(getPlannerPhase().getTargetPlannerStage(), e));
    }

    @Nonnull
    private ReferenceBuilder memoizeFinalExpressionsBuilder(@Nonnull final Collection<? extends RelationalExpression> expressions,
                                                            @Nonnull final Function<Set<? extends RelationalExpression>, Reference> refAction) {
        final var expressionSet = new LinkedIdentitySet<>(expressions);
        return new ReferenceBuilder() {
            @Nonnull
            @Override
            public Reference reference() {
                return memoizeFinalExpressionsExactly(expressions, refAction);
            }

            @Nonnull
            @Override
            public Set<? extends RelationalExpression> members() {
                return expressionSet;
            }
        };
    }

    @Nonnull
    @Override
    public ReferenceOfPlansBuilder memoizeMemberPlansBuilder(@Nonnull final Reference reference,
                                                             @Nonnull final Collection<? extends RecordQueryPlan> plans) {
        return memoizePlansBuilder(plans, reference::newReferenceFromFinalMembers);
    }

    @Nonnull
    @Override
    public ReferenceOfPlansBuilder memoizePlansBuilder(@Nonnull final Collection<? extends RecordQueryPlan> plans) {
        return memoizePlansBuilder(plans,
                expressions ->
                        Reference.ofFinalExpressions(getPlannerPhase().getTargetPlannerStage(), expressions));
    }

    @Nonnull
    private ReferenceOfPlansBuilder memoizePlansBuilder(@Nonnull final Collection<? extends RecordQueryPlan> plans,
                                                        @Nonnull final Function<Set<? extends RelationalExpression>, Reference> refAction) {
        Verify.verify(getPlannerPhase() == PlannerPhase.PLANNING);
        final var expressionSet = new LinkedIdentitySet<>(plans);
        return new ReferenceOfPlansBuilder() {
            @Nonnull
            @Override
            public Reference reference() {
                return memoizeFinalExpressionsExactly(plans, refAction);
            }

            @Nonnull
            @Override
            public Set<? extends RecordQueryPlan> members() {
                return expressionSet;
            }
        };
    }
}
