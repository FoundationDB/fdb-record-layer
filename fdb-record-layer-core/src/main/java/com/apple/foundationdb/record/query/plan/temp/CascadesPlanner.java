/*
 * CascadesPlanner.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.Location;
import com.apple.foundationdb.record.query.plan.temp.debug.RestartException;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.temp.matching.BoundMatch;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A Cascades-style query planner that converts a {@link RecordQuery} to a {@link RecordQueryPlan}, possibly using
 * secondary indexes defined in a {@link RecordMetaData} to execute the query efficiently.
 *
 * <p>
 * <a href="https://15721.courses.cs.cmu.edu/spring2017/papers/15-optimizer2/graefe-ieee1995.pdf">Cascades</a> is a
 * framework for a query optimization introduced by Graefe in 1995. In Cascades, all parsed queries, query plans, and
 * intermediate state between the two are represented in a unified tree of {@link RelationalExpression}, which includes
 * types such as {@link RecordQueryPlan} and {@link com.apple.foundationdb.record.query.expressions.QueryComponent}.
 * This highly flexible data structure reifies essentially the entire state of the planner (i.e., partially planned
 * elements, current optimization, goals, etc.) and allows individual planning steps to be modular and stateless by
 * keeping all state in the {@link RelationalExpression} tree.
 * </p>
 *
 * <p>
 * Like many optimization frameworks, Cascades is driven by a set of {@link PlannerRule}s, each of which describes a
 * particular transformation and encapsulates the logic for determining its applicability and applying it. The planner
 * searches through its {@link PlannerRuleSet} to find a matching rule and then executes that rule, creating zero or
 * more additional {@code PlannerExpression}s. A rule is defined by:
 * </p>
 * <ul>
 *     <li>
 *         An {@link com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher} that defines a
 *         finite-depth tree of matchers that inspect the structure (i.e., the type-level information) of some subgraph
 *         of the current planner expression.
 *     </li>
 *     <li>
 *         A {@link PlannerRule#onMatch(PlannerRuleCall)} method that is run for each successful match, producing zero
 *         or more new expressions.
 *     </li>
 * </ul>
 *
 * <p>
 * Since rules can be applied speculatively and need not be "reductive" in any reasonable sense, it is common for cyclic
 * rule application to occur. Furthermore, the number of possible expression trees being considered at any time can be
 * enormous, since every rule might apply to many of the existing trees under consideration by the planner. To mitigate
 * this, Cascades uses aggressive memoization, which is represented by the <em>memo</em> data structure. The memo
 * provides an efficient interface for storing a forest of expressions, where there might be substantial overlap between
 * different trees in the forest. The memo is composed of expression groups (or just <em>groups</em>), which are
 * equivalence classes of expressions. In this implementation, the memo structure is an implicit data structure
 * represented by {@link GroupExpressionRef}s, each of which represents a group expression in Cascades and contains
 * a set of {@link RelationalExpression}s. In turn, {@link RelationalExpression}s have some number of <em>children</em>, each
 * of which is a {@link GroupExpressionRef} and which can be traversed by the planner via the
 * {@link RelationalExpression#getQuantifiers()} method.
 * </p>
 *
 * <p>
 * A Cascades planner operates by repeatedly executing a {@link Task} from the task execution stack (in this case),
 * which performs some actions and may schedule other tasks by pushing them onto the stack. The tasks in this particular
 * planner are the implementors of the {@link Task} interface.
 * </p>
 *
 * <p>
 * Since a Cascades-style planner produces many possible query plans, it needs some way to decide which ones to select.
 * This is generally done with a cost model that scores plans according to some cost metric. For now, we use the
 * {@link CascadesCostModel} which is a heuristic model implemented as a {@link java.util.Comparator}.
 * </p>
 *
 * @see GroupExpressionRef
 * @see RelationalExpression
 * @see PlannerRule
 * @see CascadesCostModel
 */
@API(API.Status.EXPERIMENTAL)
public class CascadesPlanner implements QueryPlanner {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(CascadesPlanner.class);
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final RecordStoreState recordStoreState;
    @Nonnull
    private final PlannerRuleSet ruleSet;
    @Nonnull
    private GroupExpressionRef<RelationalExpression> currentRoot;
    @Nonnull
    private Deque<Task> taskStack; // Use a Dequeue instead of a Stack because we don't need synchronization.

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState) {
        this(metaData, recordStoreState, PlannerRuleSet.ALL);
    }

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState, @Nonnull PlannerRuleSet ruleSet) {
        this.metaData = metaData;
        this.recordStoreState = recordStoreState;
        this.ruleSet = ruleSet;
        // Placeholders until we get a query.
        this.currentRoot = GroupExpressionRef.empty();
        this.taskStack = new ArrayDeque<>();
    }

    @Nonnull
    @Override
    public RecordQueryPlan plan(@Nonnull RecordQuery query) {
        final PlanContext context = new MetaDataPlanContext(metaData, recordStoreState, query, ImmutableSet.of());
        Debugger.query(query, context);
        try {
            planPartial(context, () -> RelationalExpression.fromRecordQuery(query, context));
        } finally {
            Debugger.withDebugger(Debugger::onDone);
        }

        final RelationalExpression singleRoot = currentRoot.getMembers().iterator().next();
        if (singleRoot instanceof RecordQueryPlan) {
            if (logger.isDebugEnabled()) {
                logger.debug(KeyValueLogMessage.of("explain of plan",
                        "explain", PlannerGraphProperty.explain(singleRoot)));
            }

            return (RecordQueryPlan)singleRoot;
        } else {
            throw new RecordCoreException("Cascades planner could not plan query")
                    .addLogInfo("query", query)
                    .addLogInfo("finalExpression", currentRoot.get());
        }
    }

    @VisibleForTesting
    @Nonnull
    public GroupExpressionRef<RelationalExpression> planPartial(@Nonnull PlanContext context, @Nonnull Supplier<RelationalExpression> expressionSupplier) {
        currentRoot = GroupExpressionRef.of(expressionSupplier.get());
        taskStack = new ArrayDeque<>();
        taskStack.push(new OptimizeGroup(context, currentRoot));
        while (!taskStack.isEmpty()) {
            try {
                Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.ExecutingTaskEvent(currentRoot, taskStack, Objects.requireNonNull(taskStack.peek()))));
                Task nextTask = taskStack.pop();
                if (logger.isTraceEnabled()) {
                    logger.trace(KeyValueLogMessage.of("executing task", "nextTask", nextTask.toString()));
                }

                Debugger.withDebugger(debugger -> debugger.onEvent(nextTask.toTaskEvent(Location.BEGIN)));
                nextTask.execute();
                Debugger.withDebugger(debugger -> debugger.onEvent(nextTask.toTaskEvent(Location.END)));

                if (logger.isTraceEnabled()) {
                    logger.trace(KeyValueLogMessage.of("planner state",
                            "taskStackSize", taskStack.size(),
                            "memo", new GroupExpressionPrinter(currentRoot)));
                }
            } catch (final RestartException restartException) {
                if (logger.isTraceEnabled()) {
                    logger.trace(KeyValueLogMessage.of("debugger requests restart of planning",
                            "taskStackSize", taskStack.size(),
                            "memo", new GroupExpressionPrinter(currentRoot)));
                }
                taskStack.clear();
                currentRoot = GroupExpressionRef.of(expressionSupplier.get());
                taskStack.push(new OptimizeGroup(context, currentRoot));
            }
        }
        return currentRoot;
    }

    @Override
    public void setIndexScanPreference(@Nonnull IndexScanPreference indexScanPreference) {
        // nothing to do here, yet
    }

    /**
     * Represents actual tasks in the task stack of the planner.
     */
    public interface Task {
        void execute();

        Debugger.Event toTaskEvent(final Location location);
    }

    private class OptimizeGroup implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<RelationalExpression> group;

        @SuppressWarnings("unchecked")
        public OptimizeGroup(@Nonnull PlanContext context, @Nonnull ExpressionRef<? extends RelationalExpression> ref) {
            this.context = context;
            if (ref instanceof GroupExpressionRef) {
                this.group = (GroupExpressionRef<RelationalExpression>) ref;
            } else {
                throw new RecordCoreArgumentException("illegal non-group reference in group expression");
            }
        }

        public OptimizeGroup(@Nonnull PlanContext context, @Nonnull GroupExpressionRef<RelationalExpression> group) {
            this.context = context;
            this.group = group;
        }

        @Override
        public void execute() {
            if (!group.isExplored()) {
                // Explore the group, then come back here to pick an optimal expression.
                taskStack.push(this);
                for (RelationalExpression member : group.getMembers()) {
                    taskStack.push(new ExploreExpression(context, group, member));
                }
                group.setExplored();
            } else {
                // TODO this is very Volcano-style rather than Cascades, because there's no branch-and-bound pruning.
                RelationalExpression bestMember = null;
                for (RelationalExpression member : group.getMembers()) {
                    if (bestMember == null || new CascadesCostModel(context).compare(member, bestMember) < 0) {
                        bestMember = member;
                    }
                }
                if (bestMember == null) {
                    throw new RecordCoreException("there we no members in a group expression used by the Cascades planner");
                }
                group.clear();
                group.insert(bestMember);
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.OptimizeGroupEvent(currentRoot, taskStack, location, group);
        }

        @Override
        public String toString() {
            return "OptimizeGroup(" + group + ")";
        }
    }

    private class ExploreExpression implements Task {
        @Nonnull
        protected final PlanContext context;
        @Nonnull
        protected final GroupExpressionRef<RelationalExpression> group;
        @Nonnull
        protected final RelationalExpression expression;

        public ExploreExpression(@Nonnull PlanContext context,
                                 @Nonnull GroupExpressionRef<RelationalExpression> group,
                                 @Nonnull RelationalExpression expression) {
            this.context = context;
            this.group = group;
            this.expression = expression;
        }

        @Nonnull
        protected PlannerRuleSet getRules() {
            return ruleSet;
        }

        protected void addTransformTask(@Nonnull PlannerRule<? extends RelationalExpression> rule) {
            taskStack.push(new Transform(context, group, expression, rule));
        }

        @Override
        public void execute() {
            // invoke matching after all transformations have been applied and the groups underneath have been explored
            taskStack.push(new MatchExpression(context, group, expression));

            // This is closely tied to the way that rule finding works _now_. Specifically, rules are indexed only
            // by the type of their _root_, not any of the stuff lower down. As a result, we have enough information
            // right here to determine the set of all possible rules that could ever be applied here, regardless of
            // what happens towards the leaves of the tree.
            getRules().getRulesMatching(expression).forEachRemaining(this::addTransformTask);

            for (final Quantifier quantifier : expression.getQuantifiers()) {
                final ExpressionRef<? extends RelationalExpression> rangesOver = quantifier.getRangesOver();
                taskStack.push(new ExploreGroup(context, rangesOver));
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.ExploreExpressionEvent(currentRoot, taskStack, location, group, expression);
        }

        @Override
        public String toString() {
            return "ExploreExpression(" + group + ")";
        }
    }

    private class ExploreGroup implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<RelationalExpression> group;

        @SuppressWarnings("unchecked")
        public ExploreGroup(@Nonnull PlanContext context, @Nonnull ExpressionRef<? extends RelationalExpression> ref) {
            this.context = context;
            if (ref instanceof GroupExpressionRef) {
                this.group = (GroupExpressionRef<RelationalExpression>) ref;
            } else {
                throw new RecordCoreArgumentException("illegal non-group reference in group expression");
            }
        }

        @Override
        public void execute() {
            if (group.isExplored()) {
                return;
            }

            for (final RelationalExpression expression : group.getMembers()) {
                taskStack.push(new ExploreExpression(context, group, expression));
            }

            // we'll never need to reschedule this, so we don't need to wait until the exploration is actually done.
            group.setExplored();
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.ExploreGroupEvent(currentRoot, taskStack, location, group);
        }

        @Override
        public String toString() {
            return "ExploreGroup(" + group + ")";
        }
    }

    private class Transform implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<RelationalExpression> group;
        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final PlannerRule<? extends RelationalExpression> rule;

        public Transform(@Nonnull PlanContext context,
                         @Nonnull GroupExpressionRef<RelationalExpression> group,
                         @Nonnull RelationalExpression expression,
                         @Nonnull PlannerRule<? extends RelationalExpression> rule) {
            this.context = context;
            this.group = group;
            this.expression = expression;
            this.rule = rule;
        }

        @Override
        public void execute() {
            if (!group.containsExactly(expression)) { // expression is gone
                return;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Bindings: " +  expression.bindTo(rule.getMatcher()).count());
            }
            expression.bindTo(rule.getMatcher()).map(bindings -> new CascadesRuleCall(context, rule, group, bindings))
                    .forEach(ruleCall -> {
                        Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.TransformRuleCallEvent(currentRoot, taskStack, Location.BEGIN, group, expression, rule, ruleCall)));
                        executeRuleCall(ruleCall);
                        Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.TransformRuleCallEvent(currentRoot, taskStack, Location.END, group, expression, rule, ruleCall)));
                    });
        }

        private void executeRuleCall(@Nonnull CascadesRuleCall ruleCall) {
            ruleCall.run();
            for (RelationalExpression newExpression : ruleCall.getNewExpressions()) {
                if (newExpression instanceof QueryPlan) {
                    taskStack.push(new OptimizeInputs(context, group, newExpression));
                    taskStack.push(new ExploreExpression(context, group, newExpression));
                } else {
                    taskStack.push(new ExploreExpression(context, group, newExpression));
                }
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.TransformEvent(currentRoot, taskStack, location, group, expression, rule);
        }

        @Override
        public String toString() {
            return "Transform(" + rule.getClass().getSimpleName() + ")";
        }
    }

    private class MatchExpression implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<RelationalExpression> group;
        @Nonnull
        private final RelationalExpression expression;

        public MatchExpression(@Nonnull final PlanContext context, @Nonnull final GroupExpressionRef<RelationalExpression> group, @Nonnull final RelationalExpression expression) {
            this.context = context;
            this.group = group;
            this.expression = expression;
        }

        @Override
        public void execute() {
            final ImmutableList<? extends ExpressionRef<? extends RelationalExpression>> rangesOverRefs =
                    expression.getQuantifiers()
                            .stream()
                            .map(Quantifier::getRangesOver)
                            .collect(ImmutableList.toImmutableList());

            if (rangesOverRefs.isEmpty()) {
                for (final MatchCandidate matchCandidate : context.getMatchCandidates()) {
                    final ExpressionRefTraversal traversal = matchCandidate.getTraversal();
                    final Set<ExpressionRef<? extends RelationalExpression>> leafRefs = traversal.getLeafRefs();
                    for (final ExpressionRef<? extends RelationalExpression> leafRef : leafRefs) {
                        for (final RelationalExpression leafMember : leafRef.getMembers()) {
                            if (leafMember.getQuantifiers().isEmpty()) {
                                taskStack.push(new MatchExpressionWithCandidate(context,
                                        group,
                                        expression,
                                        matchCandidate,
                                        leafRef,
                                        leafMember));
                            }
                        }
                    }
                }
            } else {
                // form intersection of all possible match candidates
                final ExpressionRef<? extends RelationalExpression> firstRangesOverRef = rangesOverRefs.get(0);
                final Set<MatchCandidate> commonMatchCandidates = Sets.newHashSet(firstRangesOverRef.getMatchCandidates());
                for (int i = 0; i < rangesOverRefs.size(); i++) {
                    final ExpressionRef<? extends RelationalExpression> rangesOverGroup = rangesOverRefs.get(i);
                    commonMatchCandidates.retainAll(rangesOverGroup.getMatchCandidates());
                }

                for (final MatchCandidate matchCandidate : commonMatchCandidates) {
                    final ExpressionRefTraversal traversal = matchCandidate.getTraversal();
                    for (final ExpressionRef<? extends RelationalExpression> rangesOverRef : rangesOverRefs) {
                        final Set<PartialMatch> partialMatchesForCandidate = rangesOverRef.getPartialMatchesForCandidate(matchCandidate);
                        for (final PartialMatch partialMatch : partialMatchesForCandidate) {
                            for (final ExpressionRefTraversal.RefPath parentRefPath : traversal.getParentRefPaths(partialMatch.getCandidateRef())) {
                                taskStack.push(new MatchExpressionWithCandidate(context,
                                        group,
                                        expression,
                                        matchCandidate,
                                        parentRefPath.getRef(),
                                        parentRefPath.getExpression()));
                            }
                        }
                    }
                }
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.MatchExpressionEvent(currentRoot, taskStack, location, group, expression);
        }

        @Override
        public String toString() {
            return "MatchExpression(" + group + "; " + expression + ")";
        }
    }

    private class MatchExpressionWithCandidate implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<RelationalExpression> group;
        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final MatchCandidate matchCandidate;
        @Nonnull
        private final ExpressionRef<? extends RelationalExpression> candidateRef;
        @Nonnull
        private final RelationalExpression candidateExpression;

        public MatchExpressionWithCandidate(@Nonnull final PlanContext context,
                                            @Nonnull final GroupExpressionRef<RelationalExpression> group,
                                            @Nonnull final RelationalExpression expression,
                                            @Nonnull final MatchCandidate matchCandidate,
                                            @Nonnull final ExpressionRef<? extends RelationalExpression> candidateRef,
                                            @Nonnull final RelationalExpression candidateExpression) {
            this.context = context;
            this.group = group;
            this.expression = expression;
            this.matchCandidate = matchCandidate;
            this.candidateRef = candidateRef;
            this.candidateExpression = candidateExpression;
        }

        @Override
        public void execute() {
            final Iterable<PartialMatch> partialMatches =
                    expression.match(candidateExpression,
                            AliasMap.emptyMap(),
                            this::constraintsForQuantifier,
                            this::matchQuantifiers,
                            this::combineMatches);
            group.addAllPartialMatchesForCandidate(matchCandidate, partialMatches);
        }

        private Collection<AliasMap> constraintsForQuantifier(final Quantifier quantifier) {
            final Set<PartialMatch> partialMatchesForCandidate = quantifier.getRangesOver().getPartialMatchesForCandidate(matchCandidate);
            return partialMatchesForCandidate.stream()
                    .map(PartialMatch::getBoundAliasMap)
                    .collect(ImmutableSet.toImmutableSet());
        }

        private Iterable<PartialMatch> matchQuantifiers(final Quantifier quantifier,
                                                        final Quantifier otherQuantifier,
                                                        final AliasMap aliasMap) {
            final ExpressionRef<? extends RelationalExpression> rangesOver = quantifier.getRangesOver();
            final ExpressionRef<? extends RelationalExpression> otherRangesOver = otherQuantifier.getRangesOver();

            final Set<PartialMatch> partialMatchesForCandidate = rangesOver.getPartialMatchesForCandidate(matchCandidate);
            return partialMatchesForCandidate.stream()
                    .filter(partialMatch -> partialMatch.getCandidateRef() == otherRangesOver && partialMatch.getBoundAliasMap().isCompatible(aliasMap))
                    .collect(Collectors.toList());
        }

        @Nonnull
        private Iterable<PartialMatch> combineMatches(final AliasMap boundCorrelatedToMap,
                                                      final Iterable<BoundMatch<EnumeratingIterable<PartialMatch>>> boundMatches) {
            final Optional<BoundMatch<EnumeratingIterable<PartialMatch>>> anyMatch =
                    StreamSupport.stream(boundMatches.spliterator(), false)
                            // TODO call actual matching function instead of equalsWithoutChildren()
                            .filter(boundMatch -> expression.equalsWithoutChildren(candidateExpression, boundMatch.getAliasMap()))
                            .findAny();
            if (anyMatch.isPresent()) {
                return ImmutableList.of(new PartialMatch(boundCorrelatedToMap, group, candidateRef, ref -> ref));
            }
            return ImmutableList.of();
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.MatchExpressionWithCandidateEvent(currentRoot, taskStack, location, group, expression, matchCandidate, candidateRef, candidateExpression);
        }

        public String show() {
            return PlannerGraphProperty.show(true, currentRoot, ImmutableSet.of(matchCandidate));
        }
    }

    private class OptimizeInputs implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<RelationalExpression> group;
        @Nonnull
        private final RelationalExpression expression;

        public OptimizeInputs(@Nonnull PlanContext context,
                              @Nonnull GroupExpressionRef<RelationalExpression> group,
                              @Nonnull RelationalExpression expression) {
            this.context = context;
            this.group = group;
            this.expression = expression;
        }

        @Override
        public void execute() {
            if (!group.containsExactly(expression)) {
                return;
            }
            for (final Quantifier quantifier : expression.getQuantifiers()) {
                final ExpressionRef<? extends RelationalExpression> rangesOver = quantifier.getRangesOver();
                taskStack.push(new OptimizeGroup(context, rangesOver));
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.OptimizeInputsEvent(currentRoot, taskStack, location, group, expression);
        }

        @Override
        public String toString() {
            return "OptimizeInputs(" + group + ")";
        }
    }
}
