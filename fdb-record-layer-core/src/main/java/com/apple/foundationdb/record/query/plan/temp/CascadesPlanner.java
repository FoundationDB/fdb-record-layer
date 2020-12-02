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
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers.AliasResolver;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.Location;
import com.apple.foundationdb.record.query.plan.temp.debug.RestartException;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.temp.matchers.PartialMatchMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.function.Supplier;

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
 * Like many optimization frameworks, Cascades is driven by sets of {@link PlannerRule}s that can be defined for
 * {@link RelationalExpression}s, {@link PartialMatch}es and {@link MatchPartition}s, each of which describes a
 * particular transformation and encapsulates the logic for determining its applicability and applying it. The planner
 * searches through its {@link PlannerRuleSet} to find a matching rule and then executes that rule, creating zero or
 * more additional {@code PlannerExpression}s and/or zero or mode additional {@link PartialMatch}es. A rule is defined by:
 * </p>
 * <ul>
 *     <li>
 *         An {@link com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher} that defines a
 *         finite-depth tree of matchers that inspect the structure (i.e., the type-level information) of some subgraph
 *         of the current planner expression, the current partial match, or the current match partition.
 *     </li>
 *     <li>
 *         A {@link PlannerRule#onMatch(PlannerRuleCall)} method that is run for each successful match, producing zero
 *         or more new expressions and/or zero or more new partial matches.
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
 * Simplified enqueue/execute overview:
 *
 * {@link OptimizeGroup}
 *     if (not explored)
 *         enqueues
 *             this (again)
 *             {@link ExploreExpression} for each group member
 *         sets explored to {@code true}
 *     else
 *         prune to find best plan; done
 *
 * {@link ExploreGroup}
 *     enqueues
 *         {@link ExploreExpression} for each group member
 *     sets explored to {@code true}
 *
 * {@link ExploreExpression}
 *     enqueues
 *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
 *         all transformations ({@link TransformExpression} for current (group, expression)
 *         {@link ExploreGroup} for all ranged over groups
 *
 * after execution of any TransformXXX
 *     enqueues
 *         {@link AdjustMatch} for each yielded {@link PartialMatch}
 *         {@link OptimizeInputs} followed by {@link ExploreExpression} for each yielded {@link RecordQueryPlan}
 *         {@link ExploreExpression} for each yielded {@link RelationalExpression} that is not a {@link RecordQueryPlan}
 *
 * {@link AdjustMatch}
 *     enqueues
 *         all transformations ({@link TransformPartialMatch}) for current (group, expression, partial match)
 *
 * {@link OptimizeInputs}
 *     enqueues
 *         {@link OptimizeGroup} for all ranged over groups
 *
 * Note: Enqueued tasks are executes in typical stack machine order, that is LIFO.
 *
 * There are three different kinds of transformations:
 * <ul>
 *     <li>
 *         Transforms on expressions {@link TransformExpression}. These are the classical transforms creating new
 *         variations in the expression memoization structure. The root for the corresponding rules is always of type
 *         {@link RelationalExpression}.
 *     </li>
 *     <li>
 *         Transforms on partial matches {@link TransformPartialMatch}. These transforms are executed when a partial
 *         match is found and typically only yield other new partial matches for the <em>current</em> (group, expression)
 *         pair. The root for the corresponding rules is always of type {@link PartialMatch}.
 *     </li>
 *     <li>
 *         Transforms on match partitions {@link TransformMatchPartition}. These transforms are executed only after
 *         all transforms (both {@link TransformExpression}s and {@link TransformPartialMatch}) have been executed
 *         for a current (group, expression). Note, that this kind transformation task can be repeatedly executed for
 *         a given group but it is guaranteed to only be executed once for a (group, expression) pair.
 *         The root for the corresponding rules is always of type {@link MatchPartition}. These are the rules that react
 *         to all synthesized matches for an expression at once.
 *     </li>
 * </ul>
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
    private RecordQueryPlannerConfiguration configuration;
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final RecordStoreState recordStoreState;
    @Nonnull
    private final PlannerRuleSet ruleSet;
    @Nonnull
    private GroupExpressionRef<RelationalExpression> currentRoot;
    @Nonnull
    private AliasResolver aliasResolver;
    @Nonnull
    private Deque<Task> taskStack; // Use a Dequeue instead of a Stack because we don't need synchronization.

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState) {
        this(metaData, recordStoreState, PlannerRuleSet.ALL);
    }

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState, @Nonnull PlannerRuleSet ruleSet) {
        this.configuration = RecordQueryPlannerConfiguration.builder().build();
        this.metaData = metaData;
        this.recordStoreState = recordStoreState;
        this.ruleSet = ruleSet;
        // Placeholders until we get a query.
        this.currentRoot = GroupExpressionRef.empty();
        this.aliasResolver = AliasResolver.withRoot(currentRoot);
        this.taskStack = new ArrayDeque<>();
    }

    @Nonnull
    @Override
    public RecordQueryPlan plan(@Nonnull RecordQuery query) {
        final PlanContext context = new MetaDataPlanContext(metaData, recordStoreState, query);
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
        aliasResolver = AliasResolver.withRoot(currentRoot);
        Debugger.withDebugger(debugger -> PlannerGraphProperty.show(true, currentRoot));
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
        configuration = this.configuration.asBuilder()
                .setIndexScanPreference(indexScanPreference)
                .build();
    }

    /**
     * Represents actual tasks in the task stack of the planner.
     */
    public interface Task {
        void execute();

        Debugger.Event toTaskEvent(final Location location);
    }

    /**
     * Optimize Group task.
     *
     * Simplified enqueue/execute overview:
     *
     * {@link OptimizeGroup}
     *     if (not explored)
     *         enqueues
     *             this (again)
     *             {@link ExploreExpression} for each group member
     *         sets explored to {@code true}
     *     else
     *         prune to find best plan; done
     */
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
                    // enqueue explore expression which then in turn enqueues necessary rules for transformations
                    // and matching
                    taskStack.push(new ExploreExpression(context, group, member));
                }
                // the second time around we want to visit the else and prune the plan space
                group.setExplored();
            } else {
                // TODO this is very Volcano-style rather than Cascades, because there's no branch-and-bound pruning.
                RelationalExpression bestMember = null;
                for (RelationalExpression member : group.getMembers()) {
                    if (bestMember == null || new CascadesCostModel(configuration, context).compare(member, bestMember) < 0) {
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

    /**
     * Explore Group Task.
     *
     * Simplified enqueue/execute overview:
     *
     * {@link ExploreGroup}
     *     enqueues
     *         {@link ExploreExpression} for each group member
     *     sets explored to {@code true}
     */
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

    /**
     * Abstract base class for all tasks that have a <em>current</em> (group, expression).
     */
    private abstract class ExploreTask implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<RelationalExpression> group;
        @Nonnull
        private final RelationalExpression expression;

        public ExploreTask(@Nonnull PlanContext context,
                           @Nonnull GroupExpressionRef<RelationalExpression> group,
                           @Nonnull RelationalExpression expression) {
            this.context = context;
            this.group = group;
            this.expression = expression;
        }

        @Nonnull
        public PlanContext getContext() {
            return context;
        }

        @Nonnull
        public GroupExpressionRef<RelationalExpression> getGroup() {
            return group;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        protected PlannerRuleSet getRules() {
            return ruleSet;
        }
    }

    /**
     * Explore Expression Task.
     *
     * Simplified enqueue/execute overview:
     *
     * {@link ExploreExpression}
     *     enqueues
     *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
     *         all transformations ({@link TransformExpression} for current (group, expression)
     *         {@link ExploreGroup} for all ranged over groups
     */
    private class ExploreExpression extends ExploreTask {
        public ExploreExpression(@Nonnull PlanContext context,
                                 @Nonnull GroupExpressionRef<RelationalExpression> group,
                                 @Nonnull RelationalExpression expression) {
            super(context, group, expression);
        }

        @Override
        public void execute() {
            // Enqueue all rules that need to run after all exploration for a (group, expression) pair is done.
            ruleSet.getMatchPartitionRules().forEach(this::enqueueTransformPartialMatch);

            // This is closely tied to the way that rule finding works _now_. Specifically, rules are indexed only
            // by the type of their _root_, not any of the stuff lower down. As a result, we have enough information
            // right here to determine the set of all possible rules that could ever be applied here, regardless of
            // what happens towards the leaves of the tree.
            ruleSet.getExpressionRules(getExpression()).forEach(this::enqueueTransformTask);

            // Enqueue explore group for all groups this expression ranges over
            getExpression()
                    .getQuantifiers()
                    .stream()
                    .map(Quantifier::getRangesOver)
                    .forEach(this::enqueueExploreGroup);
        }

        private void enqueueTransformTask(@Nonnull PlannerRule<? extends RelationalExpression> rule) {
            taskStack.push(new TransformExpression(getContext(), getGroup(), getExpression(), rule));
        }

        private void enqueueTransformPartialMatch(PlannerRule<? extends MatchPartition> rule) {
            taskStack.push(new TransformMatchPartition(getContext(), getGroup(), getExpression(), rule));
        }

        private void enqueueExploreGroup(ExpressionRef<? extends RelationalExpression> rangesOver) {
            taskStack.push(new ExploreGroup(getContext(), rangesOver));
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.ExploreExpressionEvent(currentRoot, taskStack, location, getGroup(), getExpression());
        }

        @Override
        public String toString() {
            return "ExploreExpression(" + getGroup() + ")";
        }
    }

    /**
     * Abstract base class for all transformations. All transformations are defined on a sub class of {@link Bindable}.
     * This base class uses just {@link Bindable} itself as it never has to reason or pass on the actual defined type
     * of the transform rule.
     */
    private abstract class AbstractTransform implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<RelationalExpression> group;
        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final PlannerRule<? extends Bindable> rule;

        protected AbstractTransform(@Nonnull PlanContext context,
                                    @Nonnull GroupExpressionRef<RelationalExpression> group,
                                    @Nonnull RelationalExpression expression,
                                    @Nonnull PlannerRule<? extends Bindable> rule) {
            this.context = context;
            this.group = group;
            this.expression = expression;
            this.rule = rule;
        }

        @Nonnull
        public PlanContext getContext() {
            return context;
        }

        @Nonnull
        public GroupExpressionRef<RelationalExpression> getGroup() {
            return group;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        public PlannerRule<? extends Bindable> getRule() {
            return rule;
        }

        @Nonnull
        protected abstract Bindable getBindable();

        @Nonnull
        protected PlannerBindings getInitialBindings() {
            return PlannerBindings.empty();
        }

        protected boolean shouldExecute() {
            return true;
        }

        /**
         * Method that calls the actual rule and reacts to new constructs the rule yielded.
         *
         * Simplified enqueue/execute overview:
         *
         * executes rule
         * enqueues
         *     {@link AdjustMatch} for each yielded {@link PartialMatch}
         *     {@link OptimizeInputs} followed by {@link ExploreExpression} for each yielded {@link RecordQueryPlan}
         *     {@link ExploreExpression} for each yielded {@link RelationalExpression} that is not a {@link RecordQueryPlan}
         */
        @Override
        public void execute() {
            final GroupExpressionRef<RelationalExpression> group = getGroup();
            final PlannerRule<? extends Bindable> rule = getRule();
            if (!shouldExecute()) {
                return;
            }

            final PlannerBindings initialBindings = getInitialBindings();

            if (logger.isTraceEnabled()) {
                logger.trace("Bindings: " +  getBindable().bindTo(initialBindings, rule.getMatcher()).count());
            }

            getBindable().bindTo(initialBindings, rule.getMatcher())
                    .map(bindings -> new CascadesRuleCall(getContext(), rule, group, aliasResolver, bindings))
                    .forEach(ruleCall -> {
                        // we notify the debugger (if installed) that the transform task is succeeding and
                        // about begin and end of the rule call event
                        Debugger.withDebugger(debugger -> debugger.onEvent(toTaskEvent(Location.SUCCESS)));
                        Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.TransformRuleCallEvent(currentRoot, taskStack, Location.BEGIN, group, getBindable(), rule, ruleCall)));
                        executeRuleCall(ruleCall);
                        Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.TransformRuleCallEvent(currentRoot, taskStack, Location.END, group, getBindable(), rule, ruleCall)));
                    });
        }

        protected void executeRuleCall(@Nonnull CascadesRuleCall ruleCall) {
            ruleCall.run();
            for (final PartialMatch newPartialMatch : ruleCall.getNewPartialMatches()) {
                taskStack.push(new AdjustMatch(getContext(), getGroup(), getExpression(), newPartialMatch));
            }

            for (final RelationalExpression newExpression : ruleCall.getNewExpressions()) {
                if (newExpression instanceof QueryPlan) {
                    taskStack.push(new OptimizeInputs(getContext(), getGroup(), newExpression));
                    taskStack.push(new ExploreExpression(getContext(), getGroup(), newExpression));
                } else {
                    taskStack.push(new ExploreExpression(getContext(), getGroup(), newExpression));
                }
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.TransformEvent(currentRoot, taskStack, location, getGroup(), getBindable(), getRule());
        }

        @Override
        public String toString() {
            return "Transform(" + rule.getClass().getSimpleName() + ")";
        }
    }

    /**
     * Class to transform an expression using a rule.
     */
    private class TransformExpression extends AbstractTransform {
        public TransformExpression(@Nonnull PlanContext context,
                                   @Nonnull GroupExpressionRef<RelationalExpression> group,
                                   @Nonnull RelationalExpression expression,
                                   @Nonnull PlannerRule<? extends RelationalExpression> rule) {
            super(context, group, expression, rule);
        }

        @Nonnull
        @Override
        protected Bindable getBindable() {
            // note that the bindable is the current expression itself
            return getExpression();
        }

        @Override
        protected boolean shouldExecute() {
            return super.shouldExecute() && getGroup().containsExactly(getExpression());
        }

        @Nonnull
        @Override
        protected PlannerBindings getInitialBindings() {
            return PlannerBindings.newBuilder()
                    .putAll(super.getInitialBindings())
                    .putAll(PartialMatchMatcher.expressionWithCurrentPartialMatches(),
                            getGroup().getPartialMatchesForExpression(getExpression()))
                    .build();
        }
    }

    /**
     * Class to transform a match partition using a rule.
     */
    private class TransformMatchPartition extends AbstractTransform {
        @Nonnull
        private final Supplier<MatchPartition> matchPartitionSupplier;

        public TransformMatchPartition(@Nonnull PlanContext context,
                                       @Nonnull GroupExpressionRef<RelationalExpression> group,
                                       @Nonnull RelationalExpression expression,
                                       @Nonnull PlannerRule<? extends MatchPartition> rule) {
            super(context, group, expression, rule);
            this.matchPartitionSupplier = Suppliers.memoize(() -> MatchPartition.of(group, expression));
        }

        @Nonnull
        @Override
        protected Bindable getBindable() {
            return matchPartitionSupplier.get();
        }
    }

    /**
     * Class to transform a match partial match using a rule.
     */
    private class TransformPartialMatch extends AbstractTransform {
        @Nonnull
        private final PartialMatch partialMatch;

        public TransformPartialMatch(@Nonnull PlanContext context,
                                     @Nonnull GroupExpressionRef<RelationalExpression> group,
                                     @Nonnull RelationalExpression expression,
                                     @Nonnull PartialMatch partialMatch,
                                     @Nonnull PlannerRule<? extends PartialMatch> rule) {
            super(context, group, expression, rule);
            this.partialMatch = partialMatch;
        }

        @Nonnull
        @Override
        protected Bindable getBindable() {
            return partialMatch;
        }
    }

    /**
     * Adjust Match Task. Attempts to improve an existing partial partial match on a (group, expression) pair
     * to a better one by enqueuing rules defined on {@link PartialMatch}.
     *
     * Simplified enqueue/execute overview:
     *
     * {@link AdjustMatch}
     *     enqueues
     *         all transformations ({@link TransformPartialMatch}) for current (group, expression, partial match)
     */
    private class AdjustMatch extends ExploreTask {
        @Nonnull
        final PartialMatch partialMatch;

        public AdjustMatch(@Nonnull final PlanContext context,
                           @Nonnull final GroupExpressionRef<RelationalExpression> group,
                           @Nonnull final RelationalExpression expression,
                           @Nonnull final PartialMatch partialMatch) {
            super(context, group, expression);
            this.partialMatch = partialMatch;
        }

        @Override
        public void execute() {
            ruleSet.getPartialMatchRules()
                    .forEach(rule -> taskStack.push(new TransformPartialMatch(getContext(), getGroup(), getExpression(), partialMatch, rule)));
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.MatchExpressionEvent(currentRoot, taskStack, location, getGroup(), getExpression());
        }

        @Override
        public String toString() {
            return "AdjustMatch(" + getGroup() + "; " + getExpression() + ")";
        }
    }

    /**
     * Optimize Inputs Task. This task is only used for expressions that are {@link RecordQueryPlan} which are
     * physical operators. If the current expression is a {@link RecordQueryPlan} all expressions that are considered
     * children and/or descendants must also be of type {@link RecordQueryPlan}. At that moment we know that exploration
     * is done and we can optimize the children (that is we can now prune the plan space of the children).
     *
     * Simplified enqueue/execute overview:
     *
     * {@link OptimizeInputs}
     *     enqueues
     *         {@link OptimizeGroup} for all ranged over groups
     */
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
