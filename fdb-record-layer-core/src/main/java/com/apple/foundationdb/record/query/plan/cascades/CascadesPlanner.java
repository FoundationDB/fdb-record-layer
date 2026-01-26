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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.IndexMatchCandidateRegistry;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.HeuristicPlanner;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.QueryPlanInfo;
import com.apple.foundationdb.record.query.plan.QueryPlanInfoKeys;
import com.apple.foundationdb.record.query.plan.QueryPlanResult;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanComplexityException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.PlannerRule.PreOrderRule;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger.Location;
import com.apple.foundationdb.record.query.plan.cascades.debug.RestartException;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A Cascades-style query planner that converts a {@link RecordQuery} to a {@link RecordQueryPlan}, possibly using
 * secondary indexes defined in a {@link RecordMetaData} to execute the query efficiently.
 *
 * <p>
 * <a href="https://15721.courses.cs.cmu.edu/spring2017/papers/15-optimizer2/graefe-ieee1995.pdf">Cascades</a> is a
 * framework for a query optimization introduced by Graefe in 1995. In Cascades, all parsed queries, query plans, and
 * intermediate state between the two are represented in a unified tree of {@link RelationalExpression}, which includes
 * types such as {@link RecordQueryPlan}. This highly flexible data structure reifies essentially the entire state of
 * the planner (i.e., partially planned elements, current optimization, goals, etc.) and allows individual planning
 * steps to be modular and stateless by keeping all state in the {@link RelationalExpression} tree.
 * </p>
 *
 * <p>
 * Like many optimization frameworks, Cascades is driven by sets of {@link CascadesRule}s that can be defined for
 * {@link RelationalExpression}s, {@link PartialMatch}es and {@link MatchPartition}s, each of which describes a
 * particular transformation and encapsulates the logic for determining its applicability and applying it. The planner
 * searches through its {@link PlanningRuleSet} to find a matching rule and then executes that rule, creating zero or
 * more additional {@code PlannerExpression}s and/or zero or more additional {@link PartialMatch}es. A rule is defined by:
 * </p>
 * <ul>
 *     <li>
 *         An {@link BindingMatcher} that defines a
 *         finite-depth tree of matchers that inspect the structure (i.e., the type-level information) of some subgraph
 *         of the current planner expression, the current partial match, or the current match partition.
 *     </li>
 *     <li>
 *         A {@link CascadesRule#onMatch(CascadesRuleCall)} method that is run for each successful match, producing zero
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
 * represented by {@link Reference}s, each of which represents a group expression in Cascades and contains
 * a set of {@link RelationalExpression}s. In turn, {@link RelationalExpression}s have some number of <em>children</em>, each
 * of which is a {@link Reference} and which can be traversed by the planner via the
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
 * <p>
 * Simplified push/execute overview:
 * </p>
 *
 * <pre>
 * {@link InitiatePlannerPhase}
 *     if (there is a next phase)
 *         push
 *             {@link InitiatePlannerPhase} for the next phase
 *     push {@link OptimizeGroup} for the root of the expression DAG
 *     push {@link ExploreGroup} for the root of the expression DAG
 *
 * {@link OptimizeGroup}
 *     if (not explored)
 *         pushes
 *             this (again)
 *             {@link ExploreExpression} for each group member
 *         sets explored to {@code true}
 *     else
 *         prune to find best plan; done
 *
 * {@link ExploreGroup}
 *     pushes
 *         {@link ExploreExpression} for each group member
 *     sets explored to {@code true}
 *
 * {@link ExploreExpression}
 *     pushes
 *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
 *         all transformations ({@link TransformExpression}) for current (group, expression)
 *         {@link ExploreGroup} for all ranged over groups
 *
 * after execution of any TransformXXX
 *     pushes
 *         {@link AdjustMatch} for each yielded {@link PartialMatch}
 *         {@link OptimizeInputs} followed by {@link ExploreExpression} for each yielded {@link RecordQueryPlan}
 *         {@link ExploreExpression} for each yielded {@link RelationalExpression} that is not a {@link RecordQueryPlan}
 *
 * {@link AdjustMatch}
 *     pushes
 *         all transformations ({@link TransformPartialMatch}) for current (group, expression, partial match)
 *
 * {@link OptimizeInputs}
 *     pushes
 *         {@link OptimizeGroup} for all ranged over groups
 * </pre>
 *
 * Note: Pushed tasks are executed in typical stack machine order, that is LIFO.
 * <p>
 * There are three different kinds of transformations:
 * <ul>
 *     <li>
 *         Transforms on expressions {@link TransformExpression}: These are the classical transforms creating new
 *         variations in the expression memoization structure. The root for the corresponding rules is always of type
 *         {@link RelationalExpression}.
 *     </li>
 *     <li>
 *         Transforms on partial matches {@link TransformPartialMatch}: These transforms are executed when a partial
 *         match is found and typically only yield other new partial matches for the <em>current</em> (group, expression)
 *         pair. The root for the corresponding rules is always of type {@link PartialMatch}.
 *     </li>
 *     <li>
 *         Transforms on match partitions {@link TransformMatchPartition}: These transforms are executed only after
 *         all transforms (both {@link TransformExpression}s and {@link TransformPartialMatch}) have been executed
 *         for a current (group, expression). Note, that this kind transformation task can be repeatedly executed for
 *         a given group, but it is guaranteed to only be executed once for a (group, expression) pair.
 *         The root for the corresponding rules is always of type {@link MatchPartition}. These are the rules that react
 *         to all synthesized matches for an expression at once.
 *     </li>
 * </ul>
 * </p>
 *
 * @see Reference
 * @see RelationalExpression
 * @see CascadesRule
 * @see CascadesCostModel
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
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
    private final IndexMatchCandidateRegistry matchCandidateRegistry;
    @Nonnull
    private Reference currentRoot;
    @Nonnull
    private PlanContext planContext;
    @Nonnull
    private EvaluationContext evaluationContext;
    @Nonnull
    private Traversal traversal;
    @Nonnull
    private Deque<Task> taskStack; // Use a Dequeue instead of a Stack because we don't need synchronization.
    // total tasks executed for the current plan
    private int taskCount;
    // max size of the task queue encountered during the planning
    private int maxQueueSize;

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState, @Nonnull IndexMatchCandidateRegistry matchCandidateRegistry) {
        this.configuration = RecordQueryPlannerConfiguration.builder().build();
        this.metaData = metaData;
        this.recordStoreState = recordStoreState;
        this.matchCandidateRegistry = matchCandidateRegistry;
        // Placeholders until we get a query.
        this.currentRoot = Reference.empty();
        this.planContext = PlanContext.emptyContext();
        this.evaluationContext = EvaluationContext.empty();
        this.traversal = Traversal.withRoot(currentRoot);
        this.taskStack = new ArrayDeque<>();
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaData;
    }

    @Nonnull
    @Override
    public RecordStoreState getRecordStoreState() {
        return recordStoreState;
    }

    @Override
    public void setIndexScanPreference(@Nonnull IndexScanPreference indexScanPreference) {
        configuration = this.configuration.asBuilder()
                .setIndexScanPreference(indexScanPreference)
                .build();
    }

    /**
     * Set the size limit of the Cascades planner task queue.
     * If the planner tries to add a task to the queue beyond the maximum size, planning will fail.
     * Default value is 0, which means "unbound".
     * @param maxTaskQueueSize the maximum size of the queue.
     */
    @SuppressWarnings("unused")
    public void setMaxTaskQueueSize(final int maxTaskQueueSize) {
        configuration = this.configuration.asBuilder()
                .setMaxTaskQueueSize(maxTaskQueueSize)
                .build();
    }

    /**
     * Set a limit on the number of tasks that can be executed as part of the Cascades planner planning.
     * If the planner tries to execute a task after the maximum number was exceeded, planning will fail.
     * Default value is 0, which means "unbound".
     * @param maxTotalTaskCount the maximum number of tasks.
     */
    @SuppressWarnings("unused")
    public void setMaxTotalTaskCount(final int maxTotalTaskCount) {
        configuration = this.configuration.asBuilder()
                .setMaxTotalTaskCount(maxTotalTaskCount)
                .build();
    }

    /**
     * Set the maximum number of yields that are permitted per rule call within the Cascades planner.
     * Default value is 0, which means "unbound".
     * @param maxNumYieldsPerRuleCall the desired maximum number of yields that are permitted per rule call
     */
    @SuppressWarnings("unused")
    public void setMaxNumMatchesPerRuleCall(final int maxNumYieldsPerRuleCall) {
        configuration = this.configuration.asBuilder()
                .setMaxNumMatchesPerRuleCall(maxNumYieldsPerRuleCall)
                .build();
    }

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public void setConfiguration(@Nonnull final RecordQueryPlannerConfiguration configuration) {
        this.configuration = configuration;
    }

    private boolean isTaskQueueSizeExceeded(final RecordQueryPlannerConfiguration configuration, final int queueSize) {
        return ((configuration.getMaxTaskQueueSize() > 0) && (queueSize > configuration.getMaxTaskQueueSize()));
    }

    private boolean isTaskTotalCountExceeded(final RecordQueryPlannerConfiguration configuration, final int taskCount) {
        return ((configuration.getMaxTotalTaskCount() > 0) && (taskCount > configuration.getMaxTotalTaskCount()));
    }

    private boolean isMaxNumMatchesPerRuleCallExceeded(final RecordQueryPlannerConfiguration configuration, final int numMatches) {
        return ((configuration.getMaxNumMatchesPerRuleCall() > 0) && (numMatches > configuration.getMaxNumMatchesPerRuleCall()));
    }

    @HeuristicPlanner
    @Nonnull
    @Override
    public QueryPlanResult planQuery(@Nonnull final RecordQuery query,
                                     @Nonnull final ParameterRelationshipGraph parameterRelationshipGraph) {
        RecordQueryPlan plan = plan(query, parameterRelationshipGraph);
        final var constraints = QueryPlanConstraint.collectConstraints(plan);
        QueryPlanInfo info = QueryPlanInfo.newBuilder()
                .put(QueryPlanInfoKeys.TOTAL_TASK_COUNT, taskCount)
                .put(QueryPlanInfoKeys.MAX_TASK_QUEUE_SIZE, maxQueueSize)
                .put(QueryPlanInfoKeys.CONSTRAINTS, constraints)
                .put(QueryPlanInfoKeys.STATS_MAPS,
                        Debugger.getDebuggerMaybe().flatMap(Debugger::getStatsMaps)
                                .orElse(null))
                .build();
        return new QueryPlanResult(plan, info);
    }

    @HeuristicPlanner
    @Nonnull
    @Override
    public RecordQueryPlan plan(@Nonnull final RecordQuery query,
                                @Nonnull final ParameterRelationshipGraph parameterRelationshipGraph) {
        try {
            planPartial(() -> Reference.initialOf(RelationalExpression.fromRecordQuery(metaData, query)),
                    rootReference -> MetaDataPlanContext.forRecordQuery(configuration, metaData, recordStoreState, matchCandidateRegistry, query),
                    EvaluationContext.empty());
            return resultOrFail();
        } finally {
            Debugger.withDebugger(Debugger::onDone);
        }

    }

    @Nonnull
    public QueryPlanResult planGraph(@Nonnull final Supplier<Reference> referenceSupplier,
                                     @Nonnull final Optional<Collection<String>> allowedIndexesOptional,
                                     @Nonnull final IndexQueryabilityFilter indexQueryabilityFilter,
                                     @Nonnull final EvaluationContext evaluationContext) {
        try {
            planPartial(referenceSupplier,
                    rootReference ->
                            MetaDataPlanContext.forRootReference(configuration,
                                    metaData,
                                    recordStoreState,
                                    matchCandidateRegistry,
                                    rootReference,
                                    allowedIndexesOptional,
                                    indexQueryabilityFilter
                            ),
                    evaluationContext);
            final var plan = resultOrFail();
            final var constraints = QueryPlanConstraint.collectConstraints(plan);
            return new QueryPlanResult(plan,
                    QueryPlanInfo.newBuilder()
                            .put(QueryPlanInfoKeys.CONSTRAINTS, constraints)
                            .put(QueryPlanInfoKeys.STATS_MAPS,
                                    Debugger.getDebuggerMaybe()
                                            .flatMap(Debugger::getStatsMaps).orElse(null))
                            .build());
        } finally {
            Debugger.withDebugger(Debugger::onDone);
        }
    }

    private RecordQueryPlan resultOrFail() {
        final Set<RelationalExpression> finalExpressions = currentRoot.getFinalExpressions();
        Verify.verify(finalExpressions.size() <= 1, "more than one variant present");
        if (finalExpressions.isEmpty()) {
            throw new UnableToPlanException("Cascades planner could not plan query");
        }

        final RelationalExpression singleRoot = Iterables.getOnlyElement(finalExpressions);
        Verify.verify(singleRoot instanceof RecordQueryPlan, "single remaining variant must be a plan");
        if (logger.isDebugEnabled()) {
            logger.debug(KeyValueLogMessage.of("GML explain of plan",
                    "explain", PlannerGraphVisitor.explain(singleRoot)));
            logger.debug(KeyValueLogMessage.of("DOT explain of plan",
                    "explain", PlannerGraphVisitor.internalGraphicalExplain(singleRoot)));
            logger.debug(KeyValueLogMessage.of("string explain of plan",
                    "explain", ExplainPlanVisitor.toStringForDebugging((RecordQueryPlan)singleRoot)));
        }
        return (RecordQueryPlan)singleRoot;
    }

    private void planPartial(@Nonnull final Supplier<Reference> referenceSupplier,
                             @Nonnull final Function<Reference, PlanContext> contextCreatorFunction,
                             @Nonnull final EvaluationContext evaluationContext) {
        this.currentRoot = referenceSupplier.get();
        this.planContext = contextCreatorFunction.apply(currentRoot);
        this.evaluationContext = evaluationContext;

        final RelationalExpression expression = currentRoot.get();
        Debugger.withDebugger(debugger -> debugger.onQuery(expression.toString(), planContext));
        if (logger.isDebugEnabled()) {
            logger.debug(KeyValueLogMessage.of("DOT explain initial expression",
                    "explain", PlannerGraphVisitor.internalGraphicalExplain(expression)));
        }

        this.traversal = Traversal.withRoot(currentRoot);
        this.taskStack = new ArrayDeque<>();
        this.taskCount = 0;
        this.maxQueueSize = 0;
        pushInitialTasks();

        while (!taskStack.isEmpty()) {
            try {
                if (isTaskTotalCountExceeded(configuration, taskCount)) {
                    throw new RecordQueryPlanComplexityException("Maximum number of tasks was exceeded")
                            .addLogInfo(LogMessageKeys.MAX_TASK_COUNT, configuration.getMaxTotalTaskCount())
                            .addLogInfo(LogMessageKeys.TASK_COUNT, taskCount);
                }
                taskCount++;

                Debugger.withDebugger(debugger -> debugger.onEvent(
                        new Debugger.ExecutingTaskEvent(currentRoot, taskStack, Location.BEGIN,
                                Objects.requireNonNull(taskStack.peek()))));
                Task nextTask = taskStack.pop();
                try {
                    if (logger.isTraceEnabled()) {
                        logger.trace(KeyValueLogMessage.of("executing task", "nextTask", nextTask.toString()));
                    }

                    Debugger.withDebugger(debugger -> debugger.onEvent(nextTask.toTaskEvent(Location.BEGIN)));
                    try {
                        nextTask.execute();
                        Debugger.sanityCheck(() -> {
                            if (nextTask instanceof InitiatePlannerPhase) {
                                //
                                // Validate that the memo structure and the query graph are aligned.
                                // This is an expensive check, but if it finds anything, then it may
                                // be useful to enable this check so that it runs after every task
                                // to root out the underlying issue.
                                //
                                // Once we have sanity check levels, we should consider enabling
                                // this check all the time at certain levels.
                                // See: https://github.com/FoundationDB/fdb-record-layer/issues/3445
                                //
                                traversal.verifyIntegrity();
                            }
                        });

                    } finally {
                        Debugger.withDebugger(debugger -> debugger.onEvent(nextTask.toTaskEvent(Location.END)));
                    }

                    if (logger.isTraceEnabled()) {
                        logger.trace(KeyValueLogMessage.of("planner state",
                                "taskStackSize", taskStack.size()));
                    }

                    maxQueueSize = Math.max(maxQueueSize, taskStack.size());
                    if (isTaskQueueSizeExceeded(configuration, taskStack.size())) {
                        throw new RecordQueryPlanComplexityException("Maximum task queue size was exceeded")
                                .addLogInfo(LogMessageKeys.MAX_TASK_QUEUE_SIZE, configuration.getMaxTaskQueueSize())
                                .addLogInfo(LogMessageKeys.TASK_QUEUE_SIZE, taskStack.size());
                    }
                } finally {
                    Debugger.withDebugger(debugger -> debugger.onEvent(
                            new Debugger.ExecutingTaskEvent(currentRoot, taskStack, Location.END, nextTask)));
                }
            } catch (final RestartException restartException) {
                if (logger.isTraceEnabled()) {
                    logger.trace(KeyValueLogMessage.of("debugger requests restart of planning",
                            "taskStackSize", taskStack.size()));
                }
                taskStack.clear();
                this.currentRoot = referenceSupplier.get();
                this.planContext = contextCreatorFunction.apply(currentRoot);
                pushInitialTasks();
            }
        }

        // Also validate memo structure integrity at the end of planning
        Debugger.sanityCheck(() -> traversal.verifyIntegrity());
    }

    private void pushInitialTasks() {
        taskStack.push(new InitiatePlannerPhase(PlannerPhase.REWRITING));
    }

    private void exploreExpressionAndOptimizeInputs(@Nonnull final PlannerPhase plannerPhase,
                                                    @Nonnull final Reference group,
                                                    @Nonnull final RelationalExpression expression,
                                                    final boolean forceExploration) {
        taskStack.push(new OptimizeInputs(plannerPhase, group, expression));
        exploreExpression(plannerPhase, group, expression, forceExploration);
    }

    private void exploreExpression(@Nonnull final PlannerPhase plannerPhase,
                                   @Nonnull final Reference group,
                                   @Nonnull final RelationalExpression expression,
                                   final boolean forceExploration) {
        Verify.verify(group.containsExactly(expression));
        if (forceExploration) {
            taskStack.push(new ExploreExpression(plannerPhase, group, expression));
        }  else {
            taskStack.push(new ReExploreExpression(plannerPhase, group, expression));
        }
    }

    /**
     * Represents actual tasks in the task stack of the planner.
     */
    public interface Task {
        @Nonnull
        PlannerPhase getPlannerPhase();

        void execute();

        Debugger.Event toTaskEvent(Location location);
    }

    /**
     * Globally initiate a new planner phase.
     * Simplified push/execute overview:
     * <pre>
     * {@link InitiatePlannerPhase}
     *     if (there is a next phase)
     *         push
     *             {@link InitiatePlannerPhase} for the next phase
     *     push {@link OptimizeGroup} for the root of the expression DAG
     *     push {@link ExploreGroup} for the root of the expression DAG
     * </pre>
     */
    private class InitiatePlannerPhase implements Task {
        @Nonnull
        private final PlannerPhase plannerPhase;

        public InitiatePlannerPhase(@Nonnull final PlannerPhase plannerPhase) {
            this.plannerPhase = plannerPhase;
        }

        @Override
        @Nonnull
        public PlannerPhase getPlannerPhase() {
            return plannerPhase;
        }

        @Override
        public void execute() {
            if (plannerPhase.hasNextPhase()) {
                // if there is another phase push it first so it gets executed at the very end
                taskStack.push(new InitiatePlannerPhase(plannerPhase.getNextPhase()));
            }
            taskStack.push(new OptimizeGroup(plannerPhase, currentRoot));
            taskStack.push(new ExploreGroup(plannerPhase, currentRoot));
        }

        @Override
        @Nonnull
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.InitiatePlannerPhaseEvent(plannerPhase, currentRoot, taskStack, location);
        }

        @Override
        public String toString() {
            return "InitiatePlannerPhase(" + plannerPhase.name() + ")";
        }
    }

    /**
     * Optimize Group task.
     * Simplified push/execute overview:
     * <pre>
     * {@link OptimizeGroup}
     *     if (not explored)
     *         pushes
     *             this (again)
     *             {@link ExploreExpression} for each group member
     *         sets explored to {@code true}
     *     else
     *         prune to find best plan; done
     * </pre>
     */
    private class OptimizeGroup implements Task {
        @Nonnull
        final PlannerPhase plannerPhase;
        @Nonnull
        private final Reference group;

        public OptimizeGroup(@Nonnull final PlannerPhase plannerPhase,
                             @Nonnull final Reference group) {
            this.plannerPhase = plannerPhase;
            this.group = group;
        }

        @Nonnull
        @Override
        public PlannerPhase getPlannerPhase() {
            return plannerPhase;
        }

        @Override
        public void execute() {
            RelationalExpression bestFinalExpression = null;
            final var costModel = plannerPhase.createCostModel(configuration);
            for (final var finalExpression : group.getFinalExpressions()) {
                if (bestFinalExpression == null || costModel.compare(finalExpression, bestFinalExpression) < 0) {
                    if (bestFinalExpression != null) {
                        // best member is being pruned
                        traversal.removeExpression(group, bestFinalExpression);
                    }
                    bestFinalExpression = finalExpression;
                } else {
                    // member is being pruned
                    traversal.removeExpression(group, finalExpression);
                }
            }

            //
            // In the past we would iterate through ALL members to find the cheapest plan.
            //
            // 1. all non-plans get (implicitly) assigned infinite cost, losing immediately even against the worst plan,
            // 2. if there were no plans, the first non-plan expression wins
            //
            // Because of 2. we observed a weird behavior that would have the potential to change the behavior of the
            // planner if the rules in the ruleset were reordered. Also, why does one arbitrary expression survive?
            // That was pretty detrimental for observability reasons as the sole surviving expression was not kept for
            // a reason, other than being the stand-in for not having to deal with empty references.
            //
            // We now leave the exploratory members alone during pruning. That makes all exploratory expressions survive
            // that that were available when pruning occurred. There is no threat of empty references nor the need to
            // pick an expression from the exploratory set arbitrarily. I think this is a cleaner way of doing things.
            //
            // Note that all of this is only really relevant for the top-most reference and recursively for one subtree
            // of that reference as only that subtree actually harbors non-plan expressions at the time of pruning.
            // Thus, leaving the exploratory expressions around in the reference is not a problem for garbage
            // collection.
            //
            if (bestFinalExpression == null) {
                group.clearFinalExpressions();
            } else {
                group.pruneWith(bestFinalExpression);
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.OptimizeGroupEvent(plannerPhase, currentRoot, taskStack, location, group);
        }

        @Override
        public String toString() {
            return "OptimizeGroup(" + group + ")";
        }
    }

    /**
     * Explore Group Task.
     * Simplified push/execute overview:
     * <pre>
     * {@link ExploreGroup}
     *     pushes
     *         {@link ExploreExpression} for each group member
     *     sets explored to {@code true}
     * </pre>
     */
    private class ExploreGroup implements Task {
        @Nonnull
        private final PlannerPhase plannerPhase;
        @Nonnull
        private final Reference group;

        public ExploreGroup(@Nonnull final PlannerPhase plannerPhase,
                            @Nonnull final Reference ref) {
            this.plannerPhase = plannerPhase;
            this.group = ref;
        }

        @Nonnull
        @Override
        public PlannerPhase getPlannerPhase() {
            return plannerPhase;
        }

        @Override
        public void execute() {
            final var targetPlannerStage = plannerPhase.getTargetPlannerStage();
            final var groupPlannerStage = group.getPlannerStage();
            if (targetPlannerStage != groupPlannerStage) {
                if (targetPlannerStage.precedes(groupPlannerStage)) {
                    // group is further along in the planning process, do not re-explore
                    return;
                } else {
                    //
                    // targetPlannerStage succeeds groupPlannerStage
                    // Group needs to be bumped to the current target stage
                    //
                    for (final var exploratoryExpression : group.getExploratoryExpressions()) {
                        traversal.removeExpression(group, exploratoryExpression);
                    }

                    group.advancePlannerStage(targetPlannerStage);
                    //
                    // All final expression properties are reset, all final members have been cleared.
                    //
                }
            }

            //
            // Target planner stage and group's stage are now the same.
            //

            if (group.needsExploration()) {
                taskStack.push(this);

                for (final RelationalExpression expression : group.getFinalExpressions()) {
                    exploreExpressionAndOptimizeInputs(plannerPhase, group, expression, false);
                }
                for (final RelationalExpression expression : group.getExploratoryExpressions()) {
                    exploreExpression(plannerPhase, group, expression, false);
                }
                group.startExploration();
            } else {
                group.commitExploration();
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.ExploreGroupEvent(plannerPhase, currentRoot, taskStack, location, group);
        }

        @Override
        public String toString() {
            return "ExploreGroup(" + group + ")";
        }
    }

    /**
     * Abstract base class for all tasks that have a <em>current</em> (group, expression).
     */
    private abstract static class ExploreTask implements Task {
        @Nonnull
        private final PlannerPhase plannerPhase;
        @Nonnull
        private final Reference group;
        @Nonnull
        private final RelationalExpression expression;

        public ExploreTask(@Nonnull final PlannerPhase plannerPhase,
                           @Nonnull final Reference group,
                           @Nonnull final RelationalExpression expression) {
            this.plannerPhase = plannerPhase;
            this.group = group;
            this.expression = expression;
        }

        @Nonnull
        @Override
        public PlannerPhase getPlannerPhase() {
            return plannerPhase;
        }

        @Nonnull
        public Reference getGroup() {
            return group;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }
    }

    /**
     * Explore Expression Task.
     * Simplified push/execute overview:
     * <pre>
     * {@link ExploreExpression}
     *     pushes
     *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
     *         all transformations ({@link TransformExpression} for current (group, expression)
     *         {@link ExploreGroup} for all ranged over groups
     * </pre>
     */
    private abstract class AbstractExploreExpression extends ExploreTask {
        public AbstractExploreExpression(@Nonnull final PlannerPhase plannerPhase,
                                         @Nonnull final Reference group,
                                         @Nonnull final RelationalExpression expression) {
            super(plannerPhase, group, expression);
        }

        @Override
        public void execute() {
            final var ruleSet = getPlannerPhase().getRuleSet();

            // push all rules that need to run after all exploration for a (group, expression) pair is done.
            ruleSet.getMatchPartitionRules(rule -> configuration.isRuleEnabled(rule))
                    .filter(this::shouldPushRule)
                    .forEach(this::pushTransformMatchPartition);

            // This is closely tied to the way that rule finding works _now_. Specifically, rules are indexed only
            // by the type of their _root_, not any of the stuff lower down. As a result, we have enough information
            // right here to determine the set of all possible rules that could ever be applied here, regardless of
            // what happens towards the leaves of the tree.
            ruleSet.getRules(getExpression(), rule -> configuration.isRuleEnabled(rule))
                    .filter(rule -> !(rule instanceof PreOrderRule) &&
                            shouldPushRule(rule))
                    .forEach(this::pushTransformTask);

            // push explore group for all groups this expression ranges over
            getExpression()
                    .getQuantifiers()
                    .stream()
                    .map(Quantifier::getRangesOver)
                    .forEach(this::pushExploreGroup);

            ruleSet.getRules(getExpression(), rule -> configuration.isRuleEnabled(rule))
                    .filter(rule -> rule instanceof PreOrderRule &&
                            shouldPushRule(rule))
                    .forEach(this::pushTransformTask);
        }

        protected abstract boolean shouldPushRule(@Nonnull CascadesRule<?> rule);

        private void pushTransformTask(@Nonnull CascadesRule<? extends RelationalExpression> rule) {
            taskStack.push(new TransformExpression(getPlannerPhase(), getGroup(), getExpression(), rule));
        }

        private void pushTransformMatchPartition(CascadesRule<? extends MatchPartition> rule) {
            taskStack.push(new TransformMatchPartition(getPlannerPhase(), getGroup(), getExpression(), rule));
        }

        private void pushExploreGroup(Reference rangesOver) {
            taskStack.push(new ExploreGroup(getPlannerPhase(), rangesOver));
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.ExploreExpressionEvent(getPlannerPhase(), currentRoot, taskStack, location, getGroup(),
                    getExpression());
        }

        @Override
        public String toString() {
            return "ExploreExpression(" + getGroup() + ")";
        }
    }

    /**
     * Explore Expression Task.
     * Simplified push/execute overview:
     * <pre>
     * {@link ExploreExpression}
     *     pushes
     *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
     *         all transformations ({@link TransformExpression} for current (group, expression)
     *         {@link ExploreGroup} for all ranged over groups
     * </pre>
     */
    private class ReExploreExpression extends AbstractExploreExpression {
        public ReExploreExpression(@Nonnull final PlannerPhase plannerPhase,
                                   @Nonnull final Reference group,
                                   @Nonnull final RelationalExpression expression) {
            super(plannerPhase, group, expression);
        }

        @Override
        protected boolean shouldPushRule(@Nonnull final CascadesRule<?> rule) {
            final Set<PlannerConstraint<?>> requirementDependencies = rule.getConstraintDependencies();
            final Reference group = getGroup();
            if (!group.isExploring()) {
                if (logger.isWarnEnabled()) {
                    logger.warn(KeyValueLogMessage.of("transformation task run on a group that is not being explored"));
                }
            }
            return group.isFullyExploring() || !group.isExploredForAttributes(requirementDependencies);
        }
    }

    /**
     * Explore Expression Task.
     * Simplified push/execute overview:
     * <pre>
     * {@link ExploreExpression}
     *     pushes
     *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
     *         all transformations ({@link TransformExpression} for current (group, expression)
     *         {@link ExploreGroup} for all ranged over groups
     * </pre>
     */
    private class ExploreExpression extends AbstractExploreExpression {
        public ExploreExpression(@Nonnull final PlannerPhase plannerPhase,
                                 @Nonnull final Reference group,
                                 @Nonnull final RelationalExpression expression) {
            super(plannerPhase, group, expression);
        }

        @Override
        protected boolean shouldPushRule(@Nonnull final CascadesRule<?> rule) {
            return true;
        }
    }


    /**
     * Abstract base class for all transformations. All transformations are defined on a subclass of
     * {@link RelationalExpression}, {@link PartialMatch}, of {@link MatchPartition}.
     */
    private abstract class AbstractTransform implements Task {
        @Nonnull
        private final PlannerPhase plannerPhase;
        @Nonnull
        private final Reference group;
        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final CascadesRule<?> rule;

        protected AbstractTransform(@Nonnull final PlannerPhase plannerPhase,
                                    @Nonnull final Reference group,
                                    @Nonnull final RelationalExpression expression,
                                    @Nonnull final CascadesRule<?> rule) {
            this.plannerPhase = plannerPhase;
            this.group = group;
            this.expression = expression;
            this.rule = rule;
        }

        @Nonnull
        @Override
        public PlannerPhase getPlannerPhase() {
            return plannerPhase;
        }

        @Nonnull
        public Reference getGroup() {
            return group;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        public CascadesRule<?> getRule() {
            return rule;
        }

        @Nonnull
        protected abstract Object getBindable();

        @Nonnull
        protected PlannerBindings getInitialBindings() {
            return new PlannerBindings.Builder()
                    .put(ReferenceMatchers.getTopReferenceMatcher(), currentRoot)
                    .put(ReferenceMatchers.getCurrentReferenceMatcher(), group)
                    .build();
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        protected boolean shouldExecute() {
            return true;
        }

        /**
         * Method that calls the actual rule and reacts to new constructs the rule yielded.
         * Simplified push/execute overview:
         * <pre>
         * executes rule
         * pushes
         *     {@link AdjustMatch} for each yielded {@link PartialMatch}
         *     {@link OptimizeInputs} followed by {@link ExploreExpression} for each yielded {@link RecordQueryPlan}
         *     {@link ExploreExpression} for each yielded {@link RelationalExpression} that is not a {@link RecordQueryPlan}
         * </pre>
         */
        @Override
        @SuppressWarnings("java:S1117")
        public void execute() {
            if (!shouldExecute()) {
                return;
            }

            final PlannerBindings initialBindings = getInitialBindings();

            final AtomicInteger numMatches = new AtomicInteger(0);

            rule.getMatcher()
                    .bindMatches(getConfiguration(), initialBindings, getBindable())
                    .map(bindings -> new CascadesRuleCall(plannerPhase, planContext, rule, group,
                            traversal, taskStack, bindings, evaluationContext))
                    .forEach(ruleCall -> {
                        int ruleMatchesCount = numMatches.incrementAndGet();
                        if (isMaxNumMatchesPerRuleCallExceeded(configuration, ruleMatchesCount)) {
                            throw new RecordQueryPlanComplexityException("Maximum number of matches per rule call has been exceeded")
                                    .addLogInfo(LogMessageKeys.RULE, ruleCall)
                                    .addLogInfo(LogMessageKeys.RULE_MATCHES_COUNT, ruleMatchesCount)
                                    .addLogInfo(LogMessageKeys.MAX_RULE_MATCHES_COUNT,
                                            configuration.getMaxNumMatchesPerRuleCall());
                        }
                        // we notify the debugger (if installed) that the transform task is succeeding and
                        // about begin and end of the rule call event
                        Debugger.withDebugger(debugger -> debugger.onEvent(toTaskEvent(Location.MATCH_PRE)));
                        Debugger.withDebugger(debugger ->
                                debugger.onEvent(new Debugger.TransformRuleCallEvent(plannerPhase, currentRoot,
                                        taskStack, Location.BEGIN, group, getBindable(), rule, ruleCall)));
                        try {
                            executeRuleCall(ruleCall);
                        } finally {
                            Debugger.withDebugger(debugger ->
                                    debugger.onEvent(new Debugger.TransformRuleCallEvent(plannerPhase, currentRoot,
                                            taskStack, Location.END, group, getBindable(), rule, ruleCall)));
                        }
                    });
        }

        protected void executeRuleCall(@Nonnull CascadesRuleCall ruleCall) {
            ruleCall.run();

            //
            // During rule construction, references can be added to the memoizer that don't end up in
            // any final expression. Prune them here
            //
            ruleCall.pruneUnusedNewReferences();

            //
            // Handle produced artifacts (through yield...() calls)
            //
            for (final PartialMatch newPartialMatch : ruleCall.getNewPartialMatches()) {
                Debugger.withDebugger(debugger ->
                        debugger.onEvent(new Debugger.TransformRuleCallEvent(plannerPhase, currentRoot, taskStack,
                                Location.YIELD, group, getBindable(), rule, ruleCall)));
                taskStack.push(new AdjustMatch(getPlannerPhase(), getGroup(), getExpression(), newPartialMatch));
            }

            for (final RelationalExpression newExpression : ruleCall.getNewFinalExpressions()) {
                Debugger.withDebugger(debugger ->
                        debugger.onEvent(new Debugger.TransformRuleCallEvent(plannerPhase, currentRoot, taskStack,
                                Location.YIELD, group, getBindable(), rule, ruleCall)));
                exploreExpressionAndOptimizeInputs(plannerPhase, getGroup(), newExpression, true);
            }

            for (final RelationalExpression newExpression : ruleCall.getNewExploratoryExpressions()) {
                Debugger.withDebugger(debugger ->
                        debugger.onEvent(new Debugger.TransformRuleCallEvent(plannerPhase, currentRoot, taskStack,
                                Location.YIELD, group, getBindable(), rule, ruleCall)));
                exploreExpression(plannerPhase, group, newExpression, true);
            }

            final var referencesWithPushedRequirements = ruleCall.getReferencesWithPushedConstraints();
            if (!referencesWithPushedRequirements.isEmpty()) {
                //
                // There are two distinct cases:
                // (1) the rule is a pre-order rule -- we can assume that all other rules rooted at this expression
                //     will follow after the exploration of the subgraph .
                // (2) the rule is not a pre-order rule -- we need to push a new task onto the stack after the
                //     re-exploration using the newly pushed requirement.
                //
                if (!(rule instanceof PreOrderRule)) {
                    taskStack.push(this);
                }
                for (final Reference reference : referencesWithPushedRequirements) {
                    if (!reference.hasNeverBeenExplored()) {
                        taskStack.push(new ExploreGroup(plannerPhase, reference));
                    }
                }
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.TransformEvent(plannerPhase, currentRoot, taskStack, location, getGroup(),
                    getBindable(), getRule());
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
        public TransformExpression(@Nonnull final PlannerPhase plannerPhase,
                                   @Nonnull final Reference group,
                                   @Nonnull final RelationalExpression expression,
                                   @Nonnull final CascadesRule<? extends RelationalExpression> rule) {
            super(plannerPhase, group, expression, rule);
        }

        @Nonnull
        @Override
        protected Object getBindable() {
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
            return PlannerBindings.newBuilder()   // TODO either put other bindings in here OR just call super
                    .putAll(super.getInitialBindings())
                    .build();
        }
    }

    /**
     * Class to transform a match partition using a rule.
     */
    private class TransformMatchPartition extends AbstractTransform {
        @Nonnull
        private final Supplier<MatchPartition> matchPartitionSupplier;

        public TransformMatchPartition(@Nonnull final PlannerPhase plannerPhase,
                                       @Nonnull final Reference group,
                                       @Nonnull final RelationalExpression expression,
                                       @Nonnull final CascadesRule<? extends MatchPartition> rule) {
            super(plannerPhase, group, expression, rule);
            this.matchPartitionSupplier = Suppliers.memoize(() -> MatchPartition.of(group, expression));
        }

        @Nonnull
        @Override
        protected Object getBindable() {
            return matchPartitionSupplier.get();
        }
    }

    /**
     * Class to transform a match partial match using a rule.
     */
    private class TransformPartialMatch extends AbstractTransform {
        @Nonnull
        private final PartialMatch partialMatch;

        public TransformPartialMatch(@Nonnull final PlannerPhase plannerPhase,
                                     @Nonnull final Reference group,
                                     @Nonnull final RelationalExpression expression,
                                     @Nonnull final PartialMatch partialMatch,
                                     @Nonnull final CascadesRule<? extends PartialMatch> rule) {
            super(plannerPhase, group, expression, rule);
            this.partialMatch = partialMatch;
        }

        @Nonnull
        @Override
        protected Object getBindable() {
            return partialMatch;
        }
    }

    /**
     * Adjust Match Task. Attempts to improve an existing partial match on a (group, expression) pair
     * to a better one by enqueuing rules defined on {@link PartialMatch}.
     * Simplified push/execute overview:
     * <pre>
     * {@link AdjustMatch}
     *     pushes
     *         all transformations ({@link TransformPartialMatch}) for current (group, expression, partial match)
     * </pre>
     */
    private class AdjustMatch extends ExploreTask {
        @Nonnull
        final PartialMatch partialMatch;

        public AdjustMatch(@Nonnull final PlannerPhase plannerPhase,
                           @Nonnull final Reference group,
                           @Nonnull final RelationalExpression expression,
                           @Nonnull final PartialMatch partialMatch) {
            super(plannerPhase, group, expression);
            this.partialMatch = partialMatch;
        }

        @Override
        public void execute() {
            final var ruleSet = getPlannerPhase().getRuleSet();
            ruleSet.getPartialMatchRules(rule -> configuration.isRuleEnabled(rule))
                    .forEach(rule ->
                            taskStack.push(new TransformPartialMatch(getPlannerPhase(), getGroup(), getExpression(),
                                    partialMatch, rule)));
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.AdjustMatchEvent(getPlannerPhase(), currentRoot, taskStack, location, getGroup(),
                    getExpression());
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
     * is done, and we can optimize the children (that is we can now prune the plan space of the children).
     * Simplified push/execute overview:
     * <pre>
     * {@link OptimizeInputs}
     *     pushes
     *         {@link OptimizeGroup} for all ranged over groups
     * </pre>
     */
    private class OptimizeInputs implements Task {
        @Nonnull
        private final PlannerPhase plannerPhase;
        @Nonnull
        private final Reference group;
        @Nonnull
        private final RelationalExpression expression;

        public OptimizeInputs(@Nonnull final PlannerPhase plannerPhase,
                              @Nonnull final Reference group,
                              @Nonnull final RelationalExpression expression) {
            this.plannerPhase = plannerPhase;
            this.group = group;
            this.expression = expression;
        }

        @Nonnull
        @Override
        public PlannerPhase getPlannerPhase() {
            return plannerPhase;
        }

        @Override
        public void execute() {
            if (!group.containsExactly(expression)) {
                return;
            }
            for (final Quantifier quantifier : expression.getQuantifiers()) {
                final Reference rangesOver = quantifier.getRangesOver();
                taskStack.push(new OptimizeGroup(plannerPhase, rangesOver));
            }
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.OptimizeInputsEvent(plannerPhase, currentRoot, taskStack, location, group, expression);
        }

        @Override
        public String toString() {
            return "OptimizeInputs(" + group + ")";
        }
    }
}
