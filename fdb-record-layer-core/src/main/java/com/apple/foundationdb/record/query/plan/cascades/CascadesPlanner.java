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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.RecordQuery;
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
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
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
 * types such as {@link RecordQueryPlan} and {@link com.apple.foundationdb.record.query.expressions.QueryComponent}.
 * This highly flexible data structure reifies essentially the entire state of the planner (i.e., partially planned
 * elements, current optimization, goals, etc.) and allows individual planning steps to be modular and stateless by
 * keeping all state in the {@link RelationalExpression} tree.
 * </p>
 *
 * <p>
 * Like many optimization frameworks, Cascades is driven by sets of {@link CascadesRule}s that can be defined for
 * {@link RelationalExpression}s, {@link PartialMatch}es and {@link MatchPartition}s, each of which describes a
 * particular transformation and encapsulates the logic for determining its applicability and applying it. The planner
 * searches through its {@link PlannerRuleSet} to find a matching rule and then executes that rule, creating zero or
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
 * Simplified enqueue/execute overview:
 * </p>
 *
 * <pre>
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
 *         all transformations ({@link TransformExpression}) for current (group, expression)
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
 * </pre>
 *
 * Note: Enqueued tasks are executed in typical stack machine order, that is LIFO.
 * <br>
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
 *         a given group but it is guaranteed to only be executed once for a (group, expression) pair.
 *         The root for the corresponding rules is always of type {@link MatchPartition}. These are the rules that react
 *         to all synthesized matches for an expression at once.
 *     </li>
 * </ul>
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
    private final PlannerRuleSet ruleSet;
    @Nonnull
    private Reference currentRoot;
    @Nonnull
    private Traversal traversal;
    @Nonnull
    private Deque<Task> taskStack; // Use a Dequeue instead of a Stack because we don't need synchronization.
    // total tasks executed for the current plan
    private int taskCount;
    // max size of the task queue encountered during the planning
    private int maxQueueSize;

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState) {
        this(metaData, recordStoreState, defaultPlannerRuleSet());
    }

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState, @Nonnull PlannerRuleSet ruleSet) {
        this.configuration = RecordQueryPlannerConfiguration.builder().build();
        this.metaData = metaData;
        this.recordStoreState = recordStoreState;
        this.ruleSet = ruleSet;
        // Placeholders until we get a query.
        this.currentRoot = Reference.empty();
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

    @Nonnull
    @Override
    public QueryPlanResult planQuery(@Nonnull final RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
        RecordQueryPlan plan = plan(query, parameterRelationshipGraph);
        final var constraints = QueryPlanConstraint.collectConstraints(plan);
        QueryPlanInfo info = QueryPlanInfo.newBuilder()
                .put(QueryPlanInfoKeys.TOTAL_TASK_COUNT, taskCount)
                .put(QueryPlanInfoKeys.MAX_TASK_QUEUE_SIZE, maxQueueSize)
                .put(QueryPlanInfoKeys.CONSTRAINTS, constraints)
                .build();
        return new QueryPlanResult(plan, info);
    }

    @Nonnull
    @Override
    public RecordQueryPlan plan(@Nonnull RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
        try {
            planPartial(() -> Reference.of(RelationalExpression.fromRecordQuery(metaData, query)),
                    rootReference -> MetaDataPlanContext.forRecordQuery(configuration, metaData, recordStoreState, query),
                    EvaluationContext.empty());
            return resultOrFail();
        } finally {
            Debugger.withDebugger(Debugger::onDone);
        }

    }

    @Nonnull
    public QueryPlanResult planGraph(@Nonnull Supplier<Reference> referenceSupplier,
                                     @Nonnull final Optional<Collection<String>> allowedIndexesOptional,
                                     @Nonnull final IndexQueryabilityFilter indexQueryabilityFilter,
                                     @Nonnull final EvaluationContext evaluationContext) {
        try {
            planPartial(referenceSupplier,
                    rootReference ->
                            MetaDataPlanContext.forRootReference(configuration,
                                    metaData,
                                    recordStoreState,
                                    rootReference,
                                    allowedIndexesOptional,
                                    indexQueryabilityFilter
                            ),
                    evaluationContext);
            final var plan = resultOrFail();
            final var constraints = QueryPlanConstraint.collectConstraints(plan);
            return new QueryPlanResult(plan, QueryPlanInfo.newBuilder().put(QueryPlanInfoKeys.CONSTRAINTS, constraints).build());
        } finally {
            Debugger.withDebugger(Debugger::onDone);
        }
    }

    private RecordQueryPlan resultOrFail() {
        final RelationalExpression singleRoot = currentRoot.getMembers().iterator().next();
        if (singleRoot instanceof RecordQueryPlan) {
            if (logger.isDebugEnabled()) {
                logger.debug(KeyValueLogMessage.of("explain of plan",
                        "explain", PlannerGraphProperty.explain(singleRoot)));
            }

            return (RecordQueryPlan)singleRoot;
        } else {
            throw new RecordCoreException("Cascades planner could not plan query")
                    .addLogInfo("finalExpression", currentRoot.get());
        }
    }

    private void planPartial(@Nonnull Supplier<Reference> referenceSupplier,
                             @Nonnull Function<Reference, PlanContext> contextCreatorFunction,
                             @Nonnull final EvaluationContext evaluationContext) {
        currentRoot = referenceSupplier.get();
        final var context = contextCreatorFunction.apply(currentRoot);

        final RelationalExpression expression = currentRoot.get();
        Debugger.withDebugger(debugger -> debugger.onQuery(expression.toString(), context));
        traversal = Traversal.withRoot(currentRoot);
        taskStack = new ArrayDeque<>();
        taskStack.push(new OptimizeGroup(context, currentRoot, evaluationContext));
        taskCount = 0;
        maxQueueSize = 0;
        while (!taskStack.isEmpty()) {
            try {
                Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.ExecutingTaskEvent(currentRoot, taskStack, Objects.requireNonNull(taskStack.peek()))));
                if (isTaskTotalCountExceeded(configuration, taskCount)) {
                    throw new RecordQueryPlanComplexityException("Maximum number of tasks (" + configuration.getMaxTotalTaskCount() + ") was exceeded");
                }
                taskCount++;

                Task nextTask = taskStack.pop();
                if (logger.isTraceEnabled()) {
                    logger.trace(KeyValueLogMessage.of("executing task", "nextTask", nextTask.toString()));
                }

                Debugger.withDebugger(debugger -> debugger.onEvent(nextTask.toTaskEvent(Location.BEGIN)));
                try {
                    nextTask.execute();
                } finally {
                    Debugger.withDebugger(debugger -> debugger.onEvent(nextTask.toTaskEvent(Location.END)));
                }

                if (logger.isTraceEnabled()) {
                    logger.trace(KeyValueLogMessage.of("planner state",
                            "taskStackSize", taskStack.size(),
                            "memo", new ReferencePrinter(currentRoot)));
                }

                maxQueueSize = Math.max(maxQueueSize, taskStack.size());
                if (isTaskQueueSizeExceeded(configuration, taskStack.size())) {
                    throw new RecordQueryPlanComplexityException("Maximum task queue size (" + configuration.getMaxTaskQueueSize() + ") was exceeded");
                }
            } catch (final RestartException restartException) {
                if (logger.isTraceEnabled()) {
                    logger.trace(KeyValueLogMessage.of("debugger requests restart of planning",
                            "taskStackSize", taskStack.size(),
                            "memo", new ReferencePrinter(currentRoot)));
                }
                taskStack.clear();
                currentRoot = referenceSupplier.get();
                taskStack.push(new OptimizeGroup(context, currentRoot, evaluationContext));
            }
        }
    }

    private void exploreExpressionAndOptimizeInputs(@Nonnull PlanContext context,
                                                    @Nonnull Reference group,
                                                    @Nonnull final RelationalExpression expression,
                                                    final boolean forceExploration,
                                                    @Nonnull final EvaluationContext evaluationContext) {
        if (expression instanceof QueryPlan) {
            taskStack.push(new OptimizeInputs(context, group, expression, evaluationContext));
        }
        exploreExpression(context, group, expression, forceExploration, evaluationContext);
    }

    private void exploreExpression(@Nonnull PlanContext context,
                                   @Nonnull Reference group,
                                   @Nonnull final RelationalExpression expression,
                                   final boolean forceExploration,
                                   @Nonnull final EvaluationContext evaluationContext) {
        Verify.verify(group.containsExactly(expression));
        if (forceExploration) {
            taskStack.push(new ExploreExpression(context, group, expression, evaluationContext));
        }  else {
            taskStack.push(new ReExploreExpression(context, group, expression, evaluationContext));
        }
    }

    /**
     * Represents actual tasks in the task stack of the planner.
     */
    public interface Task {
        void execute();

        Debugger.Event toTaskEvent(Location location);
    }

    /**
     * Optimize Group task.
     * <br>
     * Simplified enqueue/execute overview:
     * <br>
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
        private final Reference group;
        @Nonnull
        private final EvaluationContext evaluationContext;

        public OptimizeGroup(@Nonnull PlanContext context,
                             @Nonnull Reference group,
                             @Nonnull final EvaluationContext evaluationContext) {
            this.context = context;
            this.group = group;
            this.evaluationContext = evaluationContext;
        }

        @Override
        public void execute() {
            if (group.needsExploration()) {
                // Explore the group, then come back here to pick an optimal expression.
                taskStack.push(this);
                for (RelationalExpression member : group.getMembers()) {
                    // enqueue explore expression which then in turn enqueues necessary rules for transformations
                    // and matching
                    exploreExpressionAndOptimizeInputs(context, group, member, false, evaluationContext);
                }
                // the second time around we want to visit the else and prune the plan space
                group.startExploration();
            } else {
                RelationalExpression bestMember = null;
                for (RelationalExpression member : group.getMembers()) {
                    if (bestMember == null || new CascadesCostModel(configuration).compare(member, bestMember) < 0) {
                        if (bestMember != null) {
                            // best member is being pruned
                            traversal.removeExpression(group, bestMember);
                        }
                        bestMember = member;
                    } else {
                        // member is being pruned
                        traversal.removeExpression(group, member);
                    }
                }
                if (bestMember == null) {
                    throw new RecordCoreException("there we no members in a group expression used by the Cascades planner");
                }

                group.pruneWith(bestMember);
                group.commitExploration();
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
     * <br>
     * Simplified enqueue/execute overview:
     * <br>
     * {@link ExploreGroup}
     *     enqueues
     *         {@link ExploreExpression} for each group member
     *     sets explored to {@code true}
     */
    private class ExploreGroup implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final Reference group;
        @Nonnull
        private final EvaluationContext evaluationContext;

        public ExploreGroup(@Nonnull PlanContext context,
                            @Nonnull Reference ref,
                            @Nonnull final EvaluationContext evaluationContext) {
            this.context = context;
            this.group = ref;
            this.evaluationContext = evaluationContext;
        }

        @Override
        public void execute() {
            if (group.needsExploration()) {
                taskStack.push(this);
                for (final RelationalExpression expression : group.getMembers()) {
                    exploreExpressionAndOptimizeInputs(context, group, expression, false, evaluationContext);
                }
                group.startExploration();
            } else {
                group.commitExploration();
            }
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
        private final Reference group;
        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final EvaluationContext evaluationContext;

        public ExploreTask(@Nonnull PlanContext context,
                           @Nonnull Reference group,
                           @Nonnull RelationalExpression expression,
                           @Nonnull final EvaluationContext evaluationContext) {
            this.context = context;
            this.group = group;
            this.expression = expression;
            this.evaluationContext = evaluationContext;
        }

        @Nonnull
        public PlanContext getContext() {
            return context;
        }

        @Nonnull
        public EvaluationContext getEvaluationContext() {
            return evaluationContext;
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
        protected PlannerRuleSet getRules() {
            return ruleSet;
        }
    }

    /**
     * Explore Expression Task.
     * <br>
     * Simplified enqueue/execute overview:
     * <br>
     * {@link ExploreExpression}
     *     enqueues
     *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
     *         all transformations ({@link TransformExpression} for current (group, expression)
     *         {@link ExploreGroup} for all ranged over groups
     */
    private abstract class AbstractExploreExpression extends ExploreTask {
        public AbstractExploreExpression(@Nonnull PlanContext context,
                                         @Nonnull Reference group,
                                         @Nonnull RelationalExpression expression,
                                         @Nonnull final EvaluationContext evaluationContext) {
            super(context, group, expression, evaluationContext);
        }

        @Override
        public void execute() {
            // Enqueue all rules that need to run after all exploration for a (group, expression) pair is done.
            ruleSet.getMatchPartitionRules(rule -> configuration.isRuleEnabled(rule))
                    .filter(this::shouldEnqueueRule)
                    .forEach(this::enqueueTransformMatchPartition);

            // This is closely tied to the way that rule finding works _now_. Specifically, rules are indexed only
            // by the type of their _root_, not any of the stuff lower down. As a result, we have enough information
            // right here to determine the set of all possible rules that could ever be applied here, regardless of
            // what happens towards the leaves of the tree.
            ruleSet.getExpressionRules(getExpression(), rule -> configuration.isRuleEnabled(rule))
                    .filter(rule -> !(rule instanceof PreOrderRule) &&
                                    shouldEnqueueRule(rule))
                    .forEach(this::enqueueTransformTask);

            // Enqueue explore group for all groups this expression ranges over
            getExpression()
                    .getQuantifiers()
                    .stream()
                    .map(Quantifier::getRangesOver)
                    .forEach(this::enqueueExploreGroup);

            ruleSet.getExpressionRules(getExpression(), rule -> configuration.isRuleEnabled(rule))
                    .filter(rule -> rule instanceof PreOrderRule &&
                                    shouldEnqueueRule(rule))
                    .forEach(this::enqueueTransformTask);
        }

        protected abstract boolean shouldEnqueueRule(@Nonnull CascadesRule<?> rule);

        private void enqueueTransformTask(@Nonnull CascadesRule<? extends RelationalExpression> rule) {
            taskStack.push(new TransformExpression(getContext(), getGroup(), getExpression(), rule, getEvaluationContext()));
        }

        private void enqueueTransformMatchPartition(CascadesRule<? extends MatchPartition> rule) {
            taskStack.push(new TransformMatchPartition(getContext(), getGroup(), getExpression(), rule, getEvaluationContext()));
        }

        private void enqueueExploreGroup(Reference rangesOver) {
            taskStack.push(new ExploreGroup(getContext(), rangesOver, getEvaluationContext()));
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
     * Explore Expression Task.
     * <br>
     * Simplified enqueue/execute overview:
     * <br>
     * {@link ExploreExpression}
     *     enqueues
     *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
     *         all transformations ({@link TransformExpression} for current (group, expression)
     *         {@link ExploreGroup} for all ranged over groups
     */
    private class ReExploreExpression extends AbstractExploreExpression {
        public ReExploreExpression(@Nonnull PlanContext context,
                                   @Nonnull Reference group,
                                   @Nonnull RelationalExpression expression,
                                   @Nonnull final EvaluationContext evaluationContext) {
            super(context, group, expression, evaluationContext);
        }

        @Override
        protected boolean shouldEnqueueRule(@Nonnull CascadesRule<?> rule) {
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
     * <br>
     * Simplified enqueue/execute overview:
     * <br>
     * {@link ExploreExpression}
     *     enqueues
     *         all transformations ({@link TransformMatchPartition}) for match partitions of current (group, expression)
     *         all transformations ({@link TransformExpression} for current (group, expression)
     *         {@link ExploreGroup} for all ranged over groups
     */
    private class ExploreExpression extends AbstractExploreExpression {
        public ExploreExpression(@Nonnull PlanContext context,
                                 @Nonnull Reference group,
                                 @Nonnull RelationalExpression expression,
                                 @Nonnull final EvaluationContext evaluationContext) {
            super(context, group, expression, evaluationContext);
        }

        @Override
        protected boolean shouldEnqueueRule(@Nonnull CascadesRule<?> rule) {
            return true;
        }

        @Override
        public String toString() {
            return "ExploreExpression(" + getGroup() + ")";
        }
    }


    /**
     * Abstract base class for all transformations. All transformations are defined on a sub class of
     * {@link RelationalExpression}, {@link PartialMatch}, of {@link MatchPartition}.
     */
    private abstract class AbstractTransform implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final Reference group;
        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final CascadesRule<?> rule;
        @Nonnull
        private final EvaluationContext evaluationContext;

        protected AbstractTransform(@Nonnull PlanContext context,
                                    @Nonnull Reference group,
                                    @Nonnull RelationalExpression expression,
                                    @Nonnull CascadesRule<?> rule,
                                    @Nonnull final EvaluationContext evaluationContext) {
            this.context = context;
            this.group = group;
            this.expression = expression;
            this.rule = rule;
            this.evaluationContext = evaluationContext;
        }

        @Nonnull
        public PlanContext getContext() {
            return context;
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
            return PlannerBindings.from(ReferenceMatchers.getTopReferenceMatcher(), currentRoot);
        }

        protected boolean shouldExecute() {
            return true;
        }

        /**
         * Method that calls the actual rule and reacts to new constructs the rule yielded.
         * <br>
         * Simplified enqueue/execute overview:
         * <br>
         * executes rule
         * enqueues
         *     {@link AdjustMatch} for each yielded {@link PartialMatch}
         *     {@link OptimizeInputs} followed by {@link ExploreExpression} for each yielded {@link RecordQueryPlan}
         *     {@link ExploreExpression} for each yielded {@link RelationalExpression} that is not a {@link RecordQueryPlan}
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
                    .map(bindings -> new CascadesRuleCall(getContext(), rule, group, traversal, taskStack, bindings, evaluationContext))
                    .forEach(ruleCall -> {
                        if (isMaxNumMatchesPerRuleCallExceeded(configuration, numMatches.incrementAndGet())) {
                            throw new RecordQueryPlanComplexityException("Maximum number of matches per rule call for " + rule + " of " + configuration.getMaxNumMatchesPerRuleCall() + " has been exceeded.");
                        }
                        // we notify the debugger (if installed) that the transform task is succeeding and
                        // about begin and end of the rule call event
                        Debugger.withDebugger(debugger -> debugger.onEvent(toTaskEvent(Location.MATCH_PRE)));
                        Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.TransformRuleCallEvent(currentRoot, taskStack, Location.BEGIN, group, getBindable(), rule, ruleCall)));
                        try {
                            executeRuleCall(ruleCall);
                        } finally {
                            Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.TransformRuleCallEvent(currentRoot, taskStack, Location.END, group, getBindable(), rule, ruleCall)));
                        }
                    });
        }

        protected void executeRuleCall(@Nonnull CascadesRuleCall ruleCall) {
            ruleCall.run();

            //
            // Handle produced artifacts (through yield...() calls)
            //
            for (final PartialMatch newPartialMatch : ruleCall.getNewPartialMatches()) {
                Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.TransformRuleCallEvent(currentRoot, taskStack, Location.YIELD, group, getBindable(), rule, ruleCall)));
                taskStack.push(new AdjustMatch(getContext(), getGroup(), getExpression(), newPartialMatch, evaluationContext));
            }

            for (final RelationalExpression newExpression : ruleCall.getNewExpressions()) {
                Debugger.withDebugger(debugger -> debugger.onEvent(new Debugger.TransformRuleCallEvent(currentRoot, taskStack, Location.YIELD, group, getBindable(), rule, ruleCall)));
                exploreExpressionAndOptimizeInputs(getContext(), getGroup(), newExpression, true, evaluationContext);
            }

            final var referencesWithPushedRequirements = ruleCall.getReferencesWithPushedRequirements();
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
                        taskStack.push(new ExploreGroup(context, reference, evaluationContext));
                    }
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
                                   @Nonnull Reference group,
                                   @Nonnull RelationalExpression expression,
                                   @Nonnull CascadesRule<? extends RelationalExpression> rule,
                                   @Nonnull final EvaluationContext evaluationContext) {
            super(context, group, expression, rule, evaluationContext);
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

        public TransformMatchPartition(@Nonnull PlanContext context,
                                       @Nonnull Reference group,
                                       @Nonnull RelationalExpression expression,
                                       @Nonnull CascadesRule<? extends MatchPartition> rule,
                                       @Nonnull final EvaluationContext evaluationContext) {
            super(context, group, expression, rule, evaluationContext);
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

        public TransformPartialMatch(@Nonnull PlanContext context,
                                     @Nonnull Reference group,
                                     @Nonnull RelationalExpression expression,
                                     @Nonnull PartialMatch partialMatch,
                                     @Nonnull CascadesRule<? extends PartialMatch> rule,
                                     @Nonnull final EvaluationContext evaluationContext) {
            super(context, group, expression, rule, evaluationContext);
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
     * <br>
     * Simplified enqueue/execute overview:
     * <br>
     * {@link AdjustMatch}
     *     enqueues
     *         all transformations ({@link TransformPartialMatch}) for current (group, expression, partial match)
     */
    private class AdjustMatch extends ExploreTask {
        @Nonnull
        final PartialMatch partialMatch;

        public AdjustMatch(@Nonnull final PlanContext context,
                           @Nonnull final Reference group,
                           @Nonnull final RelationalExpression expression,
                           @Nonnull final PartialMatch partialMatch,
                           @Nonnull final EvaluationContext evaluationContext) {
            super(context, group, expression, evaluationContext);
            this.partialMatch = partialMatch;
        }

        @Override
        public void execute() {
            ruleSet.getPartialMatchRules(rule -> configuration.isRuleEnabled(rule))
                    .forEach(rule -> taskStack.push(new TransformPartialMatch(getContext(), getGroup(), getExpression(), partialMatch, rule, getEvaluationContext())));
        }

        @Override
        public Debugger.Event toTaskEvent(final Location location) {
            return new Debugger.AdjustMatchEvent(currentRoot, taskStack, location, getGroup(), getExpression());
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
     * <br>
     * Simplified enqueue/execute overview:
     * <br>
     * {@link OptimizeInputs}
     *     enqueues
     *         {@link OptimizeGroup} for all ranged over groups
     */
    private class OptimizeInputs implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final Reference group;
        @Nonnull
        private final RelationalExpression expression;

        @Nonnull final EvaluationContext evaluationContext;

        public OptimizeInputs(@Nonnull PlanContext context,
                              @Nonnull Reference group,
                              @Nonnull RelationalExpression expression,
                              @Nonnull final EvaluationContext evaluationContext) {
            this.context = context;
            this.group = group;
            this.expression = expression;
            this.evaluationContext = evaluationContext;
        }

        @Override
        public void execute() {
            if (!group.containsExactly(expression)) {
                return;
            }
            for (final Quantifier quantifier : expression.getQuantifiers()) {
                final Reference rangesOver = quantifier.getRangesOver();
                taskStack.push(new OptimizeGroup(context, rangesOver, evaluationContext));
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

    /**
     * Returns the default set of transformation rules.
     * @return a {@link PlannerRuleSet} using the default set of transformation rules
     */
    @Nonnull
    public static PlannerRuleSet defaultPlannerRuleSet() {
        return PlannerRuleSet.DEFAULT;
    }
}
