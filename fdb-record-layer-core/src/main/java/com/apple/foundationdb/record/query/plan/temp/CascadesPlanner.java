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
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.expressions.NestedContextExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * A Cascades-style query planner that converts a {@link RecordQuery} to a {@link RecordQueryPlan}, possibly using
 * secondary indexes defined in a {@link RecordMetaData} to execute the query efficiently.
 *
 * <p>
 * <a href="https://15721.courses.cs.cmu.edu/spring2017/papers/15-optimizer2/graefe-ieee1995.pdf">Cascades</a> is a
 * framework for a query optimization introduced by Graefe in 1995. In Cascades, all parsed queries, query plans, and
 * intermediate state between the two are represented in a unified tree of {@link PlannerExpression}, which includes
 * types such as {@link RecordQueryPlan} and {@link com.apple.foundationdb.record.query.expressions.QueryComponent}.
 * This highly flexible data structure reifies essentially the entire state of the planner (i.e., partially planned
 * elements, current optimization, goals, etc.) and allows individual planning steps to be modular and stateless by
 * all state in the {@link PlannerExpression} tree.
 * </p>
 *
 * <p>
 * Like many optimization frameworks,Cascades is driven by {@link PlannerRule}s that describe a particular transformation
 * and encapsulate the logic for detecting when it can be applied and how to apply them. The planner searches through
 * its {@link PlannerRuleSet} to find a matching rule and then executes that rule, creating zero or more additional
 * {@code PlannerExpression}s. A rule is defined by:
 * </p>
 * <ol>
 *     <li>
 *         An {@link com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher} that defines a
 *         finite-depth tree of matchers that inspect the structure (i.e., the type-level information) of some sub-graph
 *         of the current planner expression.
 *     </li>
 *     <li>
 *         A {@link PlannerRule#onMatch(PlannerRuleCall)} method that is run for each successful match, producing zero
 *         or more new expressions.
 *     </li>
 * </ol>
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
 * a set of {@link PlannerExpression}s. In turn, {@link PlannerExpression}s have some number of <em>children</em>, each
 * of which is a {@link GroupExpressionRef} and which can be traversed by the planner via the
 * {@link PlannerExpression#getPlannerExpressionChildren()} method.
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
 * @see PlannerExpression
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
    private GroupExpressionRef<PlannerExpression> currentRoot;
    @Nonnull
    private Deque<Task> taskStack;

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState) {
        this(metaData, recordStoreState, PlannerRuleSet.ALL);
    }

    public CascadesPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState, @Nonnull PlannerRuleSet ruleSet) {
        this.metaData = metaData;
        this.recordStoreState = recordStoreState;
        this.ruleSet = ruleSet;
        // Placeholders until we get a query.
        this.currentRoot = GroupExpressionRef.EMPTY;
        this.taskStack = new ArrayDeque<>();
    }

    @Nonnull
    @Override
    public RecordQueryPlan plan(@Nonnull RecordQuery query) {
        final PlanContext context = new MetaDataPlanContext(metaData, recordStoreState, query);
        planPartial(context, RelationalPlannerExpression.fromRecordQuery(query));

        final PlannerExpression singleRoot = currentRoot.getMembers().iterator().next();
        if (singleRoot instanceof RecordQueryPlan) {
            return (RecordQueryPlan)singleRoot;
        } else {
            throw new RecordCoreException("Cascades planner could not plan query")
                    .addLogInfo("query", query)
                    .addLogInfo("finalExpression", currentRoot.get());
        }
    }

    @VisibleForTesting
    @Nonnull
    GroupExpressionRef<PlannerExpression> planPartial(@Nonnull PlanContext context, @Nonnull PlannerExpression initialPlannerExpression) {
        currentRoot = GroupExpressionRef.of(initialPlannerExpression);
        taskStack = new ArrayDeque<>();
        taskStack.push(new OptimizeGroup(context, currentRoot));
        while (!taskStack.isEmpty()) {
            Task nextTask = taskStack.pop();
            if (logger.isTraceEnabled()) {
                logger.trace(nextTask.toString());
            }
            nextTask.execute();
            if (logger.isTraceEnabled()) {
                logger.trace("Task stack size: " + taskStack.size());
                logger.trace(new GroupExpressionPrinter(currentRoot).toString());
            }
        }
        return currentRoot;
    }

    @Override
    public void setIndexScanPreference(@Nonnull IndexScanPreference indexScanPreference) {
        // nothing to do here, yet
    }

    private interface Task {
        void execute();
    }

    private class OptimizeGroup implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<PlannerExpression> group;

        @SuppressWarnings("unchecked")
        public OptimizeGroup(@Nonnull PlanContext context, @Nonnull ExpressionRef<? extends PlannerExpression> ref) {
            this.context = context;
            if (ref instanceof GroupExpressionRef) {
                this.group = (GroupExpressionRef<PlannerExpression>) ref;
            } else {
                throw new RecordCoreArgumentException("illegal non-group reference in group expression");
            }
        }

        public OptimizeGroup(@Nonnull PlanContext context, @Nonnull GroupExpressionRef<PlannerExpression> group) {
            this.context = context;
            this.group = group;
        }

        @Override
        public void execute() {
            if (!group.isExplored()) {
                // Explore the group, then come back here to pick an optimal expression.
                taskStack.push(this);
                for (PlannerExpression member : group.getMembers()) {
                    taskStack.push(new ExploreExpression(context, group, member));
                }
                group.setExplored();
            } else {
                // TODO this is very Volcano-style rather than Cascades, because there's no branch-and-bound pruning.
                PlannerExpression bestMember = null;
                for (PlannerExpression member : group.getMembers()) {
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
        public String toString() {
            return "OptimizeGroup(" + group + ")";
        }
    }

    private class ExploreExpression implements Task {
        @Nonnull
        protected final PlanContext context;
        @Nonnull
        protected final GroupExpressionRef<PlannerExpression> group;
        @Nonnull
        protected final PlannerExpression expression;

        public ExploreExpression(@Nonnull PlanContext context,
                                 @Nonnull GroupExpressionRef<PlannerExpression> group,
                                 @Nonnull PlannerExpression expression) {
            this.context = context;
            this.group = group;
            this.expression = expression;
        }

        @Nonnull
        protected PlannerRuleSet getRules() {
            return ruleSet;
        }

        protected void addTransformTask(@Nonnull PlannerRule<? extends PlannerExpression> rule) {
            taskStack.push(new Transform(context, group, expression, rule));
        }

        @Override
        public void execute() {
            // This is closely tied to the way that rule finding works _now_. Specifically, rules are indexed only
            // by the type of their _root_, not any of the stuff lower down. As a result, we have enough information
            // right here to determine the set of all possible rules that could ever be applied here, regardless of
            // what happens towards the leaves of the tree.
            getRules().getRulesMatching(expression).forEachRemaining(this::addTransformTask);

            final PlanContext relativeContext;
            if (expression instanceof NestedContextExpression) {
                relativeContext = context.asNestedWith(((NestedContextExpression)expression).getNestedContext());
            } else {
                relativeContext = context;
            }

            Iterator<? extends ExpressionRef<? extends PlannerExpression>> expressionChildren = expression.getPlannerExpressionChildren();
            while (expressionChildren.hasNext()) {
                taskStack.push(new ExploreGroup(relativeContext, expressionChildren.next()));
            }
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
        private final GroupExpressionRef<PlannerExpression> group;

        @SuppressWarnings("unchecked")
        public ExploreGroup(@Nonnull PlanContext context, @Nonnull ExpressionRef<? extends PlannerExpression> ref) {
            this.context = context;
            if (ref instanceof GroupExpressionRef) {
                this.group = (GroupExpressionRef<PlannerExpression>) ref;
            } else {
                throw new RecordCoreArgumentException("illegal non-group reference in group expression");
            }
        }

        @Override
        public void execute() {
            if (group.isExplored()) {
                return;
            }

            for (PlannerExpression expression : group.getMembers()) {
                taskStack.push(new ExploreExpression(context, group, expression));
            }

            // we'll never need to reschedule this, so we don't need to wait until the exploration is actually done.
            group.setExplored();
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
        private final GroupExpressionRef<PlannerExpression> group;
        @Nonnull
        private final PlannerExpression expression;
        @Nonnull
        private final PlannerRule<? extends PlannerExpression> rule;

        public Transform(@Nonnull PlanContext context,
                         @Nonnull GroupExpressionRef<PlannerExpression> group,
                         @Nonnull PlannerExpression expression,
                         @Nonnull PlannerRule<? extends PlannerExpression> rule) {
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
            expression.bindTo(rule.getMatcher()).map(bindings -> new CascadesRuleCall(context, rule, group, bindings))
                    .forEach(this::executeRuleCall);
        }

        private void executeRuleCall(@Nonnull CascadesRuleCall ruleCall) {
            ruleCall.run();
            for (PlannerExpression newExpression : ruleCall.getNewExpressions()) {
                if (newExpression instanceof QueryPlan) {
                    taskStack.push(new OptimizeInputs(context, group, newExpression));
                    taskStack.push(new ExploreExpression(context, group, newExpression));
                } else {
                    taskStack.push(new ExploreExpression(context, group, newExpression));
                }
            }
        }

        @Override
        public String toString() {
            return "Transform(" + rule.getClass().getSimpleName() + ")";
        }
    }

    private class OptimizeInputs implements Task {
        @Nonnull
        private final PlanContext context;
        @Nonnull
        private final GroupExpressionRef<PlannerExpression> group;
        @Nonnull
        private final PlannerExpression expression;

        public OptimizeInputs(@Nonnull PlanContext context,
                              @Nonnull GroupExpressionRef<PlannerExpression> group,
                              @Nonnull PlannerExpression expression) {
            this.context = context;
            this.group = group;
            this.expression = expression;
        }

        @Override
        public void execute() {
            if (!group.containsExactly(expression)) {
                return;
            }

            final PlanContext relativeContext;
            if (expression instanceof NestedContextExpression) {
                relativeContext = context.asNestedWith(((NestedContextExpression)expression).getNestedContext());
            } else {
                relativeContext = context;
            }

            Iterator<? extends ExpressionRef<? extends PlannerExpression>> expressionChildren = expression.getPlannerExpressionChildren();
            while (expressionChildren.hasNext()) {
                taskStack.push(new OptimizeGroup(relativeContext, expressionChildren.next()));
            }
        }

        @Override
        public String toString() {
            return "OptimizeInputs(" + group + ")";
        }
    }
}
