/*
 * Debugger.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp.debug;

import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.temp.Bindable;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner.Task;
import com.apple.foundationdb.record.query.plan.temp.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.MatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Deque;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;

/**
 * This interface functions as a stub providing hooks which can be called from the planner logic during planning.
 * As the planner is currently single-threaded as per planning of a query, we keep an instance of an implementor of
 * this class in the thread-local. (per-thread singleton).
 * The main mean of communication with the debugger is the set of statics defined within this interface.
 *
 * <b>Debugging functionality should only be enabled in test cases, never in deployments</b>.
 *
 * In order to enable debugging capabilities, clients should use {@link #setDebugger} which sets a debugger to be used
 * for the current thread. Once set, the planner starts interacting with the debugger in order to communicate important
 * state changes, like <em>begin of planning</em>, <em>end of planner</em>, etc.
 *
 * Clients using the debugger should never hold on/manage/use an instance of a debugger directly. Instead clients
 * should use {@link #withDebugger} and {@link #mapDebugger}to invoke methods on the currently installed debugger.
 * There is a guarantee that {@link #withDebugger} does not invoke any given action if there is no debugger currently
 * set for this thread. In this way, the planner implementation can freely call debug hooks which never will incur any
 * penalties (performance or otherwise) for a production deployment.
 */
@SuppressWarnings("java:S1214")
public interface Debugger {
    /**
     * The thread local variable. This constructor by itself does not set anything within the thread locals of
     * the lading thread.
     * TODO make this private when we use Java 11
     */
    ThreadLocal<Debugger> THREAD_LOCAL = new ThreadLocal<>();

    /**
     * Set the debugger. Override the currently set debugger if necessary.
     * @param debugger the new debugger
     */
    static void setDebugger(final Debugger debugger) {
        THREAD_LOCAL.set(debugger);
    }

    @Nullable
    static Debugger getDebugger() {
        return THREAD_LOCAL.get();
    }

    /**
     * Invoke the {@link Consumer} on the currently set debugger. Do not do anything if there is no debugger set.
     * @param action consumer to invoke
     */
    static void withDebugger(@Nonnull final Consumer<Debugger> action) {
        final Debugger debugger = getDebugger();
        if (debugger != null) {
            action.accept(debugger);
        }
    }

    /**
     * Apply the {@link Function} on the currently set debugger. Do not do anything if there is no debugger set.
     * @param function function to apply
     * @param <T> the type {@code function} produces
     * @return {@code Optional.empty()} if there is no debugger currently set for this thread or if the function
     *         returned {@code null}, {@code Optional.of(result)} where {@code result} is the result of applying
     *         {@code function}, otherwise.
     */
    @Nonnull
    static <T> Optional<T> mapDebugger(@Nonnull final Function<Debugger, T> function) {
        final Debugger debugger = getDebugger();
        if (debugger != null) {
            return Optional.ofNullable(function.apply(debugger));
        }
        return Optional.empty();
    }

    static void install() {
        withDebugger(Debugger::onInstall);
    }

    static void setup() {
        withDebugger(Debugger::onSetup);
    }

    static void query(final RecordQuery recordQuery, final PlanContext planContext) {
        withDebugger(debugger -> debugger.onQuery(recordQuery, planContext));
    }

    static Optional<Integer> getIndexOptional(Class<?> clazz) {
        return mapDebugger(debugger -> debugger.onGetIndex(clazz));
    }

    @Nonnull
    @CanIgnoreReturnValue
    static Optional<Integer> updateIndex(Class<?> clazz, IntUnaryOperator updateFn) {
        return mapDebugger(debugger -> debugger.onUpdateIndex(clazz, updateFn));
    }

    static void registerExpression(RelationalExpression expression) {
        withDebugger(debugger -> debugger.onRegisterExpression(expression));
    }

    static void registerReference(ExpressionRef<? extends RelationalExpression> reference) {
        withDebugger(debugger -> debugger.onRegisterReference(reference));
    }

    static void registerQuantifier(Quantifier quantifier) {
        withDebugger(debugger -> debugger.onRegisterQuantifier(quantifier));
    }

    @Nullable
    String nameForObject(@Nonnull final Object object);

    void onEvent(Event event);

    void onDone();

    int onGetIndex(@Nonnull Class<?> clazz);

    int onUpdateIndex(@Nonnull Class<?> clazz, @Nonnull final IntUnaryOperator updateFn);

    void onRegisterExpression(@Nonnull RelationalExpression expression);

    void onRegisterReference(@Nonnull ExpressionRef<? extends RelationalExpression> reference);

    void onRegisterQuantifier(@Nonnull Quantifier quantifier);

    void onInstall();

    void onSetup();

    void onQuery(final RecordQuery recordQuery, final PlanContext planContext);

    /**
     * Shorthands to identify a kind of event.
     */
    enum Shorthand {
        TASK,
        OPTGROUP,
        EXPEXP,
        EXPGROUP,
        MATCHEXP,
        MATCHEXPCAND,
        OPTINPUTS,
        RULECALL,
        TRANSFORM
    }

    /**
     * Enum to indicate where an event happened.
     */
    enum Location {
        ANY,
        BEGIN,
        END,
        SUCCESS,
        FAILURE
    }

    /**
     * Tag interface for all events.
     */
    interface Event {
        /**
         * Getter.
         * @return description of an event
         */
        @Nonnull
        String getDescription();

        /**
         * Getter.
         *
         * @return the shorthand for the event. This is the string used for interaction on the command line, e.g.
         *         setting a breakpoint, etc.
         */
        @Nonnull
        Shorthand getShorthand();

        /**
         * Getter.
         *
         * @return the location of where the event came from
         */
        @Nonnull
        Location getLocation();
    }

    /**
     * Interface for events that hold a root reference.
     */
    interface EventWithState extends Event {
        /**
         * Getter.
         * @return the root reference of the event
         */
        @Nonnull
        GroupExpressionRef<? extends RelationalExpression> getRootReference();


        /**
         * Getter.
         *
         * @return the current task stack of the planner
         */
        @Nonnull
        Deque<Task> getTaskStack();
    }

    /**
     * Abstract event class to capture {@code rootReference} amd {@code taskStack}.
     */
    abstract class AbstractEventWithState implements EventWithState {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> rootReference;

        @Nonnull
        private final Deque<Task> taskStack;

        @Nonnull
        private final Location location;

        public AbstractEventWithState(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                      @Nonnull final Deque<Task> taskStack,
                                      @Nonnull final Location location) {
            this.rootReference = rootReference;
            this.taskStack = taskStack;
            this.location = location;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getRootReference() {
            return rootReference;
        }

        @Nonnull
        @Override
        public Deque<Task> getTaskStack() {
            return taskStack;
        }

        @Nonnull
        @Override
        public Location getLocation() {
            return location;
        }
    }

    /**
     * Interface for events that hold a group ref.
     */
    interface EventWithCurrentGroupReference extends EventWithState {
        /**
         * Getter.
         * @return the current reference of the event.
         */
        @Nonnull
        ExpressionRef<? extends RelationalExpression> getCurrentGroupReference();
    }

    /**
     * Events of this class are generated every time the planner executes a task.
     */
    class ExecutingTaskEvent extends AbstractEventWithState {
        @Nonnull
        private final Task task;

        public ExecutingTaskEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                  @Nonnull final Deque<Task> taskStack,
                                  @Nonnull final Task task) {
            super(rootReference, taskStack, Location.ANY);
            this.task = task;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "executing task";
        }

        @Override
        @Nonnull
        public Shorthand getShorthand() {
            return Shorthand.TASK;
        }

        @Nonnull
        public Task getTask() {
            return task;
        }
    }

    /**
     * Events of this class are generated when the planner optimizes a group.
     */
    class OptimizeGroupEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> currentGroupReference;

        public OptimizeGroupEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                  @Nonnull final Deque<Task> taskStack,
                                  @Nonnull final Location location,
                                  @Nonnull final GroupExpressionRef<? extends RelationalExpression> currentGroupReference) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "optimizing group";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.OPTGROUP;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getCurrentGroupReference() {
            return currentGroupReference;
        }
    }

    /**
     * Events of this class are generated when the planner explores an expression.
     */
    class ExploreExpressionEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> currentGroupReference;
        @Nonnull
        private final RelationalExpression expression;

        public ExploreExpressionEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                      @Nonnull final Deque<Task> taskStack,
                                      @Nonnull final Location location,
                                      @Nonnull final GroupExpressionRef<? extends RelationalExpression> currentGroupReference,
                                      @Nonnull final RelationalExpression expression) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
            this.expression = expression;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "explore expression";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.EXPEXP;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getCurrentGroupReference() {
            return currentGroupReference;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }
    }

    /**
     * Events of this class are generated when the planner explores a group.
     */
    class ExploreGroupEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> currentGroupReference;

        public ExploreGroupEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                 @Nonnull final Deque<Task> taskStack,
                                 @Nonnull final Location location,
                                 @Nonnull final GroupExpressionRef<? extends RelationalExpression> currentGroupReference) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "explore group";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.EXPGROUP;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getCurrentGroupReference() {
            return currentGroupReference;
        }
    }

    /**
     * Events of this class are generated when the planner transforms an expression using a rule.
     */
    class TransformEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> currentGroupReference;
        @Nonnull
        private final Bindable bindable;
        @Nonnull
        private final PlannerRule<? extends Bindable> rule;

        public TransformEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                              @Nonnull final Deque<Task> taskStack,
                              @Nonnull final Location location,
                              @Nonnull final GroupExpressionRef<? extends RelationalExpression> currentGroupReference,
                              @Nonnull final Bindable bindable,
                              @Nonnull final PlannerRule<? extends Bindable> rule) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
            this.bindable = bindable;
            this.rule = rule;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "transform";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.TRANSFORM;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getCurrentGroupReference() {
            return currentGroupReference;
        }

        @Nonnull
        public Bindable getBindable() {
            return bindable;
        }

        @Nonnull
        public PlannerRule<? extends Bindable> getRule() {
            return rule;
        }
    }

    /**
     * Events of this class are generated when the planner calls a transformation rule.
     */
    class TransformRuleCallEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> currentGroupReference;
        @Nonnull
        private final Bindable bindable;
        @Nonnull
        private final PlannerRule<? extends Bindable> rule;
        @Nonnull
        private final CascadesRuleCall ruleCall;

        public TransformRuleCallEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                      @Nonnull final Deque<Task> taskStack,
                                      @Nonnull final Location location,
                                      @Nonnull final GroupExpressionRef<? extends RelationalExpression> currentGroupReference,
                                      @Nonnull final Bindable bindable,
                                      @Nonnull final PlannerRule<? extends Bindable> rule,
                                      @Nonnull final CascadesRuleCall ruleCall) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
            this.bindable = bindable;
            this.rule = rule;
            this.ruleCall = ruleCall;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "transform rule call";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.RULECALL;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getCurrentGroupReference() {
            return currentGroupReference;
        }

        @Nonnull
        public Bindable getBindable() {
            return bindable;
        }

        @Nonnull
        public PlannerRule<? extends Bindable> getRule() {
            return rule;
        }

        @Nonnull
        public CascadesRuleCall getRuleCall() {
            return ruleCall;
        }
    }

    /**
     * Events of this class are generated when the planner attempts to match an expression.
     */
    class MatchExpressionEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> currentGroupReference;
        @Nonnull
        private final RelationalExpression expression;

        public MatchExpressionEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                    @Nonnull final Deque<Task> taskStack,
                                    @Nonnull final Location location,
                                    @Nonnull final GroupExpressionRef<? extends RelationalExpression> currentGroupReference,
                                    @Nonnull final RelationalExpression expression) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
            this.expression = expression;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "match expression";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.MATCHEXP;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getCurrentGroupReference() {
            return currentGroupReference;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }
    }

    /**
     * Events of this class are generated when the planner attempts to match an expression to the expression of a
     * match candidate.
     */
    class MatchExpressionWithCandidateEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> currentGroupReference;
        @Nonnull
        private final RelationalExpression expression;
        @Nonnull
        private final MatchCandidate matchCandidate;
        @Nonnull
        private final ExpressionRef<? extends RelationalExpression> candidateRef;
        @Nonnull
        private final RelationalExpression candidateExpression;

        public MatchExpressionWithCandidateEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                                 @Nonnull final Deque<Task> taskStack,
                                                 @Nonnull final Location location,
                                                 @Nonnull final GroupExpressionRef<? extends RelationalExpression> currentGroupReference,
                                                 @Nonnull final RelationalExpression expression,
                                                 @Nonnull final MatchCandidate matchCandidate,
                                                 @Nonnull final ExpressionRef<? extends RelationalExpression> candidateRef,
                                                 @Nonnull final RelationalExpression candidateExpression) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
            this.expression = expression;
            this.matchCandidate = matchCandidate;
            this.candidateRef = candidateRef;
            this.candidateExpression = candidateExpression;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "match expression with candidate";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.MATCHEXPCAND;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getCurrentGroupReference() {
            return currentGroupReference;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        public MatchCandidate getMatchCandidate() {
            return matchCandidate;
        }

        @Nonnull
        public ExpressionRef<? extends RelationalExpression> getCandidateRef() {
            return candidateRef;
        }

        @Nonnull
        public RelationalExpression getCandidateExpression() {
            return candidateExpression;
        }
    }

    /**
     * Events of this class are generated when the planner optimizes inputs.
     */
    class OptimizeInputsEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final GroupExpressionRef<? extends RelationalExpression> currentGroupReference;
        @Nonnull
        private final RelationalExpression expression;

        public OptimizeInputsEvent(@Nonnull final GroupExpressionRef<? extends RelationalExpression> rootReference,
                                   @Nonnull final Deque<Task> taskStack,
                                   @Nonnull final Location location,
                                   @Nonnull final GroupExpressionRef<? extends RelationalExpression> currentGroupReference,
                                   @Nonnull final RelationalExpression expression) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
            this.expression = expression;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "optimize inputs";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.OPTINPUTS;
        }

        @Override
        @Nonnull
        public GroupExpressionRef<? extends RelationalExpression> getCurrentGroupReference() {
            return currentGroupReference;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }
    }
}
