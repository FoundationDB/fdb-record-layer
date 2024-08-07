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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner.Task;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PAbstractEventWithState;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PAdjustMatchEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PBindable;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PExecutingTaskEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PExploreExpressionEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PExploreGroupEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PInsertIntoMemoEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PMatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.POptimizeGroupEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.POptimizeInputsEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PPartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PRegisteredReference;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PRegisteredRelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PTransformEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PTransformRuleCallEvent;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PTranslateCorrelationsEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.Message;

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
     * the loading thread.
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
     * Invoke the {@link Consumer} on the currently set debugger. Do not do anything if there is no debugger set.
     * @param runnable to invoke that may throw an exception
     */
    static void sanityCheck(@Nonnull final Runnable runnable) {
        withDebugger(debugger -> {
            if (!debugger.isSane()) {
                runnable.run();
            }
        });
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

    static void show(@Nonnull final Reference ref) {
        withDebugger(debugger -> debugger.onShow(ref));
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

    static void registerReference(Reference reference) {
        withDebugger(debugger -> debugger.onRegisterReference(reference));
    }

    static void registerQuantifier(Quantifier quantifier) {
        withDebugger(debugger -> debugger.onRegisterQuantifier(quantifier));
    }

    static Optional<Integer> getOrRegisterSingleton(Object singleton) {
        return mapDebugger(debugger -> debugger.onGetOrRegisterSingleton(singleton));
    }

    @Nullable
    String nameForObject(@Nonnull Object object);

    boolean isSane();

    void onEvent(Event event);

    void onDone();

    int onGetIndex(@Nonnull Class<?> clazz);

    int onUpdateIndex(@Nonnull Class<?> clazz, @Nonnull IntUnaryOperator updateFn);

    void onRegisterExpression(@Nonnull RelationalExpression expression);

    void onRegisterReference(@Nonnull Reference reference);

    void onRegisterQuantifier(@Nonnull Quantifier quantifier);

    int onGetOrRegisterSingleton(@Nonnull Object singleton);

    void onInstall();

    void onSetup();

    void onShow(@Nonnull Reference ref);

    void onQuery(String queryAsString, PlanContext planContext);

    @SuppressWarnings("unused") // only used by debugger
    String showStats();

    /**
     * Shorthands to identify a kind of event.
     */
    enum Shorthand {
        TASK,
        OPTGROUP,
        EXPEXP,
        EXPGROUP,
        ADJUSTMATCH,
        MATCHEXPCAND,
        OPTINPUTS,
        RULECALL,
        TRANSFORM,
        INSERT_INTO_MEMO,
        TRANSLATE_CORRELATIONS,
    }

    /**
     * Enum to indicate where an event happened.
     */
    enum Location {
        ANY,
        BEGIN,
        END,
        MATCH_PRE,
        YIELD,
        FAILURE,
        COUNT,
        NEW,
        REUSED,
        DISCARDED_INTERSECTION_COMBINATIONS,
        ALL_INTERSECTION_COMBINATIONS
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

        @Nonnull
        Message toProto();

        @Nonnull
        default PEvent toEventProto() {
            return toEventBuilder()
                    .setDescription(getDescription())
                    .setShorthand(getShorthand().name())
                    .build();
        }

        @Nonnull
        PEvent.Builder toEventBuilder();

        @Nonnull
        static PRegisteredRelationalExpression toExpressionProto(@Nonnull final RelationalExpression expression) {
            return PRegisteredRelationalExpression.newBuilder()
                    .setName(Debugger.mapDebugger(debugger -> debugger.nameForObject(expression)).orElseThrow())
                    .setSemanticHashCode(expression.semanticHashCode())
                    .build();
        }

        @Nonnull
        static PRegisteredReference toReferenceProto(@Nonnull final Reference reference) {
            final var builder = PRegisteredReference.newBuilder()
                    .setName(Debugger.mapDebugger(debugger -> debugger.nameForObject(reference)).orElseThrow());
            for (final var member : reference.getMembers()) {
                builder.addExpressions(toExpressionProto(member));
            }
            return builder.build();
        }

        @Nonnull
        static PBindable toBindableProto(@Nonnull final Object bindable) {
            final var builder = PBindable.newBuilder();
            if (bindable instanceof RelationalExpression) {
                builder.setExpression(toExpressionProto((RelationalExpression)bindable));
            } else if (bindable instanceof PartialMatch) {
                builder.setPartialMatch(toPartialMatchProto((PartialMatch)bindable));
            } else if (bindable instanceof MatchPartition) {
                builder.setMatchPartition(toMatchPartitionProto((MatchPartition)bindable));
            }
            return builder.build();
        }

        @Nonnull
        static PPartialMatch toPartialMatchProto(@Nonnull final PartialMatch partialMatch) {
            return PPartialMatch.newBuilder()
                    .setMatchCandidate(partialMatch.toString())
                    .setQueryRef(toReferenceProto(partialMatch.getQueryRef()))
                    .setQueryExpression(toExpressionProto(partialMatch.getQueryExpression()))
                    .setCandidateRef(toReferenceProto(partialMatch.getCandidateRef()))
                    .build();
        }

        @Nonnull
        static PMatchPartition toMatchPartitionProto(@Nonnull final MatchPartition matchPartition) {
            final var builder = PMatchPartition.newBuilder();
            for (final var partialMatch : matchPartition.getPartialMatches()) {
                builder.addPartialMatches(toPartialMatchProto(partialMatch));
            }
            return builder.build();
        }
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
        Reference getRootReference();

        /**
         * Getter.
         *
         * @return the current task stack of the planner
         */
        @Nonnull
        Deque<Task> getTaskStack();
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
        Reference getCurrentReference();
    }

    /**
     * Events that are created by a or as part of a transformation rule.
     */
    interface EventWithRule {
        /**
         * Return the rule.
         * @return the rule
         */
        @Nonnull
        CascadesRule<?> getRule();
    }

    /**
     * Abstract event class to capture {@code rootReference} amd {@code taskStack}.
     */
    abstract class AbstractEventWithState implements EventWithState {
        @Nonnull
        private final Reference rootReference;

        @Nonnull
        private final Deque<Task> taskStack;

        @Nonnull
        private final Location location;

        protected AbstractEventWithState(@Nonnull final Reference rootReference,
                                         @Nonnull final Deque<Task> taskStack,
                                         @Nonnull final Location location) {
            this.rootReference = rootReference;
            this.taskStack = taskStack;
            this.location = location;
        }

        @Override
        @Nonnull
        public Reference getRootReference() {
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

        @Nonnull
        public PAbstractEventWithState toAbstractEventWithStateProto() {
            return PAbstractEventWithState.newBuilder()
                    .setRootReference(Event.toReferenceProto(rootReference))
                    .setLocation(getLocation().name())
                    .build();
        }
    }

    /**
     * Events of this class are generated every time the planner executes a task.
     */
    class ExecutingTaskEvent extends AbstractEventWithState {
        @Nonnull
        private final Task task;

        public ExecutingTaskEvent(@Nonnull final Reference rootReference,
                                  @Nonnull final Deque<Task> taskStack,
                                  @Nonnull final Task task) {
            super(rootReference, taskStack, Location.COUNT);
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

        @Nonnull
        @Override
        public PExecutingTaskEvent toProto() {
            return PExecutingTaskEvent.newBuilder()
                    .setSuper(toAbstractEventWithStateProto())
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setExecutingTaskEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner optimizes a group.
     */
    class OptimizeGroupEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final Reference currentGroupReference;

        public OptimizeGroupEvent(@Nonnull final Reference rootReference,
                                  @Nonnull final Deque<Task> taskStack,
                                  @Nonnull final Location location,
                                  @Nonnull final Reference currentGroupReference) {
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
        public Reference getCurrentReference() {
            return currentGroupReference;
        }

        @Nonnull
        @Override
        public POptimizeGroupEvent toProto() {
            return POptimizeGroupEvent.newBuilder()
                    .setSuper(toAbstractEventWithStateProto())
                    .setCurrentGroupReference(Event.toReferenceProto(currentGroupReference))
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setOptimizeGroupEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner explores an expression.
     */
    class ExploreExpressionEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final Reference currentGroupReference;
        @Nonnull
        private final RelationalExpression expression;

        public ExploreExpressionEvent(@Nonnull final Reference rootReference,
                                      @Nonnull final Deque<Task> taskStack,
                                      @Nonnull final Location location,
                                      @Nonnull final Reference currentGroupReference,
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
        public Reference getCurrentReference() {
            return currentGroupReference;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        @Override
        public PExploreExpressionEvent toProto() {
            return PExploreExpressionEvent.newBuilder()
                    .setSuper(toAbstractEventWithStateProto())
                    .setCurrentGroupReference(Event.toReferenceProto(currentGroupReference))
                    .setExpression(Event.toExpressionProto(expression))
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setExploreExpressionEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner explores a group.
     */
    class ExploreGroupEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final Reference currentGroupReference;

        public ExploreGroupEvent(@Nonnull final Reference rootReference,
                                 @Nonnull final Deque<Task> taskStack,
                                 @Nonnull final Location location,
                                 @Nonnull final Reference currentGroupReference) {
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
        public Reference getCurrentReference() {
            return currentGroupReference;
        }

        @Nonnull
        @Override
        public PExploreGroupEvent toProto() {
            return PExploreGroupEvent.newBuilder()
                    .setSuper(toAbstractEventWithStateProto())
                    .setCurrentGroupReference(Event.toReferenceProto(currentGroupReference))
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setExploreGroupEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner transforms an expression using a rule.
     */
    class TransformEvent extends AbstractEventWithState implements EventWithCurrentGroupReference, EventWithRule {
        @Nonnull
        private final Reference currentGroupReference;
        @Nonnull
        private final Object bindable;
        @Nonnull
        private final CascadesRule<?> rule;

        public TransformEvent(@Nonnull final Reference rootReference,
                              @Nonnull final Deque<Task> taskStack,
                              @Nonnull final Location location,
                              @Nonnull final Reference currentGroupReference,
                              @Nonnull final Object bindable,
                              @Nonnull final CascadesRule<?> rule) {
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
        public Reference getCurrentReference() {
            return currentGroupReference;
        }

        @Nonnull
        public Object getBindable() {
            return bindable;
        }

        @Nonnull
        @Override
        public CascadesRule<?> getRule() {
            return rule;
        }

        @Nonnull
        @Override
        public PTransformEvent toProto() {
            return PTransformEvent.newBuilder()
                    .setSuper(toAbstractEventWithStateProto())
                    .setCurrentGroupReference(Event.toReferenceProto(currentGroupReference))
                    .setBindable(Event.toBindableProto(bindable))
                    .setRule(rule.toString())
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setTransformEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner calls a transformation rule.
     */
    class TransformRuleCallEvent extends AbstractEventWithState implements EventWithCurrentGroupReference, EventWithRule {
        @Nonnull
        private final Reference currentGroupReference;
        @Nonnull
        private final Object bindable;
        @Nonnull
        private final CascadesRule<?> rule;
        @Nonnull
        private final CascadesRuleCall ruleCall;

        public TransformRuleCallEvent(@Nonnull final Reference rootReference,
                                      @Nonnull final Deque<Task> taskStack,
                                      @Nonnull final Location location,
                                      @Nonnull final Reference currentGroupReference,
                                      @Nonnull final Object bindable,
                                      @Nonnull final CascadesRule<?> rule,
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
        public Reference getCurrentReference() {
            return currentGroupReference;
        }

        @Nonnull
        public Object getBindable() {
            return bindable;
        }

        @Nonnull
        @Override
        public CascadesRule<?> getRule() {
            return rule;
        }

        @Nonnull
        public CascadesRuleCall getRuleCall() {
            return ruleCall;
        }

        @Nonnull
        @Override
        public PTransformRuleCallEvent toProto() {
            return PTransformRuleCallEvent.newBuilder()
                    .setSuper(toAbstractEventWithStateProto())
                    .setCurrentGroupReference(Event.toReferenceProto(currentGroupReference))
                    .setBindable(Event.toBindableProto(bindable))
                    .setRule(rule.toString())
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setTransformRuleCallEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner attempts to adjust an existing match.
     */
    class AdjustMatchEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final Reference currentGroupReference;
        @Nonnull
        private final RelationalExpression expression;

        public AdjustMatchEvent(@Nonnull final Reference rootReference,
                                @Nonnull final Deque<Task> taskStack,
                                @Nonnull final Location location,
                                @Nonnull final Reference currentGroupReference,
                                @Nonnull final RelationalExpression expression) {
            super(rootReference, taskStack, location);
            this.currentGroupReference = currentGroupReference;
            this.expression = expression;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "adjust match";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.ADJUSTMATCH;
        }

        @Override
        @Nonnull
        public Reference getCurrentReference() {
            return currentGroupReference;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        @Override
        public PAdjustMatchEvent toProto() {
            return PAdjustMatchEvent.newBuilder()
                    .setSuper(toAbstractEventWithStateProto())
                    .setCurrentGroupReference(Event.toReferenceProto(currentGroupReference))
                    .setExpression(Event.toExpressionProto(expression))
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setAdjustMatchEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner optimizes inputs.
     */
    class OptimizeInputsEvent extends AbstractEventWithState implements EventWithCurrentGroupReference {
        @Nonnull
        private final Reference currentGroupReference;
        @Nonnull
        private final RelationalExpression expression;

        public OptimizeInputsEvent(@Nonnull final Reference rootReference,
                                   @Nonnull final Deque<Task> taskStack,
                                   @Nonnull final Location location,
                                   @Nonnull final Reference currentGroupReference,
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
        public Reference getCurrentReference() {
            return currentGroupReference;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        @Override
        public POptimizeInputsEvent toProto() {
            return POptimizeInputsEvent.newBuilder()
                    .setSuper(toAbstractEventWithStateProto())
                    .setCurrentGroupReference(Event.toReferenceProto(currentGroupReference))
                    .setExpression(Event.toExpressionProto(expression))
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setOptimizeInputsEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner attempts to insert a new expression into the memoization
     * structures of the planner.
     */
    class InsertIntoMemoEvent implements Event {
        @Nonnull
        private final Location location;

        public InsertIntoMemoEvent(@Nonnull final Location location) {
            this.location = location;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "insert into memo";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.INSERT_INTO_MEMO;
        }

        @Nonnull
        @Override
        public Location getLocation() {
            return location;
        }

        @Nonnull
        @Override
        public PInsertIntoMemoEvent toProto() {
            return PInsertIntoMemoEvent.newBuilder()
                    .setLocation(getLocation().name())
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setInsertIntoMemoEvent(toProto());
        }
    }

    /**
     * Events of this class are generated when the planner creates new expressions as part of rebasing or as part of
     * a translation of correlations in a graph.
     */
    class TranslateCorrelationsEvent implements Event {
        @Nonnull
        private final RelationalExpression expression;

        @Nonnull
        private final Location location;

        public TranslateCorrelationsEvent(@Nonnull final RelationalExpression expression,
                                          @Nonnull final Location location) {
            this.expression = expression;
            this.location = location;
        }

        @Override
        @Nonnull
        public String getDescription() {
            return "translate correlations";
        }

        @Nonnull
        @Override
        public Shorthand getShorthand() {
            return Shorthand.TRANSLATE_CORRELATIONS;
        }

        @Nonnull
        public RelationalExpression getExpression() {
            return expression;
        }

        @Nonnull
        @Override
        public Location getLocation() {
            return location;
        }

        @Nonnull
        @Override
        public PTranslateCorrelationsEvent toProto() {
            return PTranslateCorrelationsEvent.newBuilder()
                    .setExpression(Event.toExpressionProto(expression))
                    .setLocation(location.name())
                    .build();
        }

        @Nonnull
        @Override
        public PEvent.Builder toEventBuilder() {
            return PEvent.newBuilder()
                    .setTranslateCorrelationsEvent(toProto());
        }
    }
}
