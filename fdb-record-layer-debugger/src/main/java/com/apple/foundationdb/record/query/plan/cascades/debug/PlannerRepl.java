/*
 * PlannerRepl.java
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.events.InitiatePhasePlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Location;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Shorthand;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventWithCurrentGroupReference;
import com.apple.foundationdb.record.query.plan.cascades.events.TransformPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.TransformRuleCallPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.util.ServiceLoaderProvider;
import com.google.common.cache.Cache;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;

/**
 * Implementation of a debugger as a repl.
 */
public class PlannerRepl implements Debugger {
    private static final Logger logger = LoggerFactory.getLogger(PlannerRepl.class);

    private static final String banner =
            "                                                                                       \n" +
            "   ______                          __             ____  __                            \n" +
            "  / ____/___ _______________ _____/ /__  _____   / __ \\/ /___ _____  ____  ___  _____  \n" +
            " / /   / __ `/ ___/ ___/ __ `/ __  / _ \\/ ___/  / /_/ / / __ `/ __ \\/ __ \\/ _ \\/ ___/  \n" +
            "/ /___/ /_/ (__  ) /__/ /_/ / /_/ /  __(__  )  / ____/ / /_/ / / / / / / /  __/ /      \n" +
            "\\____/\\__,_/____/\\___/\\__,_/\\__,_/\\___/____/  /_/   /_/\\__,_/_/ /_/_/ /_/\\___/_/       \n" +
            "type 'help' to get a list of available commands                                       \n" +
            "type 'quit' to exit debugger                                                          \n";

    private static final String prompt = "$ ";

    private static final SetMultimap<Class<? extends PlannerEvent>, Processors.Processor<? extends PlannerEvent>> processorsMap;
    private static final ImmutableMap<String, Commands.Command<PlannerEvent>> commandsMap;

    static {
        commandsMap = loadCommands();
        processorsMap = loadProcessors();
    }

    private final Deque<State> stateStack;

    private final BiMap<Integer, BreakPoint> breakPoints;
    private int currentBreakPointIndex;
    private int currentInternalBreakPointIndex;

    @Nullable
    private String queryAsString;
    @Nullable
    private PlanContext planContext;
    @Nonnull
    private final Map<Object, Integer> singletonToIndexMap;

    @Nonnull
    private final Terminal terminal;
    @Nullable
    private LineReader lineReader;

    private final boolean exitOnQuit;

    public PlannerRepl(@Nonnull final Terminal terminal) {
        this(terminal, true);
    }

    public PlannerRepl(@Nonnull final Terminal terminal, boolean exitOnQuit) {
        this.stateStack = new ArrayDeque<>();
        this.breakPoints = HashBiMap.create();
        this.currentBreakPointIndex = 0;
        this.currentInternalBreakPointIndex = -1;
        this.planContext = null;
        this.singletonToIndexMap = Maps.newHashMap();
        this.terminal = terminal;
        this.lineReader = null;
        this.exitOnQuit = exitOnQuit;
    }

    boolean shouldExitOnQuit() {
        return exitOnQuit;
    }

    @Nonnull
    State getCurrentState() {
        return Objects.requireNonNull(stateStack.peek());
    }

    @Nullable
    @Override
    public PlanContext getPlanContext() {
        return planContext;
    }

    @Override
    public boolean isSane() {
        // run all sanity checks
        return false;
    }

    @Override
    public int onGetIndex(@Nonnull final Class<?> clazz) {
        return getCurrentState().getIndex(clazz);
    }

    @Override
    public int onUpdateIndex(@Nonnull final Class<?> clazz, @Nonnull final IntUnaryOperator updateFn) {
        return getCurrentState().updateIndex(clazz, updateFn);
    }

    @Override
    public void onRegisterExpression(@Nonnull final RelationalExpression expression) {
        getCurrentState().registerExpression(expression);
    }

    @Override
    public void onRegisterReference(@Nonnull final Reference reference) {
        getCurrentState().registerReference(reference);
    }

    @Override
    public void onRegisterQuantifier(@Nonnull final Quantifier quantifier) {
        getCurrentState().registerQuantifier(quantifier);
    }

    @Override
    public int onGetOrRegisterSingleton(@Nonnull final Object singleton) {
        final var size = singletonToIndexMap.size();
        return singletonToIndexMap.computeIfAbsent(singleton, s -> size);
    }

    @Override
    public void onInstall() {
        lineReader = LineReaderBuilder.builder().terminal(terminal).build();
        Objects.requireNonNull(terminal).puts(InfoCmp.Capability.clear_screen);
        println(banner);
    }

    @Override
    public void onSetup() {
        if (lineReader == null) {
            onInstall();
        }

        reset();
    }

    @Override
    public void onShow(@Nonnull final Reference ref) {
        PlannerGraphVisitor.show(true, ref);
    }

    @Override
    public void onQuery(@Nonnull final String queryAsString, @Nonnull final PlanContext planContext) {
        this.stateStack.push(State.copyOf(getCurrentState()));
        this.queryAsString = queryAsString;
        this.planContext = planContext;

        printlnQuery();
        println();

        addInternalBreakPoint(new CountingTautologyBreakPoint(1));
    }

    void restartState() {
        stateStack.pop();
        stateStack.push(State.copyOf(getCurrentState()));
    }

    void addBreakPoint(final BreakPoint breakPoint) {
        breakPoints.put(currentBreakPointIndex ++, breakPoint);
    }

    void addInternalBreakPoint(final BreakPoint breakPoint) {
        breakPoints.put(currentInternalBreakPointIndex --, breakPoint);
    }

    BreakPoint removeBreakPoint(final int index) {
        return breakPoints.remove(index);
    }

    void removeAllBreakPoints() {
        breakPoints.clear();
    }

    Iterable<BreakPoint> getBreakPoints() {
        return () -> breakPoints.entrySet()
                .stream()
                .filter(entry -> entry.getKey() >= 0)
                .map(Map.Entry::getValue)
                .iterator();
    }

    @Nullable
    Integer lookupBreakPoint(final BreakPoint breakPoint) {
        return breakPoints.inverse().get(breakPoint);
    }

    @Override
    public void onEvent(final PlannerEvent plannerEvent) {
        if (lineReader == null) {
            return;
        }
        Objects.requireNonNull(queryAsString);
        Objects.requireNonNull(planContext);

        final State state = getCurrentState();

        state.addCurrentEvent(plannerEvent);

        final Set<BreakPoint> satisfiedBreakPoints = computeSatisfiedBreakPoints(plannerEvent);
        satisfiedBreakPoints.forEach(breakPoint -> breakPoint.onBreak(this));

        final boolean stop = !satisfiedBreakPoints.isEmpty();
        if (stop) {
            printKeyValue("paused in", Thread.currentThread().getName() + " at ");
            printlnKeyValue("tick", String.valueOf(state.getCurrentTick()));
            withProcessors(plannerEvent, processor -> processor.onCallback(this, plannerEvent));
            println();

            boolean isContinue = false;
            do {
                String line;
                try {
                    line = lineReader.readLine(prompt);
                } catch (UserInterruptException e) {
                    printlnError("user interrupt");
                    return;
                } catch (EndOfFileException e) {
                    printlnError("end of file caught");
                    return;
                }
                if (line.isEmpty()) {
                    continue;
                }

                final ParsedLine parsedLine = lineReader.getParsedLine();

                final boolean processed =
                        processBaseIdentifiers(parsedLine,
                                this::printlnExpression,
                                this::printlnReference,
                                this::printlnQuantifier);
                if (!processed) {
                    final Optional<Commands.Command<PlannerEvent>> commandOptional = resolveCommand(PlannerRepl.commandsMap, parsedLine, 0);
                    if (commandOptional.isPresent()) {
                        final Commands.Command<PlannerEvent> command = commandOptional.get();
                        final Optional<Boolean> isContinueOptional = getSilently("run command", () -> command.executeCommand(this, plannerEvent, parsedLine));
                        isContinue = isContinueOptional.orElse(false);
                    } else {
                        withProcessors(plannerEvent, processor -> processor.onCommand(this, plannerEvent, parsedLine));
                    }
                }
            } while (!isContinue);

            printHighlighted("continuing...");
            println();
        }
    }

    private Set<BreakPoint> computeSatisfiedBreakPoints(final PlannerEvent plannerEvent) {
        return breakPoints.values()
                .stream()
                .filter(breakPoint -> breakPoint.onCallback(this, plannerEvent))
                .collect(ImmutableSet.toImmutableSet());
    }

    private boolean processBaseIdentifiers(final ParsedLine parsedLine,
                                           final Consumer<RelationalExpression> expressionConsumer,
                                           final Consumer<Reference> referenceConsumer,
                                           final Consumer<Quantifier> quantifierConsumer) {
        final List<String> words = parsedLine.words();
        if (words.isEmpty()) {
            return false;
        }

        return processIdentifiers(words.get(0), expressionConsumer, referenceConsumer, quantifierConsumer);
    }

    boolean processIdentifiers(final String potentialIdentifier,
                               final Consumer<RelationalExpression> expressionConsumer,
                               final Consumer<Reference> referenceConsumer,
                               final Consumer<Quantifier> quantifierConsumer) {
        final State state = getCurrentState();
        final String upperCasePotentialIdentifier = potentialIdentifier.toUpperCase(Locale.ROOT);
        if (upperCasePotentialIdentifier.startsWith("EXP")) {
            @Nullable final RelationalExpression expression = lookupInCache(state.getExpressionCache(), upperCasePotentialIdentifier, "EXP");
            if (expression == null) {
                return false;
            }
            expressionConsumer.accept(expression);
            return true;
        } else if (upperCasePotentialIdentifier.startsWith("REF")) {
            @Nullable final Reference reference = lookupInCache(state.getReferenceCache(), upperCasePotentialIdentifier, "REF");
            if (reference == null) {
                return false;
            }
            referenceConsumer.accept(reference);
            return true;
        } else if (upperCasePotentialIdentifier.startsWith("QUN")) {
            @Nullable final Quantifier quantifier = lookupInCache(state.getQuantifierCache(), upperCasePotentialIdentifier, "QUN");
            if (quantifier == null) {
                return false;
            }
            quantifierConsumer.accept(quantifier);
            return true;
        }

        return false;
    }

    @Nullable
    private static <T> T lookupInCache(final Cache<Integer, T> cache, final String identifier, final String prefix) {
        @Nullable final Integer refId = getIdFromIdentifier(identifier, prefix);
        if (refId == null) {
            return null;
        }
        return cache.getIfPresent(refId);
    }

    @Nullable
    static Integer getIdFromIdentifier(final String identifier, final String prefix) {
        final String idAsString = identifier.substring(prefix.length());
        try {
            return Integer.valueOf(idAsString);
        } catch (final NumberFormatException numberFormatException) {
            return null;
        }
    }

    @Nonnull
    String nameForObjectOrNotInCache(@Nonnull final Object object) {
        return Optional.ofNullable(nameForObject(object)).orElse("not in cache");
    }

    boolean isValidEntityName(@Nonnull final String identifier) {
        final String lowerCase = identifier.toLowerCase(Locale.ROOT);
        if (!lowerCase.startsWith("exp") &&
                !lowerCase.startsWith("ref") &&
                !lowerCase.startsWith("qun")) {
            return false;
        }

        return getIdFromIdentifier(identifier, identifier.substring(0, 3)) != null;
    }


    @Nullable
    @Override
    public String nameForObject(@Nonnull final Object object) {
        final State state = getCurrentState();
        if (object instanceof RelationalExpression) {
            @Nullable final Integer id = state.getInvertedExpressionsCache().getIfPresent(object);
            return (id == null) ? null : "exp" + id;
        } else if (object instanceof Reference) {
            @Nullable final Integer id = state.getInvertedReferenceCache().getIfPresent(object);
            return (id == null) ? null : "ref" + id;
        }  else if (object instanceof Quantifier) {
            @Nullable final Integer id = state.getInvertedQuantifierCache().getIfPresent(object);
            return (id == null) ? null : "qun" + id;
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    <E extends PlannerEvent> void withProcessors(final E event, final Consumer<Processors.Processor<E>> consumer) {
        final LinkedList<Class<? extends PlannerEvent>> resolutionQueue = Lists.newLinkedList();
        final Set<Processors.Processor<? extends PlannerEvent>> resolvedProcessors = Sets.newHashSet();
        final Class<? extends PlannerEvent> eventClass = event.getClass();
        resolutionQueue.push(eventClass);
        do {
            final Class<? extends PlannerEvent> currentEventClass = resolutionQueue.pop();
            final Set<Processors.Processor<? extends PlannerEvent>> processors = processorsMap.get(currentEventClass);
            if (!processors.isEmpty()) {
                processors.stream()
                        .filter(processor -> !resolvedProcessors.contains(processor))
                        .forEach(processor -> {
                            doSilently("call processor", () -> consumer.accept((Processors.Processor<E>)processor));
                            resolvedProcessors.add(processor);
                        });
            } else {
                final Class<?> superClass = currentEventClass.getSuperclass();
                if (superClass != null) {
                    if (PlannerEvent.class.isAssignableFrom(superClass)) {
                        resolutionQueue.push((Class<? extends PlannerEvent>)superClass);
                    }
                }
                final Class<?>[] interfaces = currentEventClass.getInterfaces();
                for (final Class<?> anInterface : interfaces) {
                    if (PlannerEvent.class.isAssignableFrom(anInterface)) {
                        resolutionQueue.push((Class<? extends PlannerEvent>)anInterface);
                    }
                }
            }
        } while (!resolutionQueue.isEmpty());
        processorsMap.putAll(eventClass, resolvedProcessors);
    }

    @Override
    public void onDone() {
        reset();
    }

    private void reset() {
        this.stateStack.clear();
        this.stateStack.push(State.initial(true, true, null));
        this.breakPoints.clear();
        this.currentBreakPointIndex = 0;
        this.currentInternalBreakPointIndex = -1;
        this.planContext = null;
        this.queryAsString = null;
    }

    void printlnQuery() {
        printlnKeyValue("query", queryAsString);
    }

    void printlnReference(@Nonnull final Reference reference) {
        printlnReference(reference, "");
    }

    void printlnReference(@Nonnull final Reference reference, final String prefix) {
        printlnKeyValue(prefix + "class", reference.getClass().getSimpleName());
        getSilently("reference.toString()", reference::toString)
                .ifPresent(referenceAsString ->
                        printlnKeyValue(prefix + "reference", referenceAsString));

        printlnKeyValue(prefix + "name", nameForObjectOrNotInCache(reference));
        printlnKeyValue(prefix + "  members", "");
        for (final RelationalExpression member : reference.getAllMemberExpressions()) {
            printlnKeyValue(prefix + "  " + nameForObjectOrNotInCache(member), "");
            printlnKeyValue(prefix + "      toString()", String.valueOf(member.toString()));
        }
    }

    void printlnExpression(@Nonnull final RelationalExpression expression) {
        printlnExpression(expression, "");
    }

    void printlnExpression(@Nonnull final RelationalExpression expression, final String prefix) {
        printlnKeyValue(prefix + "class", expression.getClass().getSimpleName());
        getSilently("expression.toString()", expression::toString)
                .ifPresent(expressionAsString ->
                        printlnKeyValue(prefix + "expression", expressionAsString));
        printlnKeyValue(prefix + "name", nameForObjectOrNotInCache(expression));
        if (expression.getQuantifiers().isEmpty()) {
            printlnKeyValue(prefix + "quantifiers", "empty");
        } else {
            printlnKeyValue(prefix + "quantifiers", "");
            for (final Quantifier quantifier : expression.getQuantifiers()) {
                printKeyValue(prefix + "  name", nameForObjectOrNotInCache(quantifier) + "; ");
                printKeyValue("kind", quantifier.getShorthand() + "; ");
                printKeyValue("alias", quantifier.getAlias().toString() + "; ");
                final Reference rangesOver = quantifier.getRangesOver();
                printKeyValue("ranges over", nameForObjectOrNotInCache(rangesOver));
                println();
            }
        }
    }

    void printlnQuantifier(@Nonnull final Quantifier quantifier) {
        printlnQuantifier(quantifier, "");
    }

    void printlnQuantifier(@Nonnull final Quantifier quantifier, final String prefix) {
        printlnKeyValue(prefix + "class", quantifier.getClass().getSimpleName());
        printlnKeyValue(prefix + "name", nameForObjectOrNotInCache(quantifier));
        printlnKeyValue(prefix + "kind", quantifier.getShorthand());
        printlnKeyValue(prefix + "alias", quantifier.getAlias().toString());
        final Reference rangesOver = quantifier.getRangesOver();
        printlnKeyValue(prefix + "ranges over", nameForObjectOrNotInCache(rangesOver));
    }

    void printlnHighlighted(final String string) {
        printHighlighted(string);
        println();
    }

    void printHighlighted(final String string) {
        print(new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE + AttributedStyle.BRIGHT).bold())
                .append(string)
                .toAnsi());
    }

    void printlnError(final String string) {
        print(new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.RED + AttributedStyle.BRIGHT).bold())
                .append(string)
                .toAnsi());
        println();
    }

    void printlnKeyValue(final String key, final String value) {
        printKeyValue(key, value);
        println();
    }

    void printKeyValue(final String key, final String value) {
        print(new AttributedStringBuilder()
                .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW + AttributedStyle.BRIGHT).bold())
                .append(key)
                .append(": ")
                .style(AttributedStyle.DEFAULT)
                .append(value).toAnsi());
    }

    void print(@Nonnull final String string) {
        Objects.requireNonNull(terminal).writer().print(string);
    }

    void println(@Nonnull final String string) {
        Objects.requireNonNull(terminal).writer().println(string);
    }

    void println() {
        println("");
    }

    @SuppressWarnings({"CallToPrintStackTrace", "PMD.AvoidPrintStackTrace"})
    private void doSilently(@Nonnull final String actionName, @Nonnull final RunnableWithException runnable) {
        try {
            runnable.run();
        } catch (final RestartException rE) {
            throw rE;
        } catch (final Exception e) {
            if (logger.isWarnEnabled()) {
                logger.warn("unable to {}: {}", actionName, e.getMessage());
            }
            e.printStackTrace();
        }
    }

    @Nonnull
    @SuppressWarnings({"CallToPrintStackTrace", "PMD.AvoidPrintStackTrace"})
    private <T> Optional<T> getSilently(@Nonnull final String actionName, @Nonnull final SupplierWithException<T> supplier) {
        try {
            return Optional.ofNullable(supplier.get());
        } catch (final RestartException rE) {
            throw rE;
        } catch (final Exception e) {
            if (logger.isWarnEnabled()) {
                logger.warn("unable to get {}: {}", actionName, e.getMessage());
            }
            e.printStackTrace();
            return Optional.empty();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static ImmutableMap<String, Commands.Command<PlannerEvent>> loadCommands() {
        final ImmutableMap.Builder<String, Commands.Command<PlannerEvent>> commandsMapBuilder = ImmutableMap.builder();
        final Iterable<Commands.Command> loader =
                ServiceLoaderProvider.load(Commands.Command.class);

        loader.forEach(command -> {
            commandsMapBuilder.put(command.getCommandToken(), command);
            if (logger.isInfoEnabled()) {
                logger.info("loaded command {}", command.getCommandToken());
            }
        });

        return commandsMapBuilder.build();
    }

    @Nonnull
    static Set<Commands.Command<PlannerEvent>> getCommands() {
        return ImmutableSet.copyOf(commandsMap.values());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Nonnull
    private static SetMultimap<Class<? extends PlannerEvent>, Processors.Processor<? extends PlannerEvent>> loadProcessors() {
        SetMultimap<Class<? extends PlannerEvent>, Processors.Processor<? extends PlannerEvent>> processorsMap = HashMultimap.create();
        final Iterable<Processors.Processor> loader
                = ServiceLoaderProvider.load(Processors.Processor.class);

        loader.forEach(processor -> {
            processorsMap.put(processor.getEventType(), processor);
            if (logger.isInfoEnabled()) {
                logger.info("loaded processor for {}", processor.getEventType().getSimpleName());
            }
        });

        return processorsMap;
    }

    @Nonnull
    private static <E extends PlannerEvent> Optional<Commands.Command<E>> resolveCommand(@Nonnull final ImmutableMap<String, Commands.Command<E>> commandsMap,
                                                                                         @Nonnull final ParsedLine parsedLine,
                                                                                         final int wordIndex) {
        final List<String> words = parsedLine.words();
        if (words.size() <= wordIndex) {
            return Optional.empty();
        }
        final String commandToken = words.get(wordIndex).toUpperCase(Locale.ROOT);
        return Optional.ofNullable(commandsMap.get(commandToken));
    }

    @FunctionalInterface
    private interface RunnableWithException {
        /**
         * Run an action.
         */
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {

        /**
         * Gets a result.
         *
         * @return a result
         */
        T get() throws Exception;
    }

    /**
     * TBD.
     */
    public abstract static class BreakPoint {
        protected final Predicate<PlannerEvent> predicate;
        protected boolean isEnabled;
        protected int countDown;

        public BreakPoint(final Predicate<PlannerEvent> predicate) {
            this(predicate, -1);
        }

        public BreakPoint(final Predicate<PlannerEvent> predicate, final int countDown) {
            this.predicate = predicate;
            this.isEnabled = true;
            this.countDown = countDown;
        }

        public boolean onCallback(final PlannerRepl plannerRepl, final PlannerEvent plannerEvent) {
            if (!isEnabled) {
                return false;
            }
            if (predicate.test(plannerEvent)) {
                if (countDown < 0) {
                    return true;
                }
                countDown--;
                return (countDown == 0);
            }
            return false;
        }

        public void onBreak(final PlannerRepl plannerRepl) {
            if (countDown == 0) {
                plannerRepl.breakPoints.inverse().remove(this);
            }
        }

        public void onList(final PlannerRepl plannerRepl) {
            plannerRepl.printKeyValue("kind", this.getClass().getSimpleName() + "; ");
            plannerRepl.printKeyValue("enabled", isEnabled + "; ");
            plannerRepl.printKeyValue("count down", String.valueOf(countDown));
        }

        // force extending classes to override equals() and hashCode();

        @Override
        public abstract boolean equals(Object o);

        @Override
        public abstract int hashCode();
    }

    /**
     * Break point used for stepping.
     */
    public static class CountingTautologyBreakPoint extends BreakPoint {
        public CountingTautologyBreakPoint(final int count) {
            super(event -> true, count);
        }

        @Override
        public boolean equals(final Object o) {
            return this == o;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    /**
     * Breakpoint that breaks on a particular event.
     */
    public static class OnEventTypeBreakPoint extends BreakPoint {
        @Nonnull
        private final Shorthand shorthand;
        @Nullable
        private final String referenceName;
        @Nonnull
        private final Location location;

        public OnEventTypeBreakPoint(@Nonnull final Shorthand shorthand,
                                     @Nonnull final Location location) {
            this(shorthand, null, location);
        }

        public OnEventTypeBreakPoint(@Nonnull final Shorthand shorthand,
                                     @Nullable final String referenceName,
                                     @Nonnull final Location location) {
            super(event -> event.getShorthand() == shorthand && (location == Location.ANY || event.getLocation() == location));
            this.shorthand = shorthand;
            this.referenceName = referenceName == null ? null : referenceName.toLowerCase(Locale.ROOT);
            this.location = location;
        }

        @Nonnull
        public Shorthand getShorthand() {
            return shorthand;
        }

        @Nullable
        public String getReferenceName() {
            return referenceName;
        }

        @Nonnull
        public Location getLocation() {
            return location;
        }

        @Override
        public boolean onCallback(final PlannerRepl plannerRepl, final PlannerEvent plannerEvent) {
            if (super.onCallback(plannerRepl, plannerEvent)) {
                if (referenceName == null) {
                    return true;
                }
                if (plannerEvent instanceof PlannerEventWithCurrentGroupReference) {
                    final PlannerEventWithCurrentGroupReference eventWithCurrentGroupReference = (PlannerEventWithCurrentGroupReference)plannerEvent;
                    return referenceName.equals(plannerRepl.nameForObject(eventWithCurrentGroupReference.getCurrentReference()));
                }
            }
            return false;
        }

        @Override
        public void onList(final PlannerRepl plannerRepl) {
            super.onList(plannerRepl);
            plannerRepl.print("; ");
            plannerRepl.printKeyValue("shorthand", getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            if (getReferenceName() != null) {
                plannerRepl.printKeyValue("reference", getReferenceName().toLowerCase(Locale.ROOT) + "; ");
            }
            plannerRepl.printKeyValue("location", getLocation().name().toLowerCase(Locale.ROOT));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final OnEventTypeBreakPoint that = (OnEventTypeBreakPoint)o;
            return getShorthand().equals(that.getShorthand()) &&
                   Objects.equals(getReferenceName(), that.getReferenceName()) &&
                   getLocation() == that.getLocation();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getShorthand(), getReferenceName(), getLocation());
        }
    }

    /**
     * Breakpoint that breaks on reaching an {@link Shorthand#INITPHASE} event. The breakpoint can be configured
     * to break on any such event or on an event initiating a particular new {@link PlannerPhase}.
     */
    public static class OnPhaseBreakPoint extends BreakPoint {
        @Nonnull
        private final Location location;
        @Nullable
        private final PlannerPhase plannerPhase;

        public OnPhaseBreakPoint(@Nonnull final Location location,
                                 @Nullable final PlannerPhase plannerPhase) {
            super(event -> (event instanceof InitiatePhasePlannerEvent) &&
                    event.getShorthand() == Shorthand.INITPHASE &&
                    (location == Location.ANY || event.getLocation() == location) &&
                    (plannerPhase == null || ((InitiatePhasePlannerEvent)event).getPlannerPhase() == plannerPhase), 1);
            this.location = location;
            this.plannerPhase = plannerPhase;
        }

        @Nonnull
        public Shorthand getShorthand() {
            return Shorthand.INITPHASE;
        }

        @Nonnull
        public Location getLocation() {
            return location;
        }

        @Nullable
        public PlannerPhase getPlannerPhase() {
            return plannerPhase;
        }

        @Override
        public void onList(final PlannerRepl plannerRepl) {
            super.onList(plannerRepl);
            plannerRepl.print("; ");
            plannerRepl.printKeyValue("shorthand", getShorthand().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", getLocation().name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("plannerPhase",
                    (getPlannerPhase() == null ? getPlannerPhase().name() : "any").toLowerCase(Locale.ROOT));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final var that = (OnPhaseBreakPoint)o;
            return getShorthand().equals(that.getShorthand()) &&
                    getLocation() == that.getLocation() &&
                    getPlannerPhase() == that.getPlannerPhase();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getShorthand(), getLocation(),
                    getPlannerPhase() == null ? null : getPlannerPhase().name());
        }
    }

    /**
     * Breakpoint that breaks when a transform rule call yields an expression.
     */
    public static class OnYieldExpressionBreakPoint extends BreakPoint {
        @Nonnull
        private final String expressionName;

        public OnYieldExpressionBreakPoint(@Nonnull final String expressionName) {
            super(event -> event.getShorthand() == Shorthand.RULECALL &&
                           event.getLocation() == Location.END &&
                           event instanceof TransformRuleCallPlannerEvent);
            this.expressionName = expressionName;
        }

        @Override
        public boolean onCallback(final PlannerRepl plannerRepl, final PlannerEvent plannerEvent) {
            if (super.onCallback(plannerRepl, plannerEvent)) {
                final TransformRuleCallPlannerEvent transformRuleCallEvent = (TransformRuleCallPlannerEvent)plannerEvent;
                final var ruleCall = transformRuleCallEvent.getRuleCall();
                final var newExpressions = Iterables.concat(ruleCall.getNewFinalExpressions(),
                        ruleCall.getNewExploratoryExpressions());
                return Streams.stream(newExpressions)
                        .map(expression -> Optional.ofNullable(plannerRepl.nameForObject(expression)))
                        .anyMatch(nameOptional -> nameOptional.isPresent() && expressionName.equals(nameOptional.get()));
            }
            return false;
        }

        @Override
        public void onList(final PlannerRepl plannerRepl) {
            super.onList(plannerRepl);
            plannerRepl.print("; ");
            plannerRepl.printKeyValue("shorthand", Shorthand.RULECALL.name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", Location.END.name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("expression", expressionName);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final OnYieldExpressionBreakPoint that = (OnYieldExpressionBreakPoint)o;
            return expressionName.equals(that.expressionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expressionName);
        }
    }

    /**
     * Breakpoint that breaks when a transform rule call yields a new match for a given candidate.
     */
    public static class OnYieldMatchBreakPoint extends BreakPoint {
        @Nonnull
        private final String candidateName;

        public OnYieldMatchBreakPoint(@Nonnull final String candidateName) {
            super(event -> event.getShorthand() == Shorthand.RULECALL &&
                           event.getLocation() == Location.END &&
                           event instanceof TransformRuleCallPlannerEvent);
            this.candidateName = candidateName;
        }

        @Override
        public boolean onCallback(final PlannerRepl plannerRepl, final PlannerEvent plannerEvent) {
            if (super.onCallback(plannerRepl, plannerEvent)) {
                final TransformRuleCallPlannerEvent transformRuleCallEvent = (TransformRuleCallPlannerEvent)plannerEvent;
                return transformRuleCallEvent.getRuleCall()
                        .getNewPartialMatches()
                        .stream()
                        .anyMatch(partialMatch -> candidateName.equals(partialMatch.getMatchCandidate().getName()));
            }
            return false;
        }

        @Override
        public void onList(final PlannerRepl plannerRepl) {
            super.onList(plannerRepl);
            plannerRepl.print("; ");
            plannerRepl.printKeyValue("shorthand", Shorthand.RULECALL.name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("location", Location.END.name().toLowerCase(Locale.ROOT) + "; ");
            plannerRepl.printKeyValue("candidate", candidateName);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final OnYieldMatchBreakPoint that = (OnYieldMatchBreakPoint)o;
            return candidateName.equals(that.candidateName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(candidateName);
        }
    }

    /**
     * Breakpoint that breaks when the planner attempts to match an expression against a match candidate.
     */
    public static class OnRuleBreakPoint extends BreakPoint {

        @Nonnull
        private final String ruleNamePrefix;

        @Nonnull
        private final Location location;

        public OnRuleBreakPoint(@Nonnull final String ruleNamePrefix, @Nonnull final Location location) {
            super(event -> event.getShorthand() == Shorthand.TRANSFORM &&
                           event.getLocation() == location &&
                           event instanceof TransformPlannerEvent);
            this.ruleNamePrefix = ruleNamePrefix;
            this.location = location;
        }

        @Override
        public boolean onCallback(final PlannerRepl plannerRepl, final PlannerEvent plannerEvent) {
            if (super.onCallback(plannerRepl, plannerEvent)) {
                final TransformPlannerEvent transformEvent =
                        (TransformPlannerEvent)plannerEvent;
                return (Location.ANY == location || plannerEvent.getLocation() == location) &&
                       transformEvent
                               .getRule()
                               .getClass()
                               .getSimpleName()
                               .startsWith(ruleNamePrefix);
            }
            return false;
        }

        @Override
        public void onList(final PlannerRepl plannerRepl) {
            super.onList(plannerRepl);
            plannerRepl.print("; ");
            plannerRepl.printKeyValue("ruleNamePrefix", ruleNamePrefix + "; ");
            plannerRepl.printKeyValue("location", location.name());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final OnRuleBreakPoint that = (OnRuleBreakPoint)o;
            return ruleNamePrefix.equals(that.ruleNamePrefix) &&
                   location == that.location;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ruleNamePrefix, location);
        }
    }

    /**
     * Breakpoint that breaks when the planner attempts to match an expression against a match candidate.
     */
    public static class OnRuleCallBreakPoint extends BreakPoint {

        @Nonnull
        private final String ruleNamePrefix;

        @Nonnull
        private final Location location;

        public OnRuleCallBreakPoint(@Nonnull final String ruleNamePrefix, @Nonnull final Location location) {
            super(event -> event.getShorthand() == Shorthand.RULECALL &&
                           event.getLocation() == location &&
                           event instanceof TransformRuleCallPlannerEvent);
            this.ruleNamePrefix = ruleNamePrefix;
            this.location = location;
        }

        @Override
        public boolean onCallback(final PlannerRepl plannerRepl, final PlannerEvent plannerEvent) {
            if (super.onCallback(plannerRepl, plannerEvent)) {
                final TransformRuleCallPlannerEvent transformRuleCallEvent =
                        (TransformRuleCallPlannerEvent)plannerEvent;
                return (Location.ANY == location || plannerEvent.getLocation() == location) &&
                       transformRuleCallEvent
                               .getRule()
                               .getClass()
                               .getSimpleName()
                               .startsWith(ruleNamePrefix);
            }
            return false;
        }

        @Override
        public void onList(final PlannerRepl plannerRepl) {
            super.onList(plannerRepl);
            plannerRepl.print("; ");
            plannerRepl.printKeyValue("ruleNamePrefix", ruleNamePrefix + "; ");
            plannerRepl.printKeyValue("location", location.name());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final OnRuleCallBreakPoint that = (OnRuleCallBreakPoint)o;
            return ruleNamePrefix.equals(that.ruleNamePrefix) &&
                   location == that.location;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ruleNamePrefix, location);
        }
    }
}
