/*
 * DebuggerWithSymbolTables.java
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

package com.apple.foundationdb.record.query.plan.cascades.debug;

import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.debug.eventprotos.PEvent;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.base.Verify;
import com.google.common.cache.Cache;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty.referencesAndDependencies;

/**
 * <p>
 * Implementation of a debugger that maintains symbol tables for easier human consumption e.g. in test cases and/or
 * while debugging.
 * </p>
 * <b>Instances of this debugger should only be enabled in test cases, never in deployments</b>.
 */
@SuppressWarnings("PMD.SystemPrintln")
public class DebuggerWithSymbolTables implements SymbolDebugger, StatsDebugger {
    private static final Logger logger = LoggerFactory.getLogger(DebuggerWithSymbolTables.class);

    private final boolean isSane;
    private final boolean isRecordEvents;
    private final Iterable<PEvent> prerecordedEventProtoIterable;
    private final Deque<EventState> eventStateStack;
    private final Deque<SymbolTables> symbolTablesStack;

    @Nullable
    private String queryAsString;
    @Nullable
    private PlanContext planContext;
    @Nonnull
    private final Map<Object, Integer> singletonToIndexMap;

    private DebuggerWithSymbolTables(final boolean isSane, final boolean isRecordEvents,
                                     @Nullable final String prerecordedEventsFileName) {
        this.isSane = isSane;
        this.isRecordEvents = isRecordEvents;
        this.prerecordedEventProtoIterable = prerecordedEventsFileName == null
                                      ? null : eventProtosFromFile(prerecordedEventsFileName);
        this.eventStateStack = new ArrayDeque<>();
        this.symbolTablesStack = new ArrayDeque<>();
        this.planContext = null;
        this.singletonToIndexMap = Maps.newHashMap();
    }

    @Nonnull
    EventState getCurrentEventState() {
        return Objects.requireNonNull(eventStateStack.peek());
    }

    @Nonnull
    SymbolTables getCurrentSymbolState() {
        return Objects.requireNonNull(symbolTablesStack.peek());
    }

    @Nullable
    @Override
    public PlanContext getPlanContext() {
        return planContext;
    }

    @Override
    public boolean isSane() {
        //
        // Report insanity here which then causes all sanity checks to be run which may be CPU-intensive. Deactivate
        // this behavior by returning true if performance is measured.
        //
        return isSane;
    }

    @Override
    public int onGetIndex(@Nonnull final Class<?> clazz) {
        return getCurrentSymbolState().getIndex(clazz);
    }

    @Override
    public int onUpdateIndex(@Nonnull final Class<?> clazz, @Nonnull final IntUnaryOperator updateFn) {
        return getCurrentSymbolState().updateIndex(clazz, updateFn);
    }

    @Override
    public void onRegisterExpression(@Nonnull final RelationalExpression expression) {
        getCurrentSymbolState().registerExpression(expression);
    }

    @Override
    public void onRegisterReference(@Nonnull final Reference reference) {
        getCurrentSymbolState().registerReference(reference);
    }

    @Override
    public void onRegisterQuantifier(@Nonnull final Quantifier quantifier) {
        getCurrentSymbolState().registerQuantifier(quantifier);
    }

    @Override
    public int onGetOrRegisterSingleton(@Nonnull final Object singleton) {
        final var size = singletonToIndexMap.size();
        return singletonToIndexMap.computeIfAbsent(singleton, s -> size);
    }

    @Override
    public void onInstall() {
        // do nothing
    }

    @Override
    public void onSetup() {
        reset();
    }

    @Override
    public void onShow(@Nonnull final Reference ref) {
        // do nothing
    }

    @Override
    public void onQuery(@Nonnull final String recordQuery, @Nonnull final PlanContext planContext) {
        this.eventStateStack.push(EventState.copyOf(getCurrentEventState()));
        this.symbolTablesStack.push(SymbolTables.copyOf(getCurrentSymbolState()));
        this.queryAsString = recordQuery;
        this.planContext = planContext;

        logQuery();
    }

    void restartState() {
        eventStateStack.pop();
        eventStateStack.push(EventState.copyOf(getCurrentEventState()));

        symbolTablesStack.pop();
        symbolTablesStack.push(SymbolTables.copyOf(getCurrentSymbolState()));
    }

    @Override
    @SuppressWarnings("PMD.GuardLogStatement") // false positive
    public void onEvent(final Event event) {
        if ((queryAsString == null) || (planContext == null) || eventStateStack.isEmpty()) {
            return;
        }
        getCurrentEventState().addCurrentEvent(event);
        if (logger.isTraceEnabled()) {
            if (event.getLocation() == Location.END && event instanceof TransformRuleCallEvent) {
                final TransformRuleCallEvent transformRuleCallEvent = (TransformRuleCallEvent)event;
                final CascadesRuleCall ruleCall = transformRuleCallEvent.getRuleCall();
                final var newExpressions =
                        Iterables.concat(ruleCall.getNewFinalExpressions(), ruleCall.getNewExploratoryExpressions());
                if (!Iterables.isEmpty(newExpressions)) {
                    final var logMessage =
                            KeyValueLogMessage.build("rule yielded new expression(s)",
                            "rule", transformRuleCallEvent.getRule().getClass().getSimpleName());
                    final var name  = nameForObject(transformRuleCallEvent.getBindable());
                    if (name != null) {
                        logMessage.addKeyAndValue("name", name);
                    }

                    logMessage.addKeyAndValue("expressions", Streams.stream(newExpressions)
                            .map(this::nameForObject).collect(Collectors.joining(", ")));
                    logger.trace(logMessage.toString());
                }
            }
        }
    }

    @Nullable
    static <T> T lookupInCache(final Cache<Integer, T> cache, final String identifier, final String prefix) {
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
        final SymbolTables state = getCurrentSymbolState();
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

    @Override
    public void onDone() {
        if (!eventStateStack.isEmpty() && queryAsString != null) {
            final var state = Objects.requireNonNull(eventStateStack.peek());
            if (logger.isInfoEnabled()) {
                logger.info(KeyValueLogMessage.of("planning done",
                        "query", Objects.requireNonNull(queryAsString).substring(0, Math.min(queryAsString.length(), 30)),
                        "duration-in-ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - state.getStartTs()),
                        "ticks", state.getCurrentTick()));
            }

            final var eventProtos = state.getEventProtos();
            if (eventProtos != null) {
                writeEventsDelimitedToFile(eventProtos);
            }

            final var prerecordedEventProtoIterator = state.getPrerecordedEventProtoIterator();
            if (prerecordedEventProtoIterator != null) {
                Verify.verify(!prerecordedEventProtoIterator.hasNext(),
                        "There are more prerecorded events, there are only " + state.getCurrentTick() + " actual events.");
            }
        }
        reset();
    }

    private static void writeEventsDelimitedToFile(final List<PEvent> eventProtos) {
        try {
            final var tempFile = File.createTempFile("events-", ".bin");
            try (var fos = new FileOutputStream(tempFile)) {
                for (final var eventProto : eventProtos) {
                    eventProto.writeDelimitedTo(fos);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static Iterable<PEvent> eventProtosFromFile(@Nonnull final String fileName) {
        return () -> readEventsDelimitedFromFile(fileName);
    }

    @SuppressWarnings({"resource", "PMD.CloseResource"})
    @Nonnull
    private static Iterator<PEvent> readEventsDelimitedFromFile(@Nonnull final String fileName) {
        final var file = new File(fileName);
        final FileInputStream fis;
        try {
            fis = new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return new AbstractIterator<>() {
            @Nullable
            @Override
            protected PEvent computeNext() {
                try {
                    final var event = PEvent.parseDelimitedFrom(fis);
                    if (event == null) {
                        fis.close();
                        return endOfData();
                    }
                    return event;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Nonnull
    @Override
    public Optional<StatsMaps> getStatsMaps() {
        EventState currentEventState = eventStateStack.peek();
        if (currentEventState != null) {
            return Optional.of(currentEventState.getStatsMaps());
        }
        return Optional.empty();
    }

    private void reset() {
        this.eventStateStack.clear();
        this.eventStateStack.push(EventState.initial(isRecordEvents, isRecordEvents, prerecordedEventProtoIterable));

        this.symbolTablesStack.clear();
        this.symbolTablesStack.push(new SymbolTables());

        this.planContext = null;
        this.queryAsString = null;
    }

    void logQuery() {
        if (logger.isDebugEnabled()) {
            logger.debug(KeyValueLogMessage.of("planning started", "query", queryAsString));
        }
    }

    @Nonnull
    @SuppressWarnings({"PMD.AvoidPrintStackTrace", "unused", "CallToPrintStackTrace"})
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

    @Nonnull
    public static DebuggerWithSymbolTables withoutSanityChecks() {
        return new DebuggerWithSymbolTables(true, false, null);
    }

    @Nonnull
    public static DebuggerWithSymbolTables withSanityChecks() {
        return new DebuggerWithSymbolTables(false, false, null);
    }

    @Nonnull
    public static DebuggerWithSymbolTables withEventRecording() {
        return new DebuggerWithSymbolTables(true, false, null);
    }

    @Nonnull
    public static DebuggerWithSymbolTables withRerecordEvents() {
        return new DebuggerWithSymbolTables(true, true, null);
    }

    @Nonnull
    public static DebuggerWithSymbolTables withPrerecordedEvents(@Nonnull final String fileName) {
        return new DebuggerWithSymbolTables(true, true, fileName);
    }

    public static void printForEachExpression(@Nonnull final Reference root) {
        forEachExpression(root, expression -> {
            System.out.println("expression: " +
                    SymbolDebugger.mapDebugger(debugger -> debugger.nameForObject(expression)).orElseThrow() + "; " +
                    "hashCodeWithoutChildren: " + expression.hashCodeWithoutChildren() + "explain: " + expression);
        });
    }

    public static void forEachExpression(@Nonnull final Reference root, @Nonnull final Consumer<RelationalExpression> consumer) {
        final var references = referencesAndDependencies().evaluate(root);
        final var referenceList = TopologicalSort.anyTopologicalOrderPermutation(references).orElseThrow();
        for (final var reference : referenceList) {
            for (final var member : reference.getAllMemberExpressions()) {
                consumer.accept(member);
            }
        }
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
}
