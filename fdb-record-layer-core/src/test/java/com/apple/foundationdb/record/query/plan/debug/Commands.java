/*
 * Commands.java
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

package com.apple.foundationdb.record.query.plan.debug;

import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger.Event;
import com.apple.foundationdb.record.query.plan.temp.debug.RestartException;
import com.google.auto.service.AutoService;
import com.google.common.base.Enums;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.math.NumberUtils;
import org.jline.reader.ParsedLine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class containing all implementations of {@link Command}
 * in a <em>sealed trait</em> style (except it's not sealed).
 */
public class Commands {
    /**
     * Interface for all kinds of commands. Commands are typed by event. When we receive a callback from the
     * planner we also receive that callback using some sort of event. The kind of event defines the context for
     * all actions taken place while the REPL has control. So for instance it may be useful for a command
     * "current" to know the current event so it can properly print out information depending on the specifics
     * of the event.
     *
     * All commands are <em>discovered</em> through a {@link java.util.ServiceLoader}. In order to create the
     * correct information in the meta-info of the jar, we use the {@link AutoService} annotation for all
     * discoverable commands.
     *
     * @param <E> the type of the event
     */
    public interface Command<E extends Event> {
        /**
         * Method that is called from the REPL when a command is interpreted that starts with the string that
         * {@link #getCommandToken()} returns.
         * @param plannerRepl the REPL
         * @param event the current event
         * @param parsedLine the tokenized input string
         * @return {@code true} if planning should continue afterwards, {@code false} if the REPL should prompt
         *         for the next command after the execution of this command has finished.
         */
        boolean executeCommand(@Nonnull PlannerRepl plannerRepl, @Nonnull E event, @Nonnull ParsedLine parsedLine);

        /**
         * The command in as a string.
         * @return the comman token
         */
        @Nonnull
        String getCommandToken();

        void printUsage(@Nonnull final PlannerRepl plannerRepl);
    }

    /**
     * Break point command.
     *
     * Supports:
     * <ul>
     * <li>{@code break [list]} -- show all currently defined break points</li>
     * <li>{@code break yield <identifier>} -- set a break point when the specified expression is yielded</li>
     * <li>{@code break remove <index>} -- remove the break point at the specified index</li>
     * <li>{@code break <rule> [<location>]} -- set a break point when the specified event happens</li>
     * </ul>
     *
     */
    @AutoService(Command.class)
    public static class BreakCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            final List<String> words = parsedLine.words();

            if (words.size() == 1) {
                listBreakPoints(plannerRepl);
                return false;
            }

            if (words.size() >= 2) {
                final String word1 = words.get(1).toUpperCase();
                if (words.size() == 2) {
                    if ("LIST".equals(word1.toUpperCase())) {
                        listBreakPoints(plannerRepl);
                        return false;
                    }
                }

                if ("YIELD".equals(word1.toUpperCase())) {
                    if (words.size() == 3) {
                        final String word2 = words.get(2);
                        if (!plannerRepl.isValidEntityName(word2)) {
                            plannerRepl.printlnError("invalid identifier");
                            return false;
                        }
                        plannerRepl.addBreakPoint(new PlannerRepl.OnYieldBreakPoint(word2.toLowerCase()));
                        return false;
                    }
                    plannerRepl.printlnError("usage: break yield identifier");
                    return false;
                }

                if ("REMOVE".equals(word1.toUpperCase())) {
                    if (words.size() == 3) {
                        final String word2 = words.get(2);
                        @Nullable final Integer index = PlannerRepl.getIdFromIdentifier(word2, "");
                        if (index == null) {
                            plannerRepl.printlnError("invalid index for break point");
                            return false;
                        }
                        if (plannerRepl.removeBreakPoint(index) != null) {
                            plannerRepl.printHighlighted("break point " + index + " removed.");
                            plannerRepl.println();
                        } else {
                            plannerRepl.printlnError("unable to find break point " + index + ".");
                        }
                        return false;
                    }
                    plannerRepl.printlnError("usage: break remove <index>");
                }

                final Optional<Debugger.Shorthand> shorthandOptional =
                        Enums.getIfPresent(Debugger.Shorthand.class, word1).toJavaUtil();
                if (!shorthandOptional.isPresent()) {
                    plannerRepl.printlnError("unknown event class, should be one of [" +
                                             Arrays.stream(Debugger.Shorthand.values()).map(e -> e.name()).collect(Collectors.joining(", ")) +
                                             "].");
                    return false;
                }
                final Debugger.Shorthand shorthand = shorthandOptional.get();

                if (words.size() == 2) {
                    plannerRepl.addBreakPoint(new PlannerRepl.OnEventTypeBreakPoint(shorthand, Debugger.Location.ANY));
                    return false;
                }

                if (words.size() >= 3) {
                    final String word2 = words.get(2).toUpperCase();
                    final Debugger.Location location = Enums.getIfPresent(Debugger.Location.class, word2.toUpperCase()).toJavaUtil().orElse(Debugger.Location.ANY);
                    plannerRepl.addBreakPoint(new PlannerRepl.OnEventTypeBreakPoint(shorthand, location));
                }
                return false;
            }

            plannerRepl.printlnError("usage: break event_type [location]");
            return false;
        }

        private void listBreakPoints(final PlannerRepl plannerRepl) {
            for (PlannerRepl.BreakPoint breakPoint : plannerRepl.getBreakPoints()) {
                final int index = Objects.requireNonNull(plannerRepl.lookupBreakPoint(breakPoint));
                plannerRepl.printKeyValue("id", index + "; ");
                breakPoint.onList(plannerRepl);
                plannerRepl.println();
            }
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "BREAK";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("break [list | yield <id> | remove <index> | <event> [ <location>]]", "manage breakpoints");
        }
    }

    /**
     * Continue execution.
     */
    @AutoService(Command.class)
    public static class ContinueCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            return true;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "CONT";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("cont", "continue execution");
        }
    }

    /**
     * Dump the current event.
     */
    @AutoService(Command.class)
    public static class CurrentCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            final State state = plannerRepl.getCurrentState();
            final Event e = state.getEvents().get(state.getCurrentTick());
            plannerRepl.withProcessors(e, processor -> processor.onDetail(plannerRepl, e));
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "CURRENT";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("current", "dump current event");
        }
    }

    /**
     * List out all events already seen.
     */
    @AutoService(Command.class)
    public static class EventsCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            final State state = plannerRepl.getCurrentState();
            final List<Event> eventList = state.getEvents();
            for (int tick = 0; tick < eventList.size(); tick++) {
                final Event e = eventList.get(tick);
                if (state.getCurrentTick() == tick) {
                    plannerRepl.printHighlighted("==> ");
                } else {
                    plannerRepl.print("    ");
                }
                plannerRepl.printKeyValue("tick", tick + "; ");
                plannerRepl.withProcessors(e, processor -> processor.onList(plannerRepl, e));
                plannerRepl.println();
            }
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "EVENTS";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("events", "list history of events");
        }
    }

    /**
     * List out all expressions.
     */
    @AutoService(Command.class)
    public static class ExpsCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            final State state = plannerRepl.getCurrentState();
            final Cache<Integer, RelationalExpression> expressionCache = state.getExpressionCache();
            final List<Integer> ids = Lists.newArrayList(expressionCache.asMap().keySet());
            Collections.sort(ids);
            for (Integer id : ids) {
                plannerRepl.printKeyValue("id", "exp" + id + "; ");
                @Nullable final RelationalExpression expression = expressionCache.getIfPresent(id);
                if (expression != null) {
                    final String quantifiersString = expression.getQuantifiers()
                            .stream()
                            .map(plannerRepl::nameForObjectOrNotInCache)
                            .collect(Collectors.joining(", "));
                    plannerRepl.printKeyValue("structure", expression.getClass().getSimpleName() + "(" + quantifiersString + ")");
                }
                plannerRepl.println();
            }
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "EXPS";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("exps", "list all expressions");
        }
    }

    /**
     * List out all references.
     */
    @AutoService(Command.class)
    public static class HelpCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            plannerRepl.printlnHighlighted("Basic Usage");
            plannerRepl.println();
            for (final Command<Event> command : PlannerRepl.getCommands()) {
                command.printUsage(plannerRepl);
            }
            plannerRepl.println();
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "HELP";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("help", "print this help");
        }
    }

    /**
     * List out all references.
     */
    @AutoService(Command.class)
    public static class RefsCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            final State state = plannerRepl.getCurrentState();
            final Cache<Integer, ExpressionRef<? extends RelationalExpression>> referenceCache = state.getReferenceCache();
            final List<Integer> ids = Lists.newArrayList(referenceCache.asMap().keySet());
            Collections.sort(ids);
            for (Integer id : ids) {
                plannerRepl.printKeyValue("id", "ref" + id + "; ");
                @Nullable final ExpressionRef<? extends RelationalExpression> reference = referenceCache.getIfPresent(id);
                plannerRepl.printKeyValue("kind", reference == null ? "not in cache; " : reference.getClass().getSimpleName() + "; ");
                if (reference instanceof GroupExpressionRef) {
                    final GroupExpressionRef<? extends RelationalExpression> groupReference = (GroupExpressionRef<? extends RelationalExpression>)reference;
                    final String membersString = groupReference.getMembers()
                            .stream()
                            .map(expression -> Optional.ofNullable(plannerRepl.nameForObject(expression)))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.joining(", "));
                    plannerRepl.printKeyValue("members: ", "{" + membersString + "}");
                }
                plannerRepl.println();
            }
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "REFS";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("refs", "list all references");
        }
    }

    /**
     * Restart execution. All entity names remain stable.
     */
    @AutoService(Command.class)
    public static class RestartCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            plannerRepl.restartState();
            plannerRepl.addInternalBreakPoint(new PlannerRepl.CountingTautologyBreakPoint(1));
            plannerRepl.printHighlighted("restarting planning...");
            plannerRepl.println();
            throw new RestartException();
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "RESTART";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("restart", "restart the planner and return to tick 0");
        }
    }

    /**
     * Show an entity using its name:
     * {@code show <entityname>} where entity name is {@code exp<id>, ref<id>, or qun<id>}.
     */
    @AutoService(Command.class)
    public static class ShowCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            final List<String> words = parsedLine.words();
            if (words.size() < 2) {
                plannerRepl.printlnError("usage show [exp|ref|qun]id");
                return false;
            }

            plannerRepl.processIdentifiers(words.get(1),
                    expression -> expression.show(true),
                    reference -> {
                        if (reference instanceof GroupExpressionRef) {
                            ((GroupExpressionRef<? extends RelationalExpression>)reference).show(true);
                        } else {
                            plannerRepl.println("show is not supported for non-group references.");
                        }
                    },
                    quantifier -> plannerRepl.println("show is not supported for quantifiers."));
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "SHOW";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("show (<expId> | <refId> | <qunId>)", "render the entity graphically");
        }
    }

    /**
     * Print the current state of the task stack.
     */
    @AutoService(Command.class)
    public static class TasksCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            if (event instanceof Debugger.AbstractEventWithState) {
                final Deque<CascadesPlanner.Task> taskStack = ((Debugger.AbstractEventWithState)event).getTaskStack();
                final int size = taskStack.size();

                int i = 0;
                for (final Iterator<CascadesPlanner.Task> iterator = taskStack.descendingIterator(); iterator.hasNext(); i ++) {
                    final CascadesPlanner.Task task = iterator.next();
                    final Event e = task.toTaskEvent(Debugger.Location.ANY);

                    if (i + 1 == size) {
                        plannerRepl.printHighlighted(" ==> ");
                    } else {
                        plannerRepl.print("     ");
                    }

                    plannerRepl.withProcessors(e, p -> p.onList(plannerRepl, e));
                    plannerRepl.println();
                }

            } else {
                plannerRepl.printlnError("event does not contain information about current state of task stack.");
            }
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "TASKS";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("tasks", "show the current state of the task stack");
        }
    }

    /**
     * Continue the specified (or one) number of steps.
     * {@code step [<number>]} continue execution for the next number of steps.
     */
    @AutoService(Command.class)
    public static class StepCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            final List<String> words = parsedLine.words();
            final int steps;
            if (words.size() == 2) {
                steps = NumberUtils.toInt(words.get(1));
            } else {
                steps = 1;
            }

            if (steps == 0) {
                plannerRepl.printlnError("usage step [number]");
                return false;
            }

            plannerRepl.addInternalBreakPoint(new PlannerRepl.CountingTautologyBreakPoint(steps));
            return true;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "STEP";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("step [<number>]", "continue execution by the number of specified steps (default: 1)");
        }
    }

    /**
     * List out all quantifiers.
     */
    @AutoService(Command.class)
    public static class QunsCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            final State state = plannerRepl.getCurrentState();
            final List<Integer> ids = Lists.newArrayList(state.getQuantifierCache().asMap().keySet());
            Collections.sort(ids);
            for (Integer id : ids) {
                plannerRepl.printKeyValue("id", "qun" + id + "; ");
                @Nullable final Quantifier quantifier = state.getQuantifierCache().getIfPresent(id);
                if (quantifier != null) {
                    plannerRepl.printKeyValue("kind", quantifier.getShorthand() + "; ");
                    plannerRepl.printKeyValue("alias", quantifier.getAlias().toString() + "; ");
                    final ExpressionRef<? extends RelationalExpression> rangesOver = quantifier.getRangesOver();
                    plannerRepl.printKeyValue("ranges over", plannerRepl.nameForObjectOrNotInCache(rangesOver));
                }
                plannerRepl.println();
            }
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "QUNS";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("quns", "list all quantifiers");
        }
    }

    /**
     * List out all quantifiers.
     */
    @AutoService(Command.class)
    public static class QuitCommand implements Command<Event> {
        @Override
        public boolean executeCommand(@Nonnull final PlannerRepl plannerRepl,
                                      @Nonnull final Event event,
                                      @Nonnull final ParsedLine parsedLine) {
            plannerRepl.printlnHighlighted("I hope you found the problem.");
            System.exit(0);
            return false;
        }

        @Nonnull
        @Override
        public String getCommandToken() {
            return "QUIT";
        }

        @Override
        public void printUsage(@Nonnull final PlannerRepl plannerRepl) {
            plannerRepl.printlnKeyValue("quit", "System.exit(0)");
        }
    }
}
