/*
 * NorseTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.norse;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords4Proto.RestaurantRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.norse.dynamic.DynamicSchema;
import com.apple.foundationdb.record.query.plan.debug.PlannerRepl;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
import com.apple.foundationdb.record.query.plan.temp.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.predicates.Formatter;
import com.apple.foundationdb.record.query.predicates.Type;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class NorseTest extends FDBRecordStoreQueryTestBase {

    @Test
    void repl() throws Exception {
        setupStore();

        final PlannerRepl repl = Objects.requireNonNull((PlannerRepl)Debugger.getDebugger());
        final CascadesPlanner planner = new CascadesPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());

        while (true) {
            String command = repl.readLine(new AttributedStringBuilder()
                    .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE + AttributedStyle.BRIGHT).bold())
                    .append("norse ")
                    .toAnsi());

            // paste mode
            if (command.equals(":")) {
                final StringBuilder commandBuilder = new StringBuilder();
                String thisLine;
                do {
                    thisLine = repl.readLine(new AttributedStringBuilder()
                            .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE + AttributedStyle.BRIGHT).bold())
                            .append(">>>> ")
                            .toAnsi());
                    commandBuilder.append(thisLine);
                } while (!thisLine.isEmpty());
                command = commandBuilder.toString();
            }

            if (command.isEmpty()) {
                continue;
            }

            if (command.equalsIgnoreCase("clear")) {
                repl.println("cLeArScReEn");
                continue;
            }

            final String[] words = command.split(" ", 2);

            final DynamicSchema.Builder dynamicSchemaBuilder = DynamicSchema.newBuilder();

            try {
                repl.println();

                if (words.length == 2) {
                    if (words[0].equalsIgnoreCase(":describe")) {
                        final RelationalExpression relationalExpression = parseQuery(words[1], dynamicSchemaBuilder);
                        repl.println(relationalExpression.getResultType().toString());
                        continue;
                    } else if (words[0].equalsIgnoreCase(":debug")) {
                        planner.plan(words[1], (query, context) -> parseQuery(query, dynamicSchemaBuilder));
                        repl.printlnHighlighted("end of planner debugger");
                        continue;
                    } else if (words[0].equalsIgnoreCase(":explain")) {
                        final RecordQueryPlan recordQueryPlan = planner.plan(words[1], (query, context) -> {
                            repl.removeInternalBreakPoints();
                            return parseQuery(query, dynamicSchemaBuilder);
                        });
                        repl.println(recordQueryPlan.explain(new Formatter()));
                        continue;
                    }
                }
            } catch (final Throwable t) {
                printError(repl, t);
                repl.println();
                continue;
            }

            final RecordQueryPlan recordQueryPlan;

            try {
                recordQueryPlan = planner.plan(command, (query, context) -> {
                    // TODO remove this hack
                    repl.removeInternalBreakPoints();
                    return parseQuery(query, dynamicSchemaBuilder);
                });
            } catch (final Throwable t) {
                printError(repl, t);
                repl.println();
                continue;
            }

            try {
                long numRecords = 0;
                try (FDBRecordContext context = openContext()) {
                    openNestedRecordStore(context);
                    final Type resultType = recordQueryPlan.getResultType().getInnerType();
                    final Type.Record recordType = resultType instanceof Type.Record ? (Type.Record)resultType : null;

                    final ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
                    if (recordType != null) {
                        for (final Type.Record.Field field : Objects.requireNonNull(recordType.getFields())) {
                            columnNamesBuilder.add(field.getFieldName());
                        }
                    } else {
                        columnNamesBuilder.add("unnamed");
                    }

                    final ImmutableList<String> columnNames = columnNamesBuilder.build();
                    if (columnNames.size() == 1) {
                        repl.printlnHighlighted2(columnNames.get(0));
                        repl.printlnHighlighted2(Strings.repeat("-", columnNames.get(0).length()));
                        repl.println();
                    } else {
                        repl.printlnHighlighted2(columnNames.stream()
                                .map(columnToPrint -> Strings.padEnd(columnToPrint.length() > 40 ? columnToPrint.substring(0, 40) : columnToPrint, 40, ' '))
                                .collect(Collectors.joining("    ")));
                        repl.printlnHighlighted2(columnNames.stream()
                                .map(columnToPrint -> Strings.repeat("-", 40))
                                .collect(Collectors.joining("    ")));
                        repl.println();
                    }

                    try (RecordCursorIterator<?> cursor = recordStore.executePlan(recordQueryPlan, EvaluationContext.forBindingsAndDynamicSchema(Bindings.EMPTY_BINDINGS, dynamicSchemaBuilder.build())).asIterator()) {
                        while (cursor.hasNext()) {
                            final Object next = cursor.next();
                            final ImmutableList.Builder<String> columnsBuilder = ImmutableList.builder();
                            if (next != null) {
                                final Object result = QueryResult.unwrapValue(Objects.requireNonNull(next));
                                if (result instanceof Message) {
                                    Objects.requireNonNull(recordType);
                                    final Message message = (Message)result;
                                    final Descriptors.Descriptor descriptor = message.getDescriptorForType();
                                    for (final Type.Record.Field field : Objects.requireNonNull(recordType.getFields())) {
                                        columnsBuilder.add(resultAsString(message.getField(descriptor.findFieldByNumber(field.getFieldIndex()))));
                                    }
                                } else {
                                    columnsBuilder.add(resultAsString(result));
                                }

                                final ImmutableList<String> columns = columnsBuilder.build();
                                if (columns.size() == 1) {
                                    repl.println(columns.get(0));
                                } else {
                                    repl.println(columns.stream()
                                            .map(columnToPrint -> Strings.padEnd(columnToPrint.length() > 40 ? columnToPrint.substring(0, 40) : columnToPrint, 40, ' '))
                                            .collect(Collectors.joining("    ")));
                                }
                            } else {
                                if (columnNames.size() == 1) {
                                    repl.println("null");
                                } else {
                                    repl.println("tuple is null");
                                }
                            }
                            numRecords ++;
                        }
                    }
                }
                repl.println();
                repl.printlnHighlighted(numRecords + " record(s) selected.");
                repl.println();
            } catch (final Throwable t) {
                printError(repl, t);
                repl.println();
                continue;
            }
        }
    }

    private String resultAsString(final Object result) {
        if (result instanceof Message) {
            return TextFormat.shortDebugString((Message)result);
        } else {
            return result == null ? "null" : result.toString().replace('\n', ' ');
        }
    }

    private RelationalExpression parseQuery(@Nonnull final String command, @Nonnull DynamicSchema.Builder dynamicSchemaBuilder) {
        final ANTLRInputStream in = new ANTLRInputStream(command);
        NorseLexer lexer = new NorseLexer(in);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        NorseParser parser = new NorseParser(tokens);
        final ParseTree tree = parser.pipe();

        final ParserWalker parserWalker = new ParserWalker(dynamicSchemaBuilder, recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
        final RelationalExpression relationalExpression = parserWalker.toGraph(tree);
        final Set<Type> used = UsedTypesProperty.evaluate(relationalExpression);
        used.forEach(dynamicSchemaBuilder::addType);
        return relationalExpression;
    }

    private void printError(@Nonnull final PlannerRepl repl, @Nonnull final Throwable t) {
        if (t instanceof SemanticException || t instanceof ParserWalker.ParseException) {
            repl.printlnError(t.getMessage());
        } else {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final String utf8 = StandardCharsets.UTF_8.name();
            try (PrintStream ps = new PrintStream(baos, true, utf8)) {
                t.printStackTrace(ps);
                repl.printlnError(baos.toString());
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void setupStore() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openNestedRecordStore(context);

            TestRecords4Proto.RestaurantReviewer.Builder reviewerBuilder = TestRecords4Proto.RestaurantReviewer.newBuilder();
            reviewerBuilder.setId(1);
            reviewerBuilder.setName("Lemuel");
            recordStore.saveRecord(reviewerBuilder.build());

            reviewerBuilder.setId(2);
            reviewerBuilder.setName("Gulliver");
            recordStore.saveRecord(reviewerBuilder.build());

            RestaurantRecord.Builder recBuilder = RestaurantRecord.newBuilder();
            recBuilder.setRestNo(101);
            recBuilder.setName("The Emperor's Three Tables");
            TestRecords4Proto.RestaurantReview.Builder reviewBuilder = recBuilder.addReviewsBuilder();
            reviewBuilder.setReviewer(1);
            reviewBuilder.setRating(10);
            reviewBuilder = recBuilder.addReviewsBuilder();
            reviewBuilder.setReviewer(2);
            reviewBuilder.setRating(3);
            TestRecords4Proto.RestaurantTag.Builder tagBuilder = recBuilder.addTagsBuilder();
            tagBuilder.setValue("Lilliput");
            tagBuilder.setWeight(5);
            recordStore.saveRecord(recBuilder.build());

            recBuilder = RestaurantRecord.newBuilder();
            recBuilder.setRestNo(102);
            recBuilder.setName("Small Fry's Fried Victuals");
            reviewBuilder = recBuilder.addReviewsBuilder();
            reviewBuilder.setReviewer(1);
            reviewBuilder.setRating(5);
            reviewBuilder = recBuilder.addReviewsBuilder();
            reviewBuilder.setReviewer(2);
            reviewBuilder.setRating(5);
            tagBuilder = recBuilder.addTagsBuilder();
            tagBuilder.setValue("Lilliput");
            tagBuilder.setWeight(1);
            recordStore.saveRecord(recBuilder.build());

            commit(context);
        }
        final RecordMetaData recordMetaData = recordStore.getRecordMetaData();

        System.out.println(recordMetaData.getRecordTypes());
    }
}
