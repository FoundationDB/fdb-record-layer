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

import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.plan.debug.PlannerRepl;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.debug.Debugger;
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

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class NorseTest extends FDBRecordStoreQueryTestBase {

    @Test
    void testSimpleStatement() throws Exception {
        setupStore();

        //final ANTLRInputStream in = new ANTLRInputStream("from('RestaurantRecord') | filter r => r.name = 'Heirloom Cafe' || r.rest_no < 5");
        //final ANTLRInputStream in = new ANTLRInputStream("from 'RestaurantRecord' | filter _.name = 'Heirloom Cafe' || _.rest_no < 5 | map r => (r.name, r.rest_no)");
        //final ANTLRInputStream in = new ANTLRInputStream("from 'RestaurantRecord' | filter exists(from 'RestaurantReviewer' | filter _.id = 5)");
        //final ANTLRInputStream in = new ANTLRInputStream("[(rating, restaurant): restaurant <- from('RestaurantRecord'), review <- restaurant.reviews, rating := review.rating] | group _.rating | agg (_, restaurants) -> count(restaurants)");
        //final ANTLRInputStream in = new ANTLRInputStream("from('RestaurantRecord')");
        final ANTLRInputStream in = new ANTLRInputStream("[(restaurant, reviewer): restaurant <- from('RestaurantRecord'); reviewer <- from 'RestaurantReviewer'] | filter (r, _) => r.name = 'Heirloom Cafe'");
        NorseLexer lexer = new NorseLexer(in);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        NorseParser parser = new NorseParser(tokens);
        final ParseTree tree = parser.pipe();

        final ParserWalker visitor = new ParserWalker(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
        Object o = visitor.visit(tree);
        System.out.println(o);
    }

    @Test
    void repl() throws Exception {
        setupStore();

        final PlannerRepl repl = Objects.requireNonNull((PlannerRepl)Debugger.getDebugger());
        final CascadesPlanner planner = new CascadesPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());

        while (true) {
            final String command = repl.readLine(new AttributedStringBuilder()
                    .style(AttributedStyle.DEFAULT.foreground(AttributedStyle.WHITE + AttributedStyle.BRIGHT).bold())
                    .append("norse ")
                    .toAnsi());

            if (command.equalsIgnoreCase("clear")) {
                repl.println("cLeArScReEn");
                continue;
            }

            final String[] words = command.split(" ", 2);


            try {
                if (words.length == 2) {
                    if (words[0].equalsIgnoreCase("describe")) {
                        final RelationalExpression relationalExpression = parseQuery(words[1]);
                        repl.println(relationalExpression.getResultType().toString());
                        continue;
                    }
                }
            } catch (final Throwable t) {
                printStacktrace(repl, t);
                continue;
            }

            final RecordQueryPlan recordQueryPlan;
            try {
                recordQueryPlan = planner.plan(command, (query, context) -> {
                    // TODO remove this hack
                    //repl.removeInternalBreakPoints();
                    return parseQuery(command);
                });
            } catch (final Throwable t) {
                printStacktrace(repl, t);
                continue;
            }

            try {
                long numRecords = 0;
                try (FDBRecordContext context = openContext()) {
                    openNestedRecordStore(context);
                    try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(recordQueryPlan).asIterator()) {
                        while (cursor.hasNext()) {
                            FDBQueriedRecord<Message> rec = cursor.next();
                            repl.println(TextFormat.shortDebugString(Objects.requireNonNull(rec).getRecord()));
                            numRecords ++;
                        }
                    }
                }
                repl.println();
                repl.printlnHighlighted(numRecords + " record(s) selected.");
                repl.println();
            } catch (final Throwable t) {
                printStacktrace(repl, t);
                continue;
            }
        }
    }

    private RelationalExpression parseQuery(final String command) {
        final ANTLRInputStream in = new ANTLRInputStream(command);
        NorseLexer lexer = new NorseLexer(in);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        NorseParser parser = new NorseParser(tokens);
        final ParseTree tree = parser.pipe();

        final ParserWalker parserWalker = new ParserWalker(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
        final RelationalExpression relationalExpression = (RelationalExpression)parserWalker.visit(tree);
        return relationalExpression;
    }

    private void printStacktrace(@Nonnull final PlannerRepl repl, @Nonnull final Throwable t) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final String utf8 = StandardCharsets.UTF_8.name();
        try (PrintStream ps = new PrintStream(baos, true, utf8)) {
            t.printStackTrace(ps);
            repl.printlnError(baos.toString());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
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

            TestRecords4Proto.RestaurantRecord.Builder recBuilder = TestRecords4Proto.RestaurantRecord.newBuilder();
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

            recBuilder = TestRecords4Proto.RestaurantRecord.newBuilder();
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
