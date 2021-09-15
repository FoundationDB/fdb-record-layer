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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.google.common.collect.Iterables;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class NorseTest extends FDBRecordStoreQueryTestBase {

    @Test
    void testSimpleStatement() throws Exception {
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

        //final ANTLRInputStream in = new ANTLRInputStream("from('RestaurantRecord') | filter r => r.name = 'Heirloom Cafe' || r.rest_no < 5");
        final ANTLRInputStream in = new ANTLRInputStream("from 'RestaurantRecord' | filter _.name = 'Heirloom Cafe' || _.rest_no < 5 | map r => (r.name, r.rest_no)");
        //final ANTLRInputStream in = new ANTLRInputStream("[(rating, restaurant): restaurant <- from('RestaurantRecord'), review <- restaurant.reviews, rating := review.rating] | group _.rating | agg (_, restaurants) -> count(restaurants)");
        //final ANTLRInputStream in = new ANTLRInputStream("from('RestaurantRecord')");
        NorseLexer lexer = new NorseLexer(in);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        NorseParser parser = new NorseParser(tokens);
        final ParseTree tree = parser.pipe();

        final ParserWalker visitor = new ParserWalker(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
        Object o = visitor.visit(tree);
        final RelationalExpression fuse = Iterables.getOnlyElement(Iterables.getOnlyElement(((RelationalExpression)o).getQuantifiers()).getRangesOver().getMembers());
        System.out.println(fuse.getResultType());
        System.out.println(o);
    }
}
