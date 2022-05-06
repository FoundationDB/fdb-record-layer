/*
 * Utils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * set of utility functions to generate PB objects for testing.
 */
public final class Utils {

    static Random r = new Random(42);

    static volatile AtomicLong sequencer = new AtomicLong();

    static Iterable<Message> generateRestaurantRecords(int count, RelationalStatement statement) {
        return generateList(count, () -> Utils.generateRestaurantRecord(statement));
    }

    static Message generateRestaurantRecord(RelationalStatement statement) {
        int numReviews = r.nextInt(5) + 1;
        int numTags = r.nextInt(5) + 1;
        int numCustomers = r.nextInt(5) + 1;

        try {
            return statement.getDataBuilder("RestaurantRecord")
                    .setField("rest_no", sequencer.incrementAndGet())
                    .setField("name", "restName" + r.nextInt())
                    .setField("location", generateLocation(statement))
                    .addRepeatedFields("reviews", generateReviews(statement, numReviews))
                    .addRepeatedFields("tags", generateTags(statement, numTags))
                    .addRepeatedFields("customer", generateList(numCustomers, () -> "cust" + r.nextInt()))
                    .build();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    private static Message generateLocation(RelationalStatement statement) {
        try {
            return statement.getDataBuilder("Location")
                    .setField("address", "addr" + r.nextInt())
                    .setField("latitude", "lat" + r.nextInt())
                    .setField("longitude", "long" + r.nextInt())
                    .build();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    private static Message generateReview(RelationalStatement statement) {
        try {
            return statement.getDataBuilder("RestaurantReview")
                    .setField("rating", r.nextInt(5))
                    .setField("reviewer", r.nextInt())
                    .build();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    private static Iterable<Message> generateReviews(RelationalStatement statement, int numReviewers) {
        return generateList(numReviewers, () -> generateReview(statement));
    }

    private static Message generateTag(RelationalStatement statement) {
        try {
            return statement.getDataBuilder("RestaurantTag")
                    .setField("tag", "tag" + r.nextInt())
                    .setField("weight", r.nextInt())
                    .build();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    private static Iterable<Message> generateTags(RelationalStatement statement, int numTags) {
        return generateList(numTags, () -> generateTag(statement));
    }

    private static <T> Iterable<T> generateList(int count, Supplier<T> supplier) {
        assert count >= 0;

        List<T> result = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            result.add(supplier.get());
        }

        return result;
    }

    private Utils() {
    }
}
