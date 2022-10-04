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
        return generateList(count, () -> Utils.generateRestaurantRecordWrapped(statement));
    }

    static Message generateRestaurantRecordWrapped(RelationalStatement statement) {
        int numReviews = r.nextInt(5) + 1;
        int numTags = r.nextInt(5) + 1;
        int numCustomers = r.nextInt(5) + 1;

        try {
            return statement.getDataBuilder("RESTAURANT")
                    .setField("REST_NO", sequencer.incrementAndGet())
                    .setField("NAME", "restName" + r.nextInt())
                    .setField("LOCATION", generateLocation(statement))
                    .addRepeatedFields("REVIEWS", generateList(numReviews, () -> generateReview(statement)))
                    .addRepeatedFields("TAGS", generateList(numTags, () -> generateTag(statement)))
                    .addRepeatedFields("CUSTOMER", generateList(numCustomers, () -> "cust" + r.nextInt()))
                    .build();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    private static Message generateLocation(RelationalStatement statement) {
        try {
            return statement.getDataBuilder("LOCATION")
                    .setField("ADDRESS", "addr" + r.nextInt())
                    .setField("LATITUDE", "lat" + r.nextInt())
                    .setField("LONGITUDE", "long" + r.nextInt())
                    .build();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    private static Message generateReview(RelationalStatement statement) {
        try {
            return statement.getDataBuilder("RESTAURANT_REVIEW")
                    .setField("RATING", r.nextInt(5))
                    .setField("REVIEWER", r.nextInt())
                    .build();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    private static Message generateTag(RelationalStatement statement) {
        try {
            return statement.getDataBuilder("RESTAURANT_TAG")
                    .setField("TAG", "tag" + r.nextInt())
                    .setField("WEIGHT", r.nextInt())
                    .build();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
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
