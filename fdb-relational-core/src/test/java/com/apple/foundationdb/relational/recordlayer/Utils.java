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

import com.apple.foundationdb.record.Restaurant;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

/**
 * set of utility functions to generate PB objects for testing.
 */
public class Utils {

    static Random r = new Random(42);

    static volatile long sequencer = 0;

    static Iterable<Restaurant.RestaurantRecord> generateRestaurantRecords(int count) {
        return generateList(count, Utils::generateRestaurantRecord);
    }

    static Restaurant.RestaurantRecord generateRestaurantRecord() {
        int numReviewers = r.nextInt(5) + 1;
        int numTags = r.nextInt(5) + 1;
        int numCustomers = r.nextInt(5) + 1;

        return Restaurant.RestaurantRecord.newBuilder()
                .setRestNo(sequencer++)
                .setName("restName" + r.nextInt())
                .setLocation(generateLocation())
                .addAllReviews(generateReviewers(numReviewers))
                .addAllTags(generateTags(numTags))
                .addAllCustomer(generateList(numCustomers, () -> "cust" + r.nextInt()))
                .build();
    }

    private static Restaurant.Location generateLocation() {
        return Restaurant.Location.newBuilder()
                .setAddress("addr" + r.nextInt())
                .setLatitude("lat" + r.nextInt())
                .setLongitude("long" + r.nextInt())
                .build();
    }

    private static Restaurant.RestaurantReview generateReviewer() {
        return Restaurant.RestaurantReview.newBuilder()
                .setRating(r.nextInt(5))
                .setReviewer(r.nextInt())
                .build();
    }

    private static Iterable<Restaurant.RestaurantReview> generateReviewers(int numReviewers) {
        return generateList(numReviewers, Utils::generateReviewer);
    }

    private static Restaurant.RestaurantTag generateTag() {
        return Restaurant.RestaurantTag.newBuilder()
                .setTag("tag" + r.nextInt())
                .setWeight(r.nextInt())
                .build();
    }

    private static Iterable<Restaurant.RestaurantTag> generateTags(int numTags) {
        return generateList(numTags, Utils::generateTag);
    }

    private static <T> Iterable<T> generateList(int count, Supplier<T> supplier) {
        assert count >= 0;

        List<T> result = new ArrayList<>();

        for(int i = 0; i < count; ++i) {
            result.add(supplier.get());
        }

        return result;
    }
}
