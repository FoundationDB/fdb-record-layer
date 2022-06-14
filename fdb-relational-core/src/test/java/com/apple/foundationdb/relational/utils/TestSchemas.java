/*
 * TestSchemas.java
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

package com.apple.foundationdb.relational.utils;

/**
 * A Set of commonly used schema template configurations, so that we aren't copy-and-pasting the same DDL logic
 * everywhere.
 */
public final class TestSchemas {

    private TestSchemas() {
    }

    private static final String RESTAURANT_SCHEMA =
            "CREATE STRUCT Location (address string, latitude string, longitude string);" +
                    "CREATE STRUCT RestaurantReview (reviewer int64, rating int64);" +
                    "CREATE STRUCT RestaurantTag (tag string, weight int64);" +
                    "CREATE STRUCT ReviewerStats (start_date int64, school_name string, hometown string);" +
                    "CREATE TABLE RestaurantRecord (rest_no int64, name string, location Location, reviews RestaurantReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no));" +
                    "CREATE TABLE RestaurantReviewer (id int64, name string, email string, stats ReviewerStats, PRIMARY KEY(id));" +
                    "CREATE VALUE INDEX record_name_idx on RestaurantRecord(name);" +
                    "CREATE VALUE INDEX reviewer_name_idx on RestaurantReviewer(name) ";

    private static final String RESTAURANT_COMPLEX_SCHEMA =
            "CREATE STRUCT Location (address string, latitude string, longitude string);" +
                    "CREATE STRUCT ReviewerEndorsements (endorsementId int64, endorsementText string);" +
                    "CREATE STRUCT RestaurantComplexReview (reviewer int64, rating int64, endorsements ReviewerEndorsements array);" +
                    "CREATE STRUCT RestaurantTag (tag string, weight int64);" +
                    "CREATE STRUCT ReviewerStats (start_date int64, school_name string, hometown string);" +
                    "CREATE TABLE RestaurantComplexRecord (rest_no int64, name string, location Location, reviews RestaurantComplexReview ARRAY, tags RestaurantTag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no));" +
                    "CREATE TABLE RestaurantReviewer (id int64, name string, email string, stats ReviewerStats, PRIMARY KEY(id));" +
                    "CREATE VALUE INDEX record_name_idx on RestaurantComplexRecord(name);" +
                    "CREATE VALUE INDEX reviewer_name_idx on RestaurantReviewer(name);" +
                    "CREATE MATERIALIZED VIEW mv1 AS SELECT R.rating from RestaurantComplexRecord AS Rec, (select rating from Rec.reviews) R;" +
                    "CREATE MATERIALIZED VIEW mv2 AS SELECT endo.endorsementText FROM RestaurantComplexRecord rec, (SELECT X.endorsementText FROM rec.reviews rev, (SELECT endorsementText from rev.endorsements) X) endo";

    public static String restaurant() {
        return RESTAURANT_SCHEMA;
    }

    public static String restaurantComplex() {
        return RESTAURANT_COMPLEX_SCHEMA;
    }

    //TODO(bfines) the Query engine can't handle INCLUDE statements yet(TODO)
    public static String restaurantWithCoveringIndex() {
        return RESTAURANT_SCHEMA + "; " + "CREATE VALUE INDEX record_type_covering on RestaurantRecord(rest_no) INCLUDE (name);";
    }
}
