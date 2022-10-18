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
            "CREATE STRUCT Location (address string, latitude string, longitude string)" +
                    "CREATE STRUCT restaurant_review (reviewer int64, rating int64)" +
                    "CREATE STRUCT restaurant_tag (tag string, weight int64)" +
                    "CREATE STRUCT reviewer_stats (start_date int64, school_name string, hometown string)" +
                    "CREATE TABLE restaurant (rest_no int64, name string, location Location, reviews restaurant_review ARRAY, tags restaurant_tag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    "CREATE TABLE restaurant_reviewer (id int64, name string, email string, stats reviewer_stats, PRIMARY KEY(id))" +
                    "CREATE INDEX record_name_idx as select name from restaurant " +
                    "CREATE INDEX reviewer_name_idx as select name from restaurant_reviewer ";

    public static String restaurant() {
        return RESTAURANT_SCHEMA;
    }

    //TODO(bfines) the Query engine can't handle INCLUDE statements yet(TODO)
    public static String restaurantWithCoveringIndex() {
        return RESTAURANT_SCHEMA + " " + "CREATE INDEX record_type_covering as select rest_no, name from restaurant order by rest_no";
    }
}
