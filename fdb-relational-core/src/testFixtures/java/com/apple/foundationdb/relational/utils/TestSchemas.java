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

import javax.annotation.Nonnull;

/**
 * A Set of commonly used schema template configurations, so that we aren't copy-and-pasting the same DDL logic
 * everywhere.
 */
public final class TestSchemas {

    private TestSchemas() {
    }

    private static final String RESTAURANT_SCHEMA =
            "CREATE TYPE AS STRUCT Location (address string, latitude string, longitude string)" +
                    "CREATE TYPE AS STRUCT restaurant_review (reviewer bigint, rating bigint)" +
                    "CREATE TYPE AS STRUCT restaurant_tag (tag string, weight bigint)" +
                    "CREATE TYPE AS STRUCT reviewer_stats (start_date bigint, school_name string, hometown string)" +
                    "CREATE TABLE restaurant (rest_no bigint, name string, location Location, reviews restaurant_review ARRAY, tags restaurant_tag array, customer string array, encoded_bytes bytes, PRIMARY KEY(rest_no))" +
                    "CREATE TABLE restaurant_reviewer (id bigint, name string, email string, stats reviewer_stats, PRIMARY KEY(id))" +
                    "CREATE INDEX record_name_idx as select name from restaurant " +
                    "CREATE INDEX reviewer_name_idx as select name from restaurant_reviewer ";

    @Nonnull
    public static String restaurant() {
        return RESTAURANT_SCHEMA;
    }

    //TODO(bfines) the Query engine can't handle INCLUDE statements yet(TODO)
    @Nonnull
    public static String restaurantWithCoveringIndex() {
        return RESTAURANT_SCHEMA + " " + "CREATE INDEX record_type_covering as select rest_no, name from restaurant order by rest_no";
    }

    @Nonnull
    private static final String PLAYING_CARD =
            "CREATE TYPE AS ENUM suit ('SPADES', 'HEARTS', 'DIAMONDS', 'CLUBS') " +
                    "CREATE TABLE card (id bigint, suit suit, rank bigint, PRIMARY KEY(id))" +
                    "CREATE INDEX suit_idx AS SELECT suit FROM card ORDER BY suit";

    @Nonnull
    public static String playingCard() {
        return PLAYING_CARD;
    }

    @Nonnull
    private static final String BOOKS_SCHEMAS =
            "CREATE TABLE BOOKS(id integer, TITLE string, YEAR integer, primary key (id))" +
                    "CREATE INDEX IDX_1970 AS SELECT YEAR FROM BOOKS WHERE YEAR > 1970 AND YEAR <= 1979" +
                    "CREATE INDEX IDX_1980 AS SELECT YEAR FROM BOOKS WHERE YEAR > 1980 AND YEAR <= 1989" +
                    "CREATE INDEX IDX_1990 AS SELECT YEAR FROM BOOKS WHERE YEAR > 1990 AND YEAR <= 1999" +
                    "CREATE INDEX IDX_2000 AS SELECT YEAR FROM BOOKS WHERE YEAR > 2000";

    @Nonnull
    public static String books() {
        return BOOKS_SCHEMAS;
    }
}
