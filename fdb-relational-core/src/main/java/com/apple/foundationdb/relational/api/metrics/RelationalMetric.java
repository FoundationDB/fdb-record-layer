/*
 * RelationalMetric.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metrics;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.common.StoreTimer;

@API(API.Status.EXPERIMENTAL)
public class RelationalMetric {

    private static final String RELATIONAL_TITLE_PREFIX = "Relational";
    private static final String RELATIONAL_LOG_PREFIX = "relational_";

    public enum RelationalEvent implements StoreTimer.Event {

        /**
         * Time taken for lexing and parsing the query.
         * */
        LEX_PARSE("lex and parse query"),
        /**
         * Time taken to normalize the parsed query.
         * */
        NORMALIZE_QUERY("normalize the query"),
        /**
         * Time taken to generate a logical query plan.
         * See {@link com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor}
         * */
        GENERATE_LOGICAL_PLAN("generate logical plan for a query"),
        /**
         * Time taken to generate an executable physical plan from serialized plan in continuation.
         * */
        GENERATE_CONTINUED_PLAN("generate plan from continuation"),
        /**
         * Time taken to do a successful/unsuccessful cache lookup.
         * */
        CACHE_LOOKUP("lookup in the cache"),
        /**
         * Time taken to optimize the logical plan.
         * */
        OPTIMIZE_PLAN("optimize the plan"),
        /**
         * Time taken in the planning phase of the query.
         * */
        TOTAL_GET_PLAN_QUERY("get the physical plan"),
        /**
         * Time taken to run executePlan() on RecordQueryPlan.
         * */
        EXECUTE_RECORD_QUERY_PLAN("execute the record query plan"),
        /**
         * Time taken to create the RecordLayer result set iterator.
         * */
        CREATE_RESULT_SET_ITERATOR("create result set iterator"),

        /**
         * Time taken in the execution phase of the query.
         * */
        TOTAL_EXECUTE_QUERY("execution phase of the query"),
        /**
         * Time taken to process a SQL query end-to-end.
         * */
        TOTAL_PROCESS_QUERY("process the query")
        ;

        private final String title;
        private final String logKey;

        RelationalEvent(String title) {
            this.title = RELATIONAL_TITLE_PREFIX + " " + title;
            this.logKey = RELATIONAL_LOG_PREFIX + StoreTimer.Event.super.logKey();
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        public String logKey() {
            return logKey;
        }
    }

    public enum RelationalCount implements StoreTimer.Count {

        PLAN_CACHE_PRIMARY_MISS("primary cache miss", false),
        PLAN_CACHE_SECONDARY_MISS("secondary cache miss", false),
        PLAN_CACHE_TERTIARY_MISS("tertiary cache miss", false),
        PLAN_CACHE_TERTIARY_HIT("cache hit", false),
        CONTINUATION_ACCEPTED("continuation accepted", false),
        CONTINUATION_REJECTED("continuation rejected", false),
        CONTINUATION_DOWN_LEVEL("continuation accepted from down-level plan serialization mode", false)
        ;

        private final String title;
        private final boolean isSize;
        private final String logKey;

        RelationalCount(String title, boolean isSize) {
            this.title = RELATIONAL_TITLE_PREFIX + " " + title;
            this.logKey = RELATIONAL_LOG_PREFIX + StoreTimer.Count.super.logKey();
            this.isSize = isSize;
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        public boolean isDelayedUntilCommit() {
            return false;
        }

        @Override
        public boolean isSize() {
            return isSize;
        }

        @Override
        public String logKey() {
            return logKey;
        }
    }
}
