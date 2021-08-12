/*
 * DatabaseClientLogEventsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.clientlog;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.async.AsyncUtil;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.Executor;

class DatabaseClientLogEventsTest {

    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(String[] args) {
        String cluster = null;
        Instant start = null;
        Instant end = null;
        if (args.length > 0) {
            cluster = args[0];
        }
        if (args.length > 1) {
            start = ZonedDateTime.parse(args[1]).toInstant();
        }
        if (args.length > 2) {
            end = ZonedDateTime.parse(args[2]).toInstant();
        }
        FDB fdb = FDB.selectAPIVersion(630);
        Database database = fdb.open(cluster);
        Executor executor = database.getExecutor();
        DatabaseClientLogEvents.EventConsumer consumer = (tr, event) -> {
            System.out.println(event);
            return AsyncUtil.DONE;
        };
        DatabaseClientLogEvents.forEachEventBetweenTimestamps(database, executor, consumer, start, end, Integer.MAX_VALUE, Long.MAX_VALUE).join();
    }
}
