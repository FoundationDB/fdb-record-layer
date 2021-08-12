/*
 * ClientLogEventCounterTest.java
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

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

class ClientLogEventCounterTest {

    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(String[] args) {
        String cluster = null;
        Instant start = null;
        Instant end = null;
        boolean countReads = true;
        boolean countWrites = false;
        final boolean countSingleKeys = true;
        final boolean countRanges = true;
        if (args.length > 0) {
            cluster = args[0];
        }
        if (args.length > 1) {
            start = ZonedDateTime.parse(args[1]).toInstant();
        }
        if (args.length > 2) {
            end = ZonedDateTime.parse(args[2]).toInstant();
        }
        if (args.length > 3) {
            String arg = args[3];
            countReads = "READ".equals(arg) || "BOTH".equals(arg);
            countWrites = "WRITE".equals(arg) || "BOTH".equals(arg);
        }
        FDB fdb = FDB.selectAPIVersion(630);
        Database database = fdb.open(cluster);
        Executor executor = database.getExecutor();
        TupleKeyCountTree root = new TupleKeyCountTree();
        DatabaseClientLogEventCounter counter = new DatabaseClientLogEventCounter(root, countReads, countWrites, countSingleKeys, countRanges, true);
        TupleKeyCountTree.Printer printer = (depth, path) -> {
            for (int i = 0; i < depth; i++) {
                System.out.print("  ");
            }
            System.out.print(path.stream().map(Object::toString).collect(Collectors.joining("/")));
            int percent = (path.get(0).getCount() * 100) / path.get(0).getParent().getCount();
            System.out.println(" " + percent + "%");
        };
        int eventLimit = 10_000;
        long timeLimit = 15_000;
        DatabaseClientLogEvents events = DatabaseClientLogEvents.forEachEventBetweenTimestamps(database, executor, counter, start, end, eventLimit, timeLimit).join();
        while (true) {
            System.out.println(events.getEarliestTimestamp() + " - " + events.getLatestTimestamp());
            root.hideLessThanFraction(0.10);
            root.printTree(printer, "/");
            if (!events.hasMore()) {
                break;
            }
            events.forEachEventContinued(database, executor, counter, eventLimit, timeLimit).join();
        }
    }
}
