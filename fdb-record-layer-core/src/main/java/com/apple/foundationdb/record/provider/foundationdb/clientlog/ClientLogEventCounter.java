/*
 * ClientLogEventCounter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.clientlog;

import com.apple.foundationdb.LocalityUtil;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.clientlog.FDBClientLogEvents;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Count tuple-encoded keys into {@link TupleKeyCountTree}.
 */
public class ClientLogEventCounter implements FDBDatabaseClientLogEvents.RecordEventConsumer {
    @Nonnull
    private final TupleKeyCountTree root;
    private final boolean countReads;
    private final boolean countWrites;
    private final boolean byAddress;

    public ClientLogEventCounter(@Nonnull TupleKeyCountTree root, boolean countReads, boolean countWrites, boolean byAddress) {
        this.root = root;
        this.countReads = countReads;
        this.countWrites = countWrites;
        this.byAddress = byAddress;
    }

    /**
     * Update the count tree with keys &mdash; and optional storage server IP addresses &mdash; in an event.
     * @param context an open record context
     * @param event a parsed client latency event
     * @return a future that completes when the event has been processed
     */
    @Override
    public CompletableFuture<Void> accept(@Nonnull FDBRecordContext context, @Nonnull FDBClientLogEvents.Event event) {
        switch (event.getType()) {
            case FDBClientLogEvents.GET_VERSION_LATENCY:
                break;
            case FDBClientLogEvents.GET_LATENCY:
                if (countReads) {
                    return addKey(context, ((FDBClientLogEvents.EventGet)event).getKey());
                }
                break;
            case FDBClientLogEvents.GET_RANGE_LATENCY:
                if (countReads) {
                    return addRange(context, ((FDBClientLogEvents.EventGetRange)event).getRange());
                }
                break;
            case FDBClientLogEvents.COMMIT_LATENCY:
                if (countWrites) {
                    return addCommit(context, ((FDBClientLogEvents.EventCommit)event).getCommitRequest());
                }
                break;
            case FDBClientLogEvents.ERROR_GET:
                if (countReads) {
                    return addKey(context, ((FDBClientLogEvents.EventGetError)event).getKey());
                }
                break;
            case FDBClientLogEvents.ERROR_GET_RANGE:
                if (countReads) {
                    return addRange(context, ((FDBClientLogEvents.EventGetRangeError)event).getRange());
                }
                break;
            case FDBClientLogEvents.ERROR_COMMIT:
                if (countWrites) {
                    return addCommit(context, ((FDBClientLogEvents.EventCommitError)event).getCommitRequest());
                }
                break;
            default:
                break;
        }
        return AsyncUtil.DONE;
    }

    protected CompletableFuture<Void> addKey(@Nonnull FDBRecordContext context, @Nonnull byte[] key) {
        if (byAddress) {
            return addKeyAddresses(context, key);
        } else {
            root.add(key);
            return AsyncUtil.DONE;
        }
    }

    protected CompletableFuture<Void> addKeyAddresses(@Nonnull FDBRecordContext context, @Nonnull byte[] key) {
        return LocalityUtil.getAddressesForKey(context.ensureActive(), key).handle((addresses, ex) -> {
            if (ex == null) {
                for (String address : addresses) {
                    root.addPrefixChild(address).add(key);
                }
            }
            return null;
        });
    }

    protected CompletableFuture<Void> addRange(@Nonnull FDBRecordContext context, @Nonnull Range range) {
        if (byAddress) {
            return addKeyAddresses(context, range.begin).thenCompose(vignore -> {
                final CloseableAsyncIterator<byte[]> boundaryKeys = LocalityUtil.getBoundaryKeys(context.ensureActive(), range.begin, range.end);
                return AsyncUtil.whileTrue(() -> boundaryKeys.onHasNext().thenCompose(hasNext -> {
                    if (hasNext) {
                        final byte[] boundaryKey = boundaryKeys.next();
                        if (Arrays.equals(boundaryKey, range.begin)) {
                            return AsyncUtil.READY_TRUE;
                        } else {
                            return addKeyAddresses(context, boundaryKey).thenApply(vignore2 -> true);
                        }
                    } else {
                        return AsyncUtil.READY_FALSE;
                    }
                })).whenComplete((v, t) -> boundaryKeys.close());
            });
        } else {
            root.add(range.begin);
            return AsyncUtil.DONE;
        }
    }

    protected CompletableFuture<Void> addCommit(@Nonnull FDBRecordContext context, @Nonnull FDBClientLogEvents.CommitRequest commitRequest) {
        for (FDBClientLogEvents.Mutation mutation : commitRequest.getMutations()) {
            if (mutation.getType() == FDBClientLogEvents.Mutation.CLEAR_RANGE) {
                addRange(context, new Range(mutation.getKey(), mutation.getParam()));
            } else {
                addKey(context, mutation.getKey());
            }
        }
        return AsyncUtil.DONE;
    }

    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(String[] args) {
        String cluster = null;
        Instant start = null;
        Instant end = null;
        boolean countReads = true;
        boolean countWrites = false;
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
        FDBDatabase database = FDBDatabaseFactory.instance().getDatabase(cluster);
        TupleKeyCountTree root = new TupleKeyCountTree();
        ClientLogEventCounter counter = new ClientLogEventCounter(root, countReads, countWrites, true);
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
        FDBDatabaseClientLogEvents events = FDBDatabaseClientLogEvents.forEachEventBetweenTimestamps(database, counter, start, end, eventLimit, timeLimit).join();
        while (true) {
            System.out.println(events.getEarliestTimestamp() + " - " + events.getLatestTimestamp());
            root.hideLessThanFraction(0.10);
            root.printTree(printer, "/");
            if (!events.hasMore()) {
                break;
            }
            events.forEachEventContinued(database, counter, eventLimit, timeLimit).join();
        }
    }
}
