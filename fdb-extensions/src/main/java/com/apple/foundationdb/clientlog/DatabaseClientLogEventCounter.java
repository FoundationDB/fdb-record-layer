/*
 * ClientLogEventCounter.java
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

import com.apple.foundationdb.LocalityUtil;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Count tuple-encoded keys into {@link TupleKeyCountTree}.
 */
public class DatabaseClientLogEventCounter implements DatabaseClientLogEvents.RecordEventConsumer {
    @Nonnull
    private final TupleKeyCountTree root;
    private final boolean countReads;
    private final boolean countWrites;
    private final boolean byAddress;

    public DatabaseClientLogEventCounter(@Nonnull TupleKeyCountTree root, boolean countReads, boolean countWrites, boolean byAddress) {
        this.root = root;
        this.countReads = countReads;
        this.countWrites = countWrites;
        this.byAddress = byAddress;
    }

    /**
     * Update the count tree with keys &mdash; and optional storage server IP addresses &mdash; in an event.
     * @param tr an open record context
     * @param event a parsed client latency event
     * @return a future that completes when the event has been processed
     */
    @Override
    public CompletableFuture<Void> accept(@Nonnull Transaction tr, @Nonnull FDBClientLogEvents.Event event) {
        switch (event.getType()) {
            case FDBClientLogEvents.GET_VERSION_LATENCY:
                break;
            case FDBClientLogEvents.GET_LATENCY:
                if (countReads) {
                    return addKey(tr, ((FDBClientLogEvents.EventGet)event).getKey());
                }
                break;
            case FDBClientLogEvents.GET_RANGE_LATENCY:
                if (countReads) {
                    return addRange(tr, ((FDBClientLogEvents.EventGetRange)event).getRange());
                }
                break;
            case FDBClientLogEvents.COMMIT_LATENCY:
                if (countWrites) {
                    return addCommit(tr, ((FDBClientLogEvents.EventCommit)event).getCommitRequest());
                }
                break;
            case FDBClientLogEvents.ERROR_GET:
                if (countReads) {
                    return addKey(tr, ((FDBClientLogEvents.EventGetError)event).getKey());
                }
                break;
            case FDBClientLogEvents.ERROR_GET_RANGE:
                if (countReads) {
                    return addRange(tr, ((FDBClientLogEvents.EventGetRangeError)event).getRange());
                }
                break;
            case FDBClientLogEvents.ERROR_COMMIT:
                if (countWrites) {
                    return addCommit(tr, ((FDBClientLogEvents.EventCommitError)event).getCommitRequest());
                }
                break;
            default:
                break;
        }
        return AsyncUtil.DONE;
    }

    protected CompletableFuture<Void> addKey(@Nonnull Transaction tr, @Nonnull byte[] key) {
        if (byAddress) {
            return addKeyAddresses(tr, key);
        } else {
            root.add(key);
            return AsyncUtil.DONE;
        }
    }

    protected CompletableFuture<Void> addKeyAddresses(@Nonnull Transaction tr, @Nonnull byte[] key) {
        return LocalityUtil.getAddressesForKey(tr, key).handle((addresses, ex) -> {
            if (ex == null) {
                for (String address : addresses) {
                    root.addPrefixChild(address).add(key);
                }
            }
            return null;
        });
    }

    protected CompletableFuture<Void> addRange(@Nonnull Transaction tr, @Nonnull Range range) {
        if (byAddress) {
            return addKeyAddresses(tr, range.begin).thenCompose(vignore -> {
                final CloseableAsyncIterator<byte[]> boundaryKeys = LocalityUtil.getBoundaryKeys(tr, range.begin, range.end);
                return AsyncUtil.whileTrue(() -> boundaryKeys.onHasNext().thenCompose(hasNext -> {
                    if (hasNext) {
                        final byte[] boundaryKey = boundaryKeys.next();
                        if (Arrays.equals(boundaryKey, range.begin)) {
                            return AsyncUtil.READY_TRUE;
                        } else {
                            return addKeyAddresses(tr, boundaryKey).thenApply(vignore2 -> true);
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

    protected CompletableFuture<Void> addCommit(@Nonnull Transaction tr, @Nonnull FDBClientLogEvents.CommitRequest commitRequest) {
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (FDBClientLogEvents.Mutation mutation : commitRequest.getMutations()) {
            final CompletableFuture<Void> future;
            if (mutation.getType() == FDBClientLogEvents.Mutation.CLEAR_RANGE) {
                future = addRange(tr, new Range(mutation.getKey(), mutation.getParam()));
            } else {
                future = addKey(tr, mutation.getKey());
            }
            if (!future.isDone()) {
                futures.add(future);
            }
        }
        if (futures.isEmpty()) {
            return AsyncUtil.DONE;
        } else {
            return AsyncUtil.whenAll(futures);
        }
    }
}
