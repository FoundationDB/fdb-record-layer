/*
 * FDBLocalityUtil.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.LocalityUtil;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.CloseableAsyncIterator;

import javax.annotation.Nonnull;

/**
 * An implementation of the {@link FDBLocalityProvider} interface that uses foundationDB's {@link LocalityUtil} API to
 * discover the storage locations of keys within a cluster.
 */
public class FDBLocalityUtil implements FDBLocalityProvider {
    private static final FDBLocalityUtil INSTANCE = new FDBLocalityUtil();

    private FDBLocalityUtil() {
    }

    /**
     * Get the single instance of this utility class.
     * @return the only instance of the class
     */
    @Nonnull
    public static FDBLocalityUtil instance() {
        return INSTANCE;
    }

    /**
     * Return an estimate of the keys boundaries within the given range.
     *
     * <p>
     * The method results in a {@code CloseableAsyncIterator} of keys {@code k} such that
     * {@code begin <= k < end} and {@code k} is located at the start of a contiguous range stored on a single server.
     * </p>
     *
     * @param tr the transaction on which to base the query
     * @param begin the inclusive start of the range
     * @param end the exclusive end of the range
     *
     * @return a sequence of keys denoting the start of single-server ranges
     * @see LocalityUtil#getBoundaryKeys(Transaction, byte[], byte[])
     */
    @Nonnull
    @Override
    public CloseableAsyncIterator<byte[]> getBoundaryKeys(@Nonnull Transaction tr, @Nonnull byte[] begin, @Nonnull byte[] end) {
        return LocalityUtil.getBoundaryKeys(tr, begin, end);
    }
}
