/*
 * KeyRange.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;

/**
 * A range within a subspace specified by two byte value endpoints.
 */
@API(API.Status.UNSTABLE)
public class KeyRange {
    @Nonnull
    private final byte[] lowKey;
    @Nonnull
    private final EndpointType lowEndpoint;
    @Nonnull
    private final byte[] highKey;
    @Nonnull
    private final EndpointType highEndpoint;

    /**
     * Creates a key range.
     *
     * @param lowKey the starting key in the range. Note that a direct reference to this key is retained, so
     *    care must be taken not to modify its contents
     * @param lowEndpoint how the low endpoint is to be treated
     * @param highKey the ending key in the range. Note that a direct reference to this key is retained, so
     *    care must be taken not to modify its contents
     * @param highEndpoint how the high endpoint is to be treated
     */
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public KeyRange(@Nonnull byte[] lowKey, @Nonnull EndpointType lowEndpoint,
                    @Nonnull byte[] highKey, @Nonnull EndpointType highEndpoint) {
        this.lowKey = lowKey;
        this.lowEndpoint = lowEndpoint;
        this.highKey = highKey;
        this.highEndpoint = highEndpoint;
    }

    /**
     * Creates a key range.
     *
     * @param lowKey the starting key of the range, inclusive
     * @param highKey the ending key of the range, exclusive
     */
    public KeyRange(@Nonnull byte[] lowKey, @Nonnull byte[] highKey) {
        this(lowKey, EndpointType.RANGE_INCLUSIVE, highKey, EndpointType.RANGE_EXCLUSIVE);
    }

    /**
     * Returns the lower boundary of the range to be scanned. How this starting key is to be interpreted
     * in relation to a scan (e.g., inclusive or exclusive) is determined by the low endpoint value
     * ({@link #getLowEndpoint()}.  The value returned by this method should be treated as immutable and
     * must not be modified by the caller.
     *
     * @return the low key of the range to be scanned
     */
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    @Nonnull
    public byte[] getLowKey() {
        return lowKey;
    }

    /**
     * How the lower boundary key of the range is to be interpreted by the scan. For example, this can
     * be inclusive or exclusive.
     * @return how the lower boundary key of the range is to be interpreted by the scan
     * @see EndpointType
     */
    @Nonnull
    public EndpointType getLowEndpoint() {
        return lowEndpoint;
    }

    /**
     * Returns the upper boundary of the range to be scanned. How this key is to be interpreted
     * in relation to a scan (e.g., inclusive or exclusive) is determined by the high endpoint value
     * ({@link #getHighEndpoint()}.  The value returned by this method should be treated as immutable and
     * must not be modified by the caller.
     *
     * @return the high key of the range to be scanned
     */
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    @Nonnull
    public byte[] getHighKey() {
        return highKey;
    }

    /**
     * How the upper boundary key of the range is to be interpreted by the scan. For example, this can
     * be inclusive or exclusive.
     * @return how the upper boundary key of the range is to be interpreted by the scan
     * @see EndpointType
     */
    @Nonnull
    public EndpointType getHighEndpoint() {
        return highEndpoint;
    }
}
