/*
 * IndexEntryReturnPolicy.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

/**
 * The policy to use when using one of FDB's
 * {@link com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration.IndexFetchMethod}
 * REMOTE_SCAN options. This is an optimization that allows the caller to reduce bandwidth and CPU by not including
 * index entries that are no longer required (since the records are already available).
 * <UL>
 * <LI>ALL: always return index entries with the returned indexed records</LI>
 * <LI>NONE: never return index entries with the returned indexed records</LI>
 * <LI>MATCHED: only return index entries for records that were found for the index entry</LI>
 * <LI>UNMATCHED: only return index entries for orphan index entries (where no record was found)</LI>
 * </UL>
 * Note: FDB will always return the boundary (first and last) index entries with the payload regardless of mode, in
 * order to allow the caller to construct a continuation.
 */
public enum IndexEntryReturnPolicy {
    ALL(0),
    NONE(1),
    MATCHED(2),
    UNMATCHED(3);

    // The value that will eventually be passed on to FDB calls
    private int intValue;

    IndexEntryReturnPolicy(final int intValue) {
        this.intValue = intValue;
    }

    public int getIntValue() {
        return intValue;
    }
}
